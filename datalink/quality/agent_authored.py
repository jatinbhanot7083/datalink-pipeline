"""Bridge between the DqProposerAgent and the SuiteRegistry.

Two responsibilities:

  1. ``propose_expectation()`` — invoke the agent, run its sample SQL
     against the active warehouse to get a live preview, and return a
     ``Proposal`` record the UI renders verbatim.

  2. ``accept_proposal()`` — wrap the proposal as a ``SuiteDraft`` and
     route it through the standard SuiteRegistry state machine
     (DRAFT → PENDING_REVIEW → APPROVED → LIVE) with ``source = AGENT``
     so it shows up in DQ Author / DQ Suite Registry alongside baselines.

The agent layer never touches the warehouse for sample data — that
happens here, AFTER the LLM has produced a SQL string. This keeps the
PHI boundary clean: rows never travel through the LLM.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from datalink.adapters.protocols import LlmProvider, Warehouse
from datalink.agents.dq_author.proposer import DqProposerAgent
from datalink.logging import get_logger
from datalink.quality.registry import (
    ApprovalMode,
    SuiteDraft,
    SuiteRegistry,
    SuiteSource,
    default_approval_mode,
)

_log = get_logger(__name__)


@dataclass
class Proposal:
    """A reviewable proposal — agent output + sample run + metadata."""

    expectation_type: str
    kwargs: dict[str, Any]
    meta: dict[str, Any]
    sql_preview: str
    rationale: str
    sample_columns: list[str] = field(default_factory=list)
    sample_rows: list[dict[str, Any]] = field(default_factory=list)
    sample_error: str | None = None
    tokens_used: int = 0
    duration_ms: int = 0
    proposer_model: str = ""
    # Phase 8 RAG: the past suites the agent was shown as EXAMPLES.
    # Surfaced in the Workshop's "AI grounded on N past suites" expander
    # so the operator can audit what influenced the proposal.
    grounding: list[dict[str, Any]] = field(default_factory=list)
    temperature: float = 0.0
    embedding_model: str = ""

    def to_meta_block(self, *, source_prompt: str, author: str) -> dict[str, Any]:
        """Bake provenance into the expectation's `meta` so any downstream
        consumer (DQ Author grid, exports) can see this came from the
        AI workshop."""
        meta = dict(self.meta)
        meta.update(
            {
                "source_prompt": source_prompt,
                "authored_by": author,
                "authored_at": datetime.now(UTC).isoformat(),
                "rationale": self.rationale,
                "sql_preview": self.sql_preview,
                "temperature": self.temperature,
                "proposer_model": self.proposer_model,
                "embedding_model": self.embedding_model,
                # Grounding provenance — list of suite_ids the agent saw.
                # Each entry carries just suite_id + distance; full
                # source_text is reproducible via SuiteRegistry lookup.
                "grounded_on": [
                    {"suite_id": g["suite_id"], "distance": g["distance"]} for g in self.grounding
                ],
            }
        )
        return meta


def propose_expectation(
    *,
    llm: LlmProvider,
    warehouse: Warehouse,
    client_id: str,
    qualified_table: str,
    columns: list[dict[str, str]],
    prompt: str,
    history: list[dict[str, str]] | None = None,
    sample_limit: int = 5,
    temperature: float = 0.0,
    memory: Any = None,  # AgentMemoryStore | None — RAG layer
    grounding_k: int = 3,  # how many past suites to retrieve
    grounding_enabled: bool = True,
) -> Proposal:
    """Run the proposer + sample SQL + return a reviewable Proposal.

    Phase 8 RAG: when ``memory`` is provided and ``grounding_enabled=True``,
    we query the most-similar past LIVE suites for this client (Tier A) and
    inject them as EXAMPLES in the proposer's instruction. Hits are stored
    on the resulting ``Proposal.grounding`` so the UI can audit them.

    Sample SQL failures are swallowed into ``sample_error`` so the UI
    can still surface the proposal — the user might intentionally have
    asked for a check on a not-yet-existing table. The agent's call
    itself failing IS surfaced as an exception.
    """
    # Tier A retrieval — only fires if a memory store is wired AND grounding
    # is on. Failure to retrieve is a warning, never a block: the agent
    # gracefully falls back to ungrounded mode.
    grounding: list[dict[str, Any]] = []
    embedding_model = ""
    if memory is not None and grounding_enabled and grounding_k > 0:
        try:
            hits = memory.query_similar_suites(
                query_text=prompt,
                client_id=client_id,
                k=grounding_k,
            )
            grounding = [
                {
                    "suite_id": h.suite_id,
                    "suite_name": h.suite_name,
                    "status": h.status,
                    "source": h.source,
                    "source_type": h.source_type,
                    "source_text": h.source_text,
                    "distance": h.distance,
                }
                for h in hits
            ]
            # Read the embedder model name in one step so mypy keeps the
            # variable's type as str. Two-step getattr was Any → could shadow.
            _embedder = getattr(memory, "_embedder", None)
            embedding_model = str(getattr(_embedder, "model", "")) if _embedder else ""
            _log.info(
                "agent_authored.grounding_retrieved",
                client_id=client_id,
                hits=len(grounding),
                k=grounding_k,
            )
        except Exception as e:
            _log.warning("agent_authored.grounding_failed", error=str(e))
            grounding = []

    agent = DqProposerAgent(llm=llm, warehouse=warehouse)
    result = agent.run(
        {
            "client_id": client_id,
            "qualified_table": qualified_table,
            "columns": columns,
            "prompt": prompt,
            "history": history or [],
            "temperature": temperature,
            "grounding": grounding,
        }
    )
    if not result.success:
        raise RuntimeError(result.error or "DqProposerAgent failed without an error string.")

    spec = result.payload
    proposal = Proposal(
        expectation_type=spec["expectation_type"],
        kwargs=spec["kwargs"],
        meta=spec["meta"],
        sql_preview=spec["sql_preview"],
        rationale=spec["rationale"],
        tokens_used=result.tokens_used,
        duration_ms=result.duration_ms,
        proposer_model=getattr(llm, "model", "unknown"),
        grounding=grounding,
        temperature=temperature,
        embedding_model=embedding_model,
    )

    # Run the agent-supplied SQL for a tiny preview. Cap at sample_limit
    # rows — most checks return aggregated counts so this is mostly a
    # round-trip sanity test.
    try:
        rows = warehouse.query(proposal.sql_preview)
        if rows:
            proposal.sample_columns = list(rows[0].keys())
            proposal.sample_rows = [
                {k: _stringify(v) for k, v in r.items()} for r in rows[:sample_limit]
            ]
    except Exception as e:
        proposal.sample_error = f"{type(e).__name__}: {e}"
        _log.warning(
            "agent_authored.sample_sql_failed",
            client_id=client_id,
            qualified_table=qualified_table,
            error=proposal.sample_error,
        )

    _log.info(
        "agent_authored.proposed",
        client_id=client_id,
        qualified_table=qualified_table,
        expectation_type=proposal.expectation_type,
        tokens=proposal.tokens_used,
        duration_ms=proposal.duration_ms,
        has_sample=bool(proposal.sample_rows),
    )
    return proposal


def accept_proposal(
    *,
    registry: SuiteRegistry,
    proposal: Proposal,
    client_id: str,
    suite_name: str,
    source_prompt: str,
    author: str,
    mode: ApprovalMode | None = None,
    target_status: str | None = None,  # deprecated alias — see below
) -> str:
    """Persist the proposal as a versioned suite.

    Phase 10.1 — source-aware approval policy.

    ``mode`` controls where the suite lands. When ``None``, defaults to
    ``default_approval_mode(SuiteSource.AGENT)`` which under Option C =
    ``HITL`` (PENDING_REVIEW). Callers may override:

      * ``ApprovalMode.DRAFT``        — leaves it editable on DQ Author.
      * ``ApprovalMode.HITL``         — submits to the DQ Review queue.
      * ``ApprovalMode.AUTO_APPROVE`` — full auto-pilot. Reserved for
                                        trusted senior stewards on
                                        narrowly-scoped expectations.

    Backward compatibility: ``target_status`` accepts the legacy strings
    ``"DRAFT"`` / ``"PENDING_REVIEW"`` / ``"LIVE"`` and is mapped onto
    ``mode``. Will be removed in Phase 10.4 once UI migration is done.

    Returns the new ``suite_id``.
    """
    # Legacy adapter: support callers still passing ``target_status``.
    if target_status is not None:
        if mode is not None:
            raise ValueError("Pass either `mode` or `target_status`, not both.")
        legacy_map = {
            "DRAFT": ApprovalMode.DRAFT,
            "PENDING_REVIEW": ApprovalMode.HITL,
            "LIVE": ApprovalMode.AUTO_APPROVE,
        }
        if target_status not in legacy_map:
            raise ValueError(
                f"target_status must be DRAFT | PENDING_REVIEW | LIVE, got {target_status!r}."
            )
        mode = legacy_map[target_status]

    if mode is None:
        mode = default_approval_mode(SuiteSource.AGENT, is_edit_of_live=False)

    expectation = {
        "expectation_type": proposal.expectation_type,
        "kwargs": dict(proposal.kwargs),
        "meta": proposal.to_meta_block(source_prompt=source_prompt, author=author),
    }
    dim = proposal.meta.get("dq_dimension", "")
    draft = SuiteDraft(
        client_id=client_id,
        suite_name=suite_name,
        expectations=[expectation],
        created_by=author,
        source=SuiteSource.AGENT,
        dq_dimensions=[dim] if dim else [],
        source_type=proposal.meta.get("source_type"),
    )
    suite_id = registry.create_draft(draft)
    _log.info(
        "agent_authored.draft_created",
        suite_id=suite_id,
        client_id=client_id,
        suite_name=suite_name,
        author=author,
        mode=mode.value,
    )

    registry.submit_with_policy(
        suite_id,
        mode=mode,
        actor=author,
        approval_notes="Auto-approved by agent author."
        if mode is ApprovalMode.AUTO_APPROVE
        else None,
    )
    if mode is ApprovalMode.AUTO_APPROVE:
        _log.info("agent_authored.auto_activated", suite_id=suite_id)
    elif mode is ApprovalMode.HITL:
        _log.info("agent_authored.submitted_for_review", suite_id=suite_id)

    return suite_id


def _stringify(value: Any) -> Any:
    """Coerce sample-row values to JSON-friendly scalars for the UI."""
    if value is None:
        return None
    if isinstance(value, str | int | float | bool):
        return value
    return str(value)
