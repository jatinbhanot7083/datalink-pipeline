"""Phase 13.3 — orchestration glue between profile, session, agent, and
warehouse sample run.

The UI calls :func:`propose_silver_mapping` once per "Propose / Revise"
button click. Internally:

  1. Read the session (must be DRAFT).
  2. Snapshot the source profile if not already attached.
  3. Replay conversation history → MapperAgent → JSON proposal.
  4. Strip dbt jinja from the proposed SQL, run a wrapped ``LIMIT N``
     against the warehouse to produce a sample preview.
  5. Append a turn (user prompt + assistant proposal) to the session.
  6. Update session artifacts (silver_sql, silver_target_table,
     sample_preview).

Returns a :class:`SilverProposal` the UI renders verbatim. The session's
state stays DRAFT — operator clicks Submit / Auto-Approve separately to
walk the HITL state machine.

Stripping dbt jinja for sample run:
    The ``{{ source('bronze', 'X') }}`` and ``{{ ref('Y') }}`` macros are
    rewritten to qualified table names before sample execution. dbt-style
    ``{{ config(...) }}`` blocks are dropped. Anything more exotic
    (custom macros, ``{% set %}`` blocks) bubbles up as a sample error
    so the operator sees it in the UI.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from datalink.adapters.protocols import LlmProvider, Warehouse
from datalink.agents.mapper.agent import MapperAgent
from datalink.agents.mapper.profiler import SourceProfile, profile_source
from datalink.agents.mapper.session import (
    MappingSessionRegistry,
    SessionStatus,
    TargetMode,
)
from datalink.logging import get_logger

_log = get_logger(__name__)


# ============================================================================
# CORE TYPES
# ============================================================================


@dataclass
class SilverProposal:
    """Reviewable proposal — agent output + sample preview + session id."""

    session_id: str
    silver_target_table: str
    natural_keys: list[str]
    scd2_change_cols: list[str]
    silver_sql: str
    rationale: str
    sample_columns: list[str] = field(default_factory=list)
    sample_rows: list[dict[str, Any]] = field(default_factory=list)
    sample_error: str | None = None
    tokens_used: int = 0
    duration_ms: int = 0
    proposer_model: str = ""


# ============================================================================
# PUBLIC API
# ============================================================================


@dataclass
class GoldProposal:
    """Reviewable Gold-view proposal — agent output + session id."""

    session_id: str
    gold_target_table: str
    gold_sql: str
    rationale: str
    sample_columns: list[str] = field(default_factory=list)
    sample_rows: list[dict[str, Any]] = field(default_factory=list)
    sample_error: str | None = None
    tokens_used: int = 0
    duration_ms: int = 0


@dataclass
class PushProposal:
    """Reviewable On-Prem push proposal — one per backend."""

    session_id: str
    backend: str  # "POSTGRES" | "SQLSERVER"
    push_sql: str
    rationale: str
    optimization_score: float = 0.0
    failed_rules: list[str] = field(default_factory=list)
    tokens_used: int = 0
    duration_ms: int = 0


def propose_silver_mapping(
    *,
    llm: LlmProvider,
    warehouse: Warehouse,
    session_registry: MappingSessionRegistry,
    session_id: str,
    user_prompt: str,
    actor: str,
    sample_limit: int = 100,
    temperature: float = 0.0,
) -> SilverProposal:
    """Run one round of the Silver mapper agent.

    Idempotent on the same input: the agent is deterministic at
    ``temperature=0.0`` so re-running with the same prompt + same session
    state yields the same proposal. Each call appends a new conversation
    turn though, so the history grows monotonically.

    ``user_prompt`` is the operator's NL input for THIS turn — the
    initial Gold contract on turn 1, a revision on subsequent turns
    (e.g. "no, denial_reason should come from the latest non-null").
    """
    session = session_registry.get_session(session_id)
    if session is None:
        raise KeyError(f"Unknown session_id: {session_id!r}")
    if session.status is not SessionStatus.DRAFT:
        raise ValueError(
            f"propose_silver_mapping requires DRAFT, " f"session is {session.status.value}"
        )

    # 1. Profile snapshot (if not already attached).
    if session.source_profile is None:
        profile = profile_source(
            warehouse,
            session.source_qualified_table,
            sample_limit=5,
        )
        session_registry.update_source_profile(session_id, profile, actor=actor)
        # Refresh the session to pick up the snapshot.
        session = session_registry.get_session(session_id)
        if session is None:
            raise RuntimeError(f"session vanished after profile update: {session_id!r}")
    else:
        profile = _profile_from_dict(session.source_profile)

    # 2. Update gold contract (if first turn) OR keep existing + log the
    # revision as a new turn.
    is_first_turn = session.turn_count == 0 and not session.gold_contract_text.strip()
    if is_first_turn:
        session_registry.update_gold_contract(session_id, user_prompt, actor=actor)
        gold_contract = user_prompt
    else:
        gold_contract = session.gold_contract_text or user_prompt

    # 3. Build conversation history for the agent.
    history = [{"role": t.role, "content": t.text} for t in session.conversation]
    if not is_first_turn:
        # The current revision request is a NEW user turn fed to the agent.
        history.append({"role": "user", "content": user_prompt})

    # 4. Run agent.
    agent = MapperAgent(llm=llm, warehouse=warehouse)
    result = agent.run(
        {
            "client_id": session.client_id,
            "profile": profile,
            "gold_contract": gold_contract,
            "history": history,
            "temperature": temperature,
        }
    )
    if not result.success:
        raise RuntimeError(result.error or "MapperAgent failed without an error string.")

    spec = result.payload
    proposal = SilverProposal(
        session_id=session_id,
        silver_target_table=spec["silver_target_table"],
        natural_keys=list(spec.get("natural_keys") or []),
        scd2_change_cols=list(spec.get("scd2_change_cols") or []),
        silver_sql=spec["silver_sql"],
        rationale=spec["rationale"],
        tokens_used=result.tokens_used,
        duration_ms=result.duration_ms,
        proposer_model=getattr(llm, "model", "unknown"),
    )

    # 5. Sample preview — wrap the proposed SQL in a LIMIT and run.
    try:
        sample_sql = _prepare_for_sample(proposal.silver_sql, profile, sample_limit)
        rows = warehouse.query(sample_sql)
        if rows:
            proposal.sample_columns = list(rows[0].keys())
            proposal.sample_rows = [
                {k: _coerce_sample_value(v) for k, v in r.items()} for r in rows[:sample_limit]
            ]
    except Exception as e:
        proposal.sample_error = f"{type(e).__name__}: {e}"
        _log.warning(
            "mapper.sample.failed",
            session_id=session_id,
            error=proposal.sample_error,
        )

    # 6. Persist: append turns + update artifacts.
    session_registry.add_turn(
        session_id,
        role="user",
        text=user_prompt,
        actor=actor,
    )
    session_registry.add_turn(
        session_id,
        role="assistant",
        text=_summarise_proposal(proposal),
        actor=actor,
        tokens_used=result.tokens_used,
        duration_ms=result.duration_ms,
    )
    session_registry.update_artifacts(
        session_id,
        actor=actor,
        silver_sql=proposal.silver_sql,
        silver_target_table=proposal.silver_target_table,
        sample_preview=proposal.sample_rows or [],
    )

    _log.info(
        "mapper.propose.done",
        session_id=session_id,
        target_table=proposal.silver_target_table,
        nks=proposal.natural_keys,
        sample_rows=len(proposal.sample_rows),
        sample_error=proposal.sample_error,
        tokens=result.tokens_used,
        duration_ms=result.duration_ms,
    )
    return proposal


# ============================================================================
# INTERNAL HELPERS
# ============================================================================


_JINJA_BLOCK = re.compile(r"\{%[^%]*%\}")
_JINJA_EXPR = re.compile(r"\{\{([^}]+)\}\}")


def _prepare_for_sample(
    sql: str,
    profile: SourceProfile,
    limit: int,
) -> str:
    """Strip dbt jinja so the SQL can run directly against the warehouse.

    Substitutions:
      * ``{{ config(...) }}``   →   ""  (the config macro is dropped)
      * ``{{ source('schema', 'table') }}`` → ``schema.TABLE``
      * ``{{ ref('foo') }}``    → ``foo`` (best-effort; if foo isn't a
                                  table the run fails with a clear error)
      * Any other ``{{ ... }}`` → "" (best-effort; logged on error)

    Then wraps the whole thing in
    ``SELECT * FROM (<sql>) AS _mapper_preview LIMIT N``.
    """
    cleaned = _JINJA_BLOCK.sub("", sql)

    def _expand(match: re.Match[str]) -> str:
        body = match.group(1).strip()
        if body.startswith("config"):
            return ""
        if body.startswith("source"):
            args = re.findall(r"['\"]([^'\"]+)['\"]", body)
            if len(args) >= 2:
                # Use the profile's qualified table as the source target —
                # we don't have access to dbt sources.yml at this layer.
                return profile.qualified_table
            return profile.qualified_table
        if body.startswith("ref"):
            args = re.findall(r"['\"]([^'\"]+)['\"]", body)
            return str(args[0]) if args else ""
        return ""

    cleaned = _JINJA_EXPR.sub(_expand, cleaned)
    cleaned = cleaned.strip().rstrip(";")
    return f"SELECT * FROM (\n{cleaned}\n) AS _mapper_preview LIMIT {int(limit)}"


def propose_gold_view(
    *,
    llm: LlmProvider,
    warehouse: Warehouse,
    session_registry: MappingSessionRegistry,
    session_id: str,
    user_prompt: str,
    actor: str,
    sample_limit: int = 100,
    temperature: float = 0.0,
) -> GoldProposal:
    """Phase 13.6 — propose a Gold dbt view on top of an existing Silver
    proposal in the same session.

    Requires the session to already have a Silver proposal (call
    :func:`propose_silver_mapping` first). Appends conversation turns
    + updates ``gold_sql`` / ``gold_target_table`` on the session.
    Sample preview runs the Gold SQL with LIMIT to verify it executes.
    """
    session = session_registry.get_session(session_id)
    if session is None:
        raise KeyError(f"Unknown session_id: {session_id!r}")
    if session.status is not SessionStatus.DRAFT:
        raise ValueError(f"propose_gold_view requires DRAFT, session is {session.status.value}")
    if not session.has_silver_proposal:
        raise ValueError(
            "propose_gold_view called without a Silver proposal in session — "
            "call propose_silver_mapping() first."
        )
    if session.target_mode is TargetMode.SILVER_ONLY:
        raise ValueError(
            "session target_mode is SILVER_ONLY — switch to SILVER_AND_GOLD or "
            "FULL_STACK to author a Gold view."
        )

    profile = _profile_from_dict(session.source_profile or {})
    history = [{"role": t.role, "content": t.text} for t in session.conversation]
    if user_prompt.strip():
        history.append({"role": "user", "content": user_prompt})

    from datalink.agents.mapper.agent import MapperAgent

    agent = MapperAgent(llm=llm, warehouse=warehouse)
    spec = agent.execute_gold(
        {
            "client_id": session.client_id,
            "profile": profile,
            "silver_target_table": session.silver_target_table,
            "silver_sql": session.silver_sql,
            "natural_keys": [],  # captured in audit; agent works from contract
            "scd2_change_cols": [
                c["name"] for c in (session.source_profile or {}).get("columns", [])
            ],
            "gold_contract": session.gold_contract_text or user_prompt,
            "history": history,
            "temperature": temperature,
        }
    )
    proposal = GoldProposal(
        session_id=session_id,
        gold_target_table=spec["gold_target_table"],
        gold_sql=spec["gold_sql"],
        rationale=spec["rationale"],
    )

    # Sample preview — run the Gold SQL with LIMIT.
    try:
        sample_sql = _prepare_gold_for_sample(
            proposal.gold_sql, session.silver_target_table or "", profile, sample_limit
        )
        rows = warehouse.query(sample_sql)
        if rows:
            proposal.sample_columns = list(rows[0].keys())
            proposal.sample_rows = [
                {k: _coerce_sample_value(v) for k, v in r.items()} for r in rows[:sample_limit]
            ]
    except Exception as e:
        proposal.sample_error = f"{type(e).__name__}: {e}"
        _log.warning(
            "mapper.gold.sample_failed", session_id=session_id, error=proposal.sample_error
        )

    if user_prompt.strip():
        session_registry.add_turn(session_id, role="user", text=user_prompt, actor=actor)
    session_registry.add_turn(
        session_id,
        role="assistant",
        text=f"Proposed Gold view `{proposal.gold_target_table}`:\n{proposal.rationale}",
        actor=actor,
    )
    session_registry.update_artifacts(
        session_id,
        actor=actor,
        gold_sql=proposal.gold_sql,
        gold_target_table=proposal.gold_target_table,
    )
    _log.info(
        "mapper.gold.done",
        session_id=session_id,
        gold_target_table=proposal.gold_target_table,
        sample_rows=len(proposal.sample_rows),
    )
    return proposal


def propose_push_script(
    *,
    llm: LlmProvider,
    warehouse: Warehouse,
    session_registry: MappingSessionRegistry,
    session_id: str,
    backend: str,  # "POSTGRES" or "SQLSERVER"
    target_table: str,  # e.g. "um.PatientAuth"
    primary_keys: list[str],
    column_list: list[str],
    actor: str,
    temperature: float = 0.0,
) -> PushProposal:
    """Phase 13.7 — propose an optimized On-Prem push script for one backend.

    Calls the agent + scores against the optimization rules. Persists
    the result in the appropriate session column
    (``onprem_postgres_sql`` or ``onprem_mssql_sql``).
    """
    from datalink.agents.mapper.agent import MapperAgent
    from datalink.agents.mapper.optimization import (
        TargetBackend,
        validate_push_sql,
    )

    backend_enum = TargetBackend(backend.upper())

    session = session_registry.get_session(session_id)
    if session is None:
        raise KeyError(f"Unknown session_id: {session_id!r}")
    if session.status is not SessionStatus.DRAFT:
        raise ValueError(f"propose_push_script requires DRAFT, session is {session.status.value}")
    if session.target_mode is not TargetMode.FULL_STACK:
        raise ValueError(
            f"session target_mode is {session.target_mode.value} — switch to "
            f"FULL_STACK to author On-Prem push scripts."
        )
    if not session.gold_target_table or not session.gold_sql:
        raise ValueError(
            "propose_push_script requires a Gold proposal — call " "propose_gold_view() first."
        )

    agent = MapperAgent(llm=llm, warehouse=warehouse)
    spec = agent.execute_push(
        {
            "backend": backend_enum,
            "gold_target_table": session.gold_target_table,
            "gold_sql": session.gold_sql,
            "target_table": target_table,
            "primary_keys": primary_keys,
            "column_list": column_list,
            "temperature": temperature,
        }
    )
    push_sql = spec["push_sql"]
    report = validate_push_sql(push_sql, backend=backend_enum)

    proposal = PushProposal(
        session_id=session_id,
        backend=backend_enum.value,
        push_sql=push_sql,
        rationale=spec["rationale"],
        optimization_score=report.score,
        failed_rules=[r.rule_id for r in report.failures],
    )

    # Persist in the appropriate session column.
    if backend_enum is TargetBackend.POSTGRES:
        session_registry.update_artifacts(
            session_id,
            actor=actor,
            onprem_postgres_sql=push_sql,
        )
    else:
        session_registry.update_artifacts(
            session_id,
            actor=actor,
            onprem_mssql_sql=push_sql,
        )
    session_registry.add_turn(
        session_id,
        role="assistant",
        text=(
            f"Proposed {backend_enum.value} push script "
            f"(opt score {report.score:.2f} = {report.passing}/{report.applicable} rules):\n"
            f"{proposal.rationale}"
        ),
        actor=actor,
    )
    _log.info(
        "mapper.push.done",
        session_id=session_id,
        backend=backend_enum.value,
        optimization_score=report.score,
        failed_rules=proposal.failed_rules,
    )
    return proposal


def _prepare_gold_for_sample(
    gold_sql: str,
    silver_target_table: str,
    profile: SourceProfile,
    limit: int,
) -> str:
    """Strip dbt jinja from Gold SQL and run a LIMIT-wrapped preview.

    The Gold view references {{ ref('silver_target') }}. We rewrite that
    to either (a) the deployed Silver model name verbatim if it already
    exists in the warehouse, or (b) the Bronze qualified table — the
    Silver doesn't exist yet during proposal. Since the Bronze is the
    actual rows the mapper has access to, we rewrite ref(silver) →
    Bronze for sample preview only. Real dbt run uses the proper ref.
    """
    cleaned = _JINJA_BLOCK.sub("", gold_sql)

    def _expand(match: re.Match[str]) -> str:
        body = match.group(1).strip()
        if body.startswith("config"):
            return ""
        if body.startswith("ref"):
            args = re.findall(r"['\"]([^'\"]+)['\"]", body)
            ref_name = str(args[0]) if args else ""
            # If ref points at the Silver we just proposed, substitute Bronze
            # for sample-run purposes (Silver isn't materialized yet).
            if ref_name == silver_target_table:
                return profile.qualified_table
            return ref_name
        if body.startswith("source"):
            return profile.qualified_table
        return ""

    cleaned = _JINJA_EXPR.sub(_expand, cleaned)
    cleaned = cleaned.strip().rstrip(";")
    # Gold typically filters on is_active = TRUE, but Bronze doesn't have
    # that column. Strip the WHERE clause to give the operator SOMETHING
    # to look at — the real dbt run will use the real Silver where is_active
    # is meaningful. This is a sample-only loosening, documented in caller.
    cleaned = re.sub(
        r"\bWHERE\s+is_active\s*=\s*TRUE\b",
        "WHERE 1=1",
        cleaned,
        flags=re.IGNORECASE,
    )
    return f"SELECT * FROM (\n{cleaned}\n) AS _gold_preview LIMIT {int(limit)}"


def _coerce_sample_value(v: Any) -> Any:
    """Coerce sample row values for JSON-friendly storage in mapping_sessions."""
    if v is None:
        return None
    if isinstance(v, str | int | float | bool):
        return v
    if isinstance(v, datetime):
        return v.isoformat()
    try:
        return float(v)
    except (TypeError, ValueError):
        return str(v)


def _summarise_proposal(p: SilverProposal) -> str:
    """One-line summary of a proposal for the conversation turn log."""
    cols = ", ".join(p.scd2_change_cols[:5])
    if len(p.scd2_change_cols) > 5:
        cols += f", ...(+{len(p.scd2_change_cols) - 5})"
    sample_status = (
        f"sample={len(p.sample_rows)} rows"
        if p.sample_rows
        else f"sample_error: {p.sample_error}"
        if p.sample_error
        else "no sample"
    )
    return (
        f"Proposed Silver `{p.silver_target_table}` "
        f"NK=[{', '.join(p.natural_keys)}] "
        f"change_cols=[{cols}] · {sample_status}\n"
        f"---\n{p.rationale}"
    )


def _profile_from_dict(d: dict[str, Any]) -> SourceProfile:
    """Reconstruct SourceProfile from the JSON snapshot stored in the session."""
    from datalink.agents.mapper.profiler import ColumnProfile

    cols = tuple(
        ColumnProfile(
            name=c["name"],
            raw_type=c["raw_type"],
            logical_type=c["logical_type"],
            total_rows=c["total_rows"],
            null_count=c["null_count"],
            null_pct=c["null_pct"],
            distinct_count=c["distinct_count"],
            sample_values=tuple(c.get("sample_values") or ()),
            min_value=c.get("min_value"),
            max_value=c.get("max_value"),
            semantic_hint=c.get("semantic_hint", ""),
        )
        for c in d.get("columns", [])
    )
    profiled_at_raw = d.get("profiled_at")
    if isinstance(profiled_at_raw, str):
        try:
            profiled_at = datetime.fromisoformat(profiled_at_raw)
        except ValueError:
            profiled_at = datetime.now(UTC)
    else:
        profiled_at = datetime.now(UTC)
    if profiled_at.tzinfo is None:
        profiled_at = profiled_at.replace(tzinfo=UTC)
    return SourceProfile(
        qualified_table=d["qualified_table"],
        total_rows=int(d.get("total_rows", 0) or 0),
        columns=cols,
        profiled_at=profiled_at,
        notes=tuple(d.get("notes") or ()),
    )
