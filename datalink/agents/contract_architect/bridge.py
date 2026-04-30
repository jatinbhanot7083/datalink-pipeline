"""Bridge between the ContractArchitectAgent and the rest of the platform.

Three responsibilities:

  1. ``propose_contract()`` — pulls top-k chunks from the RAG store,
     invokes the agent, returns a ContractProposal record the UI
     renders verbatim.

  2. ``persist_design()`` — upserts a row in
     ``CONTROL.contract_designs`` with status DRAFT/PENDING_REVIEW.

  3. ``approve_design()`` — terminal flow. Generates the 5 artifacts
     (Bronze DDL committed file, contract row, GX suite scaffold,
     dbt scaffold, vendor-spec markdown) and flips status to APPROVED.

The agent layer never touches Snowflake or pgvector directly — that
happens here, AFTER the LLM has produced a proposal. Keeps PHI
boundary clean: row data never travels through the LLM.
"""

from __future__ import annotations

import json
import time
import uuid
from datetime import UTC, datetime
from typing import Any

from datalink.adapters.protocols import LlmProvider, Warehouse
from datalink.agents.contract_architect.agent import (
    ContractArchitectAgent,
    ContractMode,
    ContractProposal,
    ProposedColumn,
)
from datalink.logging import get_logger
from datalink.memory import AgentMemoryStore
from datalink.quality.control import CONTROL_SCHEMA

_log = get_logger(__name__)


# ============================================================================
# Public API
# ============================================================================


def propose_contract(
    *,
    llm: LlmProvider,
    warehouse: Warehouse,
    memory: AgentMemoryStore,
    client_id: str,
    source_type: str,
    mode: str,
    anchored_standards: list[str],
    payload: dict[str, Any],
    temperature: float = 0.0,
    grounding_k: int = 3,
    strictness: float = 0.5,
    actor: str = "operator",
) -> ContractProposal:
    """End-to-end: retrieve grounding, invoke agent, return reviewable
    proposal.

    Does NOT persist — the UI shows the proposal first; the operator
    edits and clicks Approve, which calls ``persist_design()`` then
    ``approve_design()``.
    """
    # 1. Build the RAG query text from the input
    mode_enum = ContractMode(mode.upper())
    query_text = _build_rag_query(mode_enum, payload, source_type)
    grounding_chunks: list[dict[str, Any]] = []
    grounding_meta: list[dict[str, Any]] = []
    if anchored_standards:
        try:
            hits = memory.query_similar_standard_chunks(
                query_text=query_text,
                anchored_codes=tuple(anchored_standards),
                k=grounding_k,
            )
            for h in hits:
                grounding_chunks.append(
                    {
                        "standard_code": h.standard_code,
                        "section_path": h.section_path,
                        "resource_type": h.resource_type,
                        "chunk_text": h.chunk_text,
                        "distance": h.distance,
                    }
                )
                grounding_meta.append(
                    {
                        "standard_code": h.standard_code,
                        "section_path": h.section_path,
                        "distance": float(h.distance),
                    }
                )
        except Exception as e:
            _log.warning(
                "contract_architect.rag_unavailable",
                error=str(e),
                anchored=anchored_standards,
            )

    # 2. Invoke the agent
    agent = ContractArchitectAgent(llm=llm, warehouse=warehouse)
    started = time.time()
    result = agent.run(
        {
            "client_id": client_id,
            "source_type": source_type,
            "mode": mode_enum.value,
            "anchored_standards": anchored_standards,
            "temperature": temperature,
            "grounding_k": grounding_k,
            "strictness": strictness,
            "payload": payload,
            "grounding_chunks": grounding_chunks,
        }
    )
    duration_ms = int((time.time() - started) * 1000)

    if not result.success:
        raise RuntimeError(f"ContractArchitectAgent failed: {result.error or '(no detail)'}")

    obj = result.payload

    # 3. Hydrate ContractProposal
    columns = [
        ProposedColumn(
            name=c["name"],
            type=c["type"],
            nullable=bool(c["nullable"]),
            rationale=c["rationale"],
            matches_standard=c.get("matches_standard"),
            vendor_field=c.get("vendor_field"),
            deviation=c.get("deviation"),
        )
        for c in obj["proposed_columns"]
    ]

    proposer_model = getattr(llm, "model", "")
    embedding_model = getattr(memory, "_embedder", None)
    embedding_model_name = getattr(embedding_model, "model", "") if embedding_model else ""

    return ContractProposal(
        proposed_table_name=obj["proposed_table_name"],
        proposed_columns=columns,
        proposed_ddl=obj["proposed_ddl"],
        standard_match_scores={
            k: float(v) for k, v in obj.get("standard_match_scores", {}).items()
        },
        rationale=obj.get("rationale", ""),
        deviation_log=obj.get("deviation_log", []),
        gx_scaffold=obj.get("gx_scaffold", []),
        dbt_scaffold=obj.get("dbt_scaffold", ""),
        vendor_spec_md=obj.get("vendor_spec_md", ""),
        grounding=grounding_meta,
        tokens_used=int(result.tokens_used),
        duration_ms=duration_ms,
        proposer_model=proposer_model,
        embedding_model=embedding_model_name,
        temperature=temperature,
        strictness=strictness,
        mode=mode_enum,
    )


def persist_design(
    *,
    warehouse: Warehouse,
    proposal: ContractProposal,
    client_id: str,
    source_type: str,
    anchored_standards: list[str],
    payload: dict[str, Any],
    edited_columns: list[dict[str, Any]] | None = None,
    edited_ddl: str | None = None,
    actor: str,
    approval_mode: str = "HITL",
    notes: str = "",
) -> str:
    """Insert / refresh a row in CONTROL.contract_designs. Returns design_id."""
    design_id = str(uuid.uuid4())
    status = "DRAFT" if approval_mode == "DRAFT" else "PENDING_REVIEW"

    sample_file_uri = (
        payload.get("file_name") if proposal.mode == ContractMode.FILE_DRIVEN else None
    )
    nl_description = (
        payload.get("nl_description") if proposal.mode == ContractMode.CONTRACT_FIRST else None
    )

    proposed_columns_json = json.dumps(
        [
            {
                "name": c.name,
                "type": c.type,
                "nullable": c.nullable,
                "rationale": c.rationale,
                "matches_standard": c.matches_standard,
                "vendor_field": c.vendor_field,
                "deviation": c.deviation,
            }
            for c in proposal.proposed_columns
        ]
    )

    warehouse.execute(
        f"""
        INSERT INTO {CONTROL_SCHEMA}.contract_designs
          (design_id, client_id, source_type, status, version, mode,
           anchored_standards, temperature, grounding_k, strictness,
           sample_file_uri, nl_description,
           proposed_columns, edited_columns, proposed_ddl, final_ddl,
           deviation_log, per_column_reasoning, standard_match_scores,
           vendor_spec_md, approval_mode, created_at, created_by, notes)
        VALUES ($id, $cid, $stp, $st, 1, $mode,
                $anc, $temp, $k, $strict,
                $fileuri, $nl,
                $pcols, $ecols, $pddl, $fddl,
                $devs, $reason, $scores,
                $spec, $appm, $ts, $actor, $notes)
        """,
        {
            "id": design_id,
            "cid": client_id,
            "stp": source_type.upper(),
            "st": status,
            "mode": proposal.mode.value,
            "anc": json.dumps(anchored_standards),
            "temp": float(proposal.temperature),
            "k": int(len(proposal.grounding)),
            "strict": float(proposal.strictness),
            "fileuri": sample_file_uri,
            "nl": nl_description,
            "pcols": proposed_columns_json,
            "ecols": json.dumps(edited_columns) if edited_columns else None,
            "pddl": proposal.proposed_ddl,
            "fddl": edited_ddl,
            "devs": json.dumps(proposal.deviation_log),
            "reason": json.dumps({c.name: c.rationale for c in proposal.proposed_columns}),
            "scores": json.dumps(proposal.standard_match_scores),
            "spec": proposal.vendor_spec_md,
            "appm": approval_mode,
            "ts": datetime.now(UTC),
            "actor": actor,
            "notes": notes,
        },
    )

    # Audit log
    _audit_log(
        warehouse=warehouse,
        design_id=design_id,
        client_id=client_id,
        source_type=source_type,
        action="PROPOSED",
        actor=actor,
        from_status=None,
        to_status=status,
        diff_summary={"columns": len(proposal.proposed_columns)},
        ai_reasoning=proposal.rationale[:500],
        token_count=int(proposal.tokens_used),
        latency_ms=int(proposal.duration_ms),
        notes=notes,
    )

    return design_id


def approve_design(
    *,
    warehouse: Warehouse,
    design_id: str,
    actor: str,
    notes: str = "",
) -> dict[str, Any]:
    """Terminal step. Marks status=APPROVED + writes audit row.
    Returns a summary dict the UI shows.

    Phase 14.6 will extend this to ALSO emit the 5 artifacts:
      - Bronze DDL file under datalink/pipeline/bronze/ddl/
      - SOURCE_SCHEMA_CONTRACTS row
      - DQ_SUITES row (GX scaffold)
      - dbt model file under dbt/models/silver/
      - vendor spec PDF
    """
    rows = warehouse.query(
        f"SELECT * FROM {CONTROL_SCHEMA}.contract_designs WHERE design_id = $id",
        {"id": design_id},
    )
    if not rows:
        raise ValueError(f"Design {design_id} not found.")
    cur = rows[0]
    if cur.get("status") in ("APPROVED", "ARCHIVED"):
        raise ValueError(
            f"Design {design_id} status is {cur.get('status')} — " f"cannot approve again."
        )

    warehouse.execute(
        f"""
        UPDATE {CONTROL_SCHEMA}.contract_designs
           SET status = 'APPROVED',
               approved_at = $ts,
               approved_by = $actor,
               final_ddl   = COALESCE(final_ddl, proposed_ddl),
               final_columns = COALESCE(edited_columns, proposed_columns),
               notes       = COALESCE(notes, '') || $notes
         WHERE design_id = $id
        """,
        {
            "id": design_id,
            "ts": datetime.now(UTC),
            "actor": actor,
            "notes": f"\n[approved] {notes}" if notes else "",
        },
    )

    _audit_log(
        warehouse=warehouse,
        design_id=design_id,
        client_id=str(cur.get("client_id", "")),
        source_type=str(cur.get("source_type", "")),
        action="APPROVED",
        actor=actor,
        from_status=str(cur.get("status", "")),
        to_status="APPROVED",
        diff_summary=None,
        ai_reasoning=None,
        token_count=None,
        latency_ms=None,
        notes=notes,
    )
    return {"design_id": design_id, "status": "APPROVED", "approved_by": actor}


# ============================================================================
# Internals
# ============================================================================


def _build_rag_query(mode: ContractMode, payload: dict[str, Any], source_type: str) -> str:
    """Build the natural-language query we feed to the RAG store.

    For FILE_DRIVEN: combine source_type + visible header columns.
    For CONTRACT_FIRST: use the operator's NL description directly.
    """
    if mode == ContractMode.FILE_DRIVEN:
        headers = list(payload.get("headers", []))
        return f"{source_type} feed with columns: {', '.join(headers[:20])}"
    nl = str(payload.get("nl_description", "")).strip()
    return nl or source_type


def _audit_log(
    *,
    warehouse: Warehouse,
    design_id: str,
    client_id: str,
    source_type: str,
    action: str,
    actor: str,
    from_status: str | None,
    to_status: str | None,
    diff_summary: dict[str, Any] | None,
    ai_reasoning: str | None,
    token_count: int | None,
    latency_ms: int | None,
    notes: str,
) -> None:
    warehouse.execute(
        f"""
        INSERT INTO {CONTROL_SCHEMA}.contract_design_audit_log
          (audit_id, design_id, client_id, source_type, action, actor,
           from_status, to_status, diff_summary, ai_reasoning,
           token_count, latency_ms, ts, notes)
        VALUES ($aid, $did, $cid, $stp, $act, $actor,
                $fs, $ts2, $diff, $reason,
                $tok, $lat, $ts, $notes)
        """,
        {
            "aid": str(uuid.uuid4()),
            "did": design_id,
            "cid": client_id,
            "stp": source_type.upper(),
            "act": action,
            "actor": actor,
            "fs": from_status,
            "ts2": to_status,
            "diff": json.dumps(diff_summary) if diff_summary else None,
            "reason": ai_reasoning,
            "tok": token_count,
            "lat": latency_ms,
            "ts": datetime.now(UTC),
            "notes": notes,
        },
    )
