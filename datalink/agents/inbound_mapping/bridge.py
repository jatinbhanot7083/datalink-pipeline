"""Bridge functions — UI-facing wrappers around InboundMappingAgent.

The Streamlit page calls these (not the agent directly) so the page code
stays declarative.  All persistence happens here.

Lifecycle:

    propose_mapping()   →   DRAFT row written to bronze_inbound_mapping_proposals
                            + per-field rows to bronze_inbound_field_proposals
                            + audit row 'propose'
    submit_proposal()   →   DRAFT → PENDING_REVIEW   + audit 'submit'
    approve_proposal()  →   PENDING_REVIEW → APPROVED → LIVE in one shot
                            + ARCHIVE prior LIVE for same (client, dataset)
                            + audit 'approve' + 'promote'
    reject_proposal()   →   PENDING_REVIEW → REJECTED  + audit 'reject' (notes required)
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from typing import IO, Any

from datalink.adapters.protocols import LlmProvider, Warehouse
from datalink.agents.inbound_mapping.agent import (
    InboundMappingAgent,
)
from datalink.agents.inbound_mapping.readers import (
    SourceView,
    detect_and_read,
)
from datalink.logging import get_logger
from datalink.quality.control import CONTROL_SCHEMA

_log = get_logger(__name__)


# ---------------------------------------------------------------------------
# Read canonical schema for the target dataset (so the agent has a target)
# ---------------------------------------------------------------------------


def _load_canonical_schema(wh: Warehouse, dataset_code: str) -> list[dict[str, Any]]:
    """Pull the bronze catalog fields for a dataset — these are the
    canonical targets the LLM must map TO."""
    return list(
        wh.query(
            f"SELECT bronze_column_name, field_display_name, requirement, "
            f"       logical_type, description, is_pii, is_phi, is_business_key "
            f"FROM {CONTROL_SCHEMA}.global_bronze_catalog_fields "
            f"WHERE dataset_code = $ds "
            f"ORDER BY field_order",
            {"ds": dataset_code},
        )
    )


def _load_prior_live_mapping(
    wh: Warehouse,
    *,
    client_id: str,
    dataset_code: str,
) -> dict[str, Any] | None:
    """Pull the current LIVE mapping for this (client, dataset), if any.
    Used by the agent as a stickiness prior on subsequent uploads."""
    heads = list(
        wh.query(
            f"SELECT mapping_id, version "
            f"FROM {CONTROL_SCHEMA}.bronze_inbound_mappings "
            f"WHERE client_id = $cid AND dataset_code = $ds AND status = 'LIVE' "
            f"ORDER BY version DESC LIMIT 1",
            {"cid": client_id, "ds": dataset_code},
        )
    )
    if not heads:
        return None
    mid = heads[0]["mapping_id"]
    rows = list(
        wh.query(
            f"SELECT client_field_name, client_field_path, client_field_position, "
            f"       canonical_field_code, canonical_dataset_code, transform_sql, "
            f"       transform_kind "
            f"FROM {CONTROL_SCHEMA}.bronze_inbound_field_mappings "
            f"WHERE mapping_id = $mid",
            {"mid": mid},
        )
    )
    return {
        "mapping_id": mid,
        "version": heads[0]["version"],
        "field_mappings": [dict(r) for r in rows],
    }


# ---------------------------------------------------------------------------
# propose_mapping — the AI call
# ---------------------------------------------------------------------------


def recommend_target_for_file(
    *,
    wh: Warehouse,
    llm: LlmProvider,
    file_buffer: IO[bytes] | str | None = None,
    filename: str = "",
    sheet_name: str | None = None,
    source_view: SourceView | None = None,
    top_k: int = 3,
    temperature: float = 0.0,
) -> dict[str, Any]:
    """Pure recommendation step — no persistence.  Read the file (or accept
    a pre-built SourceView for free-text input) → ask the LLM to rank
    candidate canonical datasets → return top-K + the parsed SourceView
    so the caller can keep using it for the field-mapping step without
    re-reading.

    Two call modes:
      * ``file_buffer`` + ``filename``  — standard path: read the file
      * ``source_view`` (pre-built)     — for free-text / pasted-spec input
        where there's no file to read.  UI builds the SourceView via
        ``read_text(spec)`` and hands it directly to the agent.

    Returns::

        {
            "source_view": {...},
            "recommendations": [{"dataset_code", "display_name", "confidence", "rationale"}, ...],
            "top_pick": "membership",
            "rationale": "...",
            "tokens_used": int,
            "duration_ms": int,
        }
    """
    if source_view is not None:
        # Pre-built SourceView (free-text path) — use it directly
        source = source_view
    else:
        if file_buffer is None:
            raise ValueError("either file_buffer+filename OR source_view is required")
        source = detect_and_read(file_buffer, filename=filename, sheet_name=sheet_name)
        if isinstance(source, list):
            non_empty = [s for s in source if (s.fields or s.sample_rows)]
            if not non_empty:
                raise ValueError(f"No parseable sheets in '{filename}'")
            source = non_empty[0]

    # Pull the 33 (or N) canonical datasets as candidates
    candidates = list(
        wh.query(
            f"SELECT dataset_code, display_name, category, default_frequency, "
            f"       used_by, total_fields, notes "
            f"FROM {CONTROL_SCHEMA}.global_bronze_catalog_datasets "
            f"WHERE is_active = TRUE "
            f"ORDER BY display_name"
        )
    )
    if not candidates:
        raise ValueError(
            "No canonical datasets in CONTROL.global_bronze_catalog_datasets — "
            "upload the canonical Product Catalogue first."
        )

    agent = InboundMappingAgent(llm=llm, warehouse=wh)
    rec = agent.recommend_target_datasets(
        source_view=source.to_dict(),
        candidates=[dict(c) for c in candidates],
        top_k=top_k,
        temperature=temperature,
    )

    # Enrich each recommendation with display_name (so the UI doesn't have to
    # do a second lookup)
    code_to_display = {str(c["dataset_code"]): str(c.get("display_name") or "") for c in candidates}
    for r in rec.get("recommendations", []):
        r["display_name"] = code_to_display.get(r["dataset_code"], r["dataset_code"])

    return {
        "source_view": source.to_dict(),
        **rec,
    }


def propose_mapping(
    *,
    wh: Warehouse,
    llm: LlmProvider,
    client_id: str,
    dataset_code: str,
    file_buffer: IO[bytes] | str | None = None,
    filename: str = "",
    sheet_name: str | None = None,
    actor: str = "ui:onboarding",
    temperature: float = 0.0,
    grounding_mode: str = "off",
    model: str = "",
    pre_read_source: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Read the file → run the agent → persist a DRAFT proposal.

    Returns the proposal as a dict (includes proposal_id for the UI to
    deep-link into the review page).
    """
    if not client_id or not dataset_code:
        raise ValueError("client_id and dataset_code are required")

    # Step 1 — get the SourceView.  If the caller already has one (from
    # recommend_target_for_file) we reuse it to avoid re-parsing the file
    # and to keep ONE deterministic source-of-truth across both LLM calls.
    if pre_read_source is not None:
        # Rehydrate a SourceView from a dict.
        sv_dict = pre_read_source
        source = SourceView(
            format=str(sv_dict.get("format") or ""),
            filename=str(sv_dict.get("filename") or filename or ""),
            sheet_or_section=sv_dict.get("sheet_or_section"),
            sample_rows=list(sv_dict.get("sample_rows") or []),
            row_count_sampled=int(sv_dict.get("row_count_sampled") or 0),
            row_count_total=sv_dict.get("row_count_total"),
            reader_notes=list(sv_dict.get("reader_notes") or []),
            detected_at=str(sv_dict.get("detected_at") or ""),
        )
        # Field descriptors
        from datalink.agents.inbound_mapping.readers import FieldDescriptor

        for f in sv_dict.get("fields") or []:
            source.fields.append(
                FieldDescriptor(
                    name=str(f.get("name") or ""),
                    path=f.get("path"),
                    position=f.get("position"),
                    width=f.get("width"),
                    inferred_type=str(f.get("inferred_type") or "TEXT"),
                    nullable=bool(f.get("nullable", True)),
                    sample_values=list(f.get("sample_values") or []),
                    notes=str(f.get("notes") or ""),
                )
            )
    else:
        if file_buffer is None:
            raise ValueError(
                "Either file_buffer+filename OR pre_read_source must be supplied "
                "to propose_mapping().  Free-text callers should pass the "
                "SourceView dict via pre_read_source after running read_text()."
            )
        source = detect_and_read(file_buffer, filename=filename, sheet_name=sheet_name)
        if isinstance(source, list):
            non_empty = [s for s in source if (s.fields or s.sample_rows)]
            if not non_empty:
                raise ValueError(
                    f"No parseable sheets in '{filename}'.  Hint: provide a "
                    "specific sheet via sheet_name= parameter."
                )
            source = non_empty[0]

    # Step 2 — load canonical schema + prior LIVE mapping
    canonical = _load_canonical_schema(wh, dataset_code)
    if not canonical:
        raise ValueError(
            f"Canonical dataset '{dataset_code}' has no fields in "
            f"CONTROL.global_bronze_catalog_fields.  Upload the canonical "
            f"catalogue first."
        )
    prior = _load_prior_live_mapping(wh, client_id=client_id, dataset_code=dataset_code)

    # Step 3 — run the agent
    agent = InboundMappingAgent(llm=llm, warehouse=wh)
    raw = agent.execute(
        {
            "client_id": client_id,
            "dataset_code": dataset_code,
            "source_view": source.to_dict(),
            "canonical_schema": canonical,
            "prior_live_mapping": prior,
            "temperature": temperature,
            "grounding_mode": grounding_mode,
            "model": model,
        }
    )

    # Step 4 — persist as DRAFT
    proposal_id = str(uuid.uuid4())
    next_version = _next_version(wh, client_id=client_id, dataset_code=dataset_code)
    _insert_proposal_head(
        wh,
        proposal_id=proposal_id,
        version=next_version,
        client_id=client_id,
        dataset_code=dataset_code,
        status="DRAFT",
        source=source,
        agent_output=raw,
        actor=actor,
        model=model,
        grounding_mode=grounding_mode,
    )
    _insert_field_proposals(wh, proposal_id=proposal_id, mappings=raw.get("field_mappings") or [])
    _write_audit(
        wh,
        proposal_id=proposal_id,
        client_id=client_id,
        dataset_code=dataset_code,
        action="propose",
        actor=actor,
        from_status=None,
        to_status="DRAFT",
        diff={"tokens": raw.get("tokens_used", 0), "duration_ms": raw.get("duration_ms", 0)},
    )

    raw["proposal_id"] = proposal_id
    raw["version"] = next_version
    raw["status"] = "DRAFT"
    return raw


# ---------------------------------------------------------------------------
# Lifecycle transitions
# ---------------------------------------------------------------------------


def submit_proposal(
    *,
    wh: Warehouse,
    proposal_id: str,
    actor: str,
    notes: str = "",
) -> None:
    """DRAFT → PENDING_REVIEW."""
    _transition(
        wh,
        proposal_id=proposal_id,
        expected="DRAFT",
        new_status="PENDING_REVIEW",
        actor=actor,
        action="submit",
        notes=notes,
    )


def approve_proposal(
    *,
    wh: Warehouse,
    proposal_id: str,
    actor: str,
    notes: str = "",
) -> str:
    """PENDING_REVIEW (or DRAFT) → APPROVED → LIVE in one shot.

    Side effects:
      * Old LIVE mapping for same (client, dataset) is ARCHIVED.
      * New row inserted in bronze_inbound_mappings (LIVE).
      * Per-field rows mirrored into bronze_inbound_field_mappings.
      * Audit row 'approve' + 'promote' written.

    Returns the new mapping_id.
    """
    # 1. Read proposal head
    rows = list(
        wh.query(
            f"SELECT client_id, dataset_code, status, version, source_format "
            f"FROM {CONTROL_SCHEMA}.bronze_inbound_mapping_proposals "
            f"WHERE proposal_id = $pid",
            {"pid": proposal_id},
        )
    )
    if not rows:
        raise ValueError(f"proposal_id '{proposal_id}' not found")
    head = dict(rows[0])
    if head["status"] not in ("PENDING_REVIEW", "DRAFT", "APPROVED"):
        raise ValueError(f"cannot approve from status '{head['status']}'")

    client_id = head["client_id"]
    dataset_code = head["dataset_code"]

    # 2. Mark APPROVED
    wh.execute(
        f"UPDATE {CONTROL_SCHEMA}.bronze_inbound_mapping_proposals "
        f"SET status = 'APPROVED', approved_at = $ts, approved_by = $by, "
        f"    reviewer_notes = $notes "
        f"WHERE proposal_id = $pid",
        {"ts": _ts(), "by": actor, "notes": notes[:4000], "pid": proposal_id},
    )
    _write_audit(
        wh,
        proposal_id=proposal_id,
        client_id=client_id,
        dataset_code=dataset_code,
        action="approve",
        actor=actor,
        from_status=head["status"],
        to_status="APPROVED",
        notes=notes,
    )

    # 3. ARCHIVE prior LIVE for same (client, dataset)
    wh.execute(
        f"UPDATE {CONTROL_SCHEMA}.bronze_inbound_mappings "
        f"SET status = 'ARCHIVED', archived_at = $ts "
        f"WHERE client_id = $cid AND dataset_code = $ds AND status = 'LIVE'",
        {"ts": _ts(), "cid": client_id, "ds": dataset_code},
    )

    # 4. Insert new LIVE mapping head + child field rows
    mapping_id = str(uuid.uuid4())
    wh.execute(
        f"INSERT INTO {CONTROL_SCHEMA}.bronze_inbound_mappings "
        f"(mapping_id, client_id, dataset_code, version, status, source_format, "
        f" promoted_from, promoted_by, promoted_at) "
        f"VALUES ($mid, $cid, $ds, $ver, 'LIVE', $fmt, $pid, $by, $ts)",
        {
            "mid": mapping_id,
            "cid": client_id,
            "ds": dataset_code,
            "ver": int(head.get("version") or 1),
            "fmt": head.get("source_format") or "",
            "pid": proposal_id,
            "by": actor,
            "ts": _ts(),
        },
    )
    # Mirror the (possibly edited) field proposals into LIVE
    fp_rows = list(
        wh.query(
            f"SELECT client_field_name, client_field_path, client_field_position, "
            f"       canonical_field_code, canonical_dataset_code, "
            f"       transform_sql, transform_kind "
            f"FROM {CONTROL_SCHEMA}.bronze_inbound_field_proposals "
            f"WHERE proposal_id = $pid AND review_status != 'REJECTED' "
            f"  AND canonical_field_code IS NOT NULL",
            {"pid": proposal_id},
        )
    )
    for fp in fp_rows:
        wh.execute(
            f"INSERT INTO {CONTROL_SCHEMA}.bronze_inbound_field_mappings "
            f"(field_mapping_id, mapping_id, client_field_name, "
            f" client_field_path, client_field_position, "
            f" canonical_field_code, canonical_dataset_code, "
            f" transform_sql, transform_kind) "
            f"VALUES ($fmid, $mid, $cf, $cp, $cpos, $can, $cds, $tsql, $tk)",
            {
                "fmid": str(uuid.uuid4()),
                "mid": mapping_id,
                "cf": fp.get("client_field_name"),
                "cp": fp.get("client_field_path"),
                "cpos": fp.get("client_field_position"),
                "can": fp.get("canonical_field_code"),
                "cds": fp.get("canonical_dataset_code") or dataset_code,
                "tsql": fp.get("transform_sql"),
                "tk": fp.get("transform_kind") or "direct",
            },
        )

    # 5. Mark proposal LIVE so the UI knows it's the active one
    wh.execute(
        f"UPDATE {CONTROL_SCHEMA}.bronze_inbound_mapping_proposals "
        f"SET status = 'LIVE' "
        f"WHERE proposal_id = $pid",
        {"pid": proposal_id},
    )
    _write_audit(
        wh,
        proposal_id=proposal_id,
        mapping_id=mapping_id,
        client_id=client_id,
        dataset_code=dataset_code,
        action="promote",
        actor=actor,
        from_status="APPROVED",
        to_status="LIVE",
        notes=f"Promoted to LIVE mapping_id={mapping_id}",
    )
    return mapping_id


def reject_proposal(
    *,
    wh: Warehouse,
    proposal_id: str,
    actor: str,
    notes: str,
) -> None:
    """PENDING_REVIEW → REJECTED.  Reviewer notes REQUIRED."""
    if not (notes or "").strip():
        raise ValueError("reviewer_notes are required to reject a proposal")
    _transition(
        wh,
        proposal_id=proposal_id,
        expected=None,
        new_status="REJECTED",
        actor=actor,
        action="reject",
        notes=notes,
    )


# ---------------------------------------------------------------------------
# Read helpers for the UI
# ---------------------------------------------------------------------------


def list_proposals(
    *,
    wh: Warehouse,
    client_id: str | None = None,
    dataset_code: str | None = None,
    status: str | list[str] | None = None,
    limit: int = 200,
) -> list[dict[str, Any]]:
    where: list[str] = []
    params: dict[str, Any] = {}
    if client_id:
        where.append("client_id = $cid")
        params["cid"] = client_id
    if dataset_code:
        where.append("dataset_code = $ds")
        params["ds"] = dataset_code
    if isinstance(status, str):
        where.append("status = $st")
        params["st"] = status
    elif isinstance(status, list) and status:
        # Snowflake doesn't bind list directly — inline (validated as enum-ish)
        safe = [s for s in status if isinstance(s, str) and s.replace("_", "").isalpha()]
        if safe:
            where.append("status IN (" + ",".join(f"'{s}'" for s in safe) + ")")
    clause = ("WHERE " + " AND ".join(where)) if where else ""
    return list(
        wh.query(
            f"SELECT proposal_id, client_id, dataset_code, version, status, "
            f"       source_format, source_filename, ai_token_count, ai_latency_ms, "
            f"       created_by, created_at, submitted_at, approved_at, approved_by "
            f"FROM {CONTROL_SCHEMA}.bronze_inbound_mapping_proposals "
            f"{clause} "
            f"ORDER BY created_at DESC LIMIT {int(limit)}",
            params,
        )
    )


def get_proposal(*, wh: Warehouse, proposal_id: str) -> dict[str, Any]:
    """Full detail: head + field rows + audit trail."""
    heads = list(
        wh.query(
            f"SELECT * FROM {CONTROL_SCHEMA}.bronze_inbound_mapping_proposals "
            f"WHERE proposal_id = $pid",
            {"pid": proposal_id},
        )
    )
    if not heads:
        raise ValueError(f"proposal_id '{proposal_id}' not found")
    head = dict(heads[0])
    fields = list(
        wh.query(
            f"SELECT * FROM {CONTROL_SCHEMA}.bronze_inbound_field_proposals "
            f"WHERE proposal_id = $pid "
            f"ORDER BY confidence DESC",
            {"pid": proposal_id},
        )
    )
    audit = list(
        wh.query(
            f"SELECT * FROM {CONTROL_SCHEMA}.bronze_inbound_mapping_audit "
            f"WHERE proposal_id = $pid "
            f"ORDER BY ts ASC",
            {"pid": proposal_id},
        )
    )
    head["field_proposals"] = [dict(r) for r in fields]
    head["audit_log"] = [dict(r) for r in audit]
    return head


def list_live_mappings(*, wh: Warehouse) -> list[dict[str, Any]]:
    return list(
        wh.query(
            f"SELECT mapping_id, client_id, dataset_code, version, "
            f"       source_format, promoted_at, promoted_by "
            f"FROM {CONTROL_SCHEMA}.bronze_inbound_mappings "
            f"WHERE status = 'LIVE' "
            f"ORDER BY client_id, dataset_code"
        )
    )


# ---------------------------------------------------------------------------
# Field-row editing — reviewer can edit AI proposals before approval
# ---------------------------------------------------------------------------


def edit_field_proposal(
    *,
    wh: Warehouse,
    field_proposal_id: str,
    canonical_field_code: str | None = None,
    transform_sql: str | None = None,
    transform_kind: str | None = None,
    review_status: str = "EDITED",
    actor: str = "ui:reviewer",
) -> None:
    """Update a single field-proposal row (reviewer edit)."""
    sets = ["review_status = $rs"]
    params: dict[str, Any] = {"rs": review_status}
    if canonical_field_code is not None:
        sets.append("canonical_field_code = $can")
        params["can"] = canonical_field_code
    if transform_sql is not None:
        sets.append("transform_sql = $tsql")
        params["tsql"] = transform_sql
    if transform_kind is not None:
        sets.append("transform_kind = $tk")
        params["tk"] = transform_kind
    params["fpid"] = field_proposal_id
    wh.execute(
        f"UPDATE {CONTROL_SCHEMA}.bronze_inbound_field_proposals "
        f"SET {', '.join(sets)} "
        f"WHERE field_proposal_id = $fpid",
        params,
    )
    # Audit (best-effort)
    try:
        wh.execute(
            f"INSERT INTO {CONTROL_SCHEMA}.bronze_inbound_mapping_audit "
            f"(audit_id, action, actor, ts, notes) "
            f"VALUES ($aid, 'edit_field', $by, $ts, $note)",
            {
                "aid": str(uuid.uuid4()),
                "by": actor,
                "ts": _ts(),
                "note": f"field_proposal_id={field_proposal_id} status={review_status}"[:1000],
            },
        )
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _ts() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def _next_version(wh: Warehouse, *, client_id: str, dataset_code: str) -> int:
    rows = list(
        wh.query(
            f"SELECT COALESCE(MAX(version), 0) c "
            f"FROM {CONTROL_SCHEMA}.bronze_inbound_mapping_proposals "
            f"WHERE client_id = $cid AND dataset_code = $ds",
            {"cid": client_id, "ds": dataset_code},
        )
    )
    return int(rows[0]["c"]) + 1 if rows else 1


def _insert_proposal_head(
    wh: Warehouse,
    *,
    proposal_id: str,
    version: int,
    client_id: str,
    dataset_code: str,
    status: str,
    source: SourceView,
    agent_output: dict[str, Any],
    actor: str,
    model: str,
    grounding_mode: str,
) -> None:
    wh.execute(
        f"INSERT INTO {CONTROL_SCHEMA}.bronze_inbound_mapping_proposals "
        f"(proposal_id, client_id, dataset_code, version, status, "
        f" source_format, source_filename, source_sample_rows, source_schema_json, "
        f" ai_proposal_json, ai_rationale, ai_token_count, ai_latency_ms, "
        f" ai_model, ai_grounding_mode, drift_summary, "
        f" created_by, created_at) "
        f"SELECT $pid, $cid, $ds, $ver, $st, "
        f"       $fmt, $fn, PARSE_JSON($sr), PARSE_JSON($ss), "
        f"       PARSE_JSON($ap), $rt, $tok, $lat, "
        f"       $mdl, $gm, PARSE_JSON($drift), "
        f"       $by, $ts",
        {
            "pid": proposal_id,
            "cid": client_id,
            "ds": dataset_code,
            "ver": version,
            "st": status,
            "fmt": source.format,
            "fn": source.filename,
            "sr": json.dumps(source.sample_rows[:5], default=str),
            "ss": json.dumps(source.to_dict(), default=str),
            "ap": json.dumps(agent_output, default=str)[:262144],
            "rt": str(agent_output.get("rationale") or "")[:8000],
            "tok": int(agent_output.get("tokens_used") or 0),
            "lat": int(agent_output.get("duration_ms") or 0),
            "mdl": model,
            "gm": grounding_mode,
            "drift": json.dumps(agent_output.get("drift_summary") or {}, default=str),
            "by": actor,
            "ts": _ts(),
        },
    )


def _insert_field_proposals(
    wh: Warehouse,
    *,
    proposal_id: str,
    mappings: list[dict[str, Any]],
) -> None:
    for m in mappings:
        wh.execute(
            f"INSERT INTO {CONTROL_SCHEMA}.bronze_inbound_field_proposals "
            f"(field_proposal_id, proposal_id, client_field_name, "
            f" client_field_path, client_field_position, "
            f" canonical_field_code, canonical_dataset_code, "
            f" transform_sql, transform_kind, confidence, rationale, "
            f" is_required, review_status) "
            f"VALUES ($id, $pid, $cf, $cp, $cpos, $can, $cds, "
            f"        $tsql, $tk, $conf, $rat, $req, 'PENDING')",
            {
                "id": str(uuid.uuid4()),
                "pid": proposal_id,
                "cf": str(m.get("client_field_name") or "")[:256],
                "cp": m.get("client_field_path"),
                "cpos": m.get("client_field_position"),
                "can": m.get("canonical_field_code"),
                "cds": m.get("canonical_dataset_code"),
                "tsql": m.get("transform_sql"),
                "tk": m.get("transform_kind") or "direct",
                "conf": float(m.get("confidence") or 0),
                "rat": str(m.get("rationale") or "")[:2000],
                "req": bool(m.get("is_required") or False),
            },
        )


def _transition(
    wh: Warehouse,
    *,
    proposal_id: str,
    expected: str | None,
    new_status: str,
    actor: str,
    action: str,
    notes: str = "",
) -> None:
    rows = list(
        wh.query(
            f"SELECT status, client_id, dataset_code "
            f"FROM {CONTROL_SCHEMA}.bronze_inbound_mapping_proposals "
            f"WHERE proposal_id = $pid",
            {"pid": proposal_id},
        )
    )
    if not rows:
        raise ValueError(f"proposal_id '{proposal_id}' not found")
    cur = dict(rows[0])
    if expected and cur["status"] != expected:
        raise ValueError(f"expected status '{expected}', got '{cur['status']}'")

    set_cols = ["status = $new"]
    params: dict[str, Any] = {"new": new_status, "pid": proposal_id}
    if action == "submit":
        set_cols += ["submitted_at = $ts", "submitted_by = $by"]
        params["ts"] = _ts()
        params["by"] = actor
    if action == "reject":
        set_cols += ["reviewed_at = $ts", "reviewed_by = $by", "reviewer_notes = $notes"]
        params["ts"] = _ts()
        params["by"] = actor
        params["notes"] = notes[:4000]

    wh.execute(
        f"UPDATE {CONTROL_SCHEMA}.bronze_inbound_mapping_proposals "
        f"SET {', '.join(set_cols)} WHERE proposal_id = $pid",
        params,
    )
    _write_audit(
        wh,
        proposal_id=proposal_id,
        client_id=cur["client_id"],
        dataset_code=cur["dataset_code"],
        action=action,
        actor=actor,
        from_status=cur["status"],
        to_status=new_status,
        notes=notes,
    )


def _write_audit(
    wh: Warehouse,
    *,
    proposal_id: str | None = None,
    mapping_id: str | None = None,
    client_id: str | None = None,
    dataset_code: str | None = None,
    action: str,
    actor: str,
    from_status: str | None = None,
    to_status: str | None = None,
    diff: dict[str, Any] | None = None,
    notes: str = "",
) -> None:
    try:
        wh.execute(
            f"INSERT INTO {CONTROL_SCHEMA}.bronze_inbound_mapping_audit "
            f"(audit_id, proposal_id, mapping_id, client_id, dataset_code, "
            f" action, actor, from_status, to_status, diff_summary, notes, ts) "
            f"SELECT $aid, $pid, $mid, $cid, $ds, "
            f"       $act, $by, $fs, $tos, PARSE_JSON($diff), $notes, $ts",
            {
                "aid": str(uuid.uuid4()),
                "pid": proposal_id,
                "mid": mapping_id,
                "cid": client_id,
                "ds": dataset_code,
                "act": action,
                "by": actor,
                "fs": from_status,
                "tos": to_status,
                "diff": json.dumps(diff or {}, default=str)[:8000],
                "notes": (notes or "")[:4000],
                "ts": _ts(),
            },
        )
    except Exception as exc:
        _log.warning("inbound_mapping.audit_failed", err=str(exc)[:200])
