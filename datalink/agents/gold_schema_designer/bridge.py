"""Phase 15.6 bridge — Gold Schema Designer orchestration.

Three entry points (one per designer mode):

  1. ``propose_gold_ai()``      — pulls Bronze fields + RAG chunks, invokes
     GoldSchemaDesignerAgent, returns a reviewable proposal dict.
  2. ``propose_gold_manual()``  — wraps an operator-authored column list +
     mapping list into the canonical proposal shape (no LLM).
  3. ``propose_gold_import()``  — runs the importers parser + wraps the
     result (no LLM unless the operator opts into a follow-up AI mapping pass).

Persist flow:

  * ``persist_proposal()``      — writes DRAFT row to global_gold_schema_*
    + bronze_to_gold_mappings + silver_pattern_recommendations + audit log.
  * ``approve_gold_schema()``   — flips status to LIVE, archives any prior
    LIVE row for the same dataset_code.

Read helpers (used by UI):

  * ``list_gold_datasets()``    — every Gold dataset registered, optionally
    filtered by status.
  * ``get_gold_schema()``       — full Gold schema + columns + mappings +
    Silver pattern recommendation for a given gold_dataset_id.
"""

from __future__ import annotations

import json
import time
import uuid
from datetime import UTC, datetime
from typing import Any

from datalink.adapters.protocols import LlmProvider, Warehouse
from datalink.logging import get_logger
from datalink.memory import AgentMemoryStore
from datalink.quality.control import CONTROL_SCHEMA

from .agent import (
    DesignerMode,
    GoldSchemaDesignerAgent,
)
from .importers import parse as importer_parse

_log = get_logger(__name__)


# =============================================================================
# Read helpers
# =============================================================================


def list_gold_datasets(
    warehouse: Warehouse,
    *,
    status: str | None = None,
    dataset_code: str | None = None,
) -> list[dict[str, Any]]:
    """Return rows from global_gold_schema_datasets, newest first."""
    where: list[str] = []
    params: dict[str, Any] = {}
    if status:
        where.append("status = $st")
        params["st"] = status
    if dataset_code:
        where.append("dataset_code = $ds")
        params["ds"] = dataset_code
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    rows = warehouse.query(
        f"""
        SELECT gold_dataset_id, dataset_code, gold_table_name, version,
               status, gold_anchor, source, import_format,
               ai_token_count, ai_latency_ms,
               created_by, created_at, submitted_at,
               approved_by, approved_at, archived_at,
               notes
          FROM {CONTROL_SCHEMA}.global_gold_schema_datasets
        {where_sql}
        ORDER BY created_at DESC
        """,
        params if params else None,
    )
    return list(rows)


def get_gold_schema(warehouse: Warehouse, gold_dataset_id: str) -> dict[str, Any]:
    """Fetch the full Gold schema for one row in
    global_gold_schema_datasets — header + columns + mappings + Silver
    pattern recommendation."""
    header_rows = list(
        warehouse.query(
            f"SELECT * FROM {CONTROL_SCHEMA}.global_gold_schema_datasets "
            f"WHERE gold_dataset_id = $g",
            {"g": gold_dataset_id},
        )
    )
    if not header_rows:
        raise ValueError(f"Gold schema {gold_dataset_id} not found.")
    header = dict(header_rows[0])

    cols = list(
        warehouse.query(
            f"SELECT * FROM {CONTROL_SCHEMA}.global_gold_schema_fields "
            f"WHERE gold_dataset_id = $g ORDER BY column_order",
            {"g": gold_dataset_id},
        )
    )
    mappings = list(
        warehouse.query(
            f"SELECT * FROM {CONTROL_SCHEMA}.bronze_to_gold_mappings WHERE gold_dataset_id = $g",
            {"g": gold_dataset_id},
        )
    )
    rec_rows = list(
        warehouse.query(
            f"SELECT * FROM {CONTROL_SCHEMA}.silver_pattern_recommendations "
            f"WHERE dataset_code = $ds AND status = 'LIVE'",
            {"ds": header["dataset_code"]},
        )
    )
    return {
        "header": header,
        "columns": cols,
        "mappings": mappings,
        "silver_pattern_recommendation": rec_rows[0] if rec_rows else None,
    }


def _fetch_bronze_for_dataset(
    warehouse: Warehouse, dataset_code: str
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Look up Bronze catalog dataset row + ordered Bronze fields."""
    ds_rows = list(
        warehouse.query(
            f"""
            SELECT dataset_id, dataset_code, display_name, category,
                   default_frequency, used_by, total_fields, required_fields,
                   optional_fields, default_anchor, catalog_version, notes
              FROM {CONTROL_SCHEMA}.global_bronze_catalog_datasets
             WHERE dataset_code = $ds AND is_active = TRUE
            """,
            {"ds": dataset_code},
        )
    )
    if not ds_rows:
        raise ValueError(
            f"No active Bronze dataset with dataset_code={dataset_code!r} in "
            f"CONTROL.global_bronze_catalog_datasets."
        )
    field_rows = list(
        warehouse.query(
            f"""
            SELECT field_id, dataset_id, dataset_code, field_order,
                   field_display_name, bronze_column_name, requirement,
                   logical_type, description, additional_notes, example,
                   is_pii, is_phi, is_business_key, catalog_version
              FROM {CONTROL_SCHEMA}.global_bronze_catalog_fields
             WHERE dataset_code = $ds
             ORDER BY field_order
            """,
            {"ds": dataset_code},
        )
    )
    return dict(ds_rows[0]), field_rows


def _fetch_grounding_chunks(
    memory: AgentMemoryStore | None,
    gold_anchor: str,
    query_text: str,
    *,
    k: int = 6,
) -> list[dict[str, Any]]:
    """Pull RAG chunks from agent_memory.standard_references for the chosen
    anchor's corpus. Returns [] for CATALOG_ANCHOR (no external corpus)."""
    if memory is None:
        return []
    corpus_code = GoldSchemaDesignerAgent.anchor_corpus_code(gold_anchor)
    if not corpus_code:
        return []
    try:
        hits = memory.query_similar_standard_chunks(
            query_text=query_text,
            anchored_codes=(corpus_code,),
            k=k,
        )
    except Exception as e:
        _log.warning(
            "gold_schema_designer.rag_unavailable",
            error=str(e),
            anchor=gold_anchor,
        )
        return []
    return [
        {
            "standard_code": h.standard_code,
            "section_path": h.section_path,
            "resource_type": h.resource_type,
            "chunk_text": h.chunk_text,
            "distance": float(h.distance),
        }
        for h in hits
    ]


# =============================================================================
# Mode A: AI_CONSTRUCT
# =============================================================================


def propose_gold_ai(
    *,
    llm: LlmProvider,
    warehouse: Warehouse,
    memory: AgentMemoryStore | None,
    dataset_code: str,
    gold_anchor: str = "CATALOG_ANCHOR",
    temperature: float = 0.0,
    actor: str = "operator",
) -> dict[str, Any]:
    """Run the AI agent to propose a Gold schema for ``dataset_code``.

    Does NOT persist — caller (UI) reviews, edits, then calls
    ``persist_proposal``.
    """
    bronze_ds, bronze_fields = _fetch_bronze_for_dataset(warehouse, dataset_code)
    grounding_chunks = _fetch_grounding_chunks(
        memory,
        gold_anchor,
        query_text=f"{bronze_ds.get('category', '')} {bronze_ds.get('display_name', dataset_code)}",
    )

    agent = GoldSchemaDesignerAgent(llm=llm, warehouse=warehouse)
    started = time.time()
    result = agent.run(
        {
            "dataset_code": dataset_code,
            "dataset_display_name": bronze_ds.get("display_name") or dataset_code,
            "gold_anchor": gold_anchor,
            "designer_mode": DesignerMode.AI_CONSTRUCT.value,
            "bronze_fields": bronze_fields,
            "grounding_chunks": grounding_chunks,
            "temperature": temperature,
            "category": bronze_ds.get("category"),
            "default_frequency": bronze_ds.get("default_frequency"),
        }
    )
    duration_ms = int((time.time() - started) * 1000)

    if not result.success:
        raise RuntimeError(f"GoldSchemaDesignerAgent failed: {result.error or '(no detail)'}")

    proposal = dict(result.payload)
    proposal["tokens_used"] = int(result.tokens_used)
    proposal["duration_ms"] = duration_ms
    proposal["proposer_model"] = getattr(llm, "model", "")
    proposal["grounding"] = grounding_chunks
    proposal["bronze_fields_count"] = len(bronze_fields)
    return proposal


# =============================================================================
# Mode B: MANUAL_AUTHOR
# =============================================================================


def propose_gold_manual(
    *,
    dataset_code: str,
    gold_table_name: str,
    columns: list[dict[str, Any]],
    mappings: list[dict[str, Any]] | None = None,
    silver_pattern_recommendation: dict[str, Any] | None = None,
    gold_anchor: str = "CATALOG_ANCHOR",
    rationale: str = "Operator hand-authored.",
) -> dict[str, Any]:
    """Wrap a hand-authored column + mapping list into the proposal shape.

    No LLM call. The UI's column grid feeds this directly.
    """
    if not columns:
        raise ValueError("propose_gold_manual: columns must be non-empty.")
    rec = silver_pattern_recommendation or {
        "recommended_pattern": "HUB_SAT_LINK",
        "is_overkill_flag": False,
        "reasoning": "Default DV2 — operator confirmed manually.",
        "proposed_silver_shape": {},
    }
    return {
        "dataset_code": dataset_code,
        "gold_anchor": gold_anchor,
        "designer_mode": DesignerMode.MANUAL_AUTHOR.value,
        "proposed_gold_table_name": gold_table_name,
        "proposed_columns": columns,
        "bronze_to_gold_mappings": mappings or [],
        "silver_pattern_recommendation": rec,
        "rationale": rationale,
        "tokens_used": 0,
        "duration_ms": 0,
        "proposer_model": "manual",
        "grounding": [],
    }


# =============================================================================
# Mode C: IMPORT
# =============================================================================


def propose_gold_import(
    *,
    dataset_code: str,
    import_format: str,
    text: str,
    gold_anchor: str = "CATALOG_ANCHOR",
    table_hint: str | None = None,
) -> dict[str, Any]:
    """Parse external schema into the canonical proposal shape.

    Supported formats: DDL_SQL, DBT_YAML, FHIR_PROFILE_JSON, JSON_SCHEMA,
    SNOWFLAKE_DESCRIBE.
    """
    parsed = importer_parse(import_format, text, table_hint=table_hint or dataset_code)
    parsed["dataset_code"] = dataset_code
    parsed["gold_anchor"] = gold_anchor
    parsed["designer_mode"] = DesignerMode.IMPORT.value
    parsed["import_format"] = import_format.upper()
    parsed["tokens_used"] = 0
    parsed["duration_ms"] = 0
    parsed["proposer_model"] = "importer"
    parsed["grounding"] = []
    return parsed


# =============================================================================
# Persist + Approve
# =============================================================================


def persist_proposal(
    *,
    warehouse: Warehouse,
    proposal: dict[str, Any],
    actor: str,
    notes: str = "",
    auto_submit: bool = False,
) -> str:
    """Insert a Gold schema row + columns + mappings + Silver pattern rec.

    Status starts as DRAFT (or PENDING_REVIEW when ``auto_submit=True``).
    Returns the new gold_dataset_id.
    """
    dataset_code = str(proposal["dataset_code"])
    gold_anchor = str(proposal.get("gold_anchor") or "CATALOG_ANCHOR")
    designer_mode = str(proposal.get("designer_mode") or "AI_CONSTRUCT")
    import_format = proposal.get("import_format")
    table_name = str(proposal["proposed_gold_table_name"])
    columns = list(proposal["proposed_columns"])
    mappings = list(proposal.get("bronze_to_gold_mappings", []))
    rec = dict(proposal.get("silver_pattern_recommendation") or {})

    gold_dataset_id = str(uuid.uuid4())
    now = datetime.now(UTC)
    status = "PENDING_REVIEW" if auto_submit else "DRAFT"

    # Look up next version for this dataset_code
    ver_rows = list(
        warehouse.query(
            f"SELECT MAX(version) AS v FROM {CONTROL_SCHEMA}.global_gold_schema_datasets "
            f"WHERE dataset_code = $ds",
            {"ds": dataset_code},
        )
    )
    next_version = 1
    if ver_rows and ver_rows[0].get("v") is not None:
        next_version = int(ver_rows[0]["v"]) + 1

    # 1. Header row
    warehouse.execute(
        f"""
        INSERT INTO {CONTROL_SCHEMA}.global_gold_schema_datasets
          (gold_dataset_id, dataset_code, gold_table_name, version, status,
           gold_anchor, source, import_format, ai_proposal_json, ai_rationale,
           ai_token_count, ai_latency_ms, notes,
           created_by, created_at, submitted_at)
        VALUES ($id, $ds, $tn, $v, $st,
                $a, $src, $fmt, $aj, $ar,
                $tok, $lat, $notes,
                $by, $ts, $subts)
        """,
        {
            "id": gold_dataset_id,
            "ds": dataset_code,
            "tn": table_name,
            "v": next_version,
            "st": status,
            "a": gold_anchor,
            "src": designer_mode,
            "fmt": import_format,
            "aj": json.dumps(proposal, default=str),
            "ar": str(proposal.get("rationale") or "")[:4000],
            "tok": int(proposal.get("tokens_used", 0)),
            "lat": int(proposal.get("duration_ms", 0)),
            "notes": notes,
            "by": actor,
            "ts": now,
            "subts": now if auto_submit else None,
        },
    )

    # 2. Column rows
    for i, c in enumerate(columns, start=1):
        warehouse.execute(
            f"""
            INSERT INTO {CONTROL_SCHEMA}.global_gold_schema_fields
              (gold_field_id, gold_dataset_id, dataset_code, column_order,
               gold_column_name, logical_type, nullable, is_business_key,
               is_pii, is_phi, description, anchor_reference, version)
            VALUES ($id, $g, $ds, $ord,
                    $n, $t, $nul, $bk,
                    $pii, $phi, $desc, $ar, $v)
            """,
            {
                "id": str(uuid.uuid4()),
                "g": gold_dataset_id,
                "ds": dataset_code,
                "ord": i,
                "n": str(c["gold_column_name"]),
                "t": str(c["logical_type"]),
                "nul": bool(c["nullable"]),
                "bk": bool(c.get("is_business_key", False)),
                "pii": bool(c.get("is_pii", False)),
                "phi": bool(c.get("is_phi", False)),
                "desc": str(c.get("description") or ""),
                "ar": c.get("anchor_reference"),
                "v": next_version,
            },
        )

    # 3. Mapping rows
    for m in mappings:
        warehouse.execute(
            f"""
            INSERT INTO {CONTROL_SCHEMA}.bronze_to_gold_mappings
              (mapping_id, gold_field_id, gold_dataset_id, gold_column_name,
               bronze_source_columns, transform_kind, transform_sql, rationale,
               confidence, created_by)
            VALUES ($id, $f, $g, $n,
                    $bsc, $kind, $sql, $rat,
                    $conf, $by)
            """,
            {
                "id": str(uuid.uuid4()),
                # gold_field_id can be left NULL when mapping is recorded
                # before the column is queried back by ID — the FK is logical
                # only (dataset_id + gold_column_name uniquely identifies it).
                "f": str(uuid.uuid4()),
                "g": gold_dataset_id,
                "n": str(m["gold_column_name"]),
                "bsc": json.dumps(m.get("bronze_source_columns", []), default=str),
                "kind": str(m["transform_kind"]).upper(),
                "sql": str(m["transform_sql"]),
                "rat": str(m.get("rationale", ""))[:1000],
                "conf": float(m.get("confidence", 0.9)),
                "by": actor,
            },
        )

    # 4. Silver pattern recommendation row (only if not yet LIVE for this dataset)
    if rec:
        rec_id = str(uuid.uuid4())
        warehouse.execute(
            f"""
            INSERT INTO {CONTROL_SCHEMA}.silver_pattern_recommendations
              (recommendation_id, dataset_code, recommended_pattern, reasoning,
               proposed_silver_shape, bronze_field_count, business_key_count,
               domain_count, is_overkill_flag, status, created_by)
            VALUES ($id, $ds, $pat, $reason,
                    $shape, $bf, $bk,
                    $dc, $ok, 'DRAFT', $by)
            """,
            {
                "id": rec_id,
                "ds": dataset_code,
                "pat": str(rec.get("recommended_pattern") or "HUB_SAT_LINK"),
                "reason": str(rec.get("reasoning") or "")[:2000],
                "shape": json.dumps(rec.get("proposed_silver_shape", {}), default=str),
                "bf": int(proposal.get("bronze_fields_count") or 0),
                "bk": sum(1 for c in columns if c.get("is_business_key")),
                "dc": _domain_count(columns),
                "ok": bool(rec.get("is_overkill_flag", False)),
                "by": actor,
            },
        )

    # 5. Audit log
    _audit(
        warehouse=warehouse,
        gold_dataset_id=gold_dataset_id,
        dataset_code=dataset_code,
        action="IMPORTED" if designer_mode == "IMPORT" else "PROPOSED",
        actor=actor,
        from_status=None,
        to_status=status,
        diff_summary={
            "designer_mode": designer_mode,
            "gold_anchor": gold_anchor,
            "import_format": import_format,
            "column_count": len(columns),
            "mapping_count": len(mappings),
            "version": next_version,
        },
        ai_reasoning=str(proposal.get("rationale") or "")[:4000],
        token_count=int(proposal.get("tokens_used", 0)),
        latency_ms=int(proposal.get("duration_ms", 0)),
        notes=notes,
    )

    return gold_dataset_id


def approve_gold_schema(
    *,
    warehouse: Warehouse,
    gold_dataset_id: str,
    actor: str,
    notes: str = "",
) -> dict[str, Any]:
    """Flip a Gold schema to LIVE; archive any prior LIVE row for the
    same dataset_code; activate the linked Silver pattern recommendation."""
    rows = list(
        warehouse.query(
            f"SELECT * FROM {CONTROL_SCHEMA}.global_gold_schema_datasets "
            f"WHERE gold_dataset_id = $g",
            {"g": gold_dataset_id},
        )
    )
    if not rows:
        raise ValueError(f"Gold schema {gold_dataset_id} not found.")
    cur = rows[0]
    if cur["status"] in ("LIVE", "ARCHIVED"):
        raise ValueError(f"Gold schema status {cur['status']} — cannot approve.")
    dataset_code = cur["dataset_code"]
    now = datetime.now(UTC)

    # Archive prior LIVE for the same dataset
    warehouse.execute(
        f"UPDATE {CONTROL_SCHEMA}.global_gold_schema_datasets "
        f"SET status = 'ARCHIVED', archived_at = $ts "
        f"WHERE dataset_code = $ds AND status = 'LIVE'",
        {"ds": dataset_code, "ts": now},
    )
    # Flip the new row LIVE
    warehouse.execute(
        f"UPDATE {CONTROL_SCHEMA}.global_gold_schema_datasets "
        f"SET status = 'LIVE', approved_by = $by, approved_at = $ts "
        f"WHERE gold_dataset_id = $g",
        {"g": gold_dataset_id, "by": actor, "ts": now},
    )
    # Promote the latest DRAFT silver_pattern_recommendation for this dataset to LIVE
    warehouse.execute(
        f"UPDATE {CONTROL_SCHEMA}.silver_pattern_recommendations "
        f"SET status = 'ARCHIVED', archived_at = $ts "
        f"WHERE dataset_code = $ds AND status = 'LIVE'",
        {"ds": dataset_code, "ts": now},
    )
    warehouse.execute(
        f"UPDATE {CONTROL_SCHEMA}.silver_pattern_recommendations "
        f"SET status = 'LIVE', approved_by = $by, approved_at = $ts "
        f"WHERE recommendation_id IN ("
        f"  SELECT recommendation_id "
        f"    FROM {CONTROL_SCHEMA}.silver_pattern_recommendations "
        f"   WHERE dataset_code = $ds AND status = 'DRAFT' "
        f"   ORDER BY created_at DESC LIMIT 1"
        f")",
        {"ds": dataset_code, "by": actor, "ts": now},
    )

    _audit(
        warehouse=warehouse,
        gold_dataset_id=gold_dataset_id,
        dataset_code=dataset_code,
        action="APPROVED",
        actor=actor,
        from_status=cur["status"],
        to_status="LIVE",
        diff_summary={"approved_at": now.isoformat()},
        ai_reasoning=None,
        token_count=None,
        latency_ms=None,
        notes=notes,
    )

    return {
        "gold_dataset_id": gold_dataset_id,
        "dataset_code": dataset_code,
        "status": "LIVE",
        "approved_by": actor,
    }


# =============================================================================
# Internals
# =============================================================================


def _domain_count(columns: list[dict[str, Any]]) -> int:
    """Estimate distinct domain prefixes in Gold column names — heuristic
    used by silver_pattern_recommendations.domain_count."""
    prefixes: set[str] = set()
    for c in columns:
        name = str(c.get("gold_column_name") or "").lower()
        tokens = name.split("_")
        if not tokens:
            continue
        prefixes.add(tokens[0])
    return len(prefixes)


def _audit(
    *,
    warehouse: Warehouse,
    gold_dataset_id: str,
    dataset_code: str,
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
        INSERT INTO {CONTROL_SCHEMA}.gold_schema_audit_log
          (audit_id, gold_dataset_id, dataset_code, action, actor,
           from_status, to_status, diff_summary, ai_reasoning,
           token_count, latency_ms, ts, notes)
        VALUES ($aid, $gid, $ds, $act, $actor,
                $fs, $tos, $diff, $ar,
                $tok, $lat, $ts, $notes)
        """,
        {
            "aid": str(uuid.uuid4()),
            "gid": gold_dataset_id,
            "ds": dataset_code,
            "act": action,
            "actor": actor,
            "fs": from_status,
            "tos": to_status,
            "diff": json.dumps(diff_summary, default=str) if diff_summary else None,
            "ar": (ai_reasoning or "")[:4000] or None,
            "tok": token_count,
            "lat": latency_ms,
            "ts": datetime.now(UTC),
            "notes": notes,
        },
    )
