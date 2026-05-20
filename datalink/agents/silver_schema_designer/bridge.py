"""Phase 15.8 bridge — Silver Schema Designer orchestration.

Three entry points (one per designer mode):

  1. ``propose_silver_ai()``     — pulls Bronze fields + RAG chunks, invokes
     SilverSchemaDesignerAgent, returns reviewable proposal dict.
  2. ``propose_silver_manual()`` — wraps a hand-authored shape into the
     proposal shape (no LLM).
  3. ``propose_silver_import()`` — runs the importers parser + wraps result.

Persist flow:

  * ``persist_silver_proposal()``   — DRAFT (or PENDING_REVIEW with auto_submit)
  * ``approve_silver_schema()``     — flips to LIVE; archives prior LIVE.
  * ``revert_silver_schema()``      — clones an ARCHIVED row into a new LIVE.

Read helpers:

  * ``list_silver_datasets()``      — every Silver dataset registered.
  * ``get_silver_schema()``         — full Silver schema (header + tables + cols + mappings)
  * ``fetch_live_silver()``         — find the LIVE Silver row for a dataset.
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

from .agent import SilverDesignerMode, SilverSchemaDesignerAgent
from .importers import parse as importer_parse

_log = get_logger(__name__)


# =============================================================================
# Read helpers
# =============================================================================


def list_silver_datasets(
    warehouse: Warehouse,
    *,
    status: str | None = None,
    dataset_code: str | None = None,
) -> list[dict[str, Any]]:
    """Return Silver schema header rows, newest first."""
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
        SELECT silver_dataset_id, dataset_code, silver_pattern, version,
               status, silver_anchor, source, import_format, is_overkill_flag,
               ai_token_count, ai_latency_ms,
               created_by, created_at, submitted_at,
               approved_by, approved_at, archived_at,
               notes
          FROM {CONTROL_SCHEMA}.global_silver_schema_datasets
        {where_sql}
        ORDER BY created_at DESC
        """,
        params if params else None,
    )
    return list(rows)


def fetch_live_silver(warehouse: Warehouse, dataset_code: str) -> dict[str, Any] | None:
    rows = list_silver_datasets(warehouse, status="LIVE", dataset_code=dataset_code)
    return rows[0] if rows else None


def get_silver_schema(warehouse: Warehouse, silver_dataset_id: str) -> dict[str, Any]:
    """Return header + tables + columns + mappings for a Silver schema."""
    headers = list(
        warehouse.query(
            f"SELECT * FROM {CONTROL_SCHEMA}.global_silver_schema_datasets "
            f"WHERE silver_dataset_id = $g",
            {"g": silver_dataset_id},
        )
    )
    if not headers:
        raise ValueError(f"Silver schema {silver_dataset_id} not found.")
    header = dict(headers[0])
    tables = list(
        warehouse.query(
            f"SELECT * FROM {CONTROL_SCHEMA}.global_silver_schema_tables "
            f"WHERE silver_dataset_id = $g ORDER BY table_order",
            {"g": silver_dataset_id},
        )
    )
    cols = list(
        warehouse.query(
            f"SELECT * FROM {CONTROL_SCHEMA}.global_silver_schema_columns "
            f"WHERE silver_dataset_id = $g ORDER BY silver_table_id, column_order",
            {"g": silver_dataset_id},
        )
    )
    mappings = list(
        warehouse.query(
            f"SELECT * FROM {CONTROL_SCHEMA}.bronze_to_silver_mappings "
            f"WHERE silver_dataset_id = $g",
            {"g": silver_dataset_id},
        )
    )
    return {
        "header": header,
        "tables": tables,
        "columns": cols,
        "mappings": mappings,
    }


def _fetch_bronze_for_dataset(
    warehouse: Warehouse, dataset_code: str
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    ds_rows = list(
        warehouse.query(
            f"""
            SELECT dataset_id, dataset_code, display_name, category,
                   default_frequency, total_fields, default_anchor
              FROM {CONTROL_SCHEMA}.global_bronze_catalog_datasets
             WHERE dataset_code = $ds AND is_active = TRUE
            """,
            {"ds": dataset_code},
        )
    )
    if not ds_rows:
        raise ValueError(f"No active Bronze dataset {dataset_code!r}.")
    fields = list(
        warehouse.query(
            f"""
            SELECT field_id, dataset_code, field_order, field_display_name,
                   bronze_column_name, requirement, logical_type, description,
                   is_pii, is_phi, is_business_key
              FROM {CONTROL_SCHEMA}.global_bronze_catalog_fields
             WHERE dataset_code = $ds
             ORDER BY field_order
            """,
            {"ds": dataset_code},
        )
    )
    return dict(ds_rows[0]), fields


def _fetch_grounding(
    memory: AgentMemoryStore | None,
    silver_anchor: str,
    query_text: str,
    *,
    k: int = 6,
) -> list[dict[str, Any]]:
    if memory is None:
        return []
    corpus_code = SilverSchemaDesignerAgent.anchor_corpus_code(silver_anchor)
    if not corpus_code:
        return []
    try:
        hits = memory.query_similar_standard_chunks(
            query_text=query_text, anchored_codes=(corpus_code,), k=k
        )
    except Exception as e:
        _log.warning("silver_designer.rag_unavailable", error=str(e))
        return []
    return [
        {
            "standard_code": h.standard_code,
            "section_path": h.section_path,
            "chunk_text": h.chunk_text,
            "distance": float(h.distance),
        }
        for h in hits
    ]


# =============================================================================
# Mode A: AI_CONSTRUCT
# =============================================================================


def propose_silver_ai(
    *,
    llm: LlmProvider,
    warehouse: Warehouse,
    memory: AgentMemoryStore | None,
    dataset_code: str,
    silver_anchor: str = "CATALOG_ANCHOR",
    silver_pattern: str = "HUB_SAT_LINK",
    temperature: float = 0.0,
    actor: str = "operator",
) -> dict[str, Any]:
    bronze_ds, bronze_fields = _fetch_bronze_for_dataset(warehouse, dataset_code)
    grounding = _fetch_grounding(
        memory,
        silver_anchor,
        query_text=f"{bronze_ds.get('category', '')} {bronze_ds.get('display_name', dataset_code)}",
    )

    agent = SilverSchemaDesignerAgent(llm=llm, warehouse=warehouse)
    started = time.time()
    result = agent.run(
        {
            "dataset_code": dataset_code,
            "dataset_display_name": bronze_ds.get("display_name") or dataset_code,
            "silver_anchor": silver_anchor,
            "silver_pattern": silver_pattern,
            "designer_mode": SilverDesignerMode.AI_CONSTRUCT.value,
            "bronze_fields": bronze_fields,
            "grounding_chunks": grounding,
            "temperature": temperature,
            "category": bronze_ds.get("category"),
        }
    )
    duration_ms = int((time.time() - started) * 1000)

    if not result.success:
        raise RuntimeError(f"SilverSchemaDesignerAgent failed: {result.error or '(no detail)'}")

    proposal = dict(result.payload)
    proposal["tokens_used"] = int(result.tokens_used)
    proposal["duration_ms"] = duration_ms
    proposal["proposer_model"] = getattr(llm, "model", "")
    proposal["grounding"] = grounding
    proposal["bronze_fields_count"] = len(bronze_fields)
    return proposal


# =============================================================================
# Mode B: MANUAL_AUTHOR
# =============================================================================


def propose_silver_manual(
    *,
    dataset_code: str,
    silver_pattern: str,
    silver_tables: list[dict[str, Any]],
    bronze_to_silver_mappings: list[dict[str, Any]] | None = None,
    silver_anchor: str = "CATALOG_ANCHOR",
    rationale: str = "Operator hand-authored Silver schema.",
) -> dict[str, Any]:
    if not silver_tables:
        raise ValueError("propose_silver_manual: silver_tables must be non-empty.")
    return {
        "dataset_code": dataset_code,
        "silver_anchor": silver_anchor,
        "silver_pattern": silver_pattern,
        "designer_mode": SilverDesignerMode.MANUAL_AUTHOR.value,
        "silver_tables": silver_tables,
        "bronze_to_silver_mappings": bronze_to_silver_mappings or [],
        "rationale": rationale,
        "tokens_used": 0,
        "duration_ms": 0,
        "proposer_model": "manual",
        "grounding": [],
    }


# =============================================================================
# Mode C: IMPORT
# =============================================================================


def propose_silver_import(
    *,
    dataset_code: str,
    import_format: str,
    text: str,
    silver_anchor: str = "CATALOG_ANCHOR",
) -> dict[str, Any]:
    parsed = importer_parse(import_format, text, dataset_code=dataset_code)
    parsed["dataset_code"] = dataset_code
    parsed["silver_anchor"] = silver_anchor
    parsed["designer_mode"] = SilverDesignerMode.IMPORT.value
    parsed["import_format"] = import_format.upper()
    parsed["tokens_used"] = 0
    parsed["duration_ms"] = 0
    parsed["proposer_model"] = "importer"
    parsed["grounding"] = []
    return parsed


# =============================================================================
# Persist + Approve + Revert
# =============================================================================


def persist_silver_proposal(
    *,
    warehouse: Warehouse,
    proposal: dict[str, Any],
    actor: str,
    notes: str = "",
    auto_submit: bool = False,
) -> str:
    """Insert a Silver schema row + tables + columns + mappings + audit."""
    dataset_code = str(proposal["dataset_code"])
    silver_pattern = str(proposal["silver_pattern"])
    silver_anchor = str(proposal.get("silver_anchor") or "CATALOG_ANCHOR")
    designer_mode = str(proposal.get("designer_mode") or "AI_CONSTRUCT")
    import_format = proposal.get("import_format")
    silver_tables_in = list(proposal["silver_tables"])
    mappings_in = list(proposal.get("bronze_to_silver_mappings", []))

    silver_dataset_id = str(uuid.uuid4())
    now = datetime.now(UTC)
    status = "PENDING_REVIEW" if auto_submit else "DRAFT"

    # Next version
    ver_rows = list(
        warehouse.query(
            f"SELECT MAX(version) AS v FROM {CONTROL_SCHEMA}.global_silver_schema_datasets "
            f"WHERE dataset_code = $ds",
            {"ds": dataset_code},
        )
    )
    next_version = 1
    if ver_rows and ver_rows[0].get("v") is not None:
        next_version = int(ver_rows[0]["v"]) + 1

    # Header
    warehouse.execute(
        f"""
        INSERT INTO {CONTROL_SCHEMA}.global_silver_schema_datasets
          (silver_dataset_id, dataset_code, silver_pattern, version, status,
           silver_anchor, source, import_format, is_overkill_flag,
           ai_proposal_json, ai_rationale, ai_token_count, ai_latency_ms,
           notes, created_by, created_at, submitted_at, scope_owner)
        VALUES ($id, $ds, $pat, $v, $st,
                $a, $src, $fmt, FALSE,
                $aj, $ar, $tok, $lat,
                $notes, $by, $ts, $subts, 'GLOBAL_CORP')
        """,
        {
            "id": silver_dataset_id,
            "ds": dataset_code,
            "pat": silver_pattern,
            "v": next_version,
            "st": status,
            "a": silver_anchor,
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

    # Tables + columns — BATCHED via cursor.executemany.  Previously this
    # was a row-by-row INSERT loop which took ~500 ms per Snowflake round-trip
    # → 60+ seconds for a typical 100-column Silver schema.  Same fix as
    # the Phase 22 catalog loader: drop into the raw connector and batch.
    table_id_by_name: dict[str, str] = {}
    table_rows_batch: list[dict[str, Any]] = []
    column_rows_batch: list[dict[str, Any]] = []
    for i, t in enumerate(silver_tables_in, start=1):
        silver_table_id = str(uuid.uuid4())
        table_name = str(t["table_name"])
        table_id_by_name[table_name] = silver_table_id
        table_rows_batch.append(
            {
                "id": silver_table_id,
                "sid": silver_dataset_id,
                "ds": dataset_code,
                "tn": table_name,
                "kind": str(t["table_kind"]),
                "bks": json.dumps(list(t.get("business_keys") or [])),
                "ord": i,
                "desc": str(t.get("description") or ""),
            }
        )
        for j, c in enumerate(t.get("columns") or [], start=1):
            column_rows_batch.append(
                {
                    "id": str(uuid.uuid4()),
                    "tid": silver_table_id,
                    "sid": silver_dataset_id,
                    "ord": j,
                    "n": str(c["column_name"]),
                    "t": str(c["logical_type"]),
                    "nul": bool(c.get("nullable", True)),
                    "bk": bool(c.get("is_business_key", False)),
                    "hk": bool(c.get("is_hash_key", False)),
                    "hd": bool(c.get("is_hash_diff", False)),
                    "pii": bool(c.get("is_pii", False)),
                    "phi": bool(c.get("is_phi", False)),
                    "desc": str(c.get("description") or ""),
                }
            )

    # Single batched INSERT for ALL tables, single batched INSERT for ALL columns
    if table_rows_batch:
        conn = warehouse._connect()
        cur = conn.cursor()
        try:
            cur.executemany(
                f"INSERT INTO {CONTROL_SCHEMA}.global_silver_schema_tables "
                f"(silver_table_id, silver_dataset_id, dataset_code, table_name, "
                f" table_kind, parent_silver_table_id, business_keys_json, "
                f" linked_hub_ids_json, table_order, description) "
                f"VALUES (%(id)s, %(sid)s, %(ds)s, %(tn)s, %(kind)s, NULL, "
                f"        %(bks)s, NULL, %(ord)s, %(desc)s)",
                table_rows_batch,
            )
            if column_rows_batch:
                cur.executemany(
                    f"INSERT INTO {CONTROL_SCHEMA}.global_silver_schema_columns "
                    f"(silver_column_id, silver_table_id, silver_dataset_id, column_order, "
                    f" column_name, logical_type, nullable, is_business_key, is_hash_key, "
                    f" is_hash_diff, is_pii, is_phi, description) "
                    f"VALUES (%(id)s, %(tid)s, %(sid)s, %(ord)s, %(n)s, %(t)s, "
                    f"        %(nul)s, %(bk)s, %(hk)s, %(hd)s, %(pii)s, %(phi)s, %(desc)s)",
                    column_rows_batch,
                )
        finally:
            cur.close()
    # Now resolve parent_silver_table_id + linked_hub_ids_json
    for t in silver_tables_in:
        parent = t.get("parent_hub_name")
        linked = t.get("linked_hub_names") or []
        if parent:
            parent_id = table_id_by_name.get(str(parent))
            if parent_id:
                warehouse.execute(
                    f"UPDATE {CONTROL_SCHEMA}.global_silver_schema_tables "
                    f"SET parent_silver_table_id = $pid "
                    f"WHERE silver_table_id = $tid",
                    {"pid": parent_id, "tid": table_id_by_name[str(t["table_name"])]},
                )
        if linked:
            link_ids = [table_id_by_name.get(str(h)) for h in linked]
            link_ids = [x for x in link_ids if x]
            warehouse.execute(
                f"UPDATE {CONTROL_SCHEMA}.global_silver_schema_tables "
                f"SET linked_hub_ids_json = $lj "
                f"WHERE silver_table_id = $tid",
                {
                    "lj": json.dumps(link_ids),
                    "tid": table_id_by_name[str(t["table_name"])],
                },
            )

    # Mappings — resolve silver_column_id by (table_name, column_name)
    col_id_lookup: dict[tuple[str, str], str] = {}
    rows = list(
        warehouse.query(
            f"""
            SELECT c.silver_column_id, t.table_name, c.column_name
              FROM {CONTROL_SCHEMA}.global_silver_schema_columns c
              JOIN {CONTROL_SCHEMA}.global_silver_schema_tables t
                ON c.silver_table_id = t.silver_table_id
             WHERE c.silver_dataset_id = $g
            """,
            {"g": silver_dataset_id},
        )
    )
    for r in rows:
        col_id_lookup[(r["table_name"], r["column_name"])] = r["silver_column_id"]

    # Mappings — BATCHED via cursor.executemany (was row-by-row INSERT)
    mapping_rows_batch: list[dict[str, Any]] = []
    for m in mappings_in:
        tn = str(m["silver_table_name"])
        cn = str(m["silver_column_name"])
        col_id = col_id_lookup.get((tn, cn))
        if not col_id:
            continue
        srcs = m.get("bronze_source_columns")
        if not isinstance(srcs, list):
            srcs = []
        mapping_rows_batch.append(
            {
                "id": str(uuid.uuid4()),
                "cid": col_id,
                "sid": silver_dataset_id,
                "tn": tn,
                "cn": cn,
                "bsc": json.dumps(srcs),
                "kind": str(m.get("transform_kind") or "DIRECT").upper(),
                "sql": str(m.get("transform_sql") or cn),
                "rat": str(m.get("rationale") or "")[:1000],
                "conf": float(m.get("confidence") or 0.9),
                "by": actor,
            }
        )
    if mapping_rows_batch:
        conn = warehouse._connect()
        cur = conn.cursor()
        try:
            cur.executemany(
                f"INSERT INTO {CONTROL_SCHEMA}.bronze_to_silver_mappings "
                f"(mapping_id, silver_column_id, silver_dataset_id, silver_table_name, "
                f" silver_column_name, bronze_source_columns, transform_kind, "
                f" transform_sql, rationale, confidence, created_by) "
                f"VALUES (%(id)s, %(cid)s, %(sid)s, %(tn)s, %(cn)s, %(bsc)s, "
                f"        %(kind)s, %(sql)s, %(rat)s, %(conf)s, %(by)s)",
                mapping_rows_batch,
            )
        finally:
            cur.close()

    # Audit
    _audit(
        warehouse=warehouse,
        silver_dataset_id=silver_dataset_id,
        dataset_code=dataset_code,
        action="IMPORTED" if designer_mode == "IMPORT" else "PROPOSED",
        actor=actor,
        from_status=None,
        to_status=status,
        diff_summary={
            "designer_mode": designer_mode,
            "silver_pattern": silver_pattern,
            "silver_anchor": silver_anchor,
            "import_format": import_format,
            "table_count": len(silver_tables_in),
            "mapping_count": len(mappings_in),
            "version": next_version,
        },
        notes=notes,
    )

    return silver_dataset_id


def approve_silver_schema(
    *,
    warehouse: Warehouse,
    silver_dataset_id: str,
    actor: str,
    notes: str = "",
) -> dict[str, Any]:
    rows = list(
        warehouse.query(
            f"SELECT * FROM {CONTROL_SCHEMA}.global_silver_schema_datasets "
            f"WHERE silver_dataset_id = $g",
            {"g": silver_dataset_id},
        )
    )
    if not rows:
        raise ValueError(f"Silver schema {silver_dataset_id} not found.")
    cur = rows[0]
    if cur["status"] in ("LIVE", "ARCHIVED"):
        raise ValueError(f"Silver schema status {cur['status']} — cannot approve.")
    dataset_code = cur["dataset_code"]
    now = datetime.now(UTC)

    warehouse.execute(
        f"UPDATE {CONTROL_SCHEMA}.global_silver_schema_datasets "
        f"SET status = 'ARCHIVED', archived_at = $ts "
        f"WHERE dataset_code = $ds AND status = 'LIVE'",
        {"ds": dataset_code, "ts": now},
    )
    warehouse.execute(
        f"UPDATE {CONTROL_SCHEMA}.global_silver_schema_datasets "
        f"SET status = 'LIVE', approved_by = $by, approved_at = $ts "
        f"WHERE silver_dataset_id = $g",
        {"g": silver_dataset_id, "by": actor, "ts": now},
    )

    _audit(
        warehouse=warehouse,
        silver_dataset_id=silver_dataset_id,
        dataset_code=dataset_code,
        action="APPROVED",
        actor=actor,
        from_status=cur["status"],
        to_status="LIVE",
        diff_summary={"approved_at": now.isoformat()},
        notes=notes,
    )
    return {
        "silver_dataset_id": silver_dataset_id,
        "dataset_code": dataset_code,
        "status": "LIVE",
        "approved_by": actor,
    }


def revert_silver_schema(
    *,
    warehouse: Warehouse,
    archived_silver_dataset_id: str,
    actor: str,
    notes: str = "",
) -> str:
    """Promote an ARCHIVED Silver schema back to LIVE by cloning it as a new
    version. Original archived row stays untouched (immutable history)."""
    rows = list(
        warehouse.query(
            f"SELECT * FROM {CONTROL_SCHEMA}.global_silver_schema_datasets "
            f"WHERE silver_dataset_id = $g",
            {"g": archived_silver_dataset_id},
        )
    )
    if not rows:
        raise ValueError(f"Silver schema {archived_silver_dataset_id} not found.")
    src = dict(rows[0])
    if src["status"] != "ARCHIVED":
        raise ValueError(f"revert requires an ARCHIVED source; got status={src['status']}")

    dataset_code = src["dataset_code"]
    # Archive current LIVE if any
    now = datetime.now(UTC)
    warehouse.execute(
        f"UPDATE {CONTROL_SCHEMA}.global_silver_schema_datasets "
        f"SET status = 'ARCHIVED', archived_at = $ts "
        f"WHERE dataset_code = $ds AND status = 'LIVE'",
        {"ds": dataset_code, "ts": now},
    )

    # Next version
    ver_rows = list(
        warehouse.query(
            f"SELECT MAX(version) AS v FROM {CONTROL_SCHEMA}.global_silver_schema_datasets "
            f"WHERE dataset_code = $ds",
            {"ds": dataset_code},
        )
    )
    next_version = (int(ver_rows[0]["v"]) if ver_rows and ver_rows[0]["v"] else 0) + 1

    new_silver_dataset_id = str(uuid.uuid4())
    warehouse.execute(
        f"""
        INSERT INTO {CONTROL_SCHEMA}.global_silver_schema_datasets
          (silver_dataset_id, dataset_code, silver_pattern, version, status,
           silver_anchor, source, import_format, is_overkill_flag,
           ai_proposal_json, ai_rationale, notes,
           created_by, created_at, approved_by, approved_at, scope_owner)
        VALUES ($id, $ds, $pat, $v, 'LIVE',
                $a, 'REVERT', $fmt, $ovk,
                $aj, $ar, $notes,
                $by, $ts, $by, $ts, 'GLOBAL_CORP')
        """,
        {
            "id": new_silver_dataset_id,
            "ds": dataset_code,
            "pat": src["silver_pattern"],
            "v": next_version,
            "a": src["silver_anchor"],
            "fmt": src.get("import_format"),
            "ovk": bool(src.get("is_overkill_flag", False)),
            "aj": src.get("ai_proposal_json"),
            "ar": f"REVERT from version {src['version']} (silver_dataset_id={archived_silver_dataset_id}). {notes}"[
                :4000
            ],
            "notes": f"[reverted] from {archived_silver_dataset_id}; original notes: {src.get('notes') or ''}",
            "by": actor,
            "ts": now,
        },
    )

    # Clone tables + columns + mappings
    src_tables = list(
        warehouse.query(
            f"SELECT * FROM {CONTROL_SCHEMA}.global_silver_schema_tables "
            f"WHERE silver_dataset_id = $g ORDER BY table_order",
            {"g": archived_silver_dataset_id},
        )
    )
    table_id_remap: dict[str, str] = {}
    for t in src_tables:
        new_tid = str(uuid.uuid4())
        table_id_remap[t["silver_table_id"]] = new_tid
        warehouse.execute(
            f"""
            INSERT INTO {CONTROL_SCHEMA}.global_silver_schema_tables
              (silver_table_id, silver_dataset_id, dataset_code, table_name,
               table_kind, parent_silver_table_id, business_keys_json,
               linked_hub_ids_json, table_order, description)
            VALUES ($id, $sid, $ds, $tn, $kind, NULL, $bks, $links, $ord, $desc)
            """,
            {
                "id": new_tid,
                "sid": new_silver_dataset_id,
                "ds": dataset_code,
                "tn": t["table_name"],
                "kind": t["table_kind"],
                "bks": t.get("business_keys_json"),
                "links": t.get("linked_hub_ids_json"),
                "ord": t["table_order"],
                "desc": t.get("description") or "",
            },
        )
    # Re-link parents
    for t in src_tables:
        if t.get("parent_silver_table_id"):
            new_parent = table_id_remap.get(t["parent_silver_table_id"])
            if new_parent:
                warehouse.execute(
                    f"UPDATE {CONTROL_SCHEMA}.global_silver_schema_tables "
                    f"SET parent_silver_table_id = $pid WHERE silver_table_id = $tid",
                    {"pid": new_parent, "tid": table_id_remap[t["silver_table_id"]]},
                )

    src_cols = list(
        warehouse.query(
            f"SELECT * FROM {CONTROL_SCHEMA}.global_silver_schema_columns "
            f"WHERE silver_dataset_id = $g",
            {"g": archived_silver_dataset_id},
        )
    )
    col_id_remap: dict[str, str] = {}
    for c in src_cols:
        new_cid = str(uuid.uuid4())
        col_id_remap[c["silver_column_id"]] = new_cid
        warehouse.execute(
            f"""
            INSERT INTO {CONTROL_SCHEMA}.global_silver_schema_columns
              (silver_column_id, silver_table_id, silver_dataset_id, column_order,
               column_name, logical_type, nullable, is_business_key, is_hash_key,
               is_hash_diff, is_pii, is_phi, description)
            VALUES ($id, $tid, $sid, $ord, $n, $t, $nul, $bk, $hk, $hd,
                    $pii, $phi, $desc)
            """,
            {
                "id": new_cid,
                "tid": table_id_remap[c["silver_table_id"]],
                "sid": new_silver_dataset_id,
                "ord": c["column_order"],
                "n": c["column_name"],
                "t": c["logical_type"],
                "nul": c.get("nullable", True),
                "bk": c.get("is_business_key", False),
                "hk": c.get("is_hash_key", False),
                "hd": c.get("is_hash_diff", False),
                "pii": c.get("is_pii", False),
                "phi": c.get("is_phi", False),
                "desc": c.get("description") or "",
            },
        )

    src_maps = list(
        warehouse.query(
            f"SELECT * FROM {CONTROL_SCHEMA}.bronze_to_silver_mappings "
            f"WHERE silver_dataset_id = $g",
            {"g": archived_silver_dataset_id},
        )
    )
    for m in src_maps:
        warehouse.execute(
            f"""
            INSERT INTO {CONTROL_SCHEMA}.bronze_to_silver_mappings
              (mapping_id, silver_column_id, silver_dataset_id, silver_table_name,
               silver_column_name, bronze_source_columns, transform_kind,
               transform_sql, rationale, confidence, created_by)
            VALUES ($id, $cid, $sid, $tn, $cn, $bsc, $kind, $sql, $rat, $conf, $by)
            """,
            {
                "id": str(uuid.uuid4()),
                "cid": col_id_remap.get(m["silver_column_id"], m["silver_column_id"]),
                "sid": new_silver_dataset_id,
                "tn": m["silver_table_name"],
                "cn": m["silver_column_name"],
                "bsc": m.get("bronze_source_columns") or "[]",
                "kind": m.get("transform_kind") or "DIRECT",
                "sql": m.get("transform_sql") or m["silver_column_name"],
                "rat": (m.get("rationale") or "")[:1000],
                "conf": float(m.get("confidence") or 0.9),
                "by": actor,
            },
        )

    _audit(
        warehouse=warehouse,
        silver_dataset_id=new_silver_dataset_id,
        dataset_code=dataset_code,
        action="REVERTED",
        actor=actor,
        from_status="ARCHIVED",
        to_status="LIVE",
        diff_summary={
            "reverted_from_silver_dataset_id": archived_silver_dataset_id,
            "reverted_from_version": src["version"],
            "new_version": next_version,
        },
        notes=notes,
    )
    return new_silver_dataset_id


# =============================================================================
# Internals
# =============================================================================


def _audit(
    *,
    warehouse: Warehouse,
    silver_dataset_id: str,
    dataset_code: str,
    action: str,
    actor: str,
    from_status: str | None,
    to_status: str | None,
    diff_summary: dict[str, Any] | None,
    notes: str = "",
) -> None:
    warehouse.execute(
        f"""
        INSERT INTO {CONTROL_SCHEMA}.silver_schema_audit_log
          (audit_id, silver_dataset_id, dataset_code, action, actor,
           from_status, to_status, diff_summary, ts, notes)
        VALUES ($a, $s, $d, $act, $actor, $fs, $tos, $diff, $now, $notes)
        """,
        {
            "a": str(uuid.uuid4()),
            "s": silver_dataset_id,
            "d": dataset_code,
            "act": action,
            "actor": actor,
            "fs": from_status,
            "tos": to_status,
            "diff": json.dumps(diff_summary, default=str) if diff_summary else None,
            "now": datetime.now(UTC),
            "notes": notes,
        },
    )
