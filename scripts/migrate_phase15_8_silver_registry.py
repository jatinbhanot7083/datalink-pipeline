"""Phase 15.8 migration — Silver registry tables + backfill from Phase 15.6 state.

What this does (idempotent on re-run):

  1. create_control_tables() to instantiate the 6 new Phase 15.8 tables:
       global_silver_schema_datasets
       global_silver_schema_tables
       global_silver_schema_columns
       bronze_to_silver_mappings
       silver_schema_audit_log
       silver_to_gold_mappings

  2. Backfill: for every existing LIVE Gold schema row in
     global_gold_schema_datasets, synthesize a matching LIVE Silver schema
     row + tables + columns + Bronze→Silver mappings + Silver→Gold mappings.

     - When the linked silver_pattern_recommendation says HUB_SAT_LINK with a
       proposed_silver_shape, walk the JSON {hubs, sats, links} to build
       global_silver_schema_tables + columns.
     - When NORMALIZED, synthesize a single Silver table named
       <dataset_code>_silver with the catalog Bronze columns flowing 1:1
       into Silver, and Silver→Gold mappings cloned from the existing
       bronze_to_gold_mappings rows.

  3. Audit log entry for each backfilled row (action=BACKFILLED_FROM_15.6).

The legacy bronze_to_gold_mappings table is left intact for backward
compatibility. New work uses the chained Bronze→Silver→Gold path; the
Pipeline Architect transitions over in Phase 15.8.4.

Run from the host:

    set -a && source .env && set +a
    python3 scripts/migrate_phase15_8_silver_registry.py

Or from the control_tower container:

    docker exec datalink-control-tower python3 \\
        /opt/datalink/scripts/migrate_phase15_8_silver_registry.py
"""

from __future__ import annotations

import json
import os
import sys
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from datalink.adapters.factory import build_adapters  # noqa: E402
from datalink.adapters.protocols import Warehouse  # noqa: E402
from datalink.config.loader import load_settings  # noqa: E402
from datalink.quality.control import CONTROL_SCHEMA, create_control_tables  # noqa: E402

NEW_TABLES = [
    "global_silver_schema_datasets",
    "global_silver_schema_tables",
    "global_silver_schema_columns",
    "bronze_to_silver_mappings",
    "silver_schema_audit_log",
    "silver_to_gold_mappings",
]


def _table_exists(wh: Warehouse, name: str) -> bool:
    rows = list(
        wh.query(
            "SELECT COUNT(*) AS c FROM INFORMATION_SCHEMA.TABLES "
            "WHERE TABLE_SCHEMA = $s AND TABLE_NAME = $t",
            {"s": CONTROL_SCHEMA, "t": name.upper()},
        )
    )
    return bool(rows and int(rows[0]["c"]) > 0)


def _audit(
    wh: Warehouse,
    *,
    silver_dataset_id: str,
    dataset_code: str,
    action: str,
    actor: str,
    from_status: str | None,
    to_status: str | None,
    diff_summary: dict[str, Any] | None,
    notes: str = "",
) -> None:
    wh.execute(
        f"""
        INSERT INTO {CONTROL_SCHEMA}.silver_schema_audit_log
          (audit_id, silver_dataset_id, dataset_code, action, actor,
           from_status, to_status, diff_summary, ts, notes)
        VALUES ($a, $s, $d, $act, $actor, $fs, $ts2, $diff, $now, $notes)
        """,
        {
            "a": str(uuid.uuid4()),
            "s": silver_dataset_id,
            "d": dataset_code,
            "act": action,
            "actor": actor,
            "fs": from_status,
            "ts2": to_status,
            "diff": json.dumps(diff_summary, default=str) if diff_summary else None,
            "now": datetime.now(UTC),
            "notes": notes,
        },
    )


def _build_normalized_silver(
    wh: Warehouse,
    *,
    silver_dataset_id: str,
    dataset_code: str,
    bronze_fields: list[dict[str, Any]],
    gold_dataset_id: str,
    gold_columns: list[dict[str, Any]],
    bronze_to_gold_legacy: list[dict[str, Any]],
    actor: str,
) -> None:
    """Build a single NORMALIZED Silver table for a dataset that was previously
    'NORMALIZED + overkill_flag' in silver_pattern_recommendations.

    The Silver table is structurally Gold-shaped (since NORMALIZED Silver
    is essentially a 1:1 cast preserving Gold's column list).
    """
    silver_table_id = str(uuid.uuid4())
    table_name = f"{dataset_code}_silver"

    wh.execute(
        f"""
        INSERT INTO {CONTROL_SCHEMA}.global_silver_schema_tables
          (silver_table_id, silver_dataset_id, dataset_code, table_name,
           table_kind, parent_silver_table_id, business_keys_json,
           linked_hub_ids_json, table_order, description)
        VALUES ($id, $sid, $ds, $tn, 'NORMALIZED', NULL, $bks, NULL, 1,
                'Backfilled from Phase 15.6 NORMALIZED recommendation.')
        """,
        {
            "id": silver_table_id,
            "sid": silver_dataset_id,
            "ds": dataset_code,
            "tn": table_name,
            "bks": json.dumps(
                [c["gold_column_name"] for c in gold_columns if c.get("is_business_key")]
            ),
        },
    )

    # Silver columns = Gold columns (1:1 in NORMALIZED mode)
    bronze_to_gold_by_col = {m["gold_column_name"]: m for m in bronze_to_gold_legacy}
    for c in gold_columns:
        col_name = c["gold_column_name"]
        silver_column_id = str(uuid.uuid4())
        wh.execute(
            f"""
            INSERT INTO {CONTROL_SCHEMA}.global_silver_schema_columns
              (silver_column_id, silver_table_id, silver_dataset_id, column_order,
               column_name, logical_type, nullable, is_business_key, is_hash_key,
               is_hash_diff, is_pii, is_phi, description)
            VALUES ($id, $tid, $sid, $ord, $n, $t, $nul, $bk, FALSE, FALSE,
                    $pii, $phi, $desc)
            """,
            {
                "id": silver_column_id,
                "tid": silver_table_id,
                "sid": silver_dataset_id,
                "ord": c["column_order"],
                "n": col_name,
                "t": c["logical_type"],
                "nul": bool(c.get("nullable", True)),
                "bk": bool(c.get("is_business_key", False)),
                "pii": bool(c.get("is_pii", False)),
                "phi": bool(c.get("is_phi", False)),
                "desc": c.get("description") or "Backfilled from Gold (NORMALIZED Silver = 1:1)",
            },
        )

        # Bronze→Silver mapping (cloned from Bronze→Gold legacy)
        legacy = bronze_to_gold_by_col.get(col_name)
        if legacy is not None:
            srcs = legacy.get("bronze_source_columns")
            if isinstance(srcs, str):
                try:
                    srcs = json.loads(srcs)
                except json.JSONDecodeError:
                    srcs = []
            wh.execute(
                f"""
                INSERT INTO {CONTROL_SCHEMA}.bronze_to_silver_mappings
                  (mapping_id, silver_column_id, silver_dataset_id, silver_table_name,
                   silver_column_name, bronze_source_columns, transform_kind,
                   transform_sql, rationale, confidence, created_by)
                VALUES ($id, $cid, $sid, $tn, $cn, $bsc, $kind, $sql, $rat, $conf, $by)
                """,
                {
                    "id": str(uuid.uuid4()),
                    "cid": silver_column_id,
                    "sid": silver_dataset_id,
                    "tn": table_name,
                    "cn": col_name,
                    "bsc": json.dumps(srcs or []),
                    "kind": legacy.get("transform_kind") or "DIRECT",
                    "sql": legacy.get("transform_sql") or col_name,
                    "rat": "Backfilled from Phase 15.6 bronze_to_gold_mappings.",
                    "conf": float(legacy.get("confidence") or 0.9),
                    "by": actor,
                },
            )

        # Silver→Gold mapping (1:1, since NORMALIZED Silver mirrors Gold)
        wh.execute(
            f"""
            INSERT INTO {CONTROL_SCHEMA}.silver_to_gold_mappings
              (mapping_id, gold_field_id, gold_dataset_id, gold_column_name,
               silver_source_refs, transform_kind, transform_sql, rationale,
               confidence, created_by)
            VALUES ($id, $gfid, $gdid, $gn, $refs, 'DIRECT', $sql,
                    'Backfilled from Phase 15.6: NORMALIZED Silver mirrors Gold 1:1.',
                    1.0, $by)
            """,
            {
                "id": str(uuid.uuid4()),
                "gfid": c["gold_field_id"],
                "gdid": gold_dataset_id,
                "gn": col_name,
                "refs": json.dumps([{"silver_table": table_name, "silver_column": col_name}]),
                "sql": f"{table_name}.{col_name}",
                "by": actor,
            },
        )


def _build_dv2_silver(
    wh: Warehouse,
    *,
    silver_dataset_id: str,
    dataset_code: str,
    bronze_fields: list[dict[str, Any]],
    gold_dataset_id: str,
    gold_columns: list[dict[str, Any]],
    bronze_to_gold_legacy: list[dict[str, Any]],
    proposed_shape: dict[str, Any],
    actor: str,
) -> None:
    """Build HUB_SAT_LINK Silver from the proposed_silver_shape JSON
    (hubs / sats / links) captured in silver_pattern_recommendations."""

    # 1. Hubs
    hub_id_by_name: dict[str, str] = {}
    bronze_to_gold_by_col = {m["gold_column_name"]: m for m in bronze_to_gold_legacy}
    table_order = 0

    for hub in proposed_shape.get("hubs", []) or []:
        table_order += 1
        hub_name = str(hub.get("name") or "HUB_UNKNOWN")
        bks = list(hub.get("business_keys") or [])
        hub_id = str(uuid.uuid4())
        hub_id_by_name[hub_name] = hub_id
        wh.execute(
            f"""
            INSERT INTO {CONTROL_SCHEMA}.global_silver_schema_tables
              (silver_table_id, silver_dataset_id, dataset_code, table_name,
               table_kind, parent_silver_table_id, business_keys_json,
               linked_hub_ids_json, table_order, description)
            VALUES ($id, $sid, $ds, $tn, 'HUB', NULL, $bks, NULL, $ord,
                    'Backfilled Hub from Phase 15.6 proposed_silver_shape.')
            """,
            {
                "id": hub_id,
                "sid": silver_dataset_id,
                "ds": dataset_code,
                "tn": hub_name,
                "bks": json.dumps(bks),
                "ord": table_order,
            },
        )
        # Hub columns: hash_key + business keys + audit
        col_order = 0
        col_order += 1
        wh.execute(
            f"""
            INSERT INTO {CONTROL_SCHEMA}.global_silver_schema_columns
              (silver_column_id, silver_table_id, silver_dataset_id, column_order,
               column_name, logical_type, nullable, is_business_key, is_hash_key,
               is_hash_diff, description)
            VALUES ($id, $tid, $sid, $ord, 'hash_key', 'TEXT', FALSE, FALSE, TRUE,
                    FALSE, 'SHA-256 of business keys')
            """,
            {"id": str(uuid.uuid4()), "tid": hub_id, "sid": silver_dataset_id, "ord": col_order},
        )
        for bk in bks:
            col_order += 1
            wh.execute(
                f"""
                INSERT INTO {CONTROL_SCHEMA}.global_silver_schema_columns
                  (silver_column_id, silver_table_id, silver_dataset_id, column_order,
                   column_name, logical_type, nullable, is_business_key, is_hash_key,
                   is_hash_diff, description)
                VALUES ($id, $tid, $sid, $ord, $n, 'TEXT', FALSE, TRUE, FALSE,
                        FALSE, 'Business key')
                """,
                {
                    "id": str(uuid.uuid4()),
                    "tid": hub_id,
                    "sid": silver_dataset_id,
                    "ord": col_order,
                    "n": bk,
                },
            )

    # 2. Sats
    for sat in proposed_shape.get("sats", []) or []:
        table_order += 1
        sat_name = str(sat.get("name") or "SAT_UNKNOWN")
        hub_name = str(sat.get("hub") or "")
        parent_hub_id = hub_id_by_name.get(hub_name)
        sat_id = str(uuid.uuid4())
        wh.execute(
            f"""
            INSERT INTO {CONTROL_SCHEMA}.global_silver_schema_tables
              (silver_table_id, silver_dataset_id, dataset_code, table_name,
               table_kind, parent_silver_table_id, business_keys_json,
               linked_hub_ids_json, table_order, description)
            VALUES ($id, $sid, $ds, $tn, 'SAT', $phid, NULL, NULL, $ord,
                    'Backfilled Sat from Phase 15.6 proposed_silver_shape.')
            """,
            {
                "id": sat_id,
                "sid": silver_dataset_id,
                "ds": dataset_code,
                "tn": sat_name,
                "phid": parent_hub_id,
                "ord": table_order,
            },
        )
        # Sat columns: hash_key (FK to hub) + descriptive cols + hash_diff + audit
        col_order = 0
        col_order += 1
        wh.execute(
            f"""
            INSERT INTO {CONTROL_SCHEMA}.global_silver_schema_columns
              (silver_column_id, silver_table_id, silver_dataset_id, column_order,
               column_name, logical_type, nullable, is_business_key, is_hash_key,
               is_hash_diff, description)
            VALUES ($id, $tid, $sid, $ord, 'hash_key', 'TEXT', FALSE, FALSE, TRUE,
                    FALSE, 'FK to parent Hub')
            """,
            {"id": str(uuid.uuid4()), "tid": sat_id, "sid": silver_dataset_id, "ord": col_order},
        )
        sat_columns = list(sat.get("columns") or [])
        for col_name in sat_columns:
            col_order += 1
            # Look up the Gold col for type/PII/PHI hints
            gold_col = next((g for g in gold_columns if g["gold_column_name"] == col_name), None)
            silver_column_id = str(uuid.uuid4())
            wh.execute(
                f"""
                INSERT INTO {CONTROL_SCHEMA}.global_silver_schema_columns
                  (silver_column_id, silver_table_id, silver_dataset_id, column_order,
                   column_name, logical_type, nullable, is_business_key, is_hash_key,
                   is_hash_diff, is_pii, is_phi, description)
                VALUES ($id, $tid, $sid, $ord, $n, $t, $nul, FALSE, FALSE, FALSE,
                        $pii, $phi, $desc)
                """,
                {
                    "id": silver_column_id,
                    "tid": sat_id,
                    "sid": silver_dataset_id,
                    "ord": col_order,
                    "n": col_name,
                    "t": (gold_col or {}).get("logical_type") or "TEXT",
                    "nul": bool((gold_col or {}).get("nullable", True)),
                    "pii": bool((gold_col or {}).get("is_pii", False)),
                    "phi": bool((gold_col or {}).get("is_phi", False)),
                    "desc": (gold_col or {}).get("description") or f"Sat column {col_name}",
                },
            )
            # Bronze→Silver mapping (clone Bronze→Gold legacy)
            legacy = bronze_to_gold_by_col.get(col_name)
            if legacy is not None:
                srcs = legacy.get("bronze_source_columns")
                if isinstance(srcs, str):
                    try:
                        srcs = json.loads(srcs)
                    except json.JSONDecodeError:
                        srcs = []
                wh.execute(
                    f"""
                    INSERT INTO {CONTROL_SCHEMA}.bronze_to_silver_mappings
                      (mapping_id, silver_column_id, silver_dataset_id, silver_table_name,
                       silver_column_name, bronze_source_columns, transform_kind,
                       transform_sql, rationale, confidence, created_by)
                    VALUES ($id, $cid, $sid, $tn, $cn, $bsc, $kind, $sql, $rat, $conf, $by)
                    """,
                    {
                        "id": str(uuid.uuid4()),
                        "cid": silver_column_id,
                        "sid": silver_dataset_id,
                        "tn": sat_name,
                        "cn": col_name,
                        "bsc": json.dumps(srcs or []),
                        "kind": legacy.get("transform_kind") or "DIRECT",
                        "sql": legacy.get("transform_sql") or col_name,
                        "rat": "Backfilled from Phase 15.6 bronze_to_gold_mappings.",
                        "conf": float(legacy.get("confidence") or 0.9),
                        "by": actor,
                    },
                )
        # hash_diff
        col_order += 1
        wh.execute(
            f"""
            INSERT INTO {CONTROL_SCHEMA}.global_silver_schema_columns
              (silver_column_id, silver_table_id, silver_dataset_id, column_order,
               column_name, logical_type, nullable, is_business_key, is_hash_key,
               is_hash_diff, description)
            VALUES ($id, $tid, $sid, $ord, 'hash_diff', 'TEXT', FALSE, FALSE, FALSE,
                    TRUE, 'SHA-256 of payload — change-detection')
            """,
            {"id": str(uuid.uuid4()), "tid": sat_id, "sid": silver_dataset_id, "ord": col_order},
        )

    # 3. Links — proposed shape may declare them
    for link in proposed_shape.get("links", []) or []:
        table_order += 1
        link_name = str(link.get("name") or "LINK_UNKNOWN")
        hub_names = list(link.get("hubs") or [])
        link_hub_ids = [hub_id_by_name.get(h) for h in hub_names if hub_id_by_name.get(h)]
        link_id = str(uuid.uuid4())
        wh.execute(
            f"""
            INSERT INTO {CONTROL_SCHEMA}.global_silver_schema_tables
              (silver_table_id, silver_dataset_id, dataset_code, table_name,
               table_kind, parent_silver_table_id, business_keys_json,
               linked_hub_ids_json, table_order, description)
            VALUES ($id, $sid, $ds, $tn, 'LINK', NULL, NULL, $links, $ord,
                    'Backfilled Link from Phase 15.6 proposed_silver_shape.')
            """,
            {
                "id": link_id,
                "sid": silver_dataset_id,
                "ds": dataset_code,
                "tn": link_name,
                "links": json.dumps(link_hub_ids),
                "ord": table_order,
            },
        )

    # 4. Silver→Gold mappings: each Gold column reads from its Sat column
    #    (gold_col_name matches sat_col_name when DV2 backfill was generated by AI).
    sat_for_col: dict[str, str] = {}
    for sat in proposed_shape.get("sats", []) or []:
        for col_name in sat.get("columns") or []:
            sat_for_col[col_name] = str(sat.get("name") or "")

    # Build hub-name lookup for business keys
    bk_to_hub: dict[str, str] = {}
    for hub in proposed_shape.get("hubs", []) or []:
        h = str(hub.get("name") or "")
        for bk in hub.get("business_keys") or []:
            bk_to_hub[bk] = h

    for c in gold_columns:
        col_name = c["gold_column_name"]
        # Where does this Gold col come from in Silver?
        if col_name in bk_to_hub:
            src_table = bk_to_hub[col_name]
            transform_sql = f"{src_table}.{col_name}"
        elif col_name in sat_for_col:
            src_table = sat_for_col[col_name]
            transform_sql = f"{src_table}.{col_name}"
        else:
            # Audit / derived — fall back to Hub join
            src_table = next(iter(hub_id_by_name.keys()), "HUB_UNKNOWN")
            transform_sql = f"{src_table}.{col_name}"

        wh.execute(
            f"""
            INSERT INTO {CONTROL_SCHEMA}.silver_to_gold_mappings
              (mapping_id, gold_field_id, gold_dataset_id, gold_column_name,
               silver_source_refs, transform_kind, transform_sql, rationale,
               confidence, created_by)
            VALUES ($id, $gfid, $gdid, $gn, $refs, 'DIRECT', $sql,
                    'Backfilled Silver→Gold mapping from Phase 15.6 DV2 shape.',
                    0.9, $by)
            """,
            {
                "id": str(uuid.uuid4()),
                "gfid": c["gold_field_id"],
                "gdid": gold_dataset_id,
                "gn": col_name,
                "refs": json.dumps([{"silver_table": src_table, "silver_column": col_name}]),
                "sql": transform_sql,
                "by": actor,
            },
        )


def main() -> None:
    settings = load_settings(env=os.environ.get("DL_ENV", "local"))
    adapters = build_adapters(settings)
    wh = adapters.warehouse
    print(f"Warehouse: {type(wh).__name__}")
    print()
    print("=" * 72)
    print(" Step 1: Bootstrap 6 new Phase 15.8 tables")
    print("=" * 72)
    create_control_tables(wh)
    for t in NEW_TABLES:
        if _table_exists(wh, t):
            cols = list(
                wh.query(
                    "SELECT COUNT(*) AS c FROM INFORMATION_SCHEMA.COLUMNS "
                    "WHERE TABLE_SCHEMA = $s AND TABLE_NAME = $t",
                    {"s": CONTROL_SCHEMA, "t": t.upper()},
                )
            )
            print(f"  [OK]   {t:<40} cols={cols[0]['c']}")
        else:
            print(f"  [FAIL] {t:<40}")
            return

    print()
    print("=" * 72)
    print(" Step 2: Backfill LIVE Gold schemas → matching Silver schemas")
    print("=" * 72)

    # Find existing LIVE Gold schemas without a matching Silver row
    live_golds = list(
        wh.query(
            f"""
            SELECT g.gold_dataset_id, g.dataset_code, g.gold_table_name, g.gold_anchor
              FROM {CONTROL_SCHEMA}.global_gold_schema_datasets g
             WHERE g.status = 'LIVE'
               AND NOT EXISTS (
                  SELECT 1 FROM {CONTROL_SCHEMA}.global_silver_schema_datasets s
                   WHERE s.dataset_code = g.dataset_code AND s.status = 'LIVE'
               )
            """
        )
    )
    print(f"  Found {len(live_golds)} LIVE Gold schemas without matching Silver")

    actor = "phase15_8_migration"
    backfilled = 0
    for g in live_golds:
        dataset_code = g["dataset_code"]
        gold_dataset_id = g["gold_dataset_id"]
        gold_anchor = g["gold_anchor"]

        # Pull the linked Silver pattern recommendation (if any)
        rec_rows = list(
            wh.query(
                f"SELECT * FROM {CONTROL_SCHEMA}.silver_pattern_recommendations "
                f"WHERE dataset_code = $ds AND status = 'LIVE'",
                {"ds": dataset_code},
            )
        )
        if not rec_rows:
            print(f"  [skip] {dataset_code}: no LIVE silver_pattern_recommendation, deferring")
            continue
        rec = rec_rows[0]
        pattern = rec["recommended_pattern"]
        is_overkill = bool(rec.get("is_overkill_flag", False))
        proposed_shape_raw = rec.get("proposed_silver_shape")
        proposed_shape: dict[str, Any] = {}
        if proposed_shape_raw:
            try:
                proposed_shape = json.loads(proposed_shape_raw)
            except (TypeError, json.JSONDecodeError):
                proposed_shape = {}

        # Pull Bronze fields, Gold columns, legacy Bronze→Gold mappings
        bronze_fields = list(
            wh.query(
                f"SELECT * FROM {CONTROL_SCHEMA}.global_bronze_catalog_fields "
                f"WHERE dataset_code = $ds ORDER BY field_order",
                {"ds": dataset_code},
            )
        )
        gold_columns = list(
            wh.query(
                f"SELECT * FROM {CONTROL_SCHEMA}.global_gold_schema_fields "
                f"WHERE gold_dataset_id = $g ORDER BY column_order",
                {"g": gold_dataset_id},
            )
        )
        legacy_b2g = list(
            wh.query(
                f"SELECT * FROM {CONTROL_SCHEMA}.bronze_to_gold_mappings "
                f"WHERE gold_dataset_id = $g",
                {"g": gold_dataset_id},
            )
        )

        # Insert Silver header
        silver_dataset_id = str(uuid.uuid4())
        wh.execute(
            f"""
            INSERT INTO {CONTROL_SCHEMA}.global_silver_schema_datasets
              (silver_dataset_id, dataset_code, silver_pattern, version, status,
               silver_anchor, source, is_overkill_flag,
               ai_rationale, created_by, created_at, approved_by, approved_at, notes)
            VALUES ($id, $ds, $pat, 1, 'LIVE',
                    $anc, 'AI_CONSTRUCT', $ovk,
                    $rat, $by, $ts, $by, $ts,
                    'Backfilled from Phase 15.6 LIVE Gold + silver_pattern_recommendation.')
            """,
            {
                "id": silver_dataset_id,
                "ds": dataset_code,
                "pat": pattern,
                "anc": gold_anchor,
                "ovk": is_overkill,
                "rat": (rec.get("reasoning") or "")[:4000],
                "by": actor,
                "ts": datetime.now(UTC),
            },
        )

        # Build Silver tables + columns + mappings + Silver→Gold mappings
        if pattern == "NORMALIZED":
            _build_normalized_silver(
                wh,
                silver_dataset_id=silver_dataset_id,
                dataset_code=dataset_code,
                bronze_fields=bronze_fields,
                gold_dataset_id=gold_dataset_id,
                gold_columns=gold_columns,
                bronze_to_gold_legacy=legacy_b2g,
                actor=actor,
            )
        else:
            _build_dv2_silver(
                wh,
                silver_dataset_id=silver_dataset_id,
                dataset_code=dataset_code,
                bronze_fields=bronze_fields,
                gold_dataset_id=gold_dataset_id,
                gold_columns=gold_columns,
                bronze_to_gold_legacy=legacy_b2g,
                proposed_shape=proposed_shape,
                actor=actor,
            )

        # Audit
        _audit(
            wh,
            silver_dataset_id=silver_dataset_id,
            dataset_code=dataset_code,
            action="BACKFILLED_FROM_15_6",
            actor=actor,
            from_status=None,
            to_status="LIVE",
            diff_summary={
                "pattern": pattern,
                "gold_dataset_id": gold_dataset_id,
                "gold_columns": len(gold_columns),
                "bronze_fields": len(bronze_fields),
            },
            notes="Phase 15.8 migration backfill.",
        )

        backfilled += 1
        print(
            f"  [OK]   {dataset_code:<25} pattern={pattern:<14} silver_dataset_id={silver_dataset_id[:8]}…"
        )

    print()
    print(f"  Backfilled {backfilled} Silver schema(s) to LIVE.")

    print()
    print("=" * 72)
    print(" Step 3: Verify counts")
    print("=" * 72)
    counts = {
        "global_silver_schema_datasets": list(
            wh.query(
                f"SELECT status, COUNT(*) AS c FROM {CONTROL_SCHEMA}.global_silver_schema_datasets GROUP BY status"
            )
        ),
        "global_silver_schema_tables": list(
            wh.query(
                f"SELECT table_kind, COUNT(*) AS c FROM {CONTROL_SCHEMA}.global_silver_schema_tables GROUP BY table_kind"
            )
        ),
        "global_silver_schema_columns": list(
            wh.query(f"SELECT COUNT(*) AS c FROM {CONTROL_SCHEMA}.global_silver_schema_columns")
        ),
        "bronze_to_silver_mappings": list(
            wh.query(f"SELECT COUNT(*) AS c FROM {CONTROL_SCHEMA}.bronze_to_silver_mappings")
        ),
        "silver_to_gold_mappings": list(
            wh.query(f"SELECT COUNT(*) AS c FROM {CONTROL_SCHEMA}.silver_to_gold_mappings")
        ),
    }
    for k, v in counts.items():
        print(f"  {k}: {v}")

    print()
    print("Migration complete. ✅")


if __name__ == "__main__":
    main()
