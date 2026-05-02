"""Phase 15.5 migration — rename Bronze-misnamed-as-Gold tables + bootstrap
the new Gold schema registry.

Idempotent on re-run:
  - If the OLD table exists and the NEW one doesn't: ALTER TABLE RENAME.
  - If the NEW table already exists: skip the rename, log no-op.
  - If neither exists: create_control_tables creates them at the new name.

Renames (Phase 15.5 truth-correction):
  CONTROL.global_gold_catalog_datasets -> CONTROL.global_bronze_catalog_datasets
  CONTROL.global_gold_catalog_fields   -> CONTROL.global_bronze_catalog_fields

Also renames the column
  global_bronze_catalog_fields.gold_column_name -> bronze_column_name
since the field was misnamed at the same time.

Then creates the 5 new Phase 15.5 tables via create_control_tables():
  CONTROL.global_gold_schema_datasets
  CONTROL.global_gold_schema_fields
  CONTROL.bronze_to_gold_mappings
  CONTROL.gold_schema_audit_log
  CONTROL.silver_pattern_recommendations

Run from the host:

    set -a && source .env && set +a
    python3 scripts/migrate_phase15_5_gold_schema.py

Run from inside the control_tower container:

    docker exec datalink-control-tower python3 \\
        /opt/datalink/scripts/migrate_phase15_5_gold_schema.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

# bootstrap path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from datalink.adapters.factory import build_adapters  # noqa: E402
from datalink.adapters.protocols import Warehouse  # noqa: E402
from datalink.config.loader import load_settings  # noqa: E402
from datalink.quality.control import CONTROL_SCHEMA, create_control_tables  # noqa: E402

# (old, new) — the two table renames.
RENAMES: list[tuple[str, str]] = [
    ("global_gold_catalog_datasets", "global_bronze_catalog_datasets"),
    ("global_gold_catalog_fields", "global_bronze_catalog_fields"),
]

# (table, old_column, new_column) — column rename inside the renamed table.
COLUMN_RENAMES: list[tuple[str, str, str]] = [
    ("global_bronze_catalog_fields", "gold_column_name", "bronze_column_name"),
]

# New Phase 15.5 tables — verify they exist after create_control_tables().
NEW_TABLES = [
    "global_gold_schema_datasets",
    "global_gold_schema_fields",
    "bronze_to_gold_mappings",
    "gold_schema_audit_log",
    "silver_pattern_recommendations",
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


def _column_exists(wh: Warehouse, table: str, column: str) -> bool:
    rows = list(
        wh.query(
            "SELECT COUNT(*) AS c FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_SCHEMA = $s AND TABLE_NAME = $t AND COLUMN_NAME = $c",
            {"s": CONTROL_SCHEMA, "t": table.upper(), "c": column.upper()},
        )
    )
    return bool(rows and int(rows[0]["c"]) > 0)


def _add_default_anchor_column(wh: Warehouse) -> None:
    """Phase 15.5: add default_anchor column to global_bronze_catalog_datasets
    (didn't exist when the table was first created)."""
    table = "global_bronze_catalog_datasets"
    if not _table_exists(wh, table):
        return
    if _column_exists(wh, table, "default_anchor"):
        print(f"  [skip] {table}.default_anchor already exists")
        return
    sql = (
        f"ALTER TABLE {CONTROL_SCHEMA}.{table} "
        f"ADD COLUMN default_anchor VARCHAR DEFAULT 'CATALOG_ANCHOR'"
    )
    wh.execute(sql)
    print(f"  [OK]   added column {table}.default_anchor")


def main() -> None:
    settings = load_settings(env=os.environ.get("DL_ENV", "local"))
    adapters = build_adapters(settings)
    wh = adapters.warehouse
    print(f"Warehouse: {type(wh).__name__}")
    print()

    print("=" * 72)
    print(" Step 1: Rename Bronze-misnamed-as-Gold tables")
    print("=" * 72)
    for old, new in RENAMES:
        old_exists = _table_exists(wh, old)
        new_exists = _table_exists(wh, new)
        if new_exists and not old_exists:
            print(f"  [skip] {old} -> {new} (new already exists, old gone)")
            continue
        if new_exists and old_exists:
            print(
                f"  [WARN] both {old} and {new} exist — leaving old alone "
                f"so we don't lose data; you must reconcile manually"
            )
            continue
        if not old_exists and not new_exists:
            print(f"  [skip] neither {old} nor {new} exist; create_control_tables will create new")
            continue
        # old exists, new doesn't → rename
        wh.execute(f"ALTER TABLE {CONTROL_SCHEMA}.{old} RENAME TO {CONTROL_SCHEMA}.{new}")
        print(f"  [OK]   renamed {old} -> {new}")

    print()
    print("=" * 72)
    print(" Step 2: Rename misnamed columns inside renamed tables")
    print("=" * 72)
    for table, old_col, new_col in COLUMN_RENAMES:
        if not _table_exists(wh, table):
            print(f"  [skip] {table} not present yet — column rename deferred")
            continue
        if _column_exists(wh, table, new_col) and not _column_exists(wh, table, old_col):
            print(f"  [skip] {table}.{old_col} already renamed to {new_col}")
            continue
        if not _column_exists(wh, table, old_col):
            print(f"  [skip] {table}.{old_col} not present (table likely fresh)")
            continue
        # old col exists, new col doesn't → rename
        wh.execute(f"ALTER TABLE {CONTROL_SCHEMA}.{table} RENAME COLUMN {old_col} TO {new_col}")
        print(f"  [OK]   renamed {table}.{old_col} -> {new_col}")

    print()
    print("=" * 72)
    print(" Step 3: Add new columns to existing tables (Phase 15.5)")
    print("=" * 72)
    _add_default_anchor_column(wh)

    print()
    print("=" * 72)
    print(" Step 4: Bootstrap 5 new Phase 15.5 tables")
    print("=" * 72)
    create_control_tables(wh)
    for table in NEW_TABLES:
        if _table_exists(wh, table):
            col_rows = list(
                wh.query(
                    "SELECT COUNT(*) AS c FROM INFORMATION_SCHEMA.COLUMNS "
                    "WHERE TABLE_SCHEMA = $s AND TABLE_NAME = $t",
                    {"s": CONTROL_SCHEMA, "t": table.upper()},
                )
            )
            cnt = int(col_rows[0]["c"]) if col_rows else 0
            print(f"  [OK]   {table:<40} cols={cnt}")
        else:
            print(f"  [FAIL] {table:<40} did NOT get created")

    print()
    print("=" * 72)
    print(" Step 5: Verify Bronze data preserved across rename")
    print("=" * 72)
    for new in (n for _, n in RENAMES):
        if _table_exists(wh, new):
            rows = list(wh.query(f"SELECT COUNT(*) AS c FROM {CONTROL_SCHEMA}.{new}"))
            cnt = int(rows[0]["c"]) if rows else 0
            print(f"  {new:<40} rows={cnt}")
    print()
    print("Migration complete. ✅")


if __name__ == "__main__":
    main()
