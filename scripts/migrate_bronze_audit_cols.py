"""Phase 9.1 migration: add 3 new audit cols to every existing Bronze table.

Idempotent — uses ``ADD COLUMN IF NOT EXISTS`` (DuckDB) or guards each
ALTER with a column-presence check (Snowflake doesn't have IF NOT EXISTS
on ADD COLUMN as of 2026-04). Safe to re-run.

What it adds (all NULL for legacy rows):

    _load_type         VARCHAR
    _file_row_number   BIGINT
    _record_hash       VARCHAR

Discovery: walks ``information_schema.tables`` for any schema starting
with ``BRONZE`` (covers ``BRONZE`` plus per-tenant ``BRONZE_AETNA`` etc.)
and any table beginning with ``RAW_``.

Usage:
    python -m scripts.migrate_bronze_audit_cols
    python -m scripts.migrate_bronze_audit_cols --dry-run

Run from the host (with `set -a; source .env; set +a` first), OR from
inside the airflow container (where POSTGRES/Snowflake DNS already
resolves).
"""

from __future__ import annotations

import argparse
import sys
from typing import Any

from datalink.adapters.factory import build_adapters
from datalink.config.loader import load_settings
from datalink.logging import get_logger

_log = get_logger(__name__)

# Each tuple: (col_name, col_type) — order matches the Bronze DDL audit
# trailing-cols section. The migration appends them in this order so the
# pipeline's audit-cols-at-end check stays valid.
_NEW_AUDIT_COLS: list[tuple[str, str]] = [
    ("_load_type", "VARCHAR"),
    ("_file_row_number", "BIGINT"),
    ("_record_hash", "VARCHAR"),
]


def _discover_bronze_tables(warehouse: Any) -> list[tuple[str, str]]:
    """Return list of (schema, table_name) pairs for every BRONZE_*.RAW_* table."""
    sql = (
        "SELECT table_schema, table_name FROM information_schema.tables "
        "WHERE (table_schema = 'BRONZE' OR table_schema LIKE 'BRONZE\\_%' ESCAPE '\\\\') "
        "  AND table_name LIKE 'RAW\\_%' ESCAPE '\\\\' "
        "ORDER BY table_schema, table_name"
    )
    try:
        rows = warehouse.query(sql)
    except Exception:
        # Some adapters reject the ESCAPE clause. Fall back to wildcard.
        rows = warehouse.query(
            "SELECT table_schema, table_name FROM information_schema.tables "
            "WHERE table_schema LIKE 'BRONZE%' AND table_name LIKE 'RAW_%' "
            "ORDER BY table_schema, table_name"
        )
    return [(r["table_schema"], r["table_name"]) for r in rows]


def _existing_columns(warehouse: Any, schema: str, table: str) -> set[str]:
    rows = warehouse.query(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = $s AND table_name = $t",
        {"s": schema.upper(), "t": table.upper()},
    )
    return {str(r["column_name"]).lower() for r in rows}


def migrate_table(warehouse: Any, schema: str, table: str, dry_run: bool) -> int:
    """Add any missing audit cols to one table. Returns # of cols added."""
    fq = f"{schema}.{table}"
    existing = _existing_columns(warehouse, schema, table)
    added = 0
    for col, col_type in _NEW_AUDIT_COLS:
        if col.lower() in existing:
            _log.info("migrate.skip_existing", table=fq, column=col)
            continue
        sql = f"ALTER TABLE {fq} ADD COLUMN {col} {col_type}"
        if dry_run:
            _log.info("migrate.dry_run", sql=sql)
        else:
            warehouse.execute(sql)
            _log.info("migrate.added", table=fq, column=col, type=col_type)
            added += 1
    return added


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--dry-run", action="store_true", help="Print ALTERs without running")
    args = p.parse_args(argv)

    settings = load_settings()
    adapters = build_adapters(settings)
    wh = adapters.warehouse

    tables = _discover_bronze_tables(wh)
    if not tables:
        print("No BRONZE_*.RAW_* tables found — nothing to migrate.")
        return 0

    print(f"Discovered {len(tables)} Bronze table(s):")
    total_added = 0
    for schema, table in tables:
        print(f"  {schema}.{table}")
        total_added += migrate_table(wh, schema, table, dry_run=args.dry_run)

    if args.dry_run:
        print("\nDry-run complete. No changes made.")
    else:
        print(f"\nMigration complete. Columns added: {total_added}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
