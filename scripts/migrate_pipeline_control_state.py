"""Phase 9.4 migration: pipeline_control_state → composite (pipeline_id, client_id) PK.

Pre-9.4 schema:
    pipeline_id     VARCHAR PRIMARY KEY
    status          VARCHAR
    ...

9.4 schema:
    pipeline_id     VARCHAR
    client_id       VARCHAR DEFAULT 'default'
    status          VARCHAR
    ...
    PRIMARY KEY (pipeline_id, client_id)

Snowflake doesn't allow re-stating a PK trivially, so the safe-and-portable
playbook is:
  1. ADD COLUMN client_id VARCHAR DEFAULT 'default' (idempotent — IF NOT EXISTS
     where supported, presence-check otherwise).
  2. UPDATE every existing row to set client_id='default' (no-op if defaults
     populated already).
  3. Drop the legacy single-column PK (best-effort — Snowflake doesn't enforce
     PKs and accepts the metadata change quietly; if it fails we log + carry on).
  4. Add the composite PK (idempotent via try-add).

Usage:
    python -m scripts.migrate_pipeline_control_state
    python -m scripts.migrate_pipeline_control_state --dry-run

Run from the host (with `set -a; source .env; set +a; POSTGRES_HOST=localhost...`)
or from inside the airflow container.
"""

from __future__ import annotations

import argparse
import sys
from typing import Any

from datalink.adapters.factory import build_adapters
from datalink.config.loader import load_settings
from datalink.logging import get_logger
from datalink.quality.control import CONTROL_SCHEMA

_log = get_logger(__name__)


def _column_exists(warehouse: Any, schema: str, table: str, column: str) -> bool:
    rows = warehouse.query(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = $s AND table_name = $t AND column_name = $c",
        {"s": schema.upper(), "t": table.upper(), "c": column.upper()},
    )
    return bool(rows)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args(argv)

    settings = load_settings()
    adapters = build_adapters(settings)
    wh = adapters.warehouse

    schema = CONTROL_SCHEMA
    table = "pipeline_control_state"
    fq = f"{schema}.{table}"

    # 1. Add client_id column if missing.
    if _column_exists(wh, schema, table, "client_id"):
        print(f"  client_id column already present on {fq}")
    else:
        sql = f"ALTER TABLE {fq} ADD COLUMN client_id VARCHAR " "NOT NULL DEFAULT 'default'"
        if args.dry_run:
            print(f"  [DRY] {sql}")
        else:
            wh.execute(sql)
            print(f"  added client_id column to {fq}")

    # 2. Backfill any NULLs to 'default' (defensive — DEFAULT should have
    #    already filled them, but Snowflake DEFAULT semantics on existing
    #    rows can vary by clause ordering).
    if not args.dry_run:
        wh.execute(f"UPDATE {fq} SET client_id = 'default' WHERE client_id IS NULL")
        print(f"  backfilled NULL client_id rows on {fq}")

    # 3. Drop legacy single-column PK + add composite. Snowflake doesn't
    #    enforce PKs but accepts the metadata. We use ALTER ... DROP
    #    PRIMARY KEY for the legacy PK (Snowflake-supported) and then
    #    ADD CONSTRAINT for the new one.
    drop_sql = f"ALTER TABLE {fq} DROP PRIMARY KEY"
    add_sql = f"ALTER TABLE {fq} ADD CONSTRAINT pk_{table} " "PRIMARY KEY (pipeline_id, client_id)"
    if args.dry_run:
        print(f"  [DRY] {drop_sql}")
        print(f"  [DRY] {add_sql}")
    else:
        try:
            wh.execute(drop_sql)
            print(f"  dropped legacy PK on {fq}")
        except Exception as e:
            # Legacy PK may not exist (fresh deploys built with the new
            # DDL already have the composite PK). Logged + we continue.
            _log.info("migrate.drop_pk_skipped", error=str(e)[:200])
            print(f"  (drop legacy PK skipped: {e!s:.120})")
        try:
            wh.execute(add_sql)
            print(f"  added composite PK (pipeline_id, client_id) on {fq}")
        except Exception as e:
            _log.warning("migrate.add_pk_failed", error=str(e)[:200])
            print(f"  WARN: composite PK add failed (already exists?): {e!s:.120}")

    # 4. Surface current state for sanity. Use a column-presence check so
    #    dry-run mode (where client_id wasn't actually added yet) doesn't
    #    blow up trying to SELECT it.
    print()
    print("=== Current pipeline_control_state rows ===")
    has_client_col = _column_exists(wh, schema, table, "client_id")
    if has_client_col:
        rows = wh.query(
            f"SELECT pipeline_id, client_id, status FROM {fq} " "ORDER BY pipeline_id, client_id"
        )
        for r in rows:
            print(
                f"  {r['pipeline_id']:<22}  " f"client={r['client_id']:<14}  status={r['status']}"
            )
    else:
        # Pre-migration / dry-run path: client_id column not yet present.
        rows = wh.query(f"SELECT pipeline_id, status FROM {fq} ORDER BY pipeline_id")
        for r in rows:
            print(
                f"  {r['pipeline_id']:<22}  status={r['status']}  "
                "(client_id column not yet added — run without --dry-run)"
            )
    if not rows:
        print("  (table empty — first pipeline run will populate it)")

    print()
    if args.dry_run:
        print("Dry-run complete. No DDL/DML applied.")
    else:
        print("Migration complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
