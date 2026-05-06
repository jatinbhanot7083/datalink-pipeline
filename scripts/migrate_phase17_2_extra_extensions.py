"""Phase 17.2 migration — VARIANT extension columns.

Idempotent. Direct snowflake.connector (no adapter factory dependency).

Changes applied to every existing Bronze + Silver-Sat table in the
DATALINK_DEV database:

  Bronze:
    * Rename column ``_variant_overflow VARIANT`` → ``_extra VARIANT``
      (added in Phase 15.7 under the old name; renamed in 17.2).
    * If a Bronze table is missing both names, add ``_extra VARIANT``.

  Silver Satellites:
    * ADD COLUMN ``_extensions VARIANT NULL`` if missing.
    * Sat tables are identified by name pattern ``SAT_*`` inside any
      schema starting with ``SILVER_``.

Gold tables intentionally untouched — Gold is the strict product contract,
no overflow allowed.

Run from the control-tower container:

    docker exec datalink-control-tower python \\
        /opt/datalink/scripts/migrate_phase17_2_extra_extensions.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

# Auto-load .env so the script works without an explicit `set -a` export.
_ENV = ROOT / ".env"
if _ENV.exists():
    for raw in _ENV.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        os.environ.setdefault(k.strip(), v.strip().strip("'").strip('"'))

import snowflake.connector  # noqa: E402

DATABASE = os.environ.get("SNOWFLAKE_DATABASE", "DATALINK_DEV")


def _connect():
    cfg = {
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "user": os.environ["SNOWFLAKE_USER"],
        "password": os.environ["SNOWFLAKE_PASSWORD"],
        "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        "database": DATABASE,
        "role": os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
    }
    return snowflake.connector.connect(**cfg)


def _section(title: str) -> None:
    print()
    print("=" * 72)
    print(f" {title}")
    print("=" * 72)


def _exec(cur, sql: str, *, ok_msg: str | None = None) -> bool:
    try:
        cur.execute(sql)
        print(f"  [OK]   {ok_msg or sql[:80]}")
        return True
    except Exception as exc:
        print(f"  [SKIP] {ok_msg or sql[:80]}  ({type(exc).__name__}: {str(exc)[:120]})")
        return False


def _list_bronze_tables(cur) -> list[tuple[str, str]]:
    """Return [(schema, table)] for every BRONZE_* schema's RAW_* tables."""
    cur.execute(
        f"SELECT TABLE_SCHEMA, TABLE_NAME FROM {DATABASE}.INFORMATION_SCHEMA.TABLES "
        f"WHERE TABLE_SCHEMA LIKE 'BRONZE_%' AND TABLE_NAME LIKE 'RAW_%' "
        f"ORDER BY TABLE_SCHEMA, TABLE_NAME"
    )
    return [(r[0], r[1]) for r in cur.fetchall()]


def _list_silver_sat_tables(cur) -> list[tuple[str, str]]:
    """Return [(schema, table)] for every SILVER_* schema's SAT_* tables."""
    cur.execute(
        f"SELECT TABLE_SCHEMA, TABLE_NAME FROM {DATABASE}.INFORMATION_SCHEMA.TABLES "
        f"WHERE TABLE_SCHEMA LIKE 'SILVER_%' AND TABLE_NAME LIKE 'SAT_%' "
        f"ORDER BY TABLE_SCHEMA, TABLE_NAME"
    )
    return [(r[0], r[1]) for r in cur.fetchall()]


def _columns(cur, schema: str, table: str) -> list[str]:
    cur.execute(
        f"SELECT COLUMN_NAME FROM {DATABASE}.INFORMATION_SCHEMA.COLUMNS "
        f"WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}' "
        f"ORDER BY ORDINAL_POSITION"
    )
    return [r[0] for r in cur.fetchall()]


def main() -> int:
    print(f"Database: {DATABASE}")
    conn = _connect()
    cur = conn.cursor()

    # ---- 1. Bronze: rename _variant_overflow → _extra (or ADD if missing) -----
    _section("Step 1/2 — Bronze tables: ensure _extra VARIANT column")
    bronze_tables = _list_bronze_tables(cur)
    print(f"  Discovered {len(bronze_tables)} Bronze table(s)")
    for sch, tbl in bronze_tables:
        cols_upper = {c.upper() for c in _columns(cur, sch, tbl)}
        has_old = "_VARIANT_OVERFLOW" in cols_upper
        has_new = "_EXTRA" in cols_upper
        if has_new and not has_old:
            print(f"  [skip] {sch}.{tbl}: _extra already present")
            continue
        if has_old and not has_new:
            _exec(
                cur,
                f"ALTER TABLE {sch}.{tbl} RENAME COLUMN _variant_overflow TO _extra",
                ok_msg=f"{sch}.{tbl}: _variant_overflow → _extra",
            )
        elif has_old and has_new:
            print(f"  [WARN] {sch}.{tbl}: BOTH _extra and _variant_overflow exist — manual fix needed")
        else:
            _exec(
                cur,
                f"ALTER TABLE {sch}.{tbl} ADD COLUMN _extra VARIANT NULL",
                ok_msg=f"{sch}.{tbl}: ADD _extra VARIANT",
            )

    # ---- 2. Silver Sats: ADD COLUMN _extensions VARIANT (idempotent) ----------
    _section("Step 2/2 — Silver Satellites: ensure _extensions VARIANT column")
    sat_tables = _list_silver_sat_tables(cur)
    print(f"  Discovered {len(sat_tables)} Silver Sat table(s)")
    for sch, tbl in sat_tables:
        cols_upper = {c.upper() for c in _columns(cur, sch, tbl)}
        if "_EXTENSIONS" in cols_upper:
            print(f"  [skip] {sch}.{tbl}: _extensions already present")
            continue
        _exec(
            cur,
            f"ALTER TABLE {sch}.{tbl} ADD COLUMN _extensions VARIANT NULL",
            ok_msg=f"{sch}.{tbl}: ADD _extensions VARIANT",
        )

    # ---- Verification ----------------------------------------------------------
    _section("Verification")
    if bronze_tables:
        print("Bronze tables — _extra status:")
        for sch, tbl in bronze_tables:
            cols_upper = {c.upper() for c in _columns(cur, sch, tbl)}
            mark = "OK" if "_EXTRA" in cols_upper else "MISSING"
            print(f"  [{mark}] {sch}.{tbl}")
    if sat_tables:
        print()
        print("Silver Sat tables — _extensions status:")
        for sch, tbl in sat_tables:
            cols_upper = {c.upper() for c in _columns(cur, sch, tbl)}
            mark = "OK" if "_EXTENSIONS" in cols_upper else "MISSING"
            print(f"  [{mark}] {sch}.{tbl}")

    cur.close()
    conn.close()
    print()
    print("Migration 17.2 complete. Next: docker compose down && docker compose up -d")
    return 0


if __name__ == "__main__":
    sys.exit(main())
