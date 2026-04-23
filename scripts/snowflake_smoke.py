"""Snowflake connectivity smoke test — Phase 7 Day 1 checkpoint.

Purpose:
  Verify that the DATALINK_SVC service account provisioned in
  scripts/snowflake_bootstrap.sql can actually connect, with the right
  role + warehouse + database defaults attached. This runs BEFORE any
  DataLink adapter / pipeline code touches Snowflake — it is a pure
  connectivity sanity check.

Pass criteria:
  * connection succeeds with env-var-supplied credentials
  * CURRENT_USER()      == DATALINK_SVC
  * CURRENT_ROLE()      == DATALINK_ENGINEER
  * CURRENT_WAREHOUSE() == DATALINK_WH
  * CURRENT_DATABASE()  == DATALINK_DEV
  * SELECT 1 returns 1

Usage:
  set -a; source .env; set +a
  .venv/bin/python scripts/snowflake_smoke.py

Exit codes:
  0 — all green, Day 1 checkpoint passed, proceed to Day 2
  1 — at least one assertion failed or defaults mismatch
  2 — env vars missing or snowflake-connector-python not installed
"""

from __future__ import annotations

import os
import sys


def _require(name: str) -> str:
    val = os.environ.get(name)
    if not val:
        print(f"[FAIL] env var {name} is not set")
        sys.exit(2)
    return val


def main() -> int:
    try:
        import snowflake.connector
    except ImportError:
        print("[FAIL] snowflake-connector-python is not installed.")
        print("       Run: .venv/bin/pip install snowflake-connector-python")
        return 2

    account = _require("SNOWFLAKE_ACCOUNT")
    user = _require("SNOWFLAKE_USER")
    password = _require("SNOWFLAKE_PASSWORD")
    role = os.environ.get("SNOWFLAKE_ROLE", "DATALINK_ENGINEER")
    warehouse = os.environ.get("SNOWFLAKE_WAREHOUSE", "DATALINK_WH")
    database = os.environ.get("SNOWFLAKE_DATABASE", "DATALINK_DEV")

    # Mask everything except first 4 chars of account + user, never the password
    print(f"[...] connecting to account={account[:4]}…  user={user[:4]}…  role={role}")

    try:
        ctx = snowflake.connector.connect(
            account=account,
            user=user,
            password=password,
            role=role,
            warehouse=warehouse,
            database=database,
            # Timeout so we don't hang forever if auth is mis-configured.
            login_timeout=20,
            network_timeout=20,
        )
    except Exception as exc:
        print(f"[FAIL] connection error: {type(exc).__name__}: {exc}")
        return 1

    print(f"[OK]   connected to {account}")
    cur = ctx.cursor()
    try:
        cur.execute(
            "SELECT CURRENT_USER(), CURRENT_ROLE(), " "CURRENT_WAREHOUSE(), CURRENT_DATABASE(), 1"
        )
        row = cur.fetchone()
        if row is None:
            print("[FAIL] smoke query returned no rows")
            return 1
        u, r, w, d, one = row

        print(f"[OK]   CURRENT_USER()      = {u}")
        print(f"[OK]   CURRENT_ROLE()      = {r}")
        print(f"[OK]   CURRENT_WAREHOUSE() = {w}")
        print(f"[OK]   CURRENT_DATABASE()  = {d}")
        print(f"[OK]   SELECT 1            = {one}")

        expected = {
            "CURRENT_USER()": ("DATALINK_SVC", u),
            "CURRENT_ROLE()": ("DATALINK_ENGINEER", r),
            "CURRENT_WAREHOUSE()": ("DATALINK_WH", w),
            "CURRENT_DATABASE()": ("DATALINK_DEV", d),
        }
        mismatches = [(k, exp, got) for k, (exp, got) in expected.items() if exp != got]
        if mismatches:
            print()
            print("[FAIL] Default bindings do not match the bootstrap:")
            for k, exp, got in mismatches:
                print(f"       {k}: expected {exp!r}, got {got!r}")
            return 1

        print()
        print("=" * 50)
        print("  Phase 7 Day 1 connectivity checkpoint: PASSED")
        print("=" * 50)
        return 0
    finally:
        cur.close()
        ctx.close()


if __name__ == "__main__":
    sys.exit(main())
