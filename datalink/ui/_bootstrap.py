"""UI-side warehouse bootstrap — first-touch file creation for read-only pages.

Why this exists
---------------
Every Streamlit page opens DuckDB with `read_only=True` so Airflow / DAG
tasks keep the write lock. DuckDB refuses to create a missing file in
read-only mode:

    IOException: Cannot open database in read-only mode: database does not exist

Without this module, fresh `docker compose up` makes every page stack-trace
until the first DAG run.

Fix (layered — each step independently failure-isolated so a bug in one
step NEVER prevents earlier steps from succeeding):

  Step 1: mkdir -p the parent dir               (1st try/except)
  Step 2: open read-WRITE to create the file    (2nd try/except — if this
                                                 fails nothing else runs)
  Step 3: run create_control_tables()           (3rd try/except — schema)
  Step 4: seed all 7 clients with baselines     (4th try/except — data)
  Step 5: chmod 0o666 if we created the file    (5th try/except — perms)

If step 2 succeeds but step 3/4 fail, the file still exists and read-only
opens on pages will succeed (they render empty-state). If ALL steps
succeed, the Control Tower boots with 84 baseline suites already LIVE
across 7 clients.

Called from:
  * datalink/ui/control_tower.py          (home page)
  * datalink/ui/pages/*.py                (every page)
  * docker/control-tower/entrypoint.sh    (container start, belt-and-braces)
"""

from __future__ import annotations

import contextlib
import os
import sys
import traceback
from pathlib import Path
from typing import Any


def ensure_warehouse_exists(path: str, *, verbose: bool = False) -> None:
    """Create the DuckDB file + CONTROL schema + seed baselines if absent.

    Fully idempotent. `verbose=True` writes per-step diagnostics to stderr
    so operators can see exactly which step succeeded or failed. On
    container entrypoint we pass verbose=True; on Streamlit page imports
    we leave it False (pages have their own empty-state fallbacks).

    Phase 7 Day 3: when ``DL_ADAPTERS__WAREHOUSE__TYPE=snowflake`` this
    function is a no-op. Snowflake infrastructure (database, schemas,
    role, stage) is provisioned out-of-band via ``scripts/snowflake_
    bootstrap.sql`` which runs once per environment. No warehouse.duckdb
    file needs to exist on the container filesystem in that mode.
    """
    # Short-circuit for Snowflake mode — no local file to bootstrap.
    if os.environ.get("DL_ADAPTERS__WAREHOUSE__TYPE", "duckdb").lower() == "snowflake":
        if verbose:
            print(
                "[bootstrap] Snowflake mode detected — skipping warehouse.duckdb "
                "bootstrap (Snowflake is provisioned via scripts/snowflake_bootstrap.sql).",
                file=sys.stderr,
            )
        return

    # Step 1 — parent dir -----------------------------------------------
    try:
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        already_existed = p.exists()
        if verbose:
            print(f"[bootstrap] step 1: parent dir OK — {p.parent}", file=sys.stderr)
    except Exception:
        if verbose:
            traceback.print_exc()
        return  # can't proceed without a writable dir

    # Step 2 — create the DB file ---------------------------------------
    try:
        import duckdb

        conn = duckdb.connect(path, read_only=False)
        if verbose:
            print(f"[bootstrap] step 2: file created / opened — {path}", file=sys.stderr)
    except Exception:
        if verbose:
            traceback.print_exc()
        return  # file creation is the whole point; abort if it failed

    try:
        # Step 3 — CONTROL schema + tables (forward-migrated) -----------
        try:
            from datalink.quality.control import create_control_tables

            create_control_tables(_DuckShim(conn))  # type: ignore[arg-type]
            if verbose:
                print("[bootstrap] step 3: CONTROL tables created / migrated", file=sys.stderr)
        except Exception:
            # datalink may not be importable (entrypoint runs before pip install
            # on the very first container start). Fall back to the minimum schema.
            if verbose:
                traceback.print_exc()
            try:
                conn.execute("CREATE SCHEMA IF NOT EXISTS CONTROL")
                if verbose:
                    print("[bootstrap] step 3 fallback: bare CREATE SCHEMA", file=sys.stderr)
            except Exception:
                if verbose:
                    traceback.print_exc()

        # Step 4 — seed all 7 clients with 12 suites each (84 total) ----
        try:
            from datalink.quality.baseline_seeder import seed_all_real_clients

            result = seed_all_real_clients(_DuckShim(conn))  # type: ignore[arg-type]
            if verbose:
                total = sum(result.values())
                print(
                    f"[bootstrap] step 4: seeded {total} baseline suites "
                    f"across {len(result)} clients — {result}",
                    file=sys.stderr,
                )
        except Exception:
            if verbose:
                traceback.print_exc()
    finally:
        with contextlib.suppress(Exception):
            conn.close()

    # Step 5 — chmod only if WE created the file (don't stomp on existing perms) -
    if not already_existed:
        try:
            os.chmod(path, 0o666)
            if verbose:
                print("[bootstrap] step 5: chmod 0o666 OK", file=sys.stderr)
        except OSError:
            if verbose:
                traceback.print_exc()


def default_warehouse_path() -> str:
    """Resolve the warehouse path the same way every UI page does.

    Container default: /opt/datalink/warehouse.duckdb
    Overridable via:   DL_CT_WAREHOUSE_PATH env var
    """
    return os.environ.get("DL_CT_WAREHOUSE_PATH", "/opt/datalink/warehouse.duckdb")


class _DuckShim:
    """Warehouse-protocol adapter for raw duckdb.DuckDBPyConnection.

    Implements every method that `create_control_tables` and the
    baseline seeder / registry code call on a warehouse:
      * execute(sql, params=None)
      * query(sql, params=None) -> list[dict]   (for SuiteRegistry reads)
      * create_schema_if_not_exists(schema)

    Keeping this shim in _bootstrap.py rather than using
    DuckDBWarehouse.from_config() avoids the circular-init that
    DuckDBWarehouse does (it opens its own connection from
    WarehouseConfig, which would collide with ours).
    """

    def __init__(self, conn: Any) -> None:
        self._conn = conn

    def execute(self, sql: str, params: dict[str, Any] | None = None) -> None:
        if params:
            self._conn.execute(sql, params)
        else:
            self._conn.execute(sql)

    def query(self, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        cur = self._conn.execute(sql, params) if params else self._conn.execute(sql)
        cols = [d[0] for d in cur.description] if cur.description else []
        rows = cur.fetchall()
        return [dict(zip(cols, row, strict=False)) for row in rows]

    def create_schema_if_not_exists(self, schema: str) -> None:
        self._conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
