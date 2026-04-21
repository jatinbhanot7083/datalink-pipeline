"""UI-side warehouse bootstrap — first-touch file creation for read-only pages.

Why this exists
---------------
Every Streamlit page in the Control Tower (home + DQ Author + DQ Review +
Executive + CrewAI) opens DuckDB with `read_only=True` so Airflow / DAG
tasks can keep the write lock while the UI renders.

DuckDB behaviour: `read_only=True` on a missing file raises

    IOException: Cannot open database "<path>" in read-only mode:
                 database does not exist

This hit on the very first Streamlit load after `docker compose up` (the
warehouse.duckdb file doesn't get created until bronze_ingest writes to
it). Every UI page would stack-trace until an operator kicked off a DAG.

Fix
---
`ensure_warehouse_exists(path)` opens the file ONCE in read-write mode,
materialises the CONTROL schema + tables, then closes the handle. Safe
to call from every page's top-level scope — fully idempotent, cheap
(~50 ms) on re-entry because CREATE TABLE IF NOT EXISTS is a no-op.

Called from:
  * datalink/ui/control_tower.py          (home page)
  * datalink/ui/pages/1_DQ_Author.py
  * datalink/ui/pages/2_DQ_Review.py
  * datalink/ui/pages/3_Executive_Dashboard.py
  * datalink/ui/pages/4_CrewAI_Dashboard.py
  * docker/control-tower/entrypoint.sh    (belt-and-braces on container start)
"""

from __future__ import annotations

import os
from pathlib import Path


def ensure_warehouse_exists(path: str) -> None:
    """Create the DuckDB file + CONTROL schema if absent. Idempotent.

    Swallows every exception so a failure here never blocks page render —
    if bootstrap fails, pages still fall back to empty-state UX via their
    own try/except wrappers.

    CRITICAL: chmod 0o666 after creation. The warehouse file is bind-mounted
    into BOTH the control-tower container (which runs as root and calls this
    function) AND the airflow containers (which run as uid 50000 and write
    from DAG tasks). Without the chmod, root's default 0o644 blocks airflow
    from writing, producing `Permission denied` on the first sensor poke.
    0o666 lets every container on the shared bind-mount read + write.
    """
    try:
        p = Path(path)
        # Ensure parent dir exists (e.g. /opt/datalink/ inside the container).
        p.parent.mkdir(parents=True, exist_ok=True)

        already_existed = p.exists()

        # Open read-write to create the file if missing. This is the ONLY
        # place we write to the UI-visible warehouse from Python inside the
        # control-tower container — all real writes come from Airflow tasks.
        import duckdb

        conn = duckdb.connect(path, read_only=False)
        try:
            # Idempotent DDL for the control surface. Mirrors the full list
            # in datalink.quality.control._DDL but scoped to the minimum
            # set every UI page queries. If datalink is importable we use
            # the full create_control_tables() for parity.
            try:
                from datalink.quality.control import create_control_tables

                # Thin Warehouse protocol shim — create_control_tables only
                # needs .execute() + optional create_schema_if_not_exists.
                class _Shim:
                    def __init__(self, c: duckdb.DuckDBPyConnection) -> None:
                        self._c = c

                    def execute(self, sql: str, params=None) -> None:  # type: ignore[no-untyped-def]
                        if params:
                            self._c.execute(sql, params)
                        else:
                            self._c.execute(sql)

                    def create_schema_if_not_exists(self, schema: str) -> None:
                        self._c.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

                create_control_tables(_Shim(conn))  # type: ignore[arg-type]
            except Exception:
                # datalink isn't importable (e.g. entrypoint.sh pre-install).
                # Fall back to a minimal CREATE SCHEMA so read-only opens succeed.
                conn.execute("CREATE SCHEMA IF NOT EXISTS CONTROL")
        finally:
            conn.close()

        # Only chmod if WE created the file — avoid stomping on perms
        # Airflow/dbt may have set after the fact. 0o666 so every container
        # on the shared bind-mount (control-tower as root, airflow as uid
        # 50000) can read + write.
        if not already_existed:
            # non-fatal: some filesystems don't support chmod (Windows host bind-mount).
            import contextlib

            with contextlib.suppress(OSError):
                os.chmod(path, 0o666)
    except Exception:
        # Never block page render on bootstrap failure. Pages have their own
        # empty-state fallbacks — this just improves the common case.
        pass


def default_warehouse_path() -> str:
    """Resolve the warehouse path the same way every UI page does.

    Container default: /opt/datalink/warehouse.duckdb
    Overridable via:   DL_CT_WAREHOUSE_PATH env var
    """
    return os.environ.get("DL_CT_WAREHOUSE_PATH", "/opt/datalink/warehouse.duckdb")
