"""SqlServerOperationalDb — pyodbc-backed. Current prod target.

pyodbc import is deferred to method-level so the adapter can be instantiated
on machines without unixODBC runtime installed (e.g., when Docker Desktop's
MCR proxy bug is blocking the SQL Server container). When SQL Server is
reachable, `make up-mcr` brings it up and the adapter works end-to-end.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from datalink.config.models import OperationalDbConfig
from datalink.logging import get_logger

_log = get_logger(__name__)


class SqlServerOperationalDb:
    def __init__(self, name: str, cfg: OperationalDbConfig) -> None:
        self.name = name
        self._cfg = cfg
        self._conn: Any | None = None  # pyodbc.Connection; typed Any to avoid import

    def _connect(self) -> Any:
        if self._conn is None:
            import pyodbc  # deferred — require ODBC runtime only when connecting

            conn_str = (
                f"DRIVER={{ODBC Driver 18 for SQL Server}};"
                f"SERVER={self._cfg.host},{self._cfg.port};"
                f"DATABASE={self._cfg.database};"
                f"UID={self._cfg.user};"
                f"PWD={self._cfg.password};"
                "TrustServerCertificate=yes;"
            )
            self._conn = pyodbc.connect(conn_str, autocommit=False)
        return self._conn

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    # --- protocol -----------------------------------------------------------

    def execute(self, sql_text: str, params: dict[str, Any] | None = None) -> None:
        conn = self._connect()
        cur = conn.cursor()
        if params:
            cur.execute(sql_text, tuple(params.values()))
        else:
            cur.execute(sql_text)
        conn.commit()

    def execute_script(self, sql_text: str) -> None:
        """Execute multi-statement DDL. SQL Server's batch separator is 'GO' —
        we split on ';' since our generated DDL uses per-statement ';'."""
        conn = self._connect()
        cur = conn.cursor()
        for stmt in _split_statements(sql_text):
            cur.execute(stmt)
        conn.commit()

    def bulk_upsert(
        self,
        table: str,
        rows: Iterable[dict[str, Any]],
        key_columns: list[str],
        schema: str = "UM",
    ) -> int:
        """MERGE-based UPSERT. Uses fast_executemany for throughput."""
        rows_list = list(rows)
        if not rows_list:
            return 0
        cols = list(rows_list[0].keys())
        placeholders = ", ".join(["?"] * len(cols))
        col_list = ", ".join(f"[{c}]" for c in cols)
        on_clause = " AND ".join(f"target.[{k}] = source.[{k}]" for k in key_columns)
        non_key = [c for c in cols if c not in key_columns]
        update_set = ", ".join(f"target.[{c}] = source.[{c}]" for c in non_key)
        source_cols = ", ".join(f"source.[{c}]" for c in cols)
        pascal_table = "".join(p.capitalize() for p in table.split("_"))

        stmt = (
            f"MERGE INTO [{schema}].[{pascal_table}] AS target "
            f"USING (SELECT {col_list} FROM (VALUES ({placeholders})) AS v({col_list})) AS source "
            f"ON {on_clause} "
            f"WHEN MATCHED THEN UPDATE SET {update_set} "
            f"WHEN NOT MATCHED THEN INSERT ({col_list}) VALUES ({source_cols});"
        )

        conn = self._connect()
        cur = conn.cursor()
        cur.fast_executemany = True
        cur.executemany(stmt, [tuple(r[c] for c in cols) for r in rows_list])
        conn.commit()
        return len(rows_list)


def _split_statements(script: str) -> list[str]:
    return [s.strip() for s in script.split(";") if s.strip()]
