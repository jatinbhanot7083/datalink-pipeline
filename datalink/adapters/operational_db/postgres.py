"""PostgresOperationalDb — psycopg-backed. Prod target (future) + local router demo."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

import psycopg
from psycopg import sql

from datalink.config.models import OperationalDbConfig
from datalink.logging import get_logger

_log = get_logger(__name__)


class PostgresOperationalDb:
    def __init__(self, name: str, cfg: OperationalDbConfig) -> None:
        self.name = name
        self._cfg = cfg
        self._conn: psycopg.Connection | None = None

    def _connect(self) -> psycopg.Connection:
        if self._conn is None or self._conn.closed:
            self._conn = psycopg.connect(
                host=self._cfg.host,
                port=self._cfg.port,
                user=self._cfg.user,
                password=self._cfg.password,
                dbname=self._cfg.database,
                autocommit=False,
            )
        return self._conn

    def close(self) -> None:
        if self._conn is not None and not self._conn.closed:
            self._conn.close()
        self._conn = None

    # --- protocol -----------------------------------------------------------

    def execute(self, sql_text: str, params: dict[str, Any] | None = None) -> None:
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute(sql_text, params or {})
        conn.commit()

    def execute_script(self, sql_text: str) -> None:
        """Execute multi-statement DDL. Each statement in its own transaction
        so one failure doesn't abort unrelated later statements (PG default
        behavior with psycopg)."""
        conn = self._connect()
        for stmt in _split_statements(sql_text):
            with conn.cursor() as cur:
                try:
                    cur.execute(stmt)
                    conn.commit()
                except Exception:
                    conn.rollback()
                    raise

    def bulk_upsert(
        self,
        table: str,
        rows: Iterable[dict[str, Any]],
        key_columns: list[str],
        schema: str = "um",
    ) -> int:
        """UPSERT via INSERT ... ON CONFLICT ... DO UPDATE. Idempotent.

        Returns the number of rows in the source batch.
        """
        rows_list = list(rows)
        if not rows_list:
            return 0
        cols = list(rows_list[0].keys())
        placeholders = ", ".join(["%s"] * len(cols))
        col_list = ", ".join(cols)
        pk_list = ", ".join(key_columns)
        non_key = [c for c in cols if c not in key_columns]
        if non_key:
            update_set = ", ".join(f"{c} = EXCLUDED.{c}" for c in non_key)
            on_conflict = f"ON CONFLICT ({pk_list}) DO UPDATE SET {update_set}"
        else:
            on_conflict = f"ON CONFLICT ({pk_list}) DO NOTHING"

        stmt = (
            f"INSERT INTO {schema}.{table} ({col_list}) "
            f"VALUES ({placeholders}) "
            f"{on_conflict}"
        )
        conn = self._connect()
        with conn.cursor() as cur:
            cur.executemany(stmt, [tuple(r[c] for c in cols) for r in rows_list])
        conn.commit()
        return len(rows_list)

    # --- helpers (not in protocol) -----------------------------------------

    def table_row_count(self, table: str, schema: str = "um") -> int:
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
                        sql.Identifier(schema), sql.Identifier(table)
                    )
                )
                row = cur.fetchone()
            # SELECT doesn't need commit but clear any implicit txn.
            conn.commit()
            return int(row[0]) if row else 0
        except Exception:
            conn.rollback()
            raise

    def truncate(self, table: str, schema: str = "um") -> None:
        """Empty the target (used by tests). Rolls back on error so a
        missing-table exception doesn't leave the connection in aborted
        state for the next call."""
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL("TRUNCATE TABLE {}.{} CASCADE").format(
                        sql.Identifier(schema), sql.Identifier(table)
                    )
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise


def _split_statements(script: str) -> list[str]:
    """Naive split on ';' — fine for our DDL which has no string literals with ';'."""
    return [s.strip() for s in script.split(";") if s.strip()]
