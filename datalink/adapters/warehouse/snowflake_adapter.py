"""SnowflakeWarehouse — production warehouse adapter.

Phase 7 Day 3: real implementation. All methods speak Snowflake-native SQL
(MERGE INTO, COPY INTO FROM @stage, DESCRIBE TABLE) so the call sites that
already work against DuckDBWarehouse carry over with minimal changes.

Connection is lazy: built on first `execute()` / `query()` call and
reused for the lifetime of this adapter. Callers that want to release
the connection (e.g. right before a dbt subprocess takes over) should
call `close()`.

Credentials come from `WarehouseConfig` (loaded from settings + env):

    account    — e.g. "vma92639.east-us-2.azure"
    user       — e.g. "DATALINK_SVC"
    password   — (from SNOWFLAKE_PASSWORD env var, kept out of git)
    role       — e.g. "DATALINK_ENGINEER"
    warehouse  — e.g. "DATALINK_WH"
    database   — e.g. "DATALINK_DEV"

Paramstyle
----------
The public protocol accepts ``dict | Sequence | None``. Snowflake's
connector uses ``paramstyle = 'pyformat'`` by default (``%(name)s`` for
named, ``%s`` for positional). We honour both:

  * dict  → passed verbatim for ``%(name)s`` binding
  * list  → the SQL should use ``%s`` placeholders

Legacy call sites in this codebase use DuckDB's ``?`` placeholders. To
keep them working under Snowflake, we do a best-effort rewrite of
standalone ``?`` → ``%s`` when a list-shaped params is passed. This is
imperfect for SQL containing literal ``?`` characters (string literals),
but is safe for the kinds of parameterised WHERE clauses the UI emits.

Dialect translation
-------------------
Snowflake is SQL:2016-compliant and supports ``MERGE INTO``, ``DESCRIBE
TABLE``, ``INFORMATION_SCHEMA`` natively — most statements port unchanged.
Known caveats handled here:

  * DESCRIBE returns a different column layout (name/type/kind/...). The
    `merge()` helper reads only `name`, which exists in both.
  * ``COPY INTO`` from an internal stage uses the ``@<stage>`` syntax;
    `stage_uri` is accepted either as ``@DATALINK_STAGE/path/file.csv``
    or ``DATALINK_STAGE/path/file.csv`` (the leading ``@`` is added if
    missing).
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from contextlib import suppress
from typing import TYPE_CHECKING, Any

from datalink.config.models import WarehouseConfig
from datalink.logging import get_logger

if TYPE_CHECKING:
    from snowflake.connector import SnowflakeConnection

_log = get_logger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# Match standalone ``?`` tokens. This is a best-effort heuristic; SQL with
# ``?`` inside string literals would need a full lexer. None of the current
# call sites embed ``?`` in literals.
_QMARK = re.compile(r"\?")


def _rewrite_qmark_to_pyformat(sql: str) -> str:
    """Convert DuckDB-style ``?`` placeholders to Snowflake's ``%s``.

    Called ONLY when a list/tuple of params is passed — dict params use
    ``%(name)s`` directly and don't need rewriting.
    """
    return _QMARK.sub("%s", sql)


def _stage_path(stage_uri: str) -> str:
    """Accept either ``@STAGE/path`` or ``STAGE/path``; return ``@STAGE/path``."""
    return stage_uri if stage_uri.startswith("@") else f"@{stage_uri}"


# ---------------------------------------------------------------------------
# SnowflakeWarehouse
# ---------------------------------------------------------------------------


class SnowflakeWarehouse:
    """Snowflake backend implementing the Warehouse protocol."""

    def __init__(self, cfg: WarehouseConfig) -> None:
        self._cfg = cfg
        self._conn: SnowflakeConnection | None = None

    # --- lifecycle --------------------------------------------------------

    def _connect(self) -> SnowflakeConnection:
        if self._conn is None:
            import snowflake.connector  # deferred import — dev envs without
            # the connector installed (unit tests that stub this) still work.

            # Every Snowflake field on WarehouseConfig is Optional[str];
            # reject up-front if any required field is missing so the
            # diagnostic points at the config, not a cryptic driver error.
            missing = [
                name
                for name in ("account", "user", "password", "role", "warehouse", "database")
                if getattr(self._cfg, name) is None
            ]
            if missing:
                raise RuntimeError(
                    f"SnowflakeWarehouse: WarehouseConfig is missing required "
                    f"field(s): {', '.join(missing)}. Set SNOWFLAKE_* env vars "
                    "(account / user / password / role / warehouse / database)."
                )

            _log.info(
                "snowflake.connect",
                account=str(self._cfg.account),
                user=str(self._cfg.user)[:4] + "…",
                role=self._cfg.role,
                warehouse=self._cfg.warehouse,
                database=self._cfg.database,
            )
            self._conn = snowflake.connector.connect(
                account=self._cfg.account,
                user=self._cfg.user,
                password=self._cfg.password,
                role=self._cfg.role,
                warehouse=self._cfg.warehouse,
                database=self._cfg.database,
                # Safety timeouts — fail fast rather than hang for minutes.
                login_timeout=20,
                network_timeout=60,
                # autocommit on: matches DuckDB's statement-level semantics.
                autocommit=True,
            )
        return self._conn

    def close(self) -> None:
        if self._conn is not None:
            # Driver may throw on close after timeout; not actionable here.
            with suppress(Exception):
                self._conn.close()
            self._conn = None

    # --- protocol methods -------------------------------------------------

    def execute(
        self,
        sql: str,
        params: dict[str, Any] | Sequence[Any] | None = None,
    ) -> None:
        cur = self._connect().cursor()
        try:
            sql_eff, params_eff = self._prepare(sql, params)
            cur.execute(sql_eff, params_eff)
        finally:
            cur.close()

    def query(
        self,
        sql: str,
        params: dict[str, Any] | Sequence[Any] | None = None,
    ) -> list[dict[str, Any]]:
        cur = self._connect().cursor()
        try:
            sql_eff, params_eff = self._prepare(sql, params)
            cur.execute(sql_eff, params_eff)
            cols = [d[0] for d in cur.description or []]
            rows = cur.fetchall()
            # Snowflake returns column names in UPPER CASE by default. Keep
            # them as-is; callers that need case-insensitive access should
            # normalise at the call site (several already do, e.g. the
            # schema fingerprint helper handles `column_name`/`name` both).
            return [dict(zip(cols, row, strict=True)) for row in rows]
        finally:
            cur.close()

    def copy_from_stage(
        self,
        stage_uri: str,
        target_table: str,
        file_format: str = "csv",
        options: dict[str, Any] | None = None,
    ) -> int:
        """Load a file from an internal Snowflake stage into target_table.

        ``stage_uri`` accepts either ``@STAGE/path/file.csv`` or
        ``STAGE/path/file.csv`` (leading @ added if missing).
        """
        opts = options or {}
        stage = _stage_path(stage_uri)

        # Build a FILE_FORMAT inline clause. For dev we default to HEADER=TRUE
        # and strip the header row; real prod would pre-register a named
        # FILE_FORMAT object and reference it by name instead.
        if file_format == "csv":
            header = opts.get("header", True)
            delimiter = opts.get("delimiter", ",")
            file_format_clause = (
                f"FILE_FORMAT = (TYPE = 'CSV' "
                f"FIELD_DELIMITER = '{delimiter}' "
                f"SKIP_HEADER = {1 if header else 0} "
                "FIELD_OPTIONALLY_ENCLOSED_BY = '\"' "
                "NULL_IF = ('', 'NULL'))"
            )
        elif file_format == "parquet":
            file_format_clause = "FILE_FORMAT = (TYPE = 'PARQUET')"
        else:
            raise NotImplementedError(f"file_format={file_format!r} not supported yet")

        before = self.row_count(target_table)
        conn = self._connect()
        cur = conn.cursor()
        try:
            cur.execute(
                f"COPY INTO {target_table} "
                f"FROM {stage} "
                f"{file_format_clause} "
                "ON_ERROR = 'ABORT_STATEMENT' "
                "PURGE = FALSE"
            )
        finally:
            cur.close()
        after = self.row_count(target_table)
        return after - before

    def merge(self, target: str, source: str, key_columns: list[str]) -> int:
        """Idempotent MERGE — UPDATE matched, INSERT unmatched."""
        if not key_columns:
            raise ValueError("merge() requires at least one key column")
        conn = self._connect()
        # DESCRIBE TABLE returns columns; first column is `name` in Snowflake.
        cur = conn.cursor()
        try:
            cur.execute(f"DESCRIBE TABLE {target}")
            desc_rows = cur.fetchall()
            desc_cols = [d[0].lower() for d in cur.description or []]
            name_idx = desc_cols.index("name") if "name" in desc_cols else 0
            cols = [str(r[name_idx]) for r in desc_rows]
        finally:
            cur.close()

        non_key = [c for c in cols if c not in key_columns]
        on_clause = " AND ".join(f"target.{k} = source.{k}" for k in key_columns)
        update_set = ", ".join(f"{c} = source.{c}" for c in non_key) if non_key else ""
        col_list = ", ".join(cols)
        source_cols = ", ".join(f"source.{c}" for c in cols)

        merge_sql = (
            f"MERGE INTO {target} AS target " f"USING {source} AS source " f"ON {on_clause} "
        )
        if update_set:
            merge_sql += f"WHEN MATCHED THEN UPDATE SET {update_set} "
        merge_sql += f"WHEN NOT MATCHED THEN INSERT ({col_list}) VALUES ({source_cols})"

        cur = conn.cursor()
        try:
            cur.execute(merge_sql)
        finally:
            cur.close()

        # Return source row count as upper-bound on rows touched (matches
        # DuckDBWarehouse.merge contract).
        return self.row_count(source)

    # --- helpers (not in protocol but widely used) ------------------------

    def create_schema_if_not_exists(self, schema: str) -> None:
        # Snowflake is case-insensitive for unquoted identifiers; keep the
        # schema name as passed to preserve caller intent.
        self.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    def table_exists(self, qualified_table: str) -> bool:
        try:
            # SELECT 1 LIMIT 0 is the portable existence probe — fails cleanly
            # when the table is missing, cheap when it exists.
            self.query(f"SELECT 1 FROM {qualified_table} LIMIT 0")
            return True
        except Exception:
            return False

    def row_count(self, qualified_table: str) -> int:
        rows = self.query(f"SELECT COUNT(*) AS c FROM {qualified_table}")
        if not rows:
            return 0
        # Snowflake uppercases unquoted column names; fall back to the first
        # value if the key differs.
        row = rows[0]
        if "C" in row:
            return int(row["C"])
        if "c" in row:
            return int(row["c"])
        return int(next(iter(row.values())))

    # --- internals --------------------------------------------------------

    def _prepare(
        self,
        sql: str,
        params: dict[str, Any] | Sequence[Any] | None,
    ) -> tuple[str, dict[str, Any] | Sequence[Any] | None]:
        """Rewrite paramstyle so DuckDB-style ``?`` placeholders work here.

        If `params` is a list/tuple, we translate ``?`` → ``%s``. If `params`
        is a dict, we assume named placeholders already match Snowflake's
        ``%(name)s`` paramstyle (or are not needed).
        """
        if params is None or isinstance(params, dict):
            return sql, params
        return _rewrite_qmark_to_pyformat(sql), list(params)
