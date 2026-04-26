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

import os
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

# Match DuckDB-style ``$name`` named placeholders (name must start with a
# letter or underscore, then word chars only). We deliberately avoid matching
# ``$$`` (Snowflake dollar-quoted strings) by requiring an identifier char
# immediately after ``$``.
_DOLLAR_NAME = re.compile(r"\$([A-Za-z_]\w*)")


def _rewrite_qmark_to_pyformat(sql: str) -> str:
    """Convert DuckDB-style ``?`` placeholders to Snowflake's ``%s``.

    Called ONLY when a list/tuple of params is passed — dict params use
    ``%(name)s`` directly and don't need rewriting.
    """
    return _QMARK.sub("%s", sql)


def _rewrite_dollar_to_pyformat(sql: str) -> str:
    """Convert DuckDB-style ``$name`` named placeholders to Snowflake's ``%(name)s``.

    Called ONLY when a dict of params is passed. Snowflake parses bare
    ``$name`` as a session variable reference, which is why the raw SQL
    fails with ``Session variable '$N' does not exist``.
    """
    return _DOLLAR_NAME.sub(r"%(\1)s", sql)


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

            # Resolve credentials with env-var fallback. The config loader
            # only picks up DL_-prefixed vars; SNOWFLAKE_* are the canonical
            # names used by the Snowflake connector and by scripts/
            # snowflake_smoke.py, so we read from them when the config
            # didn't carry them through.
            resolved = {
                "account": self._cfg.account or os.environ.get("SNOWFLAKE_ACCOUNT"),
                "user": self._cfg.user or os.environ.get("SNOWFLAKE_USER"),
                "password": self._cfg.password or os.environ.get("SNOWFLAKE_PASSWORD"),
                "role": self._cfg.role or os.environ.get("SNOWFLAKE_ROLE"),
                "warehouse": self._cfg.warehouse or os.environ.get("SNOWFLAKE_WAREHOUSE"),
                "database": self._cfg.database or os.environ.get("SNOWFLAKE_DATABASE"),
                # Default schema — Snowflake refuses unqualified DDL (e.g.
                # CREATE TEMPORARY TABLE) when the session has no current
                # schema. PUBLIC is auto-created by every Snowflake DB so
                # it's the safe default; override via SNOWFLAKE_SCHEMA.
                "schema": os.environ.get("SNOWFLAKE_SCHEMA", "PUBLIC"),
            }

            # schema isn't required from config — it has a sane default. Only
            # fail on the 6 credential-tier fields.
            required = ("account", "user", "password", "role", "warehouse", "database")
            missing = [k for k in required if not resolved.get(k)]
            if missing:
                raise RuntimeError(
                    f"SnowflakeWarehouse: missing required credential(s): "
                    f"{', '.join(missing)}. Either populate WarehouseConfig "
                    "via config files, or set the SNOWFLAKE_* env vars "
                    "(ACCOUNT / USER / PASSWORD / ROLE / WAREHOUSE / DATABASE)."
                )

            _log.info(
                "snowflake.connect",
                account=str(resolved["account"]),
                user=str(resolved["user"])[:4] + "…",
                role=resolved["role"],
                warehouse=resolved["warehouse"],
                database=resolved["database"],
                schema=resolved["schema"],
            )
            self._conn = snowflake.connector.connect(
                account=resolved["account"],
                user=resolved["user"],
                password=resolved["password"],
                role=resolved["role"],
                warehouse=resolved["warehouse"],
                database=resolved["database"],
                schema=resolved["schema"],
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
            # Snowflake uppercases unquoted column names by default. The
            # rest of the DataLink codebase was written against DuckDB
            # (case-preserving) and expects lowercase column keys (e.g.
            # `suite_id`, `client_id`, `column_name`). Normalise here so
            # every downstream consumer can use the same keys regardless
            # of backend. Callers who need the original Snowflake casing
            # can still get it from cur.description outside this method.
            cols = [str(d[0]).lower() for d in cur.description or []]
            rows = cur.fetchall()
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

    # --- bronze ingest helper --------------------------------------------

    def load_csv_with_audit(
        self,
        local_csv: Any,
        target_table: str,
        source_cols: list[str],
        audit_values: dict[str, Any],
        *,
        has_header: bool = True,
        delimiter: str = ",",
    ) -> int:
        """Load a local CSV into target_table, appending 7 audit cols.

        Two-step on Snowflake:
          1. PUT the local file to the target's table-level internal stage
             (``@%TABLE_NAME``). Uses AUTO_COMPRESS so the driver gzips on
             the fly and transmits over HTTPS.
          2. COPY INTO target(all_cols) FROM a SELECT over @stage that
             supplies $1..$N from the CSV plus literals + Snowflake-native
             metadata expressions for audit cols.

        Phase 9.1: extended from 4 → 7 audit columns. The 3 new columns
        complete the Snowflake-native immutable-bronze contract:

          _load_type         literal from audit_values['load_type']
          _file_row_number   METADATA$FILE_ROW_NUMBER — Snowflake's
                             native source-row position (ground truth)
          _record_hash       MD5(CONCAT_WS('|', col1, ..., colN)) over
                             BUSINESS columns. Audit-cols excluded so the
                             same row always hashes the same regardless
                             of when/how it was loaded.

        After COPY the stage is purged so re-runs don't accumulate files.
        Returns the row count of target_table AFTER load.
        """
        from pathlib import Path as PathCls

        local_path = PathCls(local_csv).resolve()
        n_src = len(source_cols)
        target_cols = [
            *source_cols,
            "_load_dt",
            "_source_file",
            "_batch_id",
            "_record_source",
            "_load_type",
            "_file_row_number",
            "_record_hash",
        ]
        col_list = ", ".join(target_cols)
        select_src = ", ".join(f"${i + 1}" for i in range(n_src))

        # Literal values for audit cols — escape single quotes by doubling.
        def _q(s: str) -> str:
            return s.replace("'", "''")

        source_file = _q(str(audit_values["source_file"]))
        batch_id = _q(str(audit_values["batch_id"]))
        record_source = _q(str(audit_values["record_source"]))
        load_type = _q(str(audit_values.get("load_type", "UNKNOWN")))

        # Hash over the business columns ($1..$N as VARCHARs joined with
        # '|'). NVL with a sentinel so NULL columns don't get silently
        # dropped by CONCAT_WS — a row of (a, NULL) must hash differently
        # from (a, '').
        hash_args = ", ".join(f"NVL(${i + 1}::VARCHAR, '∅')" for i in range(n_src))
        record_hash_expr = f"MD5(CONCAT_WS('|', {hash_args}))"

        stage_ref = f"@%{target_table.split('.')[-1]}"  # table-level stage
        # Snowflake needs forward slashes in file:// URIs regardless of OS.
        posix_path = str(local_path).replace("\\", "/")
        # PUT uploads the local file to the table stage.
        self.execute(f"PUT 'file://{posix_path}' {stage_ref} AUTO_COMPRESS = TRUE OVERWRITE = TRUE")
        # COPY INTO with inline transform for audit cols. Audit-col order
        # MUST match target_cols above (the COPY's column list).
        copy_sql = (
            f"COPY INTO {target_table} ({col_list}) "
            f"FROM (SELECT {select_src}, "
            f"CURRENT_TIMESTAMP(), "
            f"'{source_file}', '{batch_id}', '{record_source}', '{load_type}', "
            f"METADATA$FILE_ROW_NUMBER, "
            f"{record_hash_expr} "
            f"FROM {stage_ref}/{local_path.name}) "
            f"FILE_FORMAT = (TYPE = 'CSV' "
            f"FIELD_DELIMITER = '{delimiter}' "
            f"SKIP_HEADER = {1 if has_header else 0} "
            "FIELD_OPTIONALLY_ENCLOSED_BY = '\"' "
            "NULL_IF = ('', 'NULL')) "
            "ON_ERROR = 'ABORT_STATEMENT' "
            "PURGE = TRUE"
        )
        self.execute(copy_sql)
        return self.row_count(target_table)

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
        # query() lowercases keys, so `c` is always the right lookup.
        return int(rows[0].get("c", next(iter(rows[0].values()))))

    # --- internals --------------------------------------------------------

    def _prepare(
        self,
        sql: str,
        params: dict[str, Any] | Sequence[Any] | None,
    ) -> tuple[str, dict[str, Any] | Sequence[Any] | None]:
        """Rewrite paramstyle so DuckDB-style placeholders work on Snowflake.

        Legacy call sites across the code use two DuckDB styles:

          * ``?``    (positional)   — paired with list/tuple params
          * ``$name`` (named)        — paired with dict params

        Snowflake's paramstyle is pyformat: ``%s`` for positional and
        ``%(name)s`` for named. Rewrite both on the fly so callers don't
        have to choose a style per backend.
        """
        if params is None:
            return sql, params
        if isinstance(params, dict):
            return _rewrite_dollar_to_pyformat(sql), params
        return _rewrite_qmark_to_pyformat(sql), list(params)
