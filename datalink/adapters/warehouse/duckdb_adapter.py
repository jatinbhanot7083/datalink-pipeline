"""DuckDBWarehouse — local Snowflake emulator.

Key gaps papered over:
  - copy_from_stage: emulates Snowflake's COPY INTO FROM @stage by reading
    the file directly (stage_uri is a local path in the local env — the
    pipeline downloads from the object store to a temp file first).
  - merge: DuckDB supports MERGE INTO natively since 1.0.

Not implemented (explicit NotImplementedError — not used at Bronze):
  - VARIANT column type (Silver+ may need; handled via JSON type + dbt macro later).

Connection is lazy. Safe to build-then-discard in tests.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb

from datalink.config.models import WarehouseConfig


class DuckDBWarehouse:
    def __init__(self, cfg: WarehouseConfig) -> None:
        self._cfg = cfg
        self._conn: duckdb.DuckDBPyConnection | None = None

    # --- lifecycle --------------------------------------------------------

    def _connect(self) -> duckdb.DuckDBPyConnection:
        if self._conn is None:
            db_path = Path(self._cfg.path)
            # File-backed DB: ensure parent dir exists. :memory: has no parent.
            if self._cfg.path != ":memory:":
                db_path.parent.mkdir(parents=True, exist_ok=True)
            self._conn = duckdb.connect(self._cfg.path)
        return self._conn

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    # --- protocol methods -------------------------------------------------

    def execute(self, sql: str, params: dict[str, Any] | None = None) -> None:
        conn = self._connect()
        if params:
            conn.execute(sql, params)
        else:
            conn.execute(sql)

    def query(self, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        conn = self._connect()
        cursor = conn.execute(sql, params) if params else conn.execute(sql)
        cols = [d[0] for d in cursor.description or []]
        rows = cursor.fetchall()
        return [dict(zip(cols, row, strict=True)) for row in rows]

    def copy_from_stage(
        self,
        stage_uri: str,
        target_table: str,
        file_format: str = "csv",
        options: dict[str, Any] | None = None,
    ) -> int:
        """Load a file (CSV or Parquet) into target_table.

        For DuckDB, `stage_uri` is a local filesystem path. The pipeline
        downloads from the object store to a local temp file before calling
        this, keeping semantics identical to Snowflake's COPY INTO FROM @stage.
        Returns the number of rows loaded (best-effort via changes()).
        """
        opts = options or {}
        conn = self._connect()
        before_row = conn.execute(f"SELECT COUNT(*) FROM {target_table}").fetchone()
        before = int(before_row[0]) if before_row else 0
        if file_format == "csv":
            header = opts.get("header", True)
            delimiter = opts.get("delimiter", ",")
            sql = (
                f"INSERT INTO {target_table} SELECT * FROM read_csv_auto(?, header = ?, delim = ?)"
            )
            conn.execute(sql, [stage_uri, header, delimiter])
        elif file_format == "parquet":
            conn.execute(
                f"INSERT INTO {target_table} SELECT * FROM read_parquet(?)",
                [stage_uri],
            )
        else:
            raise NotImplementedError(f"file_format={file_format!r} not supported yet")
        after_row = conn.execute(f"SELECT COUNT(*) FROM {target_table}").fetchone()
        after = int(after_row[0]) if after_row else 0
        return after - before

    def merge(self, target: str, source: str, key_columns: list[str]) -> int:
        """Idempotent MERGE — UPDATE matched rows, INSERT unmatched.

        Re-running with the same source produces 0 changes (idempotency).
        Returns the source row count as an upper bound on rows touched.
        """
        if not key_columns:
            raise ValueError("merge() requires at least one key column")
        conn = self._connect()
        cols_rows = conn.execute(f"DESCRIBE {target}").fetchall()
        cols = [r[0] for r in cols_rows]
        non_key = [c for c in cols if c not in key_columns]
        on_clause = " AND ".join(f"target.{k} = source.{k}" for k in key_columns)
        update_set = ", ".join(f"{c} = source.{c}" for c in non_key)
        col_list = ", ".join(cols)
        source_cols = ", ".join(f"source.{c}" for c in cols)
        sql = (
            f"MERGE INTO {target} AS target "
            f"USING {source} AS source "
            f"ON {on_clause} "
            f"WHEN MATCHED THEN UPDATE SET {update_set} "
            f"WHEN NOT MATCHED THEN INSERT ({col_list}) VALUES ({source_cols})"
        )
        conn.execute(sql)
        count_row = conn.execute(f"SELECT COUNT(*) FROM {source}").fetchone()
        return int(count_row[0]) if count_row else 0

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
        """Load a local CSV into target_table with 4 audit columns appended.

        Phase 7 Day 4: adapter-dispatched Bronze ingest. DuckDB reads the
        local file directly via ``read_csv_auto``; the Snowflake adapter
        implements the same method using PUT + COPY INTO. Callers in
        pipeline/bronze/ingest.py just invoke this method; the backend
        choice is transparent.

        ``source_cols`` are the CSV columns in target-order (every target
        column except the 4 trailing audit columns).
        ``audit_values`` provides `source_file`, `batch_id`, `record_source`;
        `_load_dt` is always CURRENT_TIMESTAMP.

        Returns the total row count of target_table AFTER load (caller
        typically computes delta against a pre-load count).
        """
        from pathlib import Path as PathCls

        target_cols = [
            *source_cols,
            "_load_dt",
            "_source_file",
            "_batch_id",
            "_record_source",
        ]
        col_list = ", ".join(target_cols)
        src_list = ", ".join(source_cols)
        sql = (
            f"INSERT INTO {target_table} ({col_list}) "
            f"SELECT {src_list}, CURRENT_TIMESTAMP, $source_file, $batch_id, $record_source "
            f"FROM read_csv_auto($csv_path, header = $header, delim = $delim)"
        )
        self.execute(
            sql,
            {
                "csv_path": str(PathCls(local_csv)),
                "header": has_header,
                "delim": delimiter,
                "source_file": audit_values["source_file"],
                "batch_id": audit_values["batch_id"],
                "record_source": audit_values["record_source"],
            },
        )
        return self.row_count(target_table)

    # --- helpers (not in protocol) ---------------------------------------

    def create_schema_if_not_exists(self, schema: str) -> None:
        conn = self._connect()
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    def table_exists(self, qualified_table: str) -> bool:
        conn = self._connect()
        try:
            conn.execute(f"SELECT 1 FROM {qualified_table} LIMIT 0")
            return True
        except duckdb.CatalogException:
            return False

    def row_count(self, qualified_table: str) -> int:
        conn = self._connect()
        row = conn.execute(f"SELECT COUNT(*) FROM {qualified_table}").fetchone()
        return int(row[0]) if row else 0
