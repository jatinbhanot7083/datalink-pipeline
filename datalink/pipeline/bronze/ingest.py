"""Bronze ingestion — SFTP file → ObjectStore → Warehouse (MERGE with audit cols).

Contract (per Medallion doc §3.1):
  - INSERT-or-MERGE only. Idempotent re-runs.
  - Exactly 4 metadata columns added: _load_dt, _source_file, _batch_id, _record_source.
  - No type coercion, no clinical validation. That's Silver.

Flow per file:
  1. SFTP.download(remote_path, tmp_local)
  2. ObjectStore.put(tmp_local, stage_key)          -- ADLS equivalent: /raw/{kind}/{yyyy-mm-dd}/file
  3. ObjectStore.get(stage_key, tmp_staged)         -- simulates Snowflake EXTERNAL STAGE read
  4. Warehouse.copy_from_stage into a TEMP staging table
  5. Warehouse staging → target MERGE on natural key
  6. Return BronzeIngestResult with counts + audit fields
"""

from __future__ import annotations

import re
import tempfile
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

from datalink.adapters.factory import AdapterSet
from datalink.adapters.protocols import Warehouse
from datalink.logging import get_logger
from datalink.pipeline.bronze.ddl_loader import (
    BRONZE_TABLES,
    BronzeTable,
    create_bronze_schema,
    qualified_name,
)

_log = get_logger(__name__)


def _sanitize_identifier(raw: str) -> str:
    """Replace every non-[a-zA-Z0-9_] char with `_` for safe use in SQL identifiers.

    Airflow run_ids contain `:`, `.`, and `+` (e.g. `manual__2026-04-20T11:38:38.071377+00:00`)
    which DuckDB's parser rejects inside a bare table name. This maps them
    all to `_`. Also lower-cases the result and trims any leading/trailing
    underscores so the staging table name remains predictable.
    """
    return re.sub(r"[^A-Za-z0-9_]", "_", raw).lower().strip("_") or "unnamed"


@dataclass(frozen=True)
class BronzeIngestResult:
    source_type: str  # CLAIMS | MEMBERSHIP | PROVIDER
    source_file: str  # remote path (e.g., /drop/claims_20260419.csv)
    batch_id: str  # UUID per pipeline run
    target_table: str  # BRONZE.RAW_CLAIMS
    rows_in_source: int
    rows_in_target_before: int
    rows_in_target_after: int
    rows_affected: int  # rows_in_target_after - rows_in_target_before (+ updates)
    object_store_key: str  # e.g., raw/claims/2026-04-19/claims_20260419.csv


def _audit_columns_sql() -> str:
    """Expression list added to source during load — builds the 4 audit cols.

    Uses DuckDB-style named params ($name). Snowflake supports the same
    syntax in prepared statements. Placeholders bind to the dict keys in
    the params passed to warehouse.execute().
    """
    return (
        "CURRENT_TIMESTAMP AS _load_dt, "
        "$source_file AS _source_file, "
        "$batch_id AS _batch_id, "
        "$record_source AS _record_source"
    )


def _load_to_staging(
    warehouse: Warehouse,
    local_csv: Path,
    staging_table: str,
    target_table: str,
    source_file: str,
    batch_id: str,
    record_source: str,
) -> int:
    """Load a CSV file into a staging table with the 4 audit columns appended.

    DuckDB and Snowflake both support "CREATE TABLE AS SELECT ... LIMIT 0"
    to clone a schema. We use it here to get the target's exact column types
    for the staging table.
    """
    # Clone target schema into staging — empty, no rows.
    warehouse.execute(
        f"CREATE OR REPLACE TEMP TABLE {staging_table} AS SELECT * FROM {target_table} LIMIT 0"
    )

    # Count target cols vs source CSV cols. Source CSV has target_cols - 4 (no audit cols).
    # The read_csv_auto projection must align with target order, then we append audit cols.
    target_cols = _get_columns(warehouse, target_table)
    if len(target_cols) < 4:
        raise RuntimeError(
            f"Target {target_table} has fewer than 4 columns; expected 4 audit cols at end"
        )
    source_col_names = [c for c in target_cols[:-4]]  # all but last 4 (audit)
    audit_col_names = target_cols[-4:]  # sanity
    if audit_col_names != ["_load_dt", "_source_file", "_batch_id", "_record_source"]:
        raise RuntimeError(
            f"Target {target_table} audit columns in wrong order or missing: got {audit_col_names!r}"
        )
    col_list_source = ", ".join(source_col_names)
    col_list_full = ", ".join(target_cols)

    # INSERT with audit cols computed on the fly. Named params so positional
    # ordering can't drift when we change the column projection later.
    sql = (
        f"INSERT INTO {staging_table} ({col_list_full}) "
        f"SELECT {col_list_source}, {_audit_columns_sql()} "
        f"FROM read_csv_auto($csv_path, header = true)"
    )
    warehouse.execute(
        sql,
        {
            "csv_path": str(local_csv),
            "source_file": source_file,
            "batch_id": batch_id,
            "record_source": record_source,
        },
    )

    count_sql = f"SELECT COUNT(*) AS c FROM {staging_table}"
    rows = warehouse.query(count_sql)
    return int(rows[0]["c"]) if rows else 0


def _get_columns(warehouse: Warehouse, qualified_table: str) -> list[str]:
    """Return ordered column names of a table. Works on DuckDB (DESCRIBE)
    and Snowflake (INFORMATION_SCHEMA.COLUMNS)."""
    # DuckDB returns 'column_name' in the DESCRIBE result.
    try:
        rows = warehouse.query(f"DESCRIBE {qualified_table}")
        return [cast(str, r["column_name"]) for r in rows]
    except Exception:
        # Snowflake path
        schema, name = qualified_table.split(".", 1)
        rows = warehouse.query(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position",
            {"1": schema.upper(), "2": name.upper()},
        )
        return [cast(str, r["COLUMN_NAME"]) for r in rows]


def ingest_file(
    adapters: AdapterSet,
    source_type: str,
    remote_path: str,
    batch_id: str | None = None,
    record_source: str | None = None,
    schema: str = "BRONZE",
) -> BronzeIngestResult:
    """End-to-end Bronze ingest for a single file on the SFTP drop zone.

    Args:
      source_type: CLAIMS | MEMBERSHIP | PROVIDER (case-insensitive).
      remote_path: path as returned by SftpSource.list_files().
      batch_id: pipeline-run identifier. Generated if None.
      record_source: upstream system tag. Defaults to the CSV filename stem.
      schema: warehouse schema (default BRONZE).
    """
    source_type = source_type.upper()
    if source_type not in BRONZE_TABLES:
        raise ValueError(
            f"Unknown source_type {source_type!r}; expected one of {list(BRONZE_TABLES)}"
        )
    table: BronzeTable = BRONZE_TABLES[source_type]
    target = qualified_name(table, schema)
    batch_id = batch_id or f"BATCH-{uuid.uuid4().hex[:12].upper()}"
    filename = Path(remote_path).name
    record_source = record_source or Path(remote_path).stem
    _log.info(
        "bronze_ingest.start",
        source_type=source_type,
        remote_path=remote_path,
        batch_id=batch_id,
        target=target,
    )

    # Ensure target schema + tables exist (idempotent).
    create_bronze_schema(adapters.warehouse, schema)

    rows_before = _row_count(adapters.warehouse, target)

    # 1. SFTP download to local tmp.
    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)
        local_downloaded = tmp_dir / filename
        adapters.sftp.download(remote_path, local_downloaded)

        # 2. Upload to object store under a date-partitioned key.
        today = datetime.now(UTC).strftime("%Y-%m-%d")
        kind_dir = source_type.lower()
        object_key = f"raw/{kind_dir}/{today}/{filename}"
        adapters.object_store.put(local_downloaded, object_key)

        # 3. "Read back" from the object store — simulates Snowflake reading
        #    directly from ADLS via EXTERNAL STAGE.
        local_staged = tmp_dir / f"staged_{filename}"
        adapters.object_store.get(object_key, local_staged)

        # 4. Load to TEMP staging with audit cols computed.
        # Sanitize batch_id for use as part of a SQL identifier — strip
        # every character that isn't ASCII alphanumeric or underscore.
        # Airflow run_ids look like "manual__2026-04-20T11:38:38.071377+00:00"
        # which contains `:` `.` `+` — DuckDB parser rejects those in
        # identifiers. The raw batch_id still flows into the _batch_id
        # audit column untouched, so the audit-trail link is preserved.
        safe_batch = _sanitize_identifier(batch_id)
        staging_table = f"_stg_{source_type.lower()}_{safe_batch}"
        rows_in_source = _load_to_staging(
            adapters.warehouse,
            local_staged,
            staging_table,
            target,
            source_file=filename,
            batch_id=batch_id,
            record_source=record_source,
        )

        # 5. MERGE staging → target on natural key. Idempotent.
        adapters.warehouse.merge(target, staging_table, table.key_columns)

    rows_after = _row_count(adapters.warehouse, target)
    result = BronzeIngestResult(
        source_type=source_type,
        source_file=remote_path,
        batch_id=batch_id,
        target_table=target,
        rows_in_source=rows_in_source,
        rows_in_target_before=rows_before,
        rows_in_target_after=rows_after,
        rows_affected=rows_after - rows_before,
        object_store_key=object_key,
    )
    _log.info("bronze_ingest.done", **result.__dict__)
    return result


def _row_count(warehouse: Warehouse, qualified_table: str) -> int:
    helper = getattr(warehouse, "row_count", None)
    if callable(helper):
        return int(helper(qualified_table))
    rows = warehouse.query(f"SELECT COUNT(*) AS c FROM {qualified_table}")
    return int(rows[0]["c"]) if rows else 0
