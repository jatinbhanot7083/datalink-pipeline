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

from datalink.adapters.factory import AdapterSet
from datalink.adapters.protocols import Warehouse
from datalink.config.models import SourceFormat
from datalink.logging import get_logger
from datalink.pipeline.bronze.ddl_loader import (
    BRONZE_TABLES,
    BronzeTable,
    create_bronze_schema,
    qualified_name,
)
from datalink.pipeline.bronze.parsers import (
    SourceFormatConfig,
    build_parser,
)
from datalink.quality.schema_drift import (
    ColumnContract,
    SchemaDriftError,
    classify_drift,
    get_active_contract,
    log_drift_event,
)
from datalink.tenancy import DEFAULT_CLIENT, Layer, schema_for

_log = get_logger(__name__)


def _read_csv_header(local_csv: Path, fmt: SourceFormatConfig) -> list[str]:
    """Phase 11.3 — read just the CSV header line for drift pre-check.

    Returns the trimmed column names in source order. Returns ``[]`` if
    the format has no header (caller treats as unknown-schema). Uses the
    stdlib csv module so we don't drag in a pandas dependency for a
    one-line read.
    """
    if not fmt.has_header:
        return []
    import csv

    with open(local_csv, encoding=fmt.encoding or "utf-8", newline="") as f:
        reader = csv.reader(f, delimiter=fmt.delimiter or ",")
        try:
            row = next(reader)
        except StopIteration:
            return []
    return [c.strip() for c in row]


def _check_schema_drift(
    warehouse: Warehouse,
    *,
    client_id: str,
    source_type: str,
    local_csv: Path,
    fmt: SourceFormatConfig,
    source_file: str,
    batch_id: str,
) -> None:
    """Phase 11.3 — classify drift against the active contract. Raises
    :class:`SchemaDriftError` on FATAL.

    Behaviour matrix (Phase 11.3 v1, name-only check):

      * No active contract for ``(client_id, source_type)`` → log a
        warning and continue. Back-compat for tenants that haven't been
        migrated to contracts yet. Once the migration script runs, this
        path goes away.
      * Header read returns ``[]`` (no-header format) → also log warning
        and continue. Drift detection requires a header line.
      * Clean (no drift) → silent return.
      * ADDITIVE only → log to ``schema_drift_log`` with
        ``action_taken='LOGGED'``, return. Caller's load proceeds.
        Extras are not yet captured at row level — operator promotes
        via Schema Drift UI (Phase 11.5), at which point a new contract
        version covers them and future batches stay clean.
      * SUBTRACTIVE / DESTRUCTIVE / MIXED → log with
        ``action_taken='HALTED'``, raise :class:`SchemaDriftError`.
        Bronze ingest aborts; pipeline_control_state flips to PAUSED
        via the existing hooks machinery.
    """
    contract = get_active_contract(warehouse, client_id=client_id, source_type=source_type)
    if contract is None:
        _log.warning(
            "schema_drift.no_contract",
            client_id=client_id,
            source_type=source_type,
            source_file=source_file,
            note="no contract registered; drift detection skipped",
        )
        return

    actual_names = _read_csv_header(local_csv, fmt)
    if not actual_names:
        _log.warning(
            "schema_drift.no_header",
            client_id=client_id,
            source_type=source_type,
            source_file=source_file,
            note="format has no header row; drift detection skipped",
        )
        return

    # Build name-only ColumnContract — types unknown from header alone.
    actuals = [ColumnContract(name=n, logical_type="") for n in actual_names]
    report = classify_drift(actuals, contract, check_types=False)

    if report.is_clean:
        return

    action = "HALTED" if report.is_fatal else "LOGGED"
    log_drift_event(
        warehouse,
        client_id=client_id,
        source_type=source_type,
        source_file=source_file,
        batch_id=batch_id,
        report=report,
        action_taken=action,
        notes=f"detected at Bronze ingest; contract v{report.contract_version}",
    )

    if report.is_fatal:
        _log.error(
            "schema_drift.halt",
            client_id=client_id,
            source_type=source_type,
            source_file=source_file,
            batch_id=batch_id,
            summary=report.summary(),
        )
        raise SchemaDriftError(report, client_id=client_id, source_type=source_type)

    _log.warning(
        "schema_drift.warn",
        client_id=client_id,
        source_type=source_type,
        source_file=source_file,
        batch_id=batch_id,
        summary=report.summary(),
        added=list(report.added_columns),
        note="additive drift — load continues; promote via Schema Drift UI to capture future batches",
    )


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


def _audit_columns_sql(business_cols: list[str] | None = None) -> str:
    """Expression list added to source during load — builds the 7 audit cols.

    Phase 9.1: extended from 4 to 7 audit columns. Order MUST match the
    Bronze DDL trailing-column order:
      _load_dt / _source_file / _batch_id / _record_source / _load_type /
      _file_row_number / _record_hash

    Uses DuckDB-style named params ($name); Snowflake adapter rewrites to
    pyformat at execute time. The `_record_hash` expression needs the
    business column list to compose its CONCAT_WS argument set; pass via
    ``business_cols`` parameter.

    This helper is the FALLBACK path (used when the warehouse adapter
    doesn't have its own ``load_csv_with_audit``). Production uses the
    per-adapter implementation in DuckDB / Snowflake adapters above.
    """
    business_cols = business_cols or []
    if business_cols:
        hash_concat = ", ".join(f"COALESCE(CAST({c} AS VARCHAR), '∅')" for c in business_cols)
        hash_expr = f"md5(concat_ws('|', {hash_concat}))"
    else:
        hash_expr = "NULL"
    return (
        "CURRENT_TIMESTAMP AS _load_dt, "
        "$source_file AS _source_file, "
        "$batch_id AS _batch_id, "
        "$record_source AS _record_source, "
        "$load_type AS _load_type, "
        "ROW_NUMBER() OVER () AS _file_row_number, "
        f"{hash_expr} AS _record_hash"
    )


def _detect_load_type(
    remote_path: str,
    rows_in_source: int | None = None,
    rows_in_last_batch: int | None = None,
) -> str:
    """Decide whether this batch is FULL or INCREMENTAL.

    Phase 9.1: vendors are inconsistent — some name files
    ``membership_full_2026-04-26.csv``, others ``membership_inc_…``,
    others have no convention. Layered detection:

      1. Filename pattern (case-insensitive): ``_full_`` / ``-full-`` /
         ``.full.``  →  'FULL'.  ``_inc_`` / ``incremental``  →
         'INCREMENTAL'.
      2. Row-count heuristic: if the incoming batch is ≥ 5x the previous
         batch for the same source_type, it's almost certainly a fresh
         full file even if the filename doesn't say so.
      3. Default to ``UNKNOWN`` so the operator can investigate, rather
         than silently mis-classify.

    The decision is recorded on every row of the batch via the
    ``_LOAD_TYPE`` audit column — Silver SCD2 logic later branches on
    FULL (compute soft-deletes) vs INCREMENTAL (no soft-delete logic;
    missing rows are not-yet-arrived, not deleted).
    """
    name = (remote_path or "").lower()
    full_markers = ("_full_", "-full-", ".full.", "_full.", "/full/")
    inc_markers = (
        "_inc_",
        "-inc-",
        ".inc.",
        "_inc.",
        "/inc/",
        "_incremental_",
        "-incremental-",
        ".incremental.",
        "_delta_",
        "-delta-",
    )
    if any(marker in name for marker in full_markers):
        return "FULL"
    if any(marker in name for marker in inc_markers):
        return "INCREMENTAL"
    if (
        rows_in_source is not None
        and rows_in_last_batch is not None
        and rows_in_last_batch > 0
        and rows_in_source >= 5 * rows_in_last_batch
    ):
        return "FULL"
    return "UNKNOWN"


def _load_to_staging(
    warehouse: Warehouse,
    local_csv: Path,
    staging_table: str,
    target_table: str,
    source_file: str,
    batch_id: str,
    record_source: str,
    fmt: SourceFormatConfig,
    load_type: str = "UNKNOWN",
) -> int:
    """Load a CSV into a staging table with the 7 audit columns appended.

    Phase 9.1: extended from 4 → 7 audit columns. Audit contract:

        [..business cols.., _load_dt, _source_file, _batch_id,
         _record_source, _load_type, _file_row_number, _record_hash]

    Backend-agnostic via ``warehouse.load_csv_with_audit``: DuckDB reads
    the local file via ``read_csv_auto``; Snowflake PUT + COPY INTO with
    ``METADATA$FILE_ROW_NUMBER`` for ground-truth row positions. The
    staging table is a TEMP clone of the target — INSERT-into-target on
    the way out keeps schemas in lockstep.
    """
    # Clone target schema into staging — empty, no rows.
    warehouse.execute(
        f"CREATE OR REPLACE TEMP TABLE {staging_table} AS SELECT * FROM {target_table} LIMIT 0"
    )

    target_cols = _get_columns(warehouse, target_table)
    if len(target_cols) < 7:
        raise RuntimeError(
            f"Target {target_table} has fewer than 7 columns; expected 7 audit cols at end. "
            f"Did you forget to run scripts/migrate_bronze_audit_cols.py?"
        )
    source_col_names = list(target_cols[:-7])  # all but last 7 (audit)
    audit_col_names = target_cols[-7:]
    # Compare case-insensitively: DuckDB preserves DDL casing,
    # Snowflake folds unquoted identifiers to upper. Either form matches
    # the contract; what matters is order + lowercase equality.
    expected_audit = [
        "_load_dt",
        "_source_file",
        "_batch_id",
        "_record_source",
        "_load_type",
        "_file_row_number",
        "_record_hash",
    ]
    if [c.lower() for c in audit_col_names] != expected_audit:
        raise RuntimeError(
            f"Target {target_table} audit columns in wrong order or missing: "
            f"got {audit_col_names!r}, expected {expected_audit!r}"
        )

    # Prefer the adapter's native implementation if present (DuckDB +
    # Snowflake both ship one). Audit-values dict carries the 4 caller-
    # provided fields; the loader generates _load_dt, _file_row_number,
    # and _record_hash internally per-backend.
    loader = getattr(warehouse, "load_csv_with_audit", None)
    if callable(loader):
        before = _row_count(warehouse, staging_table)
        loader(
            local_csv,
            staging_table,
            source_col_names,
            {
                "source_file": source_file,
                "batch_id": batch_id,
                "record_source": record_source,
                "load_type": load_type,
            },
            has_header=fmt.has_header,
            delimiter=fmt.delimiter,
        )
        return _row_count(warehouse, staging_table) - before

    # Fallback for adapters that predate Phase 7 Day 4 (test fakes,
    # older stubs). Uses DuckDB's read_csv_auto — will NOT work on
    # Snowflake, only reachable in unit tests with a dict-based fake.
    col_list_full = ", ".join(target_cols)
    col_list_source = ", ".join(source_col_names)
    sql = (
        f"INSERT INTO {staging_table} ({col_list_full}) "
        f"SELECT {col_list_source}, {_audit_columns_sql(source_col_names)} "
        f"FROM read_csv_auto($csv_path, header = $header, delim = $delim)"
    )
    warehouse.execute(
        sql,
        {
            "csv_path": str(local_csv),
            "header": fmt.has_header,
            "delim": fmt.delimiter,
            "source_file": source_file,
            "batch_id": batch_id,
            "record_source": record_source,
            "load_type": load_type,
        },
    )
    count_sql = f"SELECT COUNT(*) AS c FROM {staging_table}"
    rows = warehouse.query(count_sql)
    return int(rows[0]["c"]) if rows else 0


def _get_columns(warehouse: Warehouse, qualified_table: str) -> list[str]:
    """Return ordered column names of a table. Backend-agnostic."""
    from datalink.adapters._describe import list_columns

    return list_columns(warehouse, qualified_table)


def ingest_file(
    adapters: AdapterSet,
    source_type: str,
    remote_path: str,
    batch_id: str | None = None,
    record_source: str | None = None,
    schema: str | None = None,
    client_id: str = DEFAULT_CLIENT,
    source_format: SourceFormat | None = None,
) -> BronzeIngestResult:
    """End-to-end Bronze ingest for a single file on the SFTP drop zone.

    Args:
      source_type: CLAIMS | MEMBERSHIP | PROVIDER (case-insensitive).
      remote_path: path as returned by SftpSource.list_files().
      batch_id: pipeline-run identifier. Generated if None.
      record_source: upstream system tag. Defaults to the CSV filename stem.
      schema: warehouse schema override. If None (default), resolved via
              `schema_for(client_id, Layer.BRONZE)` — returns "BRONZE" for
              the default client, "BRONZE_<UPPER_CLIENT>" for tenants.
              Explicit schema passed by Phase-2 tests still honoured.
      client_id: tenant identifier. Default 'default' = Phase-5.x baseline.
      source_format: per-source parser config (delimiter, header, encoding).
                     If None, defaults to comma-delimited CSV with header
                     (Phase-5.x behaviour).
    """
    source_type = source_type.upper()
    if source_type not in BRONZE_TABLES:
        raise ValueError(
            f"Unknown source_type {source_type!r}; expected one of {list(BRONZE_TABLES)}"
        )
    table: BronzeTable = BRONZE_TABLES[source_type]
    resolved_schema = schema if schema is not None else schema_for(client_id, Layer.BRONZE)
    target = qualified_name(table, resolved_schema)
    # Phase 6: resolve parser via source_format (or default CSV).
    fmt_cfg = (
        SourceFormatConfig(
            format=source_format.format,
            delimiter=source_format.delimiter,
            has_header=source_format.has_header,
            encoding=source_format.encoding,
            options=dict(source_format.options),
        )
        if source_format is not None
        else SourceFormatConfig()
    )
    parser = build_parser(fmt_cfg)
    # Validate parser picks a format the warehouse knows (raises early on EDI).
    parser.file_format()
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
    create_bronze_schema(adapters.warehouse, resolved_schema)

    rows_before = _row_count(adapters.warehouse, target)

    # 1. SFTP download to local tmp.
    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)
        local_downloaded = tmp_dir / filename
        adapters.sftp.download(remote_path, local_downloaded)

        # 1.5 Phase 11.3 — schema drift pre-check. Reads the CSV header
        # against the active contract for (client_id, source_type) BEFORE
        # any staging work. Fail-fast on FATAL: SchemaDriftError bubbles
        # up to the orchestrator's hooks which mark the batch failed and
        # flip pipeline_control_state to PAUSED. Additive drift is logged
        # and the load continues — extras drop on the floor for v1
        # (operator promotes via UI in Phase 11.5).
        _check_schema_drift(
            adapters.warehouse,
            client_id=client_id,
            source_type=source_type,
            local_csv=local_downloaded,
            fmt=fmt_cfg,
            source_file=filename,
            batch_id=batch_id,
        )

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
        # Phase 9.1: detect FULL vs INCREMENTAL by filename + row-count
        # heuristic. Recorded on every row in the batch via the
        # _LOAD_TYPE audit column. Silver SCD2 logic later branches on
        # this (FULL → compute soft-deletes, INCREMENTAL → no soft-del).
        # rows_in_last_batch lookup is best-effort — falls back to
        # filename-only detection if the warehouse query fails.
        rows_in_last_batch: int | None = None
        try:
            last_rows = adapters.warehouse.query(
                f"SELECT COUNT(*) AS c FROM {target} "
                f"WHERE _batch_id = (SELECT MAX(_batch_id) FROM {target} "
                f"                   WHERE _batch_id <> $bid AND _source_file LIKE $pattern)",
                {"bid": batch_id, "pattern": f"%{source_type.lower()}%"},
            )
            rows_in_last_batch = (
                int(last_rows[0]["c"]) if last_rows and last_rows[0].get("c") else None
            )
        except Exception:
            rows_in_last_batch = None

        load_type = _detect_load_type(
            remote_path=remote_path,
            rows_in_source=None,  # known after the load; filename usually decides
            rows_in_last_batch=rows_in_last_batch,
        )
        _log.info(
            "bronze_ingest.load_type",
            source_type=source_type,
            load_type=load_type,
            remote_path=remote_path,
            rows_in_last_batch=rows_in_last_batch,
        )

        rows_in_source = _load_to_staging(
            adapters.warehouse,
            local_staged,
            staging_table,
            target,
            source_file=filename,
            batch_id=batch_id,
            record_source=record_source,
            fmt=fmt_cfg,
            load_type=load_type,
        )

        # Re-run detection now that we know the actual row count, ONLY
        # if the filename-based decision was UNKNOWN. The row-count
        # heuristic kicks in here. We then UPDATE the staging rows so the
        # final INSERT INTO target carries the corrected _LOAD_TYPE.
        if load_type == "UNKNOWN" and rows_in_last_batch:
            corrected = _detect_load_type(
                remote_path=remote_path,
                rows_in_source=rows_in_source,
                rows_in_last_batch=rows_in_last_batch,
            )
            if corrected != "UNKNOWN":
                adapters.warehouse.execute(
                    f"UPDATE {staging_table} SET _load_type = $lt", {"lt": corrected}
                )
                _log.info(
                    "bronze_ingest.load_type_corrected",
                    from_=load_type,
                    to=corrected,
                    rows_in_source=rows_in_source,
                )
                load_type = corrected

        # 5. APPEND staging → target. Bronze is an immutable, append-only
        # ledger (Snowflake-native pattern). Every batch — full or
        # incremental — lands as new physical rows tagged with audit
        # columns (_load_dt, _batch_id, _source_file, _record_source) so
        # downstream Silver can replay/dedup at will. The previous code
        # MERGE'd on natural key, which destroyed raw-history attributes
        # on every re-arrival of the same key. Bronze ≠ system of record
        # for current state; Bronze IS the audit trail of what arrived.
        # Silver Hub dedups via QUALIFY ROW_NUMBER OVER(PARTITION BY
        # natural_key ORDER BY _load_dt DESC) = 1.
        adapters.warehouse.execute(f"INSERT INTO {target} SELECT * FROM {staging_table}")

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
