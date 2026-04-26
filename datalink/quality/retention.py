"""Phase 9.5 — Bronze retention.

Daily prune of Bronze rows older than ``retention_days``. Bronze is an
append-only ledger (Phase 9.1), so the active history grows linearly
with batch frequency. Snowflake separates compute from storage so query
performance is unaffected by row count, but storage cost compounds.
Industry-standard window for healthcare DQ: 30-90 days active in
Snowflake, 7-10 years deep-archive in ADLS (already covered by file
landings into the object store).

Safety contract — we DO NOT delete a Bronze row unless ALL of these
hold simultaneously:

  1. ``_load_dt < cutoff_dt`` — the row is older than the window.
  2. **Silver has run after ``cutoff_dt``** — proven by reading
     ``MAX(completed_at) FROM CONTROL.pipeline_checkpoints WHERE
     checkpoint_name LIKE 'silver_%'``. If Silver hasn't run since the
     cutoff, we SKIP the prune (no `_load_dt < cutoff` row was
     processed by the latest Silver build, so deleting it could lose
     history).
  3. The prune is logged to ``CONTROL.bronze_retention_log`` with
     row counts + watermark for compliance trail.

Everything is logged to ``CONTROL.bronze_retention_log`` regardless of
outcome (DRY_RUN / PRUNED / SKIPPED_NO_SILVER / FAILED).
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from datalink.adapters.protocols import Warehouse
from datalink.logging import get_logger
from datalink.quality.control import CONTROL_SCHEMA

_log = get_logger(__name__)


@dataclass
class RetentionResult:
    table_schema: str
    table_name: str
    status: str  # DRY_RUN | PRUNED | SKIPPED_NO_SILVER | FAILED
    cutoff_dt: datetime | None = None
    watermark_dt: datetime | None = None
    rows_eligible: int = 0
    rows_pruned: int = 0
    oldest_kept_dt: datetime | None = None
    error: str | None = None


def discover_bronze_tables(warehouse: Warehouse) -> list[tuple[str, str]]:
    """Return [(schema, table_name), …] for every BRONZE_*.RAW_* table.

    Mirrors scripts/migrate_bronze_audit_cols.py — single source of truth
    for "what tables are subject to retention" rolled into the
    discovery pattern we already use elsewhere.
    """
    rows = warehouse.query(
        "SELECT table_schema, table_name FROM information_schema.tables "
        "WHERE (table_schema = 'BRONZE' OR table_schema LIKE 'BRONZE\\_%' ESCAPE '\\\\') "
        "  AND table_name LIKE 'RAW\\_%' ESCAPE '\\\\' "
        "ORDER BY table_schema, table_name"
    )
    return [(r["table_schema"], r["table_name"]) for r in rows]


def latest_silver_completion(warehouse: Warehouse) -> datetime | None:
    """Highest completed_at across all silver_* checkpoints. None if Silver
    has never run.

    Drives the watermark guard — if Silver hasn't run since cutoff_dt, we
    refuse to prune anything older than cutoff. Otherwise the deleted
    Bronze rows might never have been silvered.

    Snowflake's default ``TIMESTAMP_NTZ`` returns naive datetimes; we
    normalise to UTC so comparisons against ``datetime.now(UTC)`` (the
    cutoff) don't raise ``can't compare offset-naive and offset-aware``.
    """
    rows = warehouse.query(
        f"SELECT MAX(completed_at) AS w FROM {CONTROL_SCHEMA}.pipeline_checkpoints "
        "WHERE checkpoint_name LIKE 'silver_%' AND status = 'PASSED'"
    )
    if not rows:
        return None
    val = rows[0].get("w") or rows[0].get("max")
    if not isinstance(val, datetime):
        return None
    return val if val.tzinfo is not None else val.replace(tzinfo=UTC)


def prune_bronze_for_table(
    warehouse: Warehouse,
    *,
    schema: str,
    table: str,
    retention_days: int,
    prune_run_id: str | None = None,
    dry_run: bool = False,
) -> RetentionResult:
    """Prune one Bronze table according to the retention contract.

    Idempotent — re-running with the same parameters is safe. Returns a
    structured RetentionResult. Always writes a bronze_retention_log row
    so audit captures DRY_RUN / SKIPPED runs too.
    """
    prune_run_id = prune_run_id or str(uuid.uuid4())
    fq = f"{schema}.{table}"
    cutoff_dt = datetime.now(UTC) - timedelta(days=retention_days)
    started_at = datetime.now(UTC)

    result = RetentionResult(
        table_schema=schema, table_name=table, status="PENDING", cutoff_dt=cutoff_dt
    )

    try:
        # Watermark guard — Silver must have processed past cutoff.
        watermark = latest_silver_completion(warehouse)
        result.watermark_dt = watermark
        if watermark is None:
            result.status = "SKIPPED_NO_SILVER"
            result.error = "no silver_* PASSED checkpoint found in pipeline_checkpoints"
            _log_retention(warehouse, prune_run_id, result, retention_days, started_at)
            return result
        if watermark < cutoff_dt:
            result.status = "SKIPPED_NO_SILVER"
            result.error = (
                f"latest silver completion {watermark} is older than cutoff {cutoff_dt} "
                "— pruning would risk losing un-silvered Bronze history"
            )
            _log_retention(warehouse, prune_run_id, result, retention_days, started_at)
            return result

        # Count eligible rows (always, even on dry-run).
        cnt = warehouse.query(
            f"SELECT COUNT(*) AS n FROM {fq} WHERE _load_dt < $c", {"c": cutoff_dt}
        )
        result.rows_eligible = int(cnt[0].get("n") or cnt[0].get("count(*)") or 0) if cnt else 0

        if dry_run:
            result.status = "DRY_RUN"
            # Compute the oldest row that WOULD remain post-prune for the log.
            kept = warehouse.query(
                f"SELECT MIN(_load_dt) AS oldest FROM {fq} WHERE _load_dt >= $c",
                {"c": cutoff_dt},
            )
            if kept and kept[0].get("oldest"):
                result.oldest_kept_dt = kept[0]["oldest"]
            _log_retention(warehouse, prune_run_id, result, retention_days, started_at)
            return result

        # Real prune.
        if result.rows_eligible > 0:
            warehouse.execute(f"DELETE FROM {fq} WHERE _load_dt < $c", {"c": cutoff_dt})
            result.rows_pruned = result.rows_eligible
        result.status = "PRUNED"
        kept = warehouse.query(f"SELECT MIN(_load_dt) AS oldest FROM {fq}")
        if kept and kept[0].get("oldest"):
            result.oldest_kept_dt = kept[0]["oldest"]
        _log_retention(warehouse, prune_run_id, result, retention_days, started_at)
        _log.info(
            "retention.pruned",
            table=fq,
            rows_pruned=result.rows_pruned,
            cutoff_dt=cutoff_dt,
            oldest_kept_dt=result.oldest_kept_dt,
        )
        return result

    except Exception as e:
        result.status = "FAILED"
        result.error = f"{type(e).__name__}: {e}"
        _log_retention(warehouse, prune_run_id, result, retention_days, started_at)
        _log.error("retention.failed", table=fq, error=result.error)
        raise


def _log_retention(
    warehouse: Warehouse,
    prune_run_id: str,
    result: RetentionResult,
    retention_days: int,
    started_at: datetime,
) -> None:
    """Persist a retention attempt to CONTROL.bronze_retention_log.

    PK is (prune_run_id, table_schema, table_name) so the same prune run
    can DRY_RUN every Bronze table and we still get one row per table.
    """
    warehouse.execute(
        f"INSERT INTO {CONTROL_SCHEMA}.bronze_retention_log "
        "(prune_run_id, table_schema, table_name, retention_days, "
        " cutoff_dt, watermark_dt, rows_pruned, oldest_kept_dt, "
        " started_at, finished_at, status, error) "
        "VALUES ($id, $s, $t, $rd, $cut, $wm, $rp, $ok, $st, $ft, $stat, $err)",
        {
            "id": prune_run_id,
            "s": result.table_schema,
            "t": result.table_name,
            "rd": retention_days,
            "cut": result.cutoff_dt,
            "wm": result.watermark_dt,
            "rp": result.rows_pruned,
            "ok": result.oldest_kept_dt,
            "st": started_at,
            "ft": datetime.now(UTC),
            "stat": result.status,
            "err": (result.error or "")[:1000] if result.error else None,
        },
    )


def prune_all_bronze(
    warehouse: Warehouse,
    *,
    retention_days: int = 90,
    dry_run: bool = False,
) -> list[RetentionResult]:
    """Run prune across every BRONZE_*.RAW_* table. One prune_run_id for
    the whole batch so audit log can group results."""
    prune_run_id = str(uuid.uuid4())
    tables = discover_bronze_tables(warehouse)
    results: list[RetentionResult] = []
    for schema, table in tables:
        try:
            res = prune_bronze_for_table(
                warehouse,
                schema=schema,
                table=table,
                retention_days=retention_days,
                prune_run_id=prune_run_id,
                dry_run=dry_run,
            )
            results.append(res)
        except Exception as exc:
            # FAILED is already logged inside prune_bronze_for_table; we
            # collect a stub for the caller summary and keep going.
            results.append(
                RetentionResult(
                    table_schema=schema,
                    table_name=table,
                    status="FAILED",
                    error=f"{type(exc).__name__}: {exc}",
                )
            )
    return results
