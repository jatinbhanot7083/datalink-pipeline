"""Phase 9.5 — CLI for Bronze retention pruning.

Wraps datalink.quality.retention. Idempotent. Always logs to
CONTROL.bronze_retention_log so DRY_RUN / SKIPPED / PRUNED runs are all
auditable.

Usage:
    # See what WOULD be pruned at the default 90-day window.
    python -m scripts.prune_bronze --dry-run

    # Tighter window for testing on dev data.
    python -m scripts.prune_bronze --days 1 --dry-run

    # Real prune.
    python -m scripts.prune_bronze --days 90

Schedule it from Airflow / cron / Snowflake Task with the same args.
"""

from __future__ import annotations

import argparse
import sys

from datalink.adapters.factory import build_adapters
from datalink.config.loader import load_settings
from datalink.logging import get_logger
from datalink.quality.retention import RetentionResult, prune_all_bronze

_log = get_logger(__name__)


def _format_row(r: RetentionResult) -> str:
    """Single-line summary suitable for stdout / Airflow logs."""
    fq = f"{r.table_schema}.{r.table_name}"
    cutoff = r.cutoff_dt.isoformat()[:19] if r.cutoff_dt else "—"
    wm = r.watermark_dt.isoformat()[:19] if r.watermark_dt else "—"
    kept = r.oldest_kept_dt.isoformat()[:19] if r.oldest_kept_dt else "—"
    return (
        f"  {fq:<32}  status={r.status:<18}  "
        f"cutoff={cutoff}  silver_wm={wm}  "
        f"eligible={r.rows_eligible:>6}  pruned={r.rows_pruned:>6}  "
        f"oldest_kept={kept}" + (f"  err={r.error[:80]}" if r.error else "")
    )


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--days",
        type=int,
        default=90,
        help="Retention window in days. Rows with _load_dt older than this "
        "are eligible for prune. Default 90 (industry-standard for "
        "healthcare DQ).",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Don't actually delete; just report what would happen.",
    )
    args = p.parse_args(argv)

    if args.days < 0:
        print("ERROR: --days must be >= 0.", file=sys.stderr)
        return 2

    settings = load_settings()
    adapters = build_adapters(settings)

    # Phase 9.5: ensure CONTROL.bronze_retention_log exists. ensure() is
    # idempotent — re-creates only what's missing — so this is safe on
    # every invocation. Also picks up any new tables added in 9.x DDL
    # without an explicit migration script.
    from datalink.quality.control import PipelineControl

    PipelineControl(adapters.warehouse).ensure()

    print(f"\nBronze retention prune — days={args.days}  {'DRY-RUN' if args.dry_run else 'LIVE'}\n")
    results = prune_all_bronze(
        adapters.warehouse,
        retention_days=args.days,
        dry_run=args.dry_run,
    )

    if not results:
        print("No BRONZE_*.RAW_* tables found.")
        return 0

    print(f"=== {len(results)} table(s) processed ===")
    total_pruned = 0
    total_eligible = 0
    for r in results:
        print(_format_row(r))
        total_pruned += r.rows_pruned
        total_eligible += r.rows_eligible

    # Summary line — useful for Airflow XCom + ops dashboards.
    skipped = sum(1 for r in results if r.status.startswith("SKIPPED"))
    failed = sum(1 for r in results if r.status == "FAILED")
    print()
    print(
        f"Summary: total_eligible={total_eligible:>6}  "
        f"total_pruned={total_pruned:>6}  "
        f"skipped={skipped}  failed={failed}"
    )
    if failed > 0:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
