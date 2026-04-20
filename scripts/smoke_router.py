"""End-to-end router smoke test.

Pushes Gold UM tables from the warehouse (DuckDB SILVER_gold_um schema) to
every target in features.warehouse_router.targets. Verifies:
  1. Full push: all 8 UM tables land in every target.
  2. Idempotency: re-running doesn't grow row counts.
  3. Config flip: with only one target, only that target gets updates.

Phase 4 exit gate — wired into `make verify-phase-4-router`.
"""

from __future__ import annotations

import contextlib
import os
import sys

from datalink.adapters.factory import build_adapters
from datalink.adapters.operational_db.postgres import PostgresOperationalDb
from datalink.config.loader import load_settings
from datalink.logging import configure_logging, get_logger
from datalink.pipeline.router import push_gold_um_to_operational
from datalink.pipeline.router.schema import ALL_UM_TABLES


def _postgres_targets(adapters) -> list[PostgresOperationalDb]:
    return [a for a in adapters.operational_dbs.values() if isinstance(a, PostgresOperationalDb)]


def _count_on_target(target: PostgresOperationalDb, table: str) -> int:
    try:
        return target.table_row_count(table, schema="um")
    except Exception:
        return -1


def _reset(target: PostgresOperationalDb) -> None:
    # Truncate all UM tables — caller already ran DDL via router bootstrap.
    for tbl in ALL_UM_TABLES:
        with contextlib.suppress(Exception):
            target.truncate(tbl.target, schema="um")


def main() -> int:
    configure_logging(level="INFO", fmt="console")
    log = get_logger("smoke_router")

    # Build adapters from local settings.
    settings = load_settings(env="local")
    adapters = build_adapters(settings)
    failures: list[str] = []

    # Reset all Postgres targets so row counts are deterministic.
    for t in _postgres_targets(adapters):
        try:
            _reset(t)
        except Exception as e:
            log.warning("reset_skipped", target=t.name, error=str(e))

    # -----------------------------------------------------------------
    # Test 1: full push — every target gets every table.
    # -----------------------------------------------------------------
    print("\n=== Test 1: full push to all targets ===")
    result = push_gold_um_to_operational(adapters, settings)
    if not result.all_green:
        failures.append(
            f"Test 1: push not all-green — {[t.error for t in result.per_target if t.error]}"
        )
    for per in result.per_target:
        print(
            f"  target={per.target:20} total_rows={per.total_rows:>5}  tables={len(per.tables_written)}"
        )

    # Every configured Postgres target should have N rows in patient_auth (same count across targets).
    pg_targets_in_config = [
        name
        for name in settings.features.warehouse_router.targets
        if settings.adapters.operational_dbs[name].type == "postgres"
    ]
    counts_per_target = {
        name: _count_on_target(adapters.operational_dbs[name], "patient_auth")
        for name in pg_targets_in_config
    }
    print(f"  patient_auth row counts per target: {counts_per_target}")
    if len(set(counts_per_target.values())) > 1:
        failures.append(f"Test 1: row counts differ across targets: {counts_per_target}")
    if not counts_per_target or next(iter(counts_per_target.values())) <= 0:
        failures.append(f"Test 1: expected patient_auth > 0 in targets; got {counts_per_target}")

    # -----------------------------------------------------------------
    # Test 2: idempotency — re-run, counts unchanged.
    # -----------------------------------------------------------------
    print("\n=== Test 2: idempotency (re-run) ===")
    before = dict(counts_per_target)
    push_gold_um_to_operational(adapters, settings)
    after = {
        name: _count_on_target(adapters.operational_dbs[name], "patient_auth")
        for name in pg_targets_in_config
    }
    print(f"  before={before}")
    print(f"  after ={after}")
    for name in pg_targets_in_config:
        if before[name] != after[name]:
            failures.append(
                f"Test 2: idempotency failed on {name}: {before[name]} -> {after[name]}"
            )

    # -----------------------------------------------------------------
    # Test 3: config flip — restrict to one target, re-run.
    # -----------------------------------------------------------------
    print("\n=== Test 3: config flip (DL_FEATURES__WAREHOUSE_ROUTER__TARGETS=postgres) ===")
    # Trailing comma forces the env-var coercer to treat this as a list.
    # Without the comma, a single-word value stays a string and Pydantic
    # rejects it (list field). Convention: use comma-separated always.
    os.environ["DL_FEATURES__WAREHOUSE_ROUTER__TARGETS"] = "postgres,"
    settings_flipped = load_settings(env="local")
    adapters_flipped = build_adapters(settings_flipped)
    print(f"  targets after flip: {settings_flipped.features.warehouse_router.targets}")
    if settings_flipped.features.warehouse_router.targets != ["postgres"]:
        failures.append(
            f"Test 3: flip did not apply — got {settings_flipped.features.warehouse_router.targets}"
        )
    # Reset postgres primary only, then push — only postgres should get data.
    _reset(adapters_flipped.operational_dbs["postgres"])
    # NB: postgres_replica retains its data from Test 2 — we are proving that
    # a targets=[postgres] run writes ONLY to postgres and does not touch replica.
    pre_replica = _count_on_target(
        adapters_flipped.operational_dbs["postgres_replica"], "patient_auth"
    )
    result_flipped = push_gold_um_to_operational(adapters_flipped, settings_flipped)
    post_replica = _count_on_target(
        adapters_flipped.operational_dbs["postgres_replica"], "patient_auth"
    )
    post_primary = _count_on_target(adapters_flipped.operational_dbs["postgres"], "patient_auth")
    print(f"  post primary={post_primary}  replica pre={pre_replica} post={post_replica}")
    if post_primary <= 0:
        failures.append("Test 3: primary got no rows after flip")
    if post_replica != pre_replica:
        failures.append(
            f"Test 3: replica changed despite being excluded from targets: "
            f"{pre_replica} -> {post_replica}"
        )
    if result_flipped.targets_requested != ["postgres"]:
        failures.append(f"Test 3: router saw wrong targets — {result_flipped.targets_requested}")

    # Cleanup env override.
    os.environ.pop("DL_FEATURES__WAREHOUSE_ROUTER__TARGETS", None)

    # -----------------------------------------------------------------
    # Close connections.
    # -----------------------------------------------------------------
    for t in _postgres_targets(adapters):
        t.close()
    for t in _postgres_targets(adapters_flipped):
        t.close()

    if failures:
        print(f"\nFAIL: {len(failures)} assertion(s):")
        for f in failures:
            print(f"  - {f}")
        return 1

    print("\n" + "=" * 60)
    print("ROUTER SMOKE TEST GREEN")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
