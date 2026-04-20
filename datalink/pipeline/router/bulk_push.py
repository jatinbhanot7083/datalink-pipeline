"""Dual-warehouse bulk-push router.

Reads Gold UM operational tables from the warehouse, pushes them to every
enabled OperationalDb target. Config selects targets via
features.warehouse_router.targets — a list of keys into
adapters.operational_dbs.

Idempotency: bulk_upsert on natural key. Re-running produces the same
final state in each target. This is the proof for the Phase 4 verify —
flip config, re-run, only selected targets change.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from datalink.adapters.factory import AdapterSet
from datalink.adapters.protocols import OperationalDb
from datalink.config.loader import Settings
from datalink.logging import get_logger
from datalink.pipeline.router.schema import ALL_UM_TABLES, GoldUmTable

_log = get_logger(__name__)


@dataclass(frozen=True)
class PushTargetResult:
    target: str  # "postgres", "sqlserver", "postgres_replica", …
    target_type: str  # "postgres" | "sqlserver" | …
    tables_written: dict[str, int]  # target_table → rowcount
    error: str | None = None  # None on success; message on failure

    @property
    def total_rows(self) -> int:
        return sum(self.tables_written.values())


@dataclass(frozen=True)
class PushResult:
    source_schema: str  # "SILVER_gold_um" on DuckDB
    targets_requested: list[str]
    targets_skipped: list[str]  # targets in config but not reachable/registered
    per_target: list[PushTargetResult] = field(default_factory=list)

    @property
    def all_green(self) -> bool:
        return all(t.error is None for t in self.per_target)


def push_gold_um_to_operational(
    adapters: AdapterSet,
    settings: Settings,
    source_schema: str = "SILVER_gold_um",
    tables: list[GoldUmTable] | None = None,
) -> PushResult:
    """Fan Gold UM tables out to every enabled operational DB target.

    Args:
      source_schema: the schema in the warehouse that holds the gold_* tables
        (DuckDB creates `SILVER_gold_um` when dbt profile.schema=SILVER and
        the model overrides +schema=gold_um).
      tables: subset of GoldUmTable to push (default: all).
    """
    tables = tables if tables is not None else ALL_UM_TABLES
    requested = list(settings.features.warehouse_router.targets)
    skipped: list[str] = []
    per_target: list[PushTargetResult] = []

    _log.info(
        "router.push.start",
        targets=requested,
        tables=[t.target for t in tables],
        source_schema=source_schema,
    )

    for target_name in requested:
        op_db = adapters.operational_dbs.get(target_name)
        if op_db is None:
            _log.warning(
                "router.push.target_not_registered",
                target=target_name,
                available=list(adapters.operational_dbs),
            )
            skipped.append(target_name)
            continue

        target_cfg = settings.adapters.operational_dbs.get(target_name)
        target_type = target_cfg.type if target_cfg else "unknown"

        # Bootstrap: create target schema + tables if missing.
        try:
            _ensure_schema(op_db, target_type)
        except Exception as exc:
            _log.error(
                "router.push.schema_bootstrap_failed",
                target=target_name,
                error=str(exc),
            )
            per_target.append(
                PushTargetResult(
                    target=target_name,
                    target_type=target_type,
                    tables_written={},
                    error=f"schema bootstrap: {exc}",
                )
            )
            continue

        # Push each table.
        written: dict[str, int] = {}
        error: str | None = None
        for tbl in tables:
            try:
                source_qualified = f"{source_schema}.{tbl.source}"
                rows = adapters.warehouse.query(f"SELECT * FROM {source_qualified}")
                # Normalize column names to lowercase to match target DDL.
                rows_lc = [{k.lower(): v for k, v in r.items()} for r in rows]
                count = op_db.bulk_upsert(tbl.target, rows_lc, tbl.pk_columns)
                written[tbl.target] = count
                _log.info(
                    "router.push.table_done",
                    target=target_name,
                    table=tbl.target,
                    rows=count,
                )
            except Exception as exc:
                error = f"{tbl.target}: {exc}"
                _log.error(
                    "router.push.table_failed",
                    target=target_name,
                    table=tbl.target,
                    error=str(exc),
                )
                break  # stop at first failure — operator investigates

        per_target.append(
            PushTargetResult(
                target=target_name,
                target_type=target_type,
                tables_written=written,
                error=error,
            )
        )

    result = PushResult(
        source_schema=source_schema,
        targets_requested=requested,
        targets_skipped=skipped,
        per_target=per_target,
    )
    _log.info(
        "router.push.done",
        all_green=result.all_green,
        target_count=len(per_target),
        skipped_count=len(skipped),
    )
    return result


def _ensure_schema(op_db: OperationalDb, target_type: str) -> None:
    """Run the DDL for this target dialect."""
    from datalink.pipeline.router.schema import postgres_ddl, sqlserver_ddl

    if target_type == "postgres":
        op_db.execute_script(postgres_ddl())
    elif target_type == "sqlserver":
        op_db.execute_script(sqlserver_ddl())
    else:
        raise ValueError(f"Unknown operational_db type: {target_type!r}")
