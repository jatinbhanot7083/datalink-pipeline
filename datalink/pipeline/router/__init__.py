"""Dual-warehouse bulk-push router.

Reads Gold UM operational tables from the warehouse (DuckDB local,
Snowflake prod) and pushes them to every enabled target in
features.warehouse_router.targets. Each target is a separate
OperationalDb adapter instance (Postgres, SQL Server, or future).

Idempotent: bulk_upsert is INSERT-or-UPDATE on the natural key, so
re-running the push produces the same final state in each target.
"""

from datalink.pipeline.router.bulk_push import (
    PushResult,
    PushTargetResult,
    push_gold_um_to_operational,
)
from datalink.pipeline.router.schema import GOLD_UM_TABLES, GoldUmTable

__all__ = [
    "GOLD_UM_TABLES",
    "GoldUmTable",
    "PushResult",
    "PushTargetResult",
    "push_gold_um_to_operational",
]
