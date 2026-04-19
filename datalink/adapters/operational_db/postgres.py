"""PostgresOperationalDb — future prod OLTP target."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from datalink.config.models import OperationalDbConfig


class PostgresOperationalDb:
    def __init__(self, name: str, cfg: OperationalDbConfig) -> None:
        self.name = name
        self._cfg = cfg

    def bulk_upsert(
        self,
        table: str,
        rows: Iterable[dict[str, Any]],
        key_columns: list[str],
    ) -> int:
        raise NotImplementedError(
            "PostgresOperationalDb.bulk_upsert lands in Phase 4 (COPY + INSERT ON CONFLICT)"
        )

    def execute(self, sql: str, params: dict[str, Any] | None = None) -> None:
        raise NotImplementedError("PostgresOperationalDb.execute lands in Phase 4")

    def close(self) -> None:
        return None
