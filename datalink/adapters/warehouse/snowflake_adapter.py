"""SnowflakeWarehouse — prod warehouse adapter."""

from __future__ import annotations

from typing import Any

from datalink.config.models import WarehouseConfig


class SnowflakeWarehouse:
    def __init__(self, cfg: WarehouseConfig) -> None:
        self._cfg = cfg

    def execute(self, sql: str, params: dict[str, Any] | None = None) -> None:
        raise NotImplementedError("SnowflakeWarehouse lands in Phase 4 (prod path)")

    def query(self, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        raise NotImplementedError("SnowflakeWarehouse lands in Phase 4 (prod path)")

    def copy_from_stage(
        self,
        stage_uri: str,
        target_table: str,
        file_format: str = "csv",
        options: dict[str, Any] | None = None,
    ) -> int:
        raise NotImplementedError("SnowflakeWarehouse lands in Phase 4 (prod path)")

    def merge(self, target: str, source: str, key_columns: list[str]) -> int:
        raise NotImplementedError("SnowflakeWarehouse lands in Phase 4 (prod path)")

    def close(self) -> None:
        return None
