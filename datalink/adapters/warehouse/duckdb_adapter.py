"""DuckDBWarehouse — local emulator for Snowflake.

Gaps from Snowflake papered over here:
  - `copy_from_stage` emulates Snowflake's COPY INTO FROM @stage by reading
    the file directly from the ObjectStore (Azurite) and using DuckDB's
    read_csv_auto / read_parquet into an INSERT INTO ... SELECT.
  - `merge` issues DuckDB's MERGE (supported since 1.0).
  - VARIANT type: we do not use it in Bronze (flat columns). When added later,
    a dbt macro maps to DuckDB's JSON type.

Phase 2 fills in execute/query/copy_from_stage/merge.
"""

from __future__ import annotations

from typing import Any

from datalink.config.models import WarehouseConfig


class DuckDBWarehouse:
    def __init__(self, cfg: WarehouseConfig) -> None:
        self._cfg = cfg
        self._conn: Any | None = None

    def execute(self, sql: str, params: dict[str, Any] | None = None) -> None:
        raise NotImplementedError("DuckDBWarehouse.execute lands in Phase 2")

    def query(self, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        raise NotImplementedError("DuckDBWarehouse.query lands in Phase 2")

    def copy_from_stage(
        self,
        stage_uri: str,
        target_table: str,
        file_format: str = "csv",
        options: dict[str, Any] | None = None,
    ) -> int:
        raise NotImplementedError(
            "DuckDBWarehouse.copy_from_stage lands in Phase 2 (emulates COPY INTO)"
        )

    def merge(self, target: str, source: str, key_columns: list[str]) -> int:
        raise NotImplementedError("DuckDBWarehouse.merge lands in Phase 2")

    def close(self) -> None:
        if self._conn is not None:
            # DuckDB connection close will land in Phase 2
            self._conn = None
