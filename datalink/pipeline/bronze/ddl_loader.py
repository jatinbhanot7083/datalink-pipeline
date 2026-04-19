"""Load Bronze DDL from .sql files and run it against a warehouse adapter.

DDL files are template-driven — `{schema}` is replaced with the caller's
target schema (e.g., "BRONZE" for DuckDB, or "DATALINK_BRONZE.PUBLIC" for
Snowflake). All other SQL is dialect-agnostic (VARCHAR, NUMBER, DATE, TIMESTAMP).
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from datalink.adapters.protocols import Warehouse

DDL_DIR = Path(__file__).parent / "ddl"


@dataclass(frozen=True)
class BronzeTable:
    name: str  # e.g., "RAW_CLAIMS"
    key_columns: list[str]  # e.g., ["claim_id"]
    ddl_filename: str  # e.g., "raw_claims.sql"
    source_type: str  # matches the source-file kind: "CLAIMS", "MEMBERSHIP", "PROVIDER"


BRONZE_TABLES: dict[str, BronzeTable] = {
    "CLAIMS": BronzeTable(
        name="RAW_CLAIMS",
        key_columns=["claim_id"],
        ddl_filename="raw_claims.sql",
        source_type="CLAIMS",
    ),
    "MEMBERSHIP": BronzeTable(
        name="RAW_MEMBERSHIP",
        key_columns=["member_id", "plan_id"],
        ddl_filename="raw_membership.sql",
        source_type="MEMBERSHIP",
    ),
    "PROVIDER": BronzeTable(
        name="RAW_PROVIDER",
        key_columns=["npi"],
        ddl_filename="raw_provider.sql",
        source_type="PROVIDER",
    ),
}


def create_bronze_schema(warehouse: Warehouse, schema: str = "BRONZE") -> None:
    """Idempotently create the Bronze schema + all 3 tables in the given warehouse."""
    # Schema creation — adapter-specific helper (DuckDB) vs explicit SQL (Snowflake).
    create_schema = getattr(warehouse, "create_schema_if_not_exists", None)
    if callable(create_schema):
        create_schema(schema)
    else:
        warehouse.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    for table in BRONZE_TABLES.values():
        ddl_path = DDL_DIR / table.ddl_filename
        sql = ddl_path.read_text(encoding="utf-8").replace("{schema}", schema)
        # DuckDB's execute doesn't accept multi-statement strings reliably;
        # our DDL files are single-statement though, so this is fine.
        warehouse.execute(sql)


def qualified_name(table: BronzeTable, schema: str = "BRONZE") -> str:
    return f"{schema}.{table.name}"
