"""Gold UM → Operational DB table map.

One entry per operational table fed from Gold. Column types are PostgreSQL
(used to generate the Postgres DDL). The SQL Server DDL is generated from
the same map with a dialect-adapter (see ddl.py).

Sourced from EvokeConnectCare_UM_Gold_Layer_v2.docx §2.1 — this is a subset
of the full 20+ UM tables; Phase 4 ships a representative set (5 transactional
+ 3 Lu* lookups). Remaining tables are a Phase 4.5 follow-up.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class GoldUmTable:
    source: str  # Gold model name in DuckDB (e.g., "gold_patient_auth")
    target: str  # Operational table name (e.g., "patient_auth")
    pk_columns: list[str]
    columns: list[tuple[str, str]]  # (column_name, postgres_type) in insert order
    is_lookup: bool = False


# Gold UM operational tables (5 transactional).
GOLD_UM_TABLES: list[GoldUmTable] = [
    GoldUmTable(
        source="gold_patient_auth",
        target="patient_auth",
        pk_columns=["patient_auth_id"],
        columns=[
            ("patient_auth_id", "INTEGER"),
            ("auth_code", "VARCHAR(50)"),
            ("patient_id_text", "VARCHAR(100)"),
            ("hierarchy_id", "VARCHAR(100)"),
            ("auth_type_id", "INTEGER"),
            ("auth_status_id", "INTEGER"),
            ("auth_from_date", "DATE"),
            ("auth_due_date", "TIMESTAMP"),
            ("provider_npi", "VARCHAR(10)"),
            ("requested_amount", "DECIMAL(18,2)"),
            ("source_claim_id", "VARCHAR(100)"),
            ("created_on", "TIMESTAMP"),
            ("record_source", "VARCHAR(100)"),
        ],
    ),
    GoldUmTable(
        source="gold_auth_decision",
        target="auth_decision",
        pk_columns=["auth_decision_id"],
        columns=[
            ("auth_decision_id", "INTEGER"),
            ("patient_auth_id", "INTEGER"),
            ("decision_status_id", "INTEGER"),
            ("decision_date", "DATE"),
            ("total_requested", "DECIMAL(18,2)"),
            ("total_approved", "DECIMAL(18,2)"),
            ("denial_reason", "VARCHAR(200)"),
            ("created_on", "TIMESTAMP"),
            ("record_source", "VARCHAR(100)"),
        ],
    ),
    GoldUmTable(
        source="gold_auth_code",
        target="auth_code",
        pk_columns=["auth_code_id"],
        columns=[
            ("auth_code_id", "INTEGER"),
            ("patient_auth_id", "INTEGER"),
            ("auth_code_type", "VARCHAR(20)"),
            ("auth_code_ref", "VARCHAR(50)"),
            ("requested_units", "INTEGER"),
            ("approved_units", "INTEGER"),
            ("from_date", "DATE"),
            ("to_date", "TIMESTAMP"),
            ("is_primary", "BOOLEAN"),
            ("created_on", "TIMESTAMP"),
            ("record_source", "VARCHAR(100)"),
        ],
    ),
    GoldUmTable(
        source="gold_auth_diagnoses",
        target="auth_diagnoses",
        pk_columns=["auth_diagnosis_id"],
        columns=[
            ("auth_diagnosis_id", "INTEGER"),
            ("patient_auth_id", "INTEGER"),
            ("diagnosis_code", "VARCHAR(10)"),
            ("sequence_number", "INTEGER"),
            ("is_primary", "BOOLEAN"),
            ("created_on", "TIMESTAMP"),
            ("record_source", "VARCHAR(100)"),
        ],
    ),
    GoldUmTable(
        source="gold_auth_provider",
        target="auth_provider",
        pk_columns=["auth_provider_id"],
        columns=[
            ("auth_provider_id", "INTEGER"),
            ("patient_auth_id", "INTEGER"),
            ("provider_npi", "VARCHAR(10)"),
            ("provider_name", "VARCHAR(200)"),
            ("specialty_code", "VARCHAR(20)"),
            ("network_status", "VARCHAR(30)"),
            ("provider_role", "VARCHAR(30)"),
            ("created_on", "TIMESTAMP"),
            ("record_source", "VARCHAR(100)"),
        ],
    ),
]

# Lu* lookup tables (3 — static reference data seeded by dbt).
GOLD_UM_LOOKUPS: list[GoldUmTable] = [
    GoldUmTable(
        source="lu_auth_status",
        target="lu_auth_status",
        pk_columns=["lu_auth_status_id"],
        columns=[
            ("lu_auth_status_id", "INTEGER"),
            ("name", "VARCHAR(50)"),
            ("description", "VARCHAR(500)"),
        ],
        is_lookup=True,
    ),
    GoldUmTable(
        source="lu_decision_status",
        target="lu_decision_status",
        pk_columns=["lu_decision_status_id"],
        columns=[
            ("lu_decision_status_id", "INTEGER"),
            ("name", "VARCHAR(50)"),
            ("description", "VARCHAR(500)"),
        ],
        is_lookup=True,
    ),
    GoldUmTable(
        source="lu_auth_type",
        target="lu_auth_type",
        pk_columns=["lu_auth_type_id"],
        columns=[
            ("lu_auth_type_id", "INTEGER"),
            ("name", "VARCHAR(50)"),
            ("description", "VARCHAR(500)"),
        ],
        is_lookup=True,
    ),
]


ALL_UM_TABLES: list[GoldUmTable] = GOLD_UM_LOOKUPS + GOLD_UM_TABLES


# ---------------------------------------------------------------------------
# DDL generators — dialect-aware. Used by operational adapters on startup to
# ensure target tables exist before the first bulk_upsert.
# ---------------------------------------------------------------------------


def postgres_ddl(tables: list[GoldUmTable] | None = None, schema: str = "um") -> str:
    """Emit PostgreSQL DDL for the given tables (default: all UM tables)."""
    tables = tables if tables is not None else ALL_UM_TABLES
    lines = [f"CREATE SCHEMA IF NOT EXISTS {schema};"]
    for t in tables:
        col_defs = ",\n    ".join(f"{c} {typ}" for c, typ in t.columns)
        pk = ", ".join(t.pk_columns)
        lines.append(
            f"CREATE TABLE IF NOT EXISTS {schema}.{t.target} (\n"
            f"    {col_defs},\n"
            f"    PRIMARY KEY ({pk})\n"
            f");"
        )
    return "\n\n".join(lines)


def sqlserver_ddl(tables: list[GoldUmTable] | None = None, schema: str = "UM") -> str:
    """Emit SQL Server DDL using [schema].[TableName] bracket convention."""
    tables = tables if tables is not None else ALL_UM_TABLES
    # SQL Server doesn't have BOOLEAN → BIT. TIMESTAMP → DATETIME2.
    type_map = {"BOOLEAN": "BIT", "TIMESTAMP": "DATETIME2"}

    def _map_type(t: str) -> str:
        upper = t.upper()
        for src, dst in type_map.items():
            if upper.startswith(src):
                return dst + t[len(src) :]
        return t

    out: list[str] = [
        f"IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{schema}') "
        f"EXEC('CREATE SCHEMA [{schema}]');"
    ]
    for t in tables:
        # Convert snake_case → PascalCase for SQL Server identifiers per UM-Gold-v2 §5
        pascal_target = "".join(p.capitalize() for p in t.target.split("_"))
        col_defs = ",\n    ".join(f"[{c}] {_map_type(typ)}" for c, typ in t.columns)
        pk = ", ".join(f"[{c}]" for c in t.pk_columns)
        out.append(
            f"IF NOT EXISTS (SELECT 1 FROM sys.tables t "
            f"JOIN sys.schemas s ON s.schema_id = t.schema_id "
            f"WHERE s.name = '{schema}' AND t.name = '{pascal_target}') "
            f"CREATE TABLE [{schema}].[{pascal_target}] (\n"
            f"    {col_defs},\n"
            f"    PRIMARY KEY ({pk})\n"
            f");"
        )
    return "\n\n".join(out)
