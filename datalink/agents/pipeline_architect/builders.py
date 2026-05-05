"""Deterministic artifact builders for Phase 15 Pipeline Architect.

The Global Gold Catalog IS the contract. Generating Gold DDL, Silver SQL,
Airflow DAG, and GX expectation suite from it is a *code transformation* —
it doesn't need an LLM. The agent's LLM is reserved for human-readable
judgment (clone-vs-build narrative, override review). The builders below
produce identical artifacts on every run for the same catalog version.

Each builder takes:
  - ``client_id``: 'aetna', 'caresource', etc.
  - ``dataset_code``: 'membership', 'member_claims'
  - ``catalog_fields``: list of dicts from CONTROL.global_bronze_catalog_fields
  - ``overrides``: optional list of client_field_overrides rows
  - ``bronze_anchor``: FHIR | X12 | NCPDP | FLAT_FILE | API

and returns plain text (DDL string, SQL string, DAG python string, etc.)
so the bridge layer can write to disk and persist artifact pointers.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal

# ---------------------------------------------------------------------------
# Logical-type → SQL-type map (Snowflake-portable).
# ---------------------------------------------------------------------------

_LOGICAL_TO_SQL: dict[str, str] = {
    "TEXT": "VARCHAR",
    "INTEGER": "INTEGER",
    "DECIMAL": "DECIMAL(20, 4)",
    "DATE": "DATE",
    "TIMESTAMP": "TIMESTAMP",
    "BOOLEAN": "BOOLEAN",
}

# Mandatory audit columns appended to every Gold table — same set
# Phase 14 uses for Bronze. Keeping them consistent across layers
# means lineage joins on (_batch_id, _record_source) just work.
GOLD_AUDIT_COLUMNS: list[tuple[str, str]] = [
    ("_load_dt", "TIMESTAMP"),
    ("_batch_id", "VARCHAR"),
    ("_record_source", "VARCHAR"),
    ("_record_hash", "VARCHAR"),
    ("_silver_run_id", "VARCHAR"),
    ("_gold_effective_dt", "TIMESTAMP"),
]

# Bronze anchor → human-readable description used in DAG docstrings.
BRONZE_ANCHOR_DESCRIPTIONS: dict[str, str] = {
    "FHIR": "HL7 FHIR R4 JSON resources (Bundle / individual resources)",
    "X12": "X12 EDI 5010 (837P/I claims, 834 enrollment, 270/271 eligibility)",
    "NCPDP": "NCPDP D.0 telecom (pharmacy claims)",
    "FLAT_FILE": "Pipe-delimited or CSV vendor extract",
    "API": "Vendor REST/GraphQL API (paginated JSON)",
}


# ---------------------------------------------------------------------------
# Override application
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ResolvedField:
    """A field in the global catalog with overrides applied."""

    gold_column_name: str
    field_display_name: str
    requirement: str  # 'Required' | 'Optional'
    logical_type: str
    description: str
    is_pii: bool
    is_phi: bool
    is_business_key: bool
    is_added_by_override: bool = False
    override_kind: str | None = None
    override_rationale: str | None = None


def apply_overrides(
    catalog_fields: list[dict[str, Any]],
    overrides: list[dict[str, Any]],
) -> list[ResolvedField]:
    """Layer client overrides on top of the global catalog rows.

    Override kinds (matches CONTROL.client_field_overrides.override_kind):
      * ADD_FIELD       — append a brand-new column for this client
      * RELAX_NULLABLE  — flip Required → Optional
      * TIGHTEN_NULLABLE — flip Optional → Required
      * RENAME          — change gold_column_name (rare, breaks downstream)
      * TYPE_CHANGE     — change logical_type
      * EXCLUDE         — drop this column from the client's Gold

    Returns the resolved list in catalog order, with EXCLUDE rows removed
    and ADD_FIELD rows appended at the end.
    """
    by_col: dict[str, dict[str, Any]] = {}
    excludes: set[str] = set()
    added: list[dict[str, Any]] = []
    for ov in overrides:
        kind = (ov.get("override_kind") or "").upper()
        col = (ov.get("gold_column_name") or "").lower()
        if kind == "ADD_FIELD":
            added.append(ov)
        elif kind == "EXCLUDE":
            excludes.add(col)
        else:
            by_col[col] = ov

    resolved: list[ResolvedField] = []
    for row in catalog_fields:
        col = (row.get("gold_column_name") or "").lower()
        if col in excludes:
            continue
        active_ov = by_col.get(col)
        requirement = row.get("requirement", "Optional")
        logical_type = row.get("logical_type", "TEXT")
        gold_column_name = row.get("gold_column_name", "")
        ov_kind: str | None = None
        ov_reason: str | None = None
        if active_ov is not None:
            ov_kind = active_ov.get("override_kind")
            ov_reason = active_ov.get("rationale")
            override_value = _parse_json(active_ov.get("override_value"))
            if isinstance(override_value, dict):
                if ov_kind == "RELAX_NULLABLE":
                    requirement = "Optional"
                elif ov_kind == "TIGHTEN_NULLABLE":
                    requirement = "Required"
                elif ov_kind == "TYPE_CHANGE":
                    logical_type = override_value.get("logical_type", logical_type)
                elif ov_kind == "RENAME":
                    gold_column_name = override_value.get("gold_column_name", gold_column_name)

        resolved.append(
            ResolvedField(
                gold_column_name=gold_column_name,
                field_display_name=row.get("field_display_name", ""),
                requirement=requirement,
                logical_type=logical_type,
                description=row.get("description", "") or "",
                is_pii=bool(row.get("is_pii", False)),
                is_phi=bool(row.get("is_phi", False)),
                is_business_key=bool(row.get("is_business_key", False)),
                override_kind=ov_kind,
                override_rationale=ov_reason,
            )
        )

    # Append ADD_FIELD overrides
    for ov in added:
        ov_value = _parse_json(ov.get("override_value")) or {}
        resolved.append(
            ResolvedField(
                gold_column_name=ov_value.get("gold_column_name", ov.get("gold_column_name", "")),
                field_display_name=ov_value.get(
                    "field_display_name", ov.get("gold_column_name", "")
                ),
                requirement=ov_value.get("requirement", "Optional"),
                logical_type=ov_value.get("logical_type", "TEXT"),
                description=ov_value.get("description", "") or "",
                is_pii=bool(ov_value.get("is_pii", False)),
                is_phi=bool(ov_value.get("is_phi", False)),
                is_business_key=bool(ov_value.get("is_business_key", False)),
                is_added_by_override=True,
                override_kind="ADD_FIELD",
                override_rationale=ov.get("rationale"),
            )
        )
    return resolved


def _parse_json(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, dict | list):
        return value
    try:
        return json.loads(str(value))
    except (TypeError, json.JSONDecodeError):
        return None


# ---------------------------------------------------------------------------
# Naming helpers — schema + table conventions match the existing codebase
# (BRONZE_AETNA / SILVER_AETNA / GOLD_AETNA), upper-case schema, lower-case
# table.
# ---------------------------------------------------------------------------


def schema_name(layer: Literal["BRONZE", "SILVER", "GOLD"], client_id: str) -> str:
    return f"{layer}_{client_id.upper()}"


def silver_table_name(dataset_code: str) -> str:
    return f"{dataset_code}_clean"


def gold_table_name(dataset_code: str) -> str:
    return dataset_code  # Gold matches the catalog code 1:1


def bronze_table_name(dataset_code: str) -> str:
    return f"raw_{dataset_code}"


# ---------------------------------------------------------------------------
# Gold DDL builder — the canonical Gold table for this client.
# ---------------------------------------------------------------------------


def build_gold_ddl(
    *,
    client_id: str,
    dataset_code: str,
    resolved_fields: list[ResolvedField],
    catalog_version: int = 1,
) -> str:
    """Render a CREATE TABLE for the client's Gold copy of the dataset.

    Phase 16.1 (Wave 1 Item 2):
      * Uses the shared ``_ddl_format.render_create_table()`` helper so commas
        sit BEFORE inline comments (Snowflake parses them as separators) and
        the last column has no trailing comma.
      * NOT NULL only on TRUE business keys + cols that are required AND not
        added by an override. All other cols nullable — Gold Layer handles
        downstream null handling via dbt SQL + GX expectations, not via
        CHECK-constraint friction at COPY-INTO time.
    """
    from datalink.agents.pipeline_architect._ddl_format import (
        ColumnDef,
        render_create_table,
    )

    fq = f"{schema_name('GOLD', client_id)}.{gold_table_name(dataset_code)}"
    business_cols: list[ColumnDef] = []
    for rf in resolved_fields:
        sql_type = _LOGICAL_TO_SQL.get(rf.logical_type.upper(), "VARCHAR")
        # Phase 16.1 — only true business keys and required-non-override fields
        # get NOT NULL. Everything else is permissive (let GX flag violations,
        # not the table). Conditional-required fields (like medicare_id only
        # required for Medicare members) become NULL at the DDL level.
        is_required = rf.requirement == "Required" and not rf.is_added_by_override
        nullable = not (is_required and rf.is_business_key)
        comment_bits: list[str] = []
        if rf.field_display_name and rf.field_display_name != rf.gold_column_name:
            comment_bits.append(rf.field_display_name)
        if rf.is_pii:
            comment_bits.append("[PII]")
        if rf.is_phi:
            comment_bits.append("[PHI]")
        if rf.is_business_key:
            comment_bits.append("[BUSINESS_KEY]")
        if rf.override_kind:
            comment_bits.append(f"[OVERRIDE:{rf.override_kind}]")
        comment = (
            " — ".join(comment_bits)
            if comment_bits
            else (rf.description[:80] if rf.description else "")
        )
        business_cols.append(
            ColumnDef(
                name=rf.gold_column_name, sql_type=sql_type, nullable=nullable, comment=comment
            )
        )

    audit_cols = [ColumnDef(name=n, sql_type=t, nullable=True) for n, t in GOLD_AUDIT_COLUMNS]

    return render_create_table(
        fully_qualified_table=fq,
        business_columns=business_cols,
        audit_columns=audit_cols,
        header_comments=[
            f"Phase 15 Pipeline Architect — Gold table for {client_id} / {dataset_code}",
            f"Generated from CONTROL.global_bronze_catalog_fields (catalog_version={catalog_version})",
            f"Generated at: {datetime.now(UTC).isoformat()}",
        ],
        business_section_label="Gold business columns",
        audit_section_label="DataLink Gold audit columns (auto-injected)",
    )


# ---------------------------------------------------------------------------
# Silver dbt SQL builder — staging layer between Bronze and Gold.
# Conservative defaults: SELECT-AS with type casts + null filters.
# ---------------------------------------------------------------------------


def build_silver_dbt_sql(
    *,
    client_id: str,
    dataset_code: str,
    resolved_fields: list[ResolvedField],
    bronze_anchor: str,
) -> str:
    """Generate a dbt model SQL for SILVER.{dataset}_clean."""
    bronze_schema = schema_name("BRONZE", client_id)
    bronze_table = bronze_table_name(dataset_code)

    select_lines: list[str] = []
    where_clauses: list[str] = []
    for rf in resolved_fields:
        col = rf.gold_column_name
        sql_type = _LOGICAL_TO_SQL.get(rf.logical_type.upper(), "VARCHAR")
        # Cast bronze raw value to the target type. Conservative: TRY_CAST
        # so a malformed row becomes NULL in Silver instead of failing.
        select_lines.append(f"    TRY_CAST({col} AS {sql_type})  AS {col}")
        if rf.requirement == "Required" and not rf.is_added_by_override:
            where_clauses.append(f"{col} IS NOT NULL")

    body = ",\n".join(select_lines)
    where_sql = " AND ".join(where_clauses[:8]) if where_clauses else "1=1"
    if len(where_clauses) > 8:
        where_sql += "  /* truncated to first 8 NOT NULL guards; full set in GX suite */"

    return f"""-- Phase 15 Pipeline Architect — Silver dbt model
-- Source : {bronze_schema}.{bronze_table}  (bronze_anchor={bronze_anchor})
-- Target : SILVER_{client_id.upper()}.{silver_table_name(dataset_code)}
-- Pattern: NORMALIZED (TRY_CAST + required-not-null filter)

{{{{ config(
    materialized = 'incremental',
    unique_key   = '_record_hash',
    on_schema_change = 'fail'
) }}}}

SELECT
{body},
    _load_dt,
    _batch_id,
    _record_source,
    _record_hash
FROM {{{{ source('bronze_{client_id.lower()}', '{bronze_table}') }}}}
WHERE {where_sql}
{{% if is_incremental() %}}
  AND _load_dt > (SELECT COALESCE(MAX(_load_dt), '1900-01-01') FROM {{{{ this }}}})
{{% endif %}}
"""


# ---------------------------------------------------------------------------
# Gold dbt SQL builder — the deduplicated, business-keyed version.
# ---------------------------------------------------------------------------


def build_gold_dbt_sql(
    *,
    client_id: str,
    dataset_code: str,
    resolved_fields: list[ResolvedField],
) -> str:
    """Generate a dbt model SQL for GOLD.{dataset}.

    Uses a deduplication window over business keys + _load_dt so the
    Gold table holds the latest snapshot per business entity. Falls
    back to _record_hash when no business key is flagged.
    """
    silver_schema_var = f"silver_{client_id.lower()}"
    silver_tbl = silver_table_name(dataset_code)
    business_keys = [rf.gold_column_name for rf in resolved_fields if rf.is_business_key]
    if not business_keys:
        business_keys = ["_record_hash"]
    partition_by = ", ".join(business_keys)

    cols_to_select = [rf.gold_column_name for rf in resolved_fields]
    cols_sql = ",\n        ".join(cols_to_select) if cols_to_select else "*"

    return f"""-- Phase 15 Pipeline Architect — Gold dbt model
-- Source : SILVER_{client_id.upper()}.{silver_tbl}
-- Target : GOLD_{client_id.upper()}.{gold_table_name(dataset_code)}
-- Pattern: latest-per-business-key snapshot (UM_OPERATIONAL)

{{{{ config(
    materialized = 'table',
    cluster_by   = [{", ".join(repr(k) for k in business_keys)}]
) }}}}

WITH ranked AS (
    SELECT
        {cols_sql},
        _load_dt,
        _batch_id,
        _record_source,
        _record_hash,
        ROW_NUMBER() OVER (
            PARTITION BY {partition_by}
            ORDER BY _load_dt DESC, _record_hash DESC
        ) AS _rn
    FROM {{{{ source('{silver_schema_var}', '{silver_tbl}') }}}}
)
SELECT
    {cols_sql},
    _load_dt,
    _batch_id,
    _record_source,
    _record_hash,
    _batch_id              AS _silver_run_id,
    CURRENT_TIMESTAMP()    AS _gold_effective_dt
FROM ranked
WHERE _rn = 1
"""


# ---------------------------------------------------------------------------
# Airflow DAG builder — one DAG per (client, dataset) with a 5-task chain:
# bronze_land → bronze_validate → silver_dbt → gold_dbt → onprem_push
# ---------------------------------------------------------------------------


def build_airflow_dag(
    *,
    client_id: str,
    dataset_code: str,
    bronze_anchor: str,
    schedule_cron: str,
    onprem_targets: list[dict[str, Any]],
    catalog_version: int = 1,
) -> str:
    """Render an Airflow DAG file (Python source) for this pipeline."""
    dag_id = f"{client_id.lower()}_{dataset_code}_pipeline"
    targets_summary = (
        ", ".join(
            f"{t.get('downstream_product')} → {t.get('target_system')}" for t in onprem_targets
        )
        or "(none)"
    )

    return f'''"""Auto-generated Airflow DAG — Phase 15 Pipeline Architect.

  client_id        : {client_id}
  dataset_code     : {dataset_code}
  bronze_anchor    : {bronze_anchor}  ({BRONZE_ANCHOR_DESCRIPTIONS.get(bronze_anchor, "n/a")})
  schedule         : {schedule_cron}
  catalog_version  : {catalog_version}
  onprem routing   : {targets_summary}

DO NOT EDIT BY HAND. Re-generate via `make rebuild-dag CLIENT={client_id} DATASET={dataset_code}`
or the Pipeline Architect UI. Manual edits are clobbered on re-deploy.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from datalink.orchestration.tasks import (
    bronze_land_task,
    bronze_validate_task,
    silver_dbt_task,
    gold_dbt_task,
    onprem_push_task,
)


DEFAULT_ARGS = {{
    "owner": "datalink",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
    "email_on_failure": False,
}}


with DAG(
    dag_id="{dag_id}",
    description="Phase 15 — {client_id} / {dataset_code} (Bronze→Silver→Gold→OnPrem)",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule="{schedule_cron}",
    catchup=False,
    tags=["datalink", "phase15", "{client_id.lower()}", "{dataset_code}", "{bronze_anchor.lower()}"],
    max_active_runs=1,
) as dag:

    start = EmptyOperator(task_id="start")

    bronze_land = PythonOperator(
        task_id="bronze_land",
        python_callable=bronze_land_task,
        op_kwargs={{
            "client_id": "{client_id}",
            "dataset_code": "{dataset_code}",
            "bronze_anchor": "{bronze_anchor}",
        }},
    )

    bronze_validate = PythonOperator(
        task_id="bronze_validate",
        python_callable=bronze_validate_task,
        op_kwargs={{
            "client_id": "{client_id}",
            "dataset_code": "{dataset_code}",
        }},
    )

    silver_dbt = PythonOperator(
        task_id="silver_dbt",
        python_callable=silver_dbt_task,
        op_kwargs={{
            "client_id": "{client_id}",
            "dataset_code": "{dataset_code}",
        }},
    )

    gold_dbt = PythonOperator(
        task_id="gold_dbt",
        python_callable=gold_dbt_task,
        op_kwargs={{
            "client_id": "{client_id}",
            "dataset_code": "{dataset_code}",
        }},
    )

    onprem_push = PythonOperator(
        task_id="onprem_push",
        python_callable=onprem_push_task,
        op_kwargs={{
            "client_id": "{client_id}",
            "dataset_code": "{dataset_code}",
            "targets": {json.dumps(onprem_targets, default=str)},
        }},
    )

    end = EmptyOperator(task_id="end")

    start >> bronze_land >> bronze_validate >> silver_dbt >> gold_dbt >> onprem_push >> end
'''


# ---------------------------------------------------------------------------
# GX expectation suite scaffold — auto-anchors per-field expectations from
# the catalog rows (required → not-null, NPI → regex, etc.).
# ---------------------------------------------------------------------------


def build_gx_suite_scaffold(
    *,
    client_id: str,
    dataset_code: str,
    resolved_fields: list[ResolvedField],
) -> dict[str, Any]:
    """Generate a GX-shaped expectation suite from the catalog.

    Returns a dict matching the structure persisted in CONTROL.dq_suites
    (suite_name + expectations array + dimensions array).
    """
    suite_name = f"gold_{client_id.lower()}_{dataset_code}"
    expectations: list[dict[str, Any]] = []

    for rf in resolved_fields:
        col = rf.gold_column_name
        # Required → NOT NULL
        if rf.requirement == "Required" and not rf.is_added_by_override:
            expectations.append(
                {
                    "expectation_type": "expect_column_values_to_not_be_null",
                    "column": col,
                    "kwargs": {},
                    "dimension": "Completeness",
                    "severity": "HIGH" if rf.is_business_key else "MEDIUM",
                    "rationale": f"Catalog requires {rf.field_display_name}",
                }
            )

        # Business key → uniqueness
        if rf.is_business_key:
            expectations.append(
                {
                    "expectation_type": "expect_column_values_to_be_unique",
                    "column": col,
                    "kwargs": {},
                    "dimension": "Uniqueness",
                    "severity": "HIGH",
                    "rationale": f"{rf.field_display_name} is the natural business key for this dataset",
                }
            )

        # NPI → 10-digit numeric regex
        name_l = (rf.field_display_name or "").lower()
        if "npi" in name_l:
            expectations.append(
                {
                    "expectation_type": "expect_column_values_to_match_regex",
                    "column": col,
                    "kwargs": {"regex": "^[0-9]{10}$"},
                    "dimension": "Validity",
                    "severity": "HIGH",
                    "rationale": "NPI must be a 10-digit numeric per CMS",
                }
            )

        # ZIP code → 5- or 9-digit
        if "zip" in name_l:
            expectations.append(
                {
                    "expectation_type": "expect_column_values_to_match_regex",
                    "column": col,
                    "kwargs": {"regex": r"^\d{5}(-?\d{4})?$"},
                    "dimension": "Validity",
                    "severity": "MEDIUM",
                    "rationale": "ZIP must be 5- or 9-digit USPS format",
                }
            )

        # Type-driven expectations
        if rf.logical_type == "DATE":
            expectations.append(
                {
                    "expectation_type": "expect_column_values_to_match_strftime_format",
                    "column": col,
                    "kwargs": {"strftime_format": "%Y-%m-%d"},
                    "dimension": "Validity",
                    "severity": "MEDIUM",
                    "rationale": "Catalog logical_type=DATE",
                }
            )

    dimensions = sorted({e["dimension"] for e in expectations})
    return {
        "suite_name": suite_name,
        "client_id": client_id,
        "source_type": dataset_code.upper(),
        "expectations": expectations,
        "dq_dimensions": dimensions,
        "expectation_count": len(expectations),
    }


# ---------------------------------------------------------------------------
# Routing plan builder — turns the catalog 'used_by' matrix + per-client
# overrides into a concrete list of OnPrem push destinations.
# ---------------------------------------------------------------------------


@dataclass
class RoutingPlanEntry:
    downstream_product: str
    target_system: str
    target_uri: str
    is_default: bool
    action: str  # ENABLE | DISABLED_BY_CLIENT_OVERRIDE | OVERRIDDEN_BY_CLIENT
    rationale: str | None = None


def build_routing_plan(
    *,
    client_id: str,
    dataset_code: str,
    default_rules: list[dict[str, Any]],
    client_overrides: list[dict[str, Any]],
) -> list[RoutingPlanEntry]:
    """Materialize the effective OnPrem routing for this (client, dataset)."""
    overrides_by_product: dict[str, dict[str, Any]] = {
        (o.get("downstream_product") or "").upper(): o for o in client_overrides
    }
    plan: list[RoutingPlanEntry] = []
    seen_products: set[str] = set()
    for rule in default_rules:
        product = (rule.get("downstream_product") or "").upper()
        seen_products.add(product)
        ov = overrides_by_product.get(product)
        if ov is None:
            plan.append(
                RoutingPlanEntry(
                    downstream_product=product,
                    target_system=rule.get("target_system", ""),
                    target_uri=rule.get("target_uri", ""),
                    is_default=True,
                    action="ENABLE",
                    rationale="Default routing from product catalog Used-by matrix",
                )
            )
            continue
        action = (ov.get("action") or "").upper()
        if action == "DISABLE":
            plan.append(
                RoutingPlanEntry(
                    downstream_product=product,
                    target_system=rule.get("target_system", ""),
                    target_uri=rule.get("target_uri", ""),
                    is_default=False,
                    action="DISABLED_BY_CLIENT_OVERRIDE",
                    rationale=ov.get("rationale"),
                )
            )
        else:
            plan.append(
                RoutingPlanEntry(
                    downstream_product=product,
                    target_system=ov.get("target_system") or rule.get("target_system", ""),
                    target_uri=ov.get("target_uri") or rule.get("target_uri", ""),
                    is_default=False,
                    action="OVERRIDDEN_BY_CLIENT",
                    rationale=ov.get("rationale"),
                )
            )

    # Pick up overrides for products NOT in the default rule set (rare —
    # operator routes to a product the catalog doesn't predict).
    for product, ov in overrides_by_product.items():
        if product in seen_products:
            continue
        if (ov.get("action") or "").upper() != "ENABLE":
            continue
        plan.append(
            RoutingPlanEntry(
                downstream_product=product,
                target_system=ov.get("target_system", ""),
                target_uri=ov.get("target_uri", ""),
                is_default=False,
                action="ENABLED_BY_CLIENT_OVERRIDE",
                rationale=ov.get("rationale"),
            )
        )

    return plan


# ---------------------------------------------------------------------------
# Diff helper — the UI needs to render override deltas vs. the global catalog.
# ---------------------------------------------------------------------------


@dataclass
class CatalogDeviation:
    gold_column_name: str
    kind: str  # ADD_FIELD | RELAX_NULLABLE | TIGHTEN_NULLABLE | RENAME | TYPE_CHANGE | EXCLUDE
    before: dict[str, Any] | None
    after: dict[str, Any] | None
    rationale: str | None = None


def diff_against_global(
    catalog_fields: list[dict[str, Any]],
    overrides: list[dict[str, Any]],
) -> list[CatalogDeviation]:
    """Compute one CatalogDeviation per override row, for UI rendering."""
    catalog_by_col: dict[str, dict[str, Any]] = {
        (f.get("gold_column_name") or "").lower(): f for f in catalog_fields
    }
    out: list[CatalogDeviation] = []
    for ov in overrides:
        col = (ov.get("gold_column_name") or "").lower()
        kind = (ov.get("override_kind") or "").upper()
        before = None
        after = _parse_json(ov.get("override_value"))
        if kind != "ADD_FIELD":
            cat_row = catalog_by_col.get(col)
            if cat_row is not None:
                before = {
                    "gold_column_name": cat_row.get("gold_column_name"),
                    "requirement": cat_row.get("requirement"),
                    "logical_type": cat_row.get("logical_type"),
                    "field_display_name": cat_row.get("field_display_name"),
                }
        out.append(
            CatalogDeviation(
                gold_column_name=ov.get("gold_column_name", col),
                kind=kind or "UNKNOWN",
                before=before,
                after=after if isinstance(after, dict) else None,
                rationale=ov.get("rationale"),
            )
        )
    return out
