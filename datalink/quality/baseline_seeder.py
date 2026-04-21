"""Seed CONTROL.dq_suites with the hand-coded Python suites as v1 LIVE baselines.

Phase 5.8 introduces the DQ Control Plane — a DB-backed, version-tracked,
multi-tenant registry for expectation suites. But all existing pipelines
validate against the 3 hand-coded suites at `datalink/quality/suites/*.py`.

To preserve continuity, on first boot we:
  1. Check if `client_id='default'` has any rows in dq_suites.
  2. If not, convert each `build_*_suite()` result to JSON and insert as:
       (client_id='default', version=1, status=LIVE, source=baseline_python)
  3. From that point on, the runtime checkpoint uses `SuiteRegistry.get_live`
     which returns the LIVE row (this seeded one, until a UI-authored
     version is approved + activated).

Idempotent — safe to call every `docker compose up` / every DAG run.
"""

from __future__ import annotations

from typing import Any

from datalink.adapters.protocols import Warehouse
from datalink.logging import get_logger
from datalink.quality.control import CONTROL_SCHEMA
from datalink.quality.registry import (
    DqDimension,
    SuiteDraft,
    SuiteRegistry,
    SuiteSource,
)

_log = get_logger(__name__)

DEFAULT_CLIENT = "default"


# ----------------------------------------------------------------------------
# Hand-authored JSON baselines — these MIRROR the GX objects the suite files
# instantiate programmatically in `datalink/quality/suites/*.py`.
#
# Why duplicate them as JSON here (rather than introspecting the Python suite):
# GX 1.x's ExpectationSuite objects don't round-trip cleanly to JSON in
# current versions (internal IDs, ephemeral wrappers). Hand-authoring the
# JSON is cleaner, and — since UI edits will diverge anyway — locking the
# baselines here becomes the "schema spec" for what the UI allows.
# ----------------------------------------------------------------------------


# NOTE on the "meta" block: these annotations are what the UI renders as
# dimension tags + severity badges. They're not passed to GX (GX ignores
# unknown keys in `meta`), so we're safe to add UI-only fields.

BRONZE_STRUCTURAL_BASELINE: list[dict[str, Any]] = [
    # -------- ROW-LEVEL STRUCTURE --------
    {
        "expectation_type": "expect_table_row_count_to_be_between",
        "kwargs": {"min_value": 100, "max_value": 10_000_000},
        "meta": {
            "dq_dimension": DqDimension.TIMELINESS.value,
            "severity": "MEDIUM",
            "description": "Row-count sanity — detects truncated or exploded files.",
        },
    },
    # -------- SCHEMA DRIFT --------
    {
        "expectation_type": "expect_table_columns_to_match_ordered_list",
        "kwargs": {
            "column_list": [
                "claim_id",
                "member_id",
                "provider_npi",
                "cpt_code",
                "icd10_primary",
                "icd10_secondary",
                "service_date",
                "billed_amount",
                "claim_status",
                "plan_id",
                "prior_auth_ref",
                "_load_dt",
                "_source_file",
                "_batch_id",
                "_record_source",
            ]
        },
        "meta": {
            "dq_dimension": DqDimension.VALIDITY.value,
            "severity": "HIGH",
            "description": "Column order must match Bronze DDL — schema-drift detector.",
        },
    },
    # -------- COMPLETENESS (natural keys) --------
    {
        "expectation_type": "expect_column_values_to_not_be_null",
        "kwargs": {"column": "claim_id"},
        "meta": {
            "dq_dimension": DqDimension.COMPLETENESS.value,
            "severity": "HIGH",
            "description": "Every claim must have a natural key.",
        },
    },
    {
        "expectation_type": "expect_column_values_to_not_be_null",
        "kwargs": {"column": "member_id"},
        "meta": {
            "dq_dimension": DqDimension.COMPLETENESS.value,
            "severity": "HIGH",
            "description": "Every claim must be tied to a member.",
        },
    },
    {
        "expectation_type": "expect_column_values_to_not_be_null",
        "kwargs": {"column": "provider_npi"},
        "meta": {
            "dq_dimension": DqDimension.COMPLETENESS.value,
            "severity": "HIGH",
            "description": "Every claim must name a provider.",
        },
    },
    # -------- UNIQUENESS (PK candidates) --------
    {
        "expectation_type": "expect_column_values_to_be_unique",
        "kwargs": {"column": "claim_id"},
        "meta": {
            "dq_dimension": DqDimension.UNIQUENESS.value,
            "severity": "HIGH",
            "description": "claim_id is the Bronze primary key — duplicates break MERGE.",
        },
    },
    # -------- VALIDITY (format / regex) --------
    {
        "expectation_type": "expect_column_values_to_match_regex",
        "kwargs": {"column": "provider_npi", "regex": r"^\d{10}$", "mostly": 0.98},
        "meta": {
            "dq_dimension": DqDimension.VALIDITY.value,
            "severity": "HIGH",
            "description": "NPI must be 10 digits (CMS standard). Allow 2% dirty.",
        },
    },
    # -------- VALIDITY (range) --------
    {
        "expectation_type": "expect_column_values_to_be_between",
        "kwargs": {"column": "billed_amount", "min_value": 0, "max_value": 2_000_000},
        "meta": {
            "dq_dimension": DqDimension.VALIDITY.value,
            "severity": "MEDIUM",
            "description": "Billed amount must be non-negative and below fraud-threshold.",
        },
    },
    # -------- VALIDITY (value set) --------
    {
        "expectation_type": "expect_column_values_to_be_in_set",
        "kwargs": {
            "column": "claim_status",
            "value_set": ["SUBMITTED", "APPROVED", "DENIED", "PENDED"],
        },
        "meta": {
            "dq_dimension": DqDimension.VALIDITY.value,
            "severity": "HIGH",
            "description": "Claim status must be one of the 4 standard codes.",
        },
    },
]


SILVER_CLINICAL_BASELINE: list[dict[str, Any]] = [
    # -------- VALIDITY (CPT format) --------
    {
        "expectation_type": "expect_column_values_to_match_regex",
        "kwargs": {"column": "cpt_code", "regex": r"^\d{5}$|^[A-Z]\d{4}$", "mostly": 0.95},
        "meta": {
            "dq_dimension": DqDimension.VALIDITY.value,
            "severity": "HIGH",
            "description": "CPT: 5 digits (Category I) or HCPCS alpha + 4 digits.",
        },
    },
    # -------- VALIDITY (ICD-10 format) --------
    {
        "expectation_type": "expect_column_values_to_match_regex",
        "kwargs": {
            "column": "icd10_primary",
            "regex": r"^[A-TV-Z]\d{2}(\.\w{1,4})?$",
            "mostly": 0.98,
        },
        "meta": {
            "dq_dimension": DqDimension.VALIDITY.value,
            "severity": "HIGH",
            "description": "Primary ICD-10 must conform to CMS/WHO format.",
        },
    },
    # -------- VALIDITY (positive amounts) --------
    {
        "expectation_type": "expect_column_values_to_be_between",
        "kwargs": {"column": "billed_amount", "min_value": 0.01, "max_value": 2_000_000},
        "meta": {
            "dq_dimension": DqDimension.VALIDITY.value,
            "severity": "MEDIUM",
            "description": "Silver claims must have strictly-positive billed amounts.",
        },
    },
    # -------- COMPLETENESS (clinical keys) --------
    {
        "expectation_type": "expect_column_values_to_not_be_null",
        "kwargs": {"column": "cpt_code"},
        "meta": {
            "dq_dimension": DqDimension.COMPLETENESS.value,
            "severity": "HIGH",
            "description": "Silver requires CPT — cleaning happened at this tier.",
        },
    },
]


GOLD_BUSINESS_BASELINE: list[dict[str, Any]] = [
    # -------- CONSISTENCY (A > B date ordering) --------
    {
        "expectation_type": "expect_column_pair_values_a_to_be_greater_than_b",
        "kwargs": {"column_A": "auth_due_date", "column_B": "auth_from_date", "or_equal": True},
        "meta": {
            "dq_dimension": DqDimension.CONSISTENCY.value,
            "severity": "HIGH",
            "description": "Auth due-date must not be before its from-date.",
        },
    },
    # -------- COMPLETENESS (PK) --------
    {
        "expectation_type": "expect_column_values_to_not_be_null",
        "kwargs": {"column": "patient_auth_pk"},
        "meta": {
            "dq_dimension": DqDimension.COMPLETENESS.value,
            "severity": "HIGH",
            "description": "Gold surrogate key must always populate.",
        },
    },
    # -------- UNIQUENESS (PK) --------
    {
        "expectation_type": "expect_column_values_to_be_unique",
        "kwargs": {"column": "patient_auth_pk"},
        "meta": {
            "dq_dimension": DqDimension.UNIQUENESS.value,
            "severity": "HIGH",
            "description": "patient_auth_pk is the operational-DB upsert key.",
        },
    },
    # -------- VALIDITY (status set) --------
    {
        "expectation_type": "expect_column_values_to_be_in_set",
        "kwargs": {
            "column": "auth_status",
            "value_set": ["PENDING", "APPROVED", "DENIED", "PARTIAL", "WITHDRAWN"],
        },
        "meta": {
            "dq_dimension": DqDimension.VALIDITY.value,
            "severity": "HIGH",
            "description": "Auth status must map to a Lu_AuthStatus code.",
        },
    },
]


BASELINES: dict[str, list[dict[str, Any]]] = {
    "bronze_structural": BRONZE_STRUCTURAL_BASELINE,
    "silver_clinical": SILVER_CLINICAL_BASELINE,
    "gold_business": GOLD_BUSINESS_BASELINE,
}


def seed_baselines(warehouse: Warehouse, client_id: str = DEFAULT_CLIENT) -> int:
    """Insert the 3 baseline suites as v1 LIVE for the given client, iff none exist yet.

    Returns the number of suites seeded (0 if already present — idempotent).
    """
    rows = warehouse.query(
        f"SELECT COUNT(*) AS c FROM {CONTROL_SCHEMA}.dq_suites "
        "WHERE client_id = $c AND source = $src",
        {"c": client_id, "src": SuiteSource.BASELINE_PYTHON.value},
    )
    already_seeded = int(rows[0]["c"]) if rows else 0
    if already_seeded >= len(BASELINES):
        return 0

    reg = SuiteRegistry(warehouse)
    seeded = 0
    for suite_name, expectations in BASELINES.items():
        existing = reg.list_versions(client_id, suite_name)
        if existing:
            continue
        dims = sorted({e["meta"].get("dq_dimension") for e in expectations if e.get("meta")})
        draft = SuiteDraft(
            client_id=client_id,
            suite_name=suite_name,
            expectations=expectations,
            created_by="system:baseline_seeder",
            source=SuiteSource.BASELINE_PYTHON,
            dq_dimensions=[d for d in dims if d],
        )
        suite_id = reg.create_draft(draft)
        # Fast-track: DRAFT → PENDING_REVIEW → APPROVED → LIVE in one shot.
        # Baselines ARE pre-approved — they ARE the Phase 5 code.
        reg.submit_for_review(suite_id, actor="system:baseline_seeder")
        reg.approve(
            suite_id,
            actor="system:baseline_seeder",
            notes="baseline python suite — auto-approved on first seed",
        )
        reg.activate(suite_id, actor="system:baseline_seeder")
        seeded += 1
        _log.info(
            "dq_suite.baseline_seeded",
            suite_name=suite_name,
            client_id=client_id,
            expectation_count=len(expectations),
        )

    return seeded
