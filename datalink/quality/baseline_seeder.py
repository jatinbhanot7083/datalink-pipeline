"""Seed CONTROL.dq_suites with one LIVE baseline per (client, source, stage).

Phase 6 refactor: the 3 stage-level baselines of Phase 5.8 are replaced by
9 per-(source_type, stage) baselines — NO MERGING across sources. This
matches Jatin's explicit directive for Phase 6:

    "Membership for Client1 has its own suite, Claim for Client1 has its
    own; DO NOT MERGE OR MIX"  (~135 suites = 15 clients x 3 sources x 3 stages)

Naming convention:  f"{stage}_{source_lower}"  — e.g.:

    bronze_claims, bronze_membership, bronze_provider,
    silver_claims, silver_membership, silver_provider,
    gold_claims,   gold_membership,   gold_provider

For backward compatibility with Phase 5.7 scripts that still reference the
3 aggregate suite names (`bronze_structural` / `silver_clinical` /
`gold_business`), those 3 LEGACY_BASELINES are ALSO seeded (kept as Phase
5.8 "aggregate" rows). They carry `source_type=NULL` and are marked by the
Control Tower UI as legacy. The 9 per-source baselines are the authoritative
source going forward.

Idempotent — safe to call every `docker compose up` / every DAG run.
"""

from __future__ import annotations

from typing import Any

from datalink.adapters.protocols import Warehouse
from datalink.logging import get_logger
from datalink.quality.registry import (
    DqDimension,
    SuiteDraft,
    SuiteRegistry,
    SuiteSource,
    SuiteStatus,
)

_log = get_logger(__name__)

DEFAULT_CLIENT = "default"

# Canonical source_type tags — case matches what bronze.ingest.ingest_file uses.
SOURCE_CLAIMS = "CLAIMS"
SOURCE_MEMBERSHIP = "MEMBERSHIP"
SOURCE_PROVIDER = "PROVIDER"
ALL_SOURCES = (SOURCE_CLAIMS, SOURCE_MEMBERSHIP, SOURCE_PROVIDER)

# Phase 6 production clients - the 6 real payer tenants Jatin flagged.
# Seeding these on first boot means the Control Tower client-dropdown
# is populated out of the box (no manual onboarding step for demo). Each
# client gets the full 9 per-source + 3 legacy baseline suite set = 12
# suites per client * 6 clients = 72 tenant suites + 12 for 'default' = 84 total.
REAL_CLIENTS: tuple[str, ...] = (
    "aetna",
    "caresource",
    "affinity",
    "coaccess",
    "dhmp",
    "hcsc",
)


# ============================================================================
# BRONZE baselines — structural checks on RAW_<SOURCE> tables.
#
# Every Bronze expectation is ROW-level and ASSUMES the 4 audit columns
# (_load_dt, _source_file, _batch_id, _record_source) are already appended
# by datalink.pipeline.bronze.ingest._load_to_staging.
# ============================================================================


def _not_null(col: str, severity: str, desc: str, mostly: float | None = None) -> dict[str, Any]:
    kw: dict[str, Any] = {"column": col}
    if mostly is not None:
        kw["mostly"] = mostly
    return {
        "expectation_type": "expect_column_values_to_not_be_null",
        "kwargs": kw,
        "meta": {
            "dq_dimension": DqDimension.COMPLETENESS.value,
            "severity": severity,
            "description": desc,
        },
    }


def _unique(col: str, severity: str, desc: str) -> dict[str, Any]:
    return {
        "expectation_type": "expect_column_values_to_be_unique",
        "kwargs": {"column": col},
        "meta": {
            "dq_dimension": DqDimension.UNIQUENESS.value,
            "severity": severity,
            "description": desc,
        },
    }


def _regex(col: str, regex: str, severity: str, desc: str, mostly: float = 1.0) -> dict[str, Any]:
    return {
        "expectation_type": "expect_column_values_to_match_regex",
        "kwargs": {"column": col, "regex": regex, "mostly": mostly},
        "meta": {
            "dq_dimension": DqDimension.VALIDITY.value,
            "severity": severity,
            "description": desc,
        },
    }


def _between(
    col: str,
    lo: float,
    hi: float,
    severity: str,
    desc: str,
    dim: DqDimension = DqDimension.VALIDITY,
) -> dict[str, Any]:
    return {
        "expectation_type": "expect_column_values_to_be_between",
        "kwargs": {"column": col, "min_value": lo, "max_value": hi},
        "meta": {
            "dq_dimension": dim.value,
            "severity": severity,
            "description": desc,
        },
    }


def _in_set(col: str, values: list[str], severity: str, desc: str) -> dict[str, Any]:
    return {
        "expectation_type": "expect_column_values_to_be_in_set",
        "kwargs": {"column": col, "value_set": values},
        "meta": {
            "dq_dimension": DqDimension.VALIDITY.value,
            "severity": severity,
            "description": desc,
        },
    }


def _row_count_between(lo: int, hi: int, severity: str, desc: str) -> dict[str, Any]:
    return {
        "expectation_type": "expect_table_row_count_to_be_between",
        "kwargs": {"min_value": lo, "max_value": hi},
        "meta": {
            "dq_dimension": DqDimension.TIMELINESS.value,
            "severity": severity,
            "description": desc,
        },
    }


# ---------- Phase 9.1: shared audit-column expectations -------------------
# Every Bronze suite validates that the 7 audit cols are populated. Phase
# 9.1 added _load_type / _file_row_number / _record_hash; legacy rows (pre-
# migration) have NULL for these so we use mostly=0.99 to tolerate the
# small backfill gap. Once history rolls past the migration date, we can
# tighten to mostly=1.0.
#
# IMPORTANT: post Phase 9.1 we removed the natural-key uniqueness asserts
# from Bronze (claim_id, npi). Bronze is now an immutable APPEND-ONLY
# ledger — same key WILL legitimately appear in multiple batches. Natural-
# key uniqueness moved to Silver Hub (Phase 9.2).

_BRONZE_AUDIT_COLS: list[dict[str, Any]] = [
    _not_null("_load_dt", "HIGH", "Audit: every row must carry a load timestamp.", mostly=1.0),
    _not_null("_batch_id", "HIGH", "Audit: every row must carry a batch_id.", mostly=1.0),
    _not_null(
        "_source_file", "HIGH", "Audit: every row must record its source filename.", mostly=1.0
    ),
    _not_null("_load_type", "MEDIUM", "Audit: load_type must be set (Phase 9.1).", mostly=0.99),
    _in_set(
        "_load_type",
        ["FULL", "INCREMENTAL", "UNKNOWN"],
        "MEDIUM",
        "load_type must be one of three controlled values.",
    ),
    _not_null(
        "_record_hash",
        "MEDIUM",
        "Audit: record_hash powers Silver SCD2 diffing (Phase 9.1).",
        mostly=0.99,
    ),
    _not_null(
        "_file_row_number",
        "LOW",
        "Audit: row position within source file (Phase 9.1).",
        mostly=0.99,
    ),
]


# ---------- BRONZE_CLAIMS — validates BRONZE.RAW_CLAIMS ----------

BRONZE_CLAIMS_BASELINE: list[dict[str, Any]] = [
    _row_count_between(100, 10_000_000, "MEDIUM", "Claim volume sanity — detects truncated files."),
    _not_null("claim_id", "HIGH", "Every claim must have a natural key."),
    _not_null("member_id", "HIGH", "Every claim must tie to a member."),
    _not_null("provider_npi", "HIGH", "Every claim must name a provider."),
    # claim_id uniqueness moved to Silver Hub (Phase 9.1) — Bronze can
    # legitimately have the same claim_id in multiple batches.
    _regex("provider_npi", r"^\d{10}$", "HIGH", "NPI must be 10 digits (CMS).", mostly=0.98),
    _between(
        "billed_amount", 0, 2_000_000, "MEDIUM", "Billed amount non-negative + fraud-threshold."
    ),
    _in_set(
        "claim_status",
        ["SUBMITTED", "APPROVED", "DENIED", "PENDED"],
        "HIGH",
        "Claim status must be one of 4 standard codes.",
    ),
    *_BRONZE_AUDIT_COLS,
]


# ---------- BRONZE_MEMBERSHIP — validates BRONZE.RAW_MEMBERSHIP ----------

BRONZE_MEMBERSHIP_BASELINE: list[dict[str, Any]] = [
    _row_count_between(100, 10_000_000, "MEDIUM", "Enrollment volume sanity."),
    _not_null("member_id", "HIGH", "Every enrollment row must name a member."),
    _not_null("plan_id", "HIGH", "Enrollment requires a plan reference."),
    # member_id + plan_id uniqueness is a Silver Hub concern; in Bronze
    # the same (member_id, plan_id) pair appears in every batch the
    # member shows up in.
    _not_null("effective_date", "HIGH", "Effective date required."),
    *_BRONZE_AUDIT_COLS,
]


# ---------- BRONZE_PROVIDER — validates BRONZE.RAW_PROVIDER ----------

BRONZE_PROVIDER_BASELINE: list[dict[str, Any]] = [
    _row_count_between(1, 10_000_000, "LOW", "Provider registry row-count sanity."),
    _not_null("npi", "HIGH", "Every provider row requires an NPI."),
    # NPI uniqueness moved to Silver Hub (Phase 9.1).
    _regex("npi", r"^\d{10}$", "HIGH", "NPI must be 10 digits (CMS)."),
    *_BRONZE_AUDIT_COLS,
]


# ============================================================================
# SILVER baselines — Silver DV 2.0 satellite checks.
#
# Silver's per-source split: sat_claim_details / sat_member_demographics /
# sat_provider_info. Each suite targets its own satellite. Accuracy +
# Consistency checks require cross-system joins so they surface at Silver+.
# ============================================================================

# Phase 9.2: shared SCD2 expectations applied to every Silver Sat. Each
# Sat now carries explicit ``effective_start_date`` / ``effective_end_date``
# / ``is_active`` columns; we assert they're populated. These are the
# columns Gold reads to materialize "current state" tables, so a missing
# value here = a broken downstream contract.
_SILVER_SCD2_COLS: list[dict[str, Any]] = [
    _not_null(
        "effective_start_date",
        "HIGH",
        "SCD2: every Silver row must have effective_start_date.",
    ),
    _not_null(
        "is_active",
        "HIGH",
        "SCD2: every Silver row must declare its is_active state (TRUE/FALSE).",
    ),
    # effective_end_date is NULL on the active row by design, so no
    # not-null check on that column.
]


SILVER_CLAIMS_BASELINE: list[dict[str, Any]] = [
    _regex(
        "cpt_code",
        r"^\d{5}$|^[A-Z]\d{4}$",
        "HIGH",
        "CPT: 5 digits (Cat I) or HCPCS alpha+4.",
        mostly=0.95,
    ),
    _regex(
        "icd10_primary",
        r"^[A-TV-Z]\d{2}(\.\w{1,4})?$",
        "HIGH",
        "Primary ICD-10 must conform to CMS/WHO format.",
        mostly=0.98,
    ),
    _between(
        "billed_amount",
        0.01,
        2_000_000,
        "MEDIUM",
        "Silver claims must have strictly-positive billed amounts.",
    ),
    _not_null("cpt_code", "HIGH", "Silver requires CPT — cleaning happened at this tier."),
    *_SILVER_SCD2_COLS,
]


SILVER_MEMBERSHIP_BASELINE: list[dict[str, Any]] = [
    # Phase 9.2: sat_member_demographics doesn't expose member_id/plan_id
    # (those live on hub_member). We assert hub_member_hk presence + the
    # descriptive attrs that the Sat actually carries.
    _not_null("hub_member_hk", "HIGH", "Every Silver member row must reference its hub."),
    _not_null("dob", "MEDIUM", "DOB required for UM workflows.", mostly=0.99),
    *_SILVER_SCD2_COLS,
]


SILVER_PROVIDER_BASELINE: list[dict[str, Any]] = [
    _not_null("hub_provider_hk", "HIGH", "Every Silver provider row must reference its hub."),
    _not_null("provider_name", "MEDIUM", "Provider name required.", mostly=0.99),
    *_SILVER_SCD2_COLS,
]


# ============================================================================
# GOLD baselines — UM operational business-rule checks.
#
# Gold UM aggregates all 3 sources into auth-centric tables. Each per-source
# suite validates the aspect of Gold its source is responsible for.
# ============================================================================

GOLD_CLAIMS_BASELINE: list[dict[str, Any]] = [
    {
        "expectation_type": "expect_column_pair_values_a_to_be_greater_than_b",
        "kwargs": {"column_A": "auth_due_date", "column_B": "auth_from_date", "or_equal": True},
        "meta": {
            "dq_dimension": DqDimension.CONSISTENCY.value,
            "severity": "HIGH",
            "description": "Auth due-date must not be before its from-date (72h TAT standard).",
        },
    },
    _not_null("patient_auth_pk", "HIGH", "Gold surrogate key must always populate."),
    _unique("patient_auth_pk", "HIGH", "patient_auth_pk is the ops-DB upsert key."),
    _in_set(
        "auth_status",
        ["PENDING", "APPROVED", "DENIED", "PARTIAL", "WITHDRAWN"],
        "HIGH",
        "Auth status must map to a Lu_AuthStatus code.",
    ),
]


GOLD_MEMBERSHIP_BASELINE: list[dict[str, Any]] = [
    # Gold member aspects are enforced on gold_patient_auth (patient_id_text column).
    _not_null("patient_id_text", "HIGH", "Every auth row must tie to a member (patient_id)."),
]


GOLD_PROVIDER_BASELINE: list[dict[str, Any]] = [
    _not_null("provider_npi", "HIGH", "Every auth row must tie to a provider NPI."),
    _regex(
        "provider_npi",
        r"^\d{10}$",
        "HIGH",
        "Provider NPI in Gold must be 10-digit CMS format.",
    ),
]


# ============================================================================
# 9-suite registry: per (stage, source_type) → baseline.
# ============================================================================


BASELINES_PER_SOURCE: dict[tuple[str, str], list[dict[str, Any]]] = {
    # Bronze
    ("bronze_claims", SOURCE_CLAIMS): BRONZE_CLAIMS_BASELINE,
    ("bronze_membership", SOURCE_MEMBERSHIP): BRONZE_MEMBERSHIP_BASELINE,
    ("bronze_provider", SOURCE_PROVIDER): BRONZE_PROVIDER_BASELINE,
    # Silver DV
    ("silver_claims", SOURCE_CLAIMS): SILVER_CLAIMS_BASELINE,
    ("silver_membership", SOURCE_MEMBERSHIP): SILVER_MEMBERSHIP_BASELINE,
    ("silver_provider", SOURCE_PROVIDER): SILVER_PROVIDER_BASELINE,
    # Gold UM
    ("gold_claims", SOURCE_CLAIMS): GOLD_CLAIMS_BASELINE,
    ("gold_membership", SOURCE_MEMBERSHIP): GOLD_MEMBERSHIP_BASELINE,
    ("gold_provider", SOURCE_PROVIDER): GOLD_PROVIDER_BASELINE,
}


# Legacy aggregate suites — kept for Phase 5.8 backward-compatibility with
# scripts/demo_phase_5.py + tests/phase/test_phase_5.py which still reference
# the 3 stage-level constants. source_type=NULL marks them as legacy.
LEGACY_BASELINES: dict[str, list[dict[str, Any]]] = {
    "bronze_structural": BRONZE_CLAIMS_BASELINE,  # Bronze aggregate == claims (same table validated)
    "silver_clinical": SILVER_CLAIMS_BASELINE,
    "gold_business": GOLD_CLAIMS_BASELINE,
}


def seed_baselines(warehouse: Warehouse, client_id: str = DEFAULT_CLIENT) -> int:
    """Insert the 9 per-source + 3 legacy suites as v1 LIVE for `client_id`.

    Idempotent — returns the number of NEW suites seeded this call. On the
    very first invocation for a fresh client, seeds 12 (9 per-source + 3
    legacy). Subsequent calls return 0.
    """
    reg = SuiteRegistry(warehouse)
    seeded = 0

    # Seed 9 per-source suites first.
    for (suite_name, source_type), expectations in BASELINES_PER_SOURCE.items():
        # Skip only if an ACTIVE version exists (LIVE / APPROVED / PENDING_REVIEW
        # / DRAFT). If every prior version is ARCHIVED or REJECTED — e.g.
        # after a Phase 9.1-style baseline refresh — we DO want to create
        # a fresh version with the latest expectations. The state machine
        # auto-bumps version numbers, so this never collides.
        existing = reg.list_versions(client_id, suite_name)
        terminal = {SuiteStatus.ARCHIVED, SuiteStatus.REJECTED}
        if any(v.status not in terminal for v in existing):
            continue
        dims = sorted({e["meta"].get("dq_dimension") for e in expectations if e.get("meta")})
        draft = SuiteDraft(
            client_id=client_id,
            suite_name=suite_name,
            expectations=expectations,
            created_by="system:baseline_seeder",
            source=SuiteSource.BASELINE_PYTHON,
            dq_dimensions=[d for d in dims if d],
            source_type=source_type,
        )
        suite_id = reg.create_draft(draft)
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
            source_type=source_type,
            expectation_count=len(expectations),
        )

    # Seed 3 legacy aggregate suites for backward compat. source_type=NULL.
    for suite_name, expectations in LEGACY_BASELINES.items():
        # Skip only if an ACTIVE version exists (LIVE / APPROVED / PENDING_REVIEW
        # / DRAFT). If every prior version is ARCHIVED or REJECTED — e.g.
        # after a Phase 9.1-style baseline refresh — we DO want to create
        # a fresh version with the latest expectations. The state machine
        # auto-bumps version numbers, so this never collides.
        existing = reg.list_versions(client_id, suite_name)
        terminal = {SuiteStatus.ARCHIVED, SuiteStatus.REJECTED}
        if any(v.status not in terminal for v in existing):
            continue
        dims = sorted({e["meta"].get("dq_dimension") for e in expectations if e.get("meta")})
        draft = SuiteDraft(
            client_id=client_id,
            suite_name=suite_name,
            expectations=expectations,
            created_by="system:baseline_seeder",
            source=SuiteSource.BASELINE_PYTHON,
            dq_dimensions=[d for d in dims if d],
            source_type=None,  # legacy aggregate — no per-source tag
        )
        suite_id = reg.create_draft(draft)
        reg.submit_for_review(suite_id, actor="system:baseline_seeder")
        reg.approve(
            suite_id,
            actor="system:baseline_seeder",
            notes="legacy aggregate suite — retained for Phase 5.8 scripts",
        )
        reg.activate(suite_id, actor="system:baseline_seeder")
        seeded += 1
        _log.info(
            "dq_suite.legacy_baseline_seeded",
            suite_name=suite_name,
            client_id=client_id,
            expectation_count=len(expectations),
        )

    return seeded


# ----------------------------------------------------------------------------
# Aliases kept for Phase 5.7 code that still imports these symbols directly.
# DO NOT remove without updating scripts/demo_phase_5.py + tests/phase/test_phase_5.py.
# ----------------------------------------------------------------------------

BRONZE_STRUCTURAL_BASELINE = BRONZE_CLAIMS_BASELINE
SILVER_CLINICAL_BASELINE = SILVER_CLAIMS_BASELINE
GOLD_BUSINESS_BASELINE = GOLD_CLAIMS_BASELINE

BASELINES = LEGACY_BASELINES  # historical name


# ----------------------------------------------------------------------------
# Phase 6: seed every real client on first boot so dashboards + client
# dropdowns show a populated tenant list.
# ----------------------------------------------------------------------------


def seed_all_real_clients(warehouse: Warehouse) -> dict[str, int]:
    """Seed the 6 production payer tenants with baseline suites.

    Idempotent — each client gets its 12 suites on first call, 0 on
    subsequent calls. Returns map of client_id -> suites_seeded.
    """
    result: dict[str, int] = {DEFAULT_CLIENT: seed_baselines(warehouse, DEFAULT_CLIENT)}
    for client in REAL_CLIENTS:
        result[client] = seed_baselines(warehouse, client)
    _log.info("dq_suite.all_real_clients_seeded", result=result)
    return result
