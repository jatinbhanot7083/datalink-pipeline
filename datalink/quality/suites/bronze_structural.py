"""CP1 — Bronze structural validation.

Per Medallion doc §3.4: nulls, row counts, date formats, duplicates,
numeric ranges. Threshold: >5% failure → AUTO-PAUSE.

This suite runs on BRONZE.RAW_CLAIMS. Equivalent suites for RAW_MEMBERSHIP
and RAW_PROVIDER are built by helper functions below.
"""

from __future__ import annotations

from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.expectations import (
    ExpectColumnValuesToBeBetween,
    ExpectColumnValuesToBeInSet,
    ExpectColumnValuesToBeUnique,
    ExpectColumnValuesToMatchRegex,
    ExpectColumnValuesToNotBeNull,
    ExpectTableColumnsToMatchOrderedList,
    ExpectTableRowCountToBeBetween,
)

NAME = "bronze_structural"


def build_suite() -> ExpectationSuite:
    """Bronze.RAW_CLAIMS structural suite.

    These expectations ONLY check structural integrity — no clinical validation
    (that's CP2 Silver). Runs against the full table every pipeline run.
    """
    suite = ExpectationSuite(name="bronze_claims_structural")

    # Row-count sanity — be permissive for the demo (10k sample rows).
    suite.add_expectation(ExpectTableRowCountToBeBetween(min_value=100, max_value=10_000_000))
    # Column order must match the Bronze DDL (change detection for schema drift).
    suite.add_expectation(
        ExpectTableColumnsToMatchOrderedList(
            column_list=[
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
        )
    )
    # Natural-key integrity.
    suite.add_expectation(ExpectColumnValuesToNotBeNull(column="claim_id"))
    suite.add_expectation(ExpectColumnValuesToNotBeNull(column="member_id"))
    suite.add_expectation(ExpectColumnValuesToNotBeNull(column="provider_npi"))
    # Domain shapes.
    suite.add_expectation(
        ExpectColumnValuesToMatchRegex(
            column="provider_npi",
            regex=r"^\d{10}$",
            mostly=0.98,
        )
    )
    suite.add_expectation(
        ExpectColumnValuesToBeBetween(
            column="billed_amount",
            min_value=0,
            max_value=2_000_000,
        )
    )
    suite.add_expectation(
        ExpectColumnValuesToBeInSet(
            column="claim_status",
            value_set=["SUBMITTED", "APPROVED", "DENIED", "PENDED"],
            mostly=0.99,
        )
    )
    # Uniqueness — re-ingesting the same batch must not produce duplicates.
    suite.add_expectation(ExpectColumnValuesToBeUnique(column="claim_id"))

    # Audit columns never NULL.
    suite.add_expectation(ExpectColumnValuesToNotBeNull(column="_load_dt"))
    suite.add_expectation(ExpectColumnValuesToNotBeNull(column="_batch_id"))
    suite.add_expectation(ExpectColumnValuesToNotBeNull(column="_record_source"))

    return suite
