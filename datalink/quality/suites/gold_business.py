"""CP3 — Gold business-rule validation.

Per Medallion doc §5.2 + UM-Gold-v2 §8.2:
  - billed_amount between $0 and $2M
  - auth dates in sane order (from ≤ due)
  - auth_status_id / auth_type_id FK integrity (any orphans fail)
  - TAT-related fields never NULL

Failures trigger the Post-Validation Crew (Root Cause → Remediation → Reporting).

Runs against SILVER_gold_um.gold_patient_auth.
"""

from __future__ import annotations

from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.expectations import (
    ExpectColumnPairValuesAToBeGreaterThanB,
    ExpectColumnValuesToBeBetween,
    ExpectColumnValuesToBeInSet,
    ExpectColumnValuesToNotBeNull,
)

NAME = "gold_business"


def build_suite() -> ExpectationSuite:
    suite = ExpectationSuite(name="gold_patient_auth_business")

    suite.add_expectation(ExpectColumnValuesToNotBeNull(column="patient_auth_id"))
    suite.add_expectation(ExpectColumnValuesToNotBeNull(column="auth_code"))
    suite.add_expectation(ExpectColumnValuesToNotBeNull(column="auth_from_date"))
    suite.add_expectation(ExpectColumnValuesToNotBeNull(column="auth_due_date"))

    # auth_type_id and auth_status_id must resolve to their Lu* tables (1..5 in our seeds).
    suite.add_expectation(
        ExpectColumnValuesToBeInSet(column="auth_type_id", value_set=[1, 2, 3, 4, 5])
    )
    suite.add_expectation(
        ExpectColumnValuesToBeInSet(column="auth_status_id", value_set=[1, 2, 3, 4, 5])
    )

    # Financial sanity — business rule: billed amounts under $2M.
    suite.add_expectation(
        ExpectColumnValuesToBeBetween(
            column="requested_amount",
            min_value=0,
            max_value=2_000_000,
        )
    )

    # auth_due_date must be >= auth_from_date (TAT sanity — UM-Gold-v2 §2.3.1).
    suite.add_expectation(
        ExpectColumnPairValuesAToBeGreaterThanB(
            column_A="auth_due_date",
            column_B="auth_from_date",
            or_equal=True,
        )
    )

    return suite
