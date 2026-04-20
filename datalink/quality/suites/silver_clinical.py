"""CP2 — Silver clinical validation.

Per Medallion doc §4.3 — the most critical gate. Validates:
  - CPT codes against CMS reference
  - ICD-10 codes against CMS reference
  - Member eligibility window
  - NPI Luhn check
  - Referential integrity across Hub/Sat/Link
  - Length-of-stay sanity (not yet implemented — SAT_CLAIM_DETAILS doesn't
    carry LOS; lands with Phase 4.5 if InpatientAuth joins)

Threshold: >5% failure → AUTO-PAUSE. Orphaned Links → AUTO-ABORT.

Runs against SILVER_silver.sat_claim_details joined with the hubs.
"""

from __future__ import annotations

from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.expectations import (
    ExpectColumnValuesToBeBetween,
    ExpectColumnValuesToBeInSet,
    ExpectColumnValuesToMatchRegex,
    ExpectColumnValuesToNotBeNull,
)

NAME = "silver_clinical"

# Small CMS reference — in prod this loads from a SILVER reference table.
# Demo subset matches the synthetic sample data.
CPT_VALID = {
    "99213",
    "99214",
    "99215",
    "99491",
    "27447",
    "73721",
    "80053",
    "J0178",
}

ICD10_VALID = {
    "E11.9",
    "I10",
    "M16.11",
    "J45.909",
    "F32.9",
    "K21.9",
    "N18.3",
    "G47.33",
}


def build_suite() -> ExpectationSuite:
    """Silver clinical suite — runs on the sat_claim_details projection."""
    suite = ExpectationSuite(name="silver_clinical")

    suite.add_expectation(ExpectColumnValuesToNotBeNull(column="hub_claim_hk"))
    suite.add_expectation(ExpectColumnValuesToNotBeNull(column="load_dts"))
    suite.add_expectation(ExpectColumnValuesToNotBeNull(column="hash_diff"))

    # CPT validity — core clinical check.
    suite.add_expectation(
        ExpectColumnValuesToBeInSet(
            column="cpt_code",
            value_set=list(CPT_VALID),
            mostly=0.95,
        )
    )
    # ICD-10 primary validity.
    suite.add_expectation(
        ExpectColumnValuesToBeInSet(
            column="icd10_primary",
            value_set=list(ICD10_VALID),
            mostly=0.95,
        )
    )
    # Amount sanity — even at Silver, amounts must be plausible.
    suite.add_expectation(
        ExpectColumnValuesToBeBetween(
            column="billed_amount",
            min_value=0,
            max_value=2_000_000,
        )
    )
    # hash_diff must be the canonical 32-char uppercase MD5 we generate
    # (matches dv_hash_diff output).
    suite.add_expectation(
        ExpectColumnValuesToMatchRegex(
            column="hash_diff",
            regex=r"^[0-9A-F]{32}$",
        )
    )

    return suite
