"""Tests for datalink.agents.pre_validation.rules — Phase 6 Commit 3.

Each emitter is a pure function over the Profiler's profile dict, so the
tests take canned profile inputs and assert on expectation output shape.
No warehouse, no LLM, no side effects.
"""

from __future__ import annotations

from typing import Any

import pytest

from datalink.agents.pre_validation.rules import (
    DATE_PAIR_SUFFIXES,
    HEALTHCARE_REGEXES,
    emit_all_rules,
    emit_completeness_rules,
    emit_consistency_pair_rules,
    emit_enum_review_rules,
    emit_timeliness_rules,
    emit_uniqueness_rules,
    emit_validity_regex_rules,
)


def _by_type(rules: list[dict[str, Any]], expectation_type: str) -> list[dict[str, Any]]:
    return [r for r in rules if r.get("expectation_type") == expectation_type]


# ---------- completeness ----------


@pytest.mark.unit
def test_completeness_zero_nulls_is_strict_not_null() -> None:
    profile = {"MemberNo": {"null_pct": 0.0, "element_count": 100, "distinct_count": 100}}
    rules = emit_completeness_rules(profile)
    assert len(rules) == 1
    assert rules[0]["expectation_type"] == "expect_column_values_to_not_be_null"
    assert rules[0]["column"] == "MemberNo"
    assert "mostly" not in rules[0]  # strict — no soft threshold


@pytest.mark.unit
def test_completeness_subpercent_nulls_uses_mostly_099() -> None:
    """Observed 0 < null_pct < 1 → soft threshold (mostly=0.99) per doc §1.1."""
    profile = {"phone": {"null_pct": 0.3, "element_count": 1000, "distinct_count": 900}}
    rules = emit_completeness_rules(profile)
    assert len(rules) == 1
    assert rules[0]["mostly"] == 0.99


@pytest.mark.unit
def test_completeness_skips_column_with_many_nulls() -> None:
    """>=1% nulls → no completeness rule (we don't want flapping suites)."""
    profile = {"optional_field": {"null_pct": 5.0, "element_count": 100, "distinct_count": 50}}
    rules = emit_completeness_rules(profile)
    assert rules == []


# ---------- uniqueness ----------


@pytest.mark.unit
def test_uniqueness_fires_on_pk_pattern_when_observed_unique() -> None:
    profile = {"MemberNo": {"null_pct": 0.0, "element_count": 1000, "distinct_count": 1000}}
    rules = emit_uniqueness_rules(profile)
    assert len(rules) == 1
    assert rules[0]["expectation_type"] == "expect_column_values_to_be_unique"
    assert rules[0]["column"] == "MemberNo"
    assert rules[0]["dq_dimension"] == "Uniqueness"


@pytest.mark.unit
def test_uniqueness_skips_non_pk_column_even_if_unique() -> None:
    """Being observed-unique is not enough — need the PK name pattern too."""
    profile = {"random_string": {"null_pct": 0.0, "element_count": 100, "distinct_count": 100}}
    rules = emit_uniqueness_rules(profile)
    assert rules == []


@pytest.mark.unit
def test_uniqueness_skips_pk_pattern_when_duplicates_observed() -> None:
    profile = {"claim_id": {"null_pct": 0.0, "element_count": 1000, "distinct_count": 900}}
    rules = emit_uniqueness_rules(profile)
    assert rules == []  # 90% distinct < 99.9% threshold


@pytest.mark.unit
def test_uniqueness_accepts_999_threshold() -> None:
    """One straggler duplicate in 1000 rows should still qualify."""
    profile = {"claim_id": {"null_pct": 0.0, "element_count": 1000, "distinct_count": 999}}
    rules = emit_uniqueness_rules(profile)
    assert len(rules) == 1


# ---------- timeliness ----------


@pytest.mark.unit
def test_timeliness_fires_when_name_and_type_both_match() -> None:
    profile = {
        "load_datetime": {
            "dtype": "TIMESTAMP",
            "null_pct": 0.0,
            "element_count": 10,
            "distinct_count": 10,
        }
    }
    rules = emit_timeliness_rules(profile)
    assert len(rules) == 1
    r = rules[0]
    assert r["expectation_type"] == "expect_column_max_to_be_between"
    assert r["sla_hours"] == 24
    assert r["parse_strings_as_datetimes"] is True
    assert r["dq_dimension"] == "Timeliness"


@pytest.mark.unit
def test_timeliness_requires_both_name_and_type() -> None:
    """A TIMESTAMP column named `amount` should NOT fire."""
    profile_only_name = {"load_date": {"dtype": "VARCHAR", "element_count": 1}}
    profile_only_type = {"amount": {"dtype": "TIMESTAMP", "element_count": 1}}
    assert emit_timeliness_rules(profile_only_name) == []
    assert emit_timeliness_rules(profile_only_type) == []


# ---------- validity regex (healthcare standards) ----------


@pytest.mark.unit
@pytest.mark.parametrize(
    "col,expected_regex",
    [
        ("ProviderNPIId", r"^[0-9]{10}$"),
        ("npi", r"^[0-9]{10}$"),
        ("icd10_code", r"^[A-Z][0-9]{2}(\.[0-9A-Z]{1,4})?$"),
        ("diagnosis_code", r"^[A-Z][0-9]{2}(\.[0-9A-Z]{1,4})?$"),
        ("cpt_code", r"^[0-9]{4}[0-9A-Z]$"),
        ("tin", r"^[0-9]{9}$"),
        ("zip_code", r"^[0-9]{5}(-[0-9]{4})?$"),
        ("state_code", r"^[A-Z]{2}$"),
        ("home_phone", r"^[0-9]{10,11}$"),
        ("SSN", r"^[0-9]{9}$"),
        ("email_address", r"^[^@\s]+@[^@\s]+\.[^@\s]+$"),
    ],
)
def test_validity_regex_matches_expected_pattern(col: str, expected_regex: str) -> None:
    profile = {col: {"dtype": "VARCHAR", "element_count": 100, "distinct_count": 100}}
    rules = emit_validity_regex_rules(profile)
    assert len(rules) == 1, f"expected 1 rule for {col!r}, got {rules}"
    assert rules[0]["expectation_type"] == "expect_column_values_to_match_regex"
    assert rules[0]["regex"] == expected_regex
    assert rules[0]["mostly"] == 0.99
    assert rules[0]["dq_dimension"] == "Validity"


@pytest.mark.unit
def test_validity_regex_skips_unknown_column_names() -> None:
    profile = {"favorite_color": {"dtype": "VARCHAR", "element_count": 100, "distinct_count": 5}}
    assert emit_validity_regex_rules(profile) == []


@pytest.mark.unit
def test_validity_regex_dispatch_is_first_match() -> None:
    """Overlapping name hints shouldn't produce duplicate rules."""
    profile = {"member_zip_code": {"dtype": "VARCHAR", "element_count": 100, "distinct_count": 50}}
    rules = emit_validity_regex_rules(profile)
    assert len(rules) == 1  # ZIP wins; must not also fire on a generic hit


# ---------- consistency pair ----------


@pytest.mark.unit
def test_consistency_pair_matches_stop_and_start_with_same_prefix() -> None:
    profile = {
        "SERVICE_STOP_DATE": {"dtype": "DATE", "element_count": 1},
        "SERVICE_START_DATE": {"dtype": "DATE", "element_count": 1},
    }
    rules = emit_consistency_pair_rules(profile)
    assert len(rules) == 1
    r = rules[0]
    assert r["expectation_type"] == "expect_column_pair_values_a_to_be_greater_than_b"
    assert r["column_A"] == "SERVICE_STOP_DATE"
    assert r["column_B"] == "SERVICE_START_DATE"
    assert r["or_equal"] is True
    assert r["dq_dimension"] == "Consistency"


@pytest.mark.unit
def test_consistency_pair_skips_when_sibling_missing() -> None:
    profile = {"service_stop_date": {"dtype": "DATE", "element_count": 1}}
    assert emit_consistency_pair_rules(profile) == []


@pytest.mark.unit
def test_consistency_pair_thru_from() -> None:
    profile = {
        "CLAIM_THRU_DATE": {"dtype": "DATE", "element_count": 1},
        "CLAIM_FROM_DATE": {"dtype": "DATE", "element_count": 1},
    }
    rules = emit_consistency_pair_rules(profile)
    assert len(rules) == 1
    assert rules[0]["column_A"] == "CLAIM_THRU_DATE"
    assert rules[0]["column_B"] == "CLAIM_FROM_DATE"


@pytest.mark.unit
def test_date_pair_suffixes_shape() -> None:
    """Sanity: every suffix pair is distinct + non-empty."""
    assert len(DATE_PAIR_SUFFIXES) >= 5
    for stop, start in DATE_PAIR_SUFFIXES:
        assert stop
        assert start
        assert stop != start


# ---------- enum review (HITL) ----------


@pytest.mark.unit
def test_enum_review_is_only_emitter_flagging_review_required() -> None:
    """Guardrail: no auto-emitter should slip review_required=True."""
    profile = {
        "StatusFlag": {
            "dtype": "VARCHAR",
            "null_pct": 0.0,
            "element_count": 100,
            "distinct_count": 3,
        },
        "MemberNo": {
            "dtype": "VARCHAR",
            "null_pct": 0.0,
            "element_count": 100,
            "distinct_count": 100,
        },
    }
    rules = emit_all_rules(profile)
    flagged = [r for r in rules if r.get("review_required")]
    # Exactly one flagged rule — the enum on StatusFlag.
    assert len(flagged) == 1
    assert flagged[0]["expectation_type"] == "expect_column_values_to_be_in_set"
    assert flagged[0]["column"] == "StatusFlag"


@pytest.mark.unit
def test_enum_review_ignores_high_cardinality() -> None:
    profile = {"MemberNo": {"null_pct": 0.0, "element_count": 1000, "distinct_count": 1000}}
    assert emit_enum_review_rules(profile) == []


@pytest.mark.unit
def test_enum_review_ignores_single_value_columns() -> None:
    """distinct==1 is usually a constant / bug, not an enum — skip."""
    profile = {"tenant_id": {"null_pct": 0.0, "element_count": 1000, "distinct_count": 1}}
    assert emit_enum_review_rules(profile) == []


# ---------- integration: emit_all_rules ----------


@pytest.mark.unit
def test_emit_all_rules_on_membership_like_profile() -> None:
    """End-to-end: a realistic membership profile emits all 6 dimensions."""
    profile = {
        "MemberNo": {
            "dtype": "VARCHAR",
            "null_pct": 0.0,
            "element_count": 1000,
            "distinct_count": 1000,
        },
        "DateOfBirth": {
            "dtype": "DATE",
            "null_pct": 0.5,
            "element_count": 1000,
            "distinct_count": 950,
        },
        "StatusFlag": {
            "dtype": "VARCHAR",
            "null_pct": 0.0,
            "element_count": 1000,
            "distinct_count": 2,
        },
        "ProviderNPIId": {
            "dtype": "VARCHAR",
            "null_pct": 0.0,
            "element_count": 1000,
            "distinct_count": 200,
        },
        "SERVICE_START_DATE": {"dtype": "DATE", "element_count": 1000, "distinct_count": 500},
        "SERVICE_STOP_DATE": {"dtype": "DATE", "element_count": 1000, "distinct_count": 520},
        "load_datetime": {
            "dtype": "TIMESTAMP",
            "null_pct": 0.0,
            "element_count": 1000,
            "distinct_count": 1000,
        },
    }
    rules = emit_all_rules(profile)

    # At least one rule of every dimension we expect from this fixture.
    assert _by_type(rules, "expect_column_values_to_not_be_null")  # Completeness
    assert _by_type(rules, "expect_column_values_to_be_unique")  # Uniqueness
    assert _by_type(rules, "expect_column_max_to_be_between")  # Timeliness
    assert _by_type(rules, "expect_column_values_to_match_regex")  # Validity
    assert _by_type(rules, "expect_column_pair_values_a_to_be_greater_than_b")  # Consistency
    assert _by_type(rules, "expect_column_values_to_be_in_set")  # HITL

    # Only the enum is flagged for review — auto/HITL split invariant.
    flagged = [r for r in rules if r.get("review_required")]
    assert all(r["expectation_type"] == "expect_column_values_to_be_in_set" for r in flagged)


@pytest.mark.unit
def test_healthcare_regex_library_is_nonempty_and_well_formed() -> None:
    """Guardrail: every regex entry has the required keys."""
    assert len(HEALTHCARE_REGEXES) >= 5
    for _pat, spec in HEALTHCARE_REGEXES.items():
        assert "regex" in spec
        assert "description" in spec
        assert "severity" in spec
        assert spec["severity"] in ("LOW", "MEDIUM", "HIGH")
