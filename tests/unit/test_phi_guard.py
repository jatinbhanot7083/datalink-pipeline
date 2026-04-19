"""PHI boundary — the most important guarantee in the agent layer."""

from __future__ import annotations

import pytest

from datalink.phi.guard import (
    MAX_FREETEXT_LEN,
    MAX_LIST_LEN,
    PhiBoundaryViolation,
    PhiRedactionLayer,
)


@pytest.fixture()
def phi() -> PhiRedactionLayer:
    return PhiRedactionLayer()


@pytest.mark.unit
def test_allows_pure_metadata_payload(phi: PhiRedactionLayer) -> None:
    payload = {
        "batch_id": "B-001",
        "table_name": "RAW_CLAIMS",
        "row_count": 12345,
        "null_pct": 0.02,
        "column_names": ["claim_id", "member_id", "cpt_code"],
        "profile": {"claim_id": {"null_pct": 0.0, "distinct_count": 12345}},
    }
    phi.assert_clean(payload)  # must not raise


@pytest.mark.unit
def test_rejects_unknown_top_level_key(phi: PhiRedactionLayer) -> None:
    with pytest.raises(PhiBoundaryViolation) as exc:
        phi.assert_clean({"raw_data": [1, 2, 3]})
    assert "not in SAFE_FIELDS" in str(exc.value)


@pytest.mark.unit
@pytest.mark.parametrize(
    "phi_key",
    ["ssn", "member_name", "patient_name", "dob", "street", "mrn", "medical_record_number"],
)
def test_rejects_nested_phi_keys(phi: PhiRedactionLayer, phi_key: str) -> None:
    with pytest.raises(PhiBoundaryViolation):
        phi.assert_clean({"profile": {phi_key: "anything"}})


@pytest.mark.unit
def test_rejects_oversize_list(phi: PhiRedactionLayer) -> None:
    payload = {"top_values": list(range(MAX_LIST_LEN + 1))}
    with pytest.raises(PhiBoundaryViolation) as exc:
        phi.assert_clean(payload)
    assert "row-like" in str(exc.value)


@pytest.mark.unit
def test_rejects_oversize_freetext(phi: PhiRedactionLayer) -> None:
    payload = {"context": "a" * (MAX_FREETEXT_LEN + 1)}
    with pytest.raises(PhiBoundaryViolation) as exc:
        phi.assert_clean(payload)
    assert "clinical note leak" in str(exc.value)


@pytest.mark.unit
def test_safe_subset_drops_unknown_keys(phi: PhiRedactionLayer) -> None:
    payload = {"batch_id": "B-1", "raw_row": {"patient_name": "J. Doe"}}
    assert phi.safe_subset(payload) == {"batch_id": "B-1"}


@pytest.mark.unit
def test_scrub_iterable_keys_drops_phi_shaped_keys(phi: PhiRedactionLayer) -> None:
    items = [
        {"column_name": "claim_id", "null_pct": 0.0, "ssn": "x"},
        {"column_name": "member_id", "null_pct": 0.01, "member_name": "y"},
    ]
    scrubbed = phi.scrub_iterable_keys(items)
    for item in scrubbed:
        assert "ssn" not in item
        assert "member_name" not in item
    assert scrubbed[0]["column_name"] == "claim_id"


@pytest.mark.unit
def test_violation_report_includes_path(phi: PhiRedactionLayer) -> None:
    report = phi.inspect_payload({"profile": {"nested": {"ssn": "x"}}})
    assert not report.ok
    assert any("profile.nested.ssn" in v for v in report.violations)
