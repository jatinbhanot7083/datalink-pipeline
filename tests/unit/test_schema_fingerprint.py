"""Tests for datalink.quality.schema_fingerprint — Phase 6 Commit 2."""

from __future__ import annotations

import pytest

from datalink.quality.schema_fingerprint import (
    columns_from_fingerprint_debug,
    compute_fingerprint,
)


class _FakeWarehouse:
    """Minimal warehouse double — returns canned DESCRIBE output."""

    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows

    def query(self, sql: str, params: dict | None = None) -> list[dict]:
        assert sql.startswith("DESCRIBE ")
        return list(self._rows)


# ---------- basics ----------


@pytest.mark.unit
def test_fingerprint_is_16_hex_chars() -> None:
    wh = _FakeWarehouse(
        [
            {"column_name": "claim_id", "column_type": "VARCHAR"},
            {"column_name": "member_id", "column_type": "VARCHAR"},
        ]
    )
    fp = compute_fingerprint(wh, "BRONZE.RAW_CLAIMS")
    assert len(fp) == 16
    assert all(c in "0123456789abcdef" for c in fp)


@pytest.mark.unit
def test_fingerprint_is_deterministic_across_calls() -> None:
    wh = _FakeWarehouse(
        [
            {"column_name": "a", "column_type": "VARCHAR"},
            {"column_name": "b", "column_type": "INTEGER"},
        ]
    )
    fp1 = compute_fingerprint(wh, "X")
    fp2 = compute_fingerprint(wh, "X")
    assert fp1 == fp2


@pytest.mark.unit
def test_fingerprint_column_order_does_not_matter() -> None:
    """DESCRIBE may return cols in any order — hash must normalise."""
    wh_ab = _FakeWarehouse(
        [
            {"column_name": "a", "column_type": "VARCHAR"},
            {"column_name": "b", "column_type": "INTEGER"},
        ]
    )
    wh_ba = _FakeWarehouse(
        [
            {"column_name": "b", "column_type": "INTEGER"},
            {"column_name": "a", "column_type": "VARCHAR"},
        ]
    )
    assert compute_fingerprint(wh_ab, "X") == compute_fingerprint(wh_ba, "X")


# ---------- drift detection ----------


@pytest.mark.unit
def test_adding_a_column_changes_fingerprint() -> None:
    wh_before = _FakeWarehouse(
        [
            {"column_name": "a", "column_type": "VARCHAR"},
        ]
    )
    wh_after = _FakeWarehouse(
        [
            {"column_name": "a", "column_type": "VARCHAR"},
            {"column_name": "b", "column_type": "INTEGER"},
        ]
    )
    assert compute_fingerprint(wh_before, "X") != compute_fingerprint(wh_after, "X")


@pytest.mark.unit
def test_removing_a_column_changes_fingerprint() -> None:
    wh_before = _FakeWarehouse(
        [
            {"column_name": "a", "column_type": "VARCHAR"},
            {"column_name": "b", "column_type": "INTEGER"},
        ]
    )
    wh_after = _FakeWarehouse(
        [
            {"column_name": "a", "column_type": "VARCHAR"},
        ]
    )
    assert compute_fingerprint(wh_before, "X") != compute_fingerprint(wh_after, "X")


@pytest.mark.unit
def test_retyping_a_column_changes_fingerprint() -> None:
    """VARCHAR → INTEGER should always invalidate the cache."""
    wh_before = _FakeWarehouse([{"column_name": "a", "column_type": "VARCHAR"}])
    wh_after = _FakeWarehouse([{"column_name": "a", "column_type": "INTEGER"}])
    assert compute_fingerprint(wh_before, "X") != compute_fingerprint(wh_after, "X")


@pytest.mark.unit
def test_case_difference_in_column_name_does_not_change_fingerprint() -> None:
    """CLAIM_ID vs claim_id is the same column — shouldn't flap the cache."""
    wh_upper = _FakeWarehouse([{"column_name": "CLAIM_ID", "column_type": "VARCHAR"}])
    wh_lower = _FakeWarehouse([{"column_name": "claim_id", "column_type": "VARCHAR"}])
    assert compute_fingerprint(wh_upper, "X") == compute_fingerprint(wh_lower, "X")


# ---------- error path ----------


@pytest.mark.unit
def test_empty_describe_raises_cleanly() -> None:
    """Missing table → empty DESCRIBE → must raise, not silently return ''."""
    wh = _FakeWarehouse([])
    with pytest.raises(RuntimeError, match="zero columns"):
        compute_fingerprint(wh, "DOES.NOT.EXIST")


# ---------- debug helper ----------


@pytest.mark.unit
def test_debug_helper_returns_sorted_tuples() -> None:
    wh = _FakeWarehouse(
        [
            {"column_name": "b", "column_type": "INTEGER"},
            {"column_name": "a", "column_type": "VARCHAR"},
        ]
    )
    cols = columns_from_fingerprint_debug(wh, "X")
    assert cols == [("a", "VARCHAR"), ("b", "INTEGER")]
