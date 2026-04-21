"""Phase 5.8 — SuiteRegistry unit tests.

Exercises the full state machine + invariants against an in-memory DuckDB.
No Airflow, no containers — just the registry logic.
"""

from __future__ import annotations

import pytest

from datalink.adapters.warehouse.duckdb_adapter import DuckDBWarehouse
from datalink.config.models import WarehouseConfig
from datalink.quality import (
    DqDimension,
    SuiteDraft,
    SuiteRegistry,
    SuiteSource,
    SuiteStatus,
    create_control_tables,
    seed_baselines,
)


@pytest.fixture
def wh():
    """Fresh in-memory DuckDB per test."""
    w = DuckDBWarehouse(WarehouseConfig(type="duckdb", path=":memory:"))
    create_control_tables(w)
    yield w
    w.close()


@pytest.fixture
def reg(wh):
    return SuiteRegistry(wh)


# ----------------------------------------------------------------------------
# Basic CRUD
# ----------------------------------------------------------------------------


@pytest.mark.unit
def test_create_draft_produces_version_1(reg: SuiteRegistry) -> None:
    sid = reg.create_draft(
        SuiteDraft(
            client_id="default",
            suite_name="bronze_structural",
            expectations=[
                {
                    "expectation_type": "expect_column_values_to_not_be_null",
                    "kwargs": {"column": "id"},
                }
            ],
            created_by="alice",
        )
    )
    sv = reg.get_by_id(sid)
    assert sv is not None
    assert sv.version == 1
    assert sv.status is SuiteStatus.DRAFT
    assert sv.source is SuiteSource.UI
    assert sv.created_by == "alice"
    assert len(sv.expectations) == 1


@pytest.mark.unit
def test_second_draft_bumps_version(reg: SuiteRegistry) -> None:
    for _ in range(3):
        reg.create_draft(
            SuiteDraft(
                client_id="default",
                suite_name="bronze_structural",
                expectations=[],
                created_by="alice",
            )
        )
    versions = reg.list_versions("default", "bronze_structural")
    assert [v.version for v in versions] == [3, 2, 1]


@pytest.mark.unit
def test_update_draft_replaces_expectations(reg: SuiteRegistry) -> None:
    sid = reg.create_draft(
        SuiteDraft(
            client_id="default", suite_name="bronze_structural", expectations=[], created_by="alice"
        )
    )
    new_exps = [
        {"expectation_type": "expect_column_values_to_be_unique", "kwargs": {"column": "claim_id"}}
    ]
    reg.update_draft(sid, new_exps, dq_dimensions=["Uniqueness"], actor="alice")
    sv = reg.get_by_id(sid)
    assert sv is not None
    assert sv.expectations == new_exps
    assert sv.dq_dimensions == ["Uniqueness"]


@pytest.mark.unit
def test_update_draft_rejects_non_draft(reg: SuiteRegistry) -> None:
    sid = reg.create_draft(
        SuiteDraft(
            client_id="default", suite_name="bronze_structural", expectations=[], created_by="alice"
        )
    )
    reg.submit_for_review(sid, actor="alice")
    with pytest.raises(ValueError, match="only DRAFT is editable"):
        reg.update_draft(sid, [], [], actor="alice")


# ----------------------------------------------------------------------------
# State-machine transitions
# ----------------------------------------------------------------------------


@pytest.mark.unit
def test_happy_path_draft_to_live(reg: SuiteRegistry) -> None:
    sid = reg.create_draft(
        SuiteDraft(
            client_id="default", suite_name="bronze_structural", expectations=[], created_by="alice"
        )
    )
    reg.submit_for_review(sid, actor="alice")
    assert reg.get_by_id(sid).status is SuiteStatus.PENDING_REVIEW  # type: ignore[union-attr]

    reg.approve(sid, actor="bob_reviewer", notes="looks good")
    sv = reg.get_by_id(sid)
    assert sv is not None
    assert sv.status is SuiteStatus.APPROVED
    assert sv.reviewed_by == "bob_reviewer"

    reg.activate(sid, actor="bob_reviewer")
    sv = reg.get_by_id(sid)
    assert sv is not None
    assert sv.status is SuiteStatus.LIVE
    live = reg.get_live("default", "bronze_structural")
    assert live is not None and live.suite_id == sid


@pytest.mark.unit
def test_activate_archives_previous_live(reg: SuiteRegistry) -> None:
    """The unique-LIVE invariant — activating v2 must archive v1."""
    # v1 lifecycle
    sid1 = reg.create_draft(
        SuiteDraft(
            client_id="default", suite_name="bronze_structural", expectations=[], created_by="alice"
        )
    )
    reg.submit_for_review(sid1, actor="alice")
    reg.approve(sid1, actor="bob")
    reg.activate(sid1, actor="bob")

    # v2 lifecycle
    sid2 = reg.create_draft(
        SuiteDraft(
            client_id="default", suite_name="bronze_structural", expectations=[], created_by="alice"
        )
    )
    reg.submit_for_review(sid2, actor="alice")
    reg.approve(sid2, actor="bob")
    reg.activate(sid2, actor="bob")

    # After both activations, v1 is ARCHIVED, v2 is LIVE.
    v1 = reg.get_by_id(sid1)
    v2 = reg.get_by_id(sid2)
    assert v1 is not None and v1.status is SuiteStatus.ARCHIVED
    assert v2 is not None and v2.status is SuiteStatus.LIVE

    # Exactly one LIVE per (client, suite)
    live = reg.get_live("default", "bronze_structural")
    assert live is not None and live.suite_id == sid2


@pytest.mark.unit
def test_reject_from_pending_review(reg: SuiteRegistry) -> None:
    sid = reg.create_draft(
        SuiteDraft(client_id="default", suite_name="s", expectations=[], created_by="alice")
    )
    reg.submit_for_review(sid, actor="alice")
    reg.reject(sid, actor="bob", notes="violates HIPAA")
    sv = reg.get_by_id(sid)
    assert sv is not None and sv.status is SuiteStatus.REJECTED
    assert sv.review_notes == "violates HIPAA"


@pytest.mark.unit
def test_request_changes_returns_to_draft(reg: SuiteRegistry) -> None:
    sid = reg.create_draft(
        SuiteDraft(client_id="default", suite_name="s", expectations=[], created_by="alice")
    )
    reg.submit_for_review(sid, actor="alice")
    reg.request_changes(sid, actor="bob", notes="add NPI validation")
    sv = reg.get_by_id(sid)
    assert sv is not None and sv.status is SuiteStatus.DRAFT
    # Draft is editable again
    reg.update_draft(sid, [{"expectation_type": "x"}], [], actor="alice")


@pytest.mark.unit
def test_illegal_transition_raises(reg: SuiteRegistry) -> None:
    sid = reg.create_draft(
        SuiteDraft(client_id="default", suite_name="s", expectations=[], created_by="alice")
    )
    # Can't approve a DRAFT — must go through PENDING_REVIEW first.
    with pytest.raises(ValueError, match="illegal suite transition"):
        reg.approve(sid, actor="bob")


# ----------------------------------------------------------------------------
# Multi-tenant
# ----------------------------------------------------------------------------


@pytest.mark.unit
def test_two_clients_have_independent_live(reg: SuiteRegistry) -> None:
    """Activating client_a v1 LIVE must NOT archive client_b's LIVE."""

    def activate_one(client: str) -> str:
        sid = reg.create_draft(
            SuiteDraft(
                client_id=client,
                suite_name="bronze_structural",
                expectations=[],
                created_by="alice",
            )
        )
        reg.submit_for_review(sid, actor="alice")
        reg.approve(sid, actor="bob")
        reg.activate(sid, actor="bob")
        return sid

    a_sid = activate_one("client_a")
    b_sid = activate_one("client_b")

    assert reg.get_live("client_a", "bronze_structural").suite_id == a_sid  # type: ignore[union-attr]
    assert reg.get_live("client_b", "bronze_structural").suite_id == b_sid  # type: ignore[union-attr]


# ----------------------------------------------------------------------------
# Pending review queue
# ----------------------------------------------------------------------------


@pytest.mark.unit
def test_pending_review_queue(reg: SuiteRegistry) -> None:
    for client in ["client_a", "client_b", "client_c"]:
        sid = reg.create_draft(
            SuiteDraft(
                client_id=client,
                suite_name="bronze_structural",
                expectations=[],
                created_by="alice",
            )
        )
        reg.submit_for_review(sid, actor="alice")
    # one DRAFT that should NOT be in queue
    reg.create_draft(
        SuiteDraft(
            client_id="client_d",
            suite_name="bronze_structural",
            expectations=[],
            created_by="alice",
        )
    )
    queue = reg.list_pending_reviews()
    assert len(queue) == 3
    assert {q.client_id for q in queue} == {"client_a", "client_b", "client_c"}


# ----------------------------------------------------------------------------
# Baseline seeder
# ----------------------------------------------------------------------------


@pytest.mark.unit
def test_seed_baselines_creates_three_live_suites(wh, reg: SuiteRegistry) -> None:
    n = seed_baselines(wh, client_id="default")
    assert n == 3
    for suite_name in ["bronze_structural", "silver_clinical", "gold_business"]:
        live = reg.get_live("default", suite_name)
        assert live is not None
        assert live.source is SuiteSource.BASELINE_PYTHON
        assert live.status is SuiteStatus.LIVE
        assert live.version == 1
        assert len(live.expectations) > 0
        # At least one expectation with a recognised dq_dimension.
        assert any(
            e.get("meta", {}).get("dq_dimension") in {d.value for d in DqDimension}
            for e in live.expectations
        )


@pytest.mark.unit
def test_seed_baselines_is_idempotent(wh) -> None:
    assert seed_baselines(wh) == 3
    assert seed_baselines(wh) == 0  # already seeded; no-op
    assert seed_baselines(wh) == 0


@pytest.mark.unit
def test_audit_log_captures_transitions(wh, reg: SuiteRegistry) -> None:
    sid = reg.create_draft(
        SuiteDraft(client_id="default", suite_name="s", expectations=[], created_by="alice")
    )
    reg.submit_for_review(sid, actor="alice")
    reg.approve(sid, actor="bob")
    reg.activate(sid, actor="bob")

    rows = wh.query(
        "SELECT from_status, to_status, actor FROM CONTROL.dq_suite_audit_log "
        "WHERE suite_id = $i ORDER BY ts",
        {"i": sid},
    )
    # 4 transitions: created→DRAFT, DRAFT→PENDING_REVIEW, PENDING_REVIEW→APPROVED, APPROVED→LIVE
    assert len(rows) == 4
    assert [r["to_status"] for r in rows] == ["DRAFT", "PENDING_REVIEW", "APPROVED", "LIVE"]
