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
# Per-source helpers (Phase 6)
# ----------------------------------------------------------------------------


@pytest.mark.unit
def test_list_live_by_source_filters_to_source_type(wh, reg: SuiteRegistry) -> None:
    """After seeding, list_live_by_source('CLAIMS') returns exactly 3 suites
    (bronze_claims, silver_claims, gold_claims) — one per stage."""
    seed_baselines(wh, client_id="default")
    claims_suites = reg.list_live_by_source("default", "CLAIMS")
    assert {s.suite_name for s in claims_suites} == {
        "bronze_claims",
        "silver_claims",
        "gold_claims",
    }
    # case-insensitive
    claims_lower = reg.list_live_by_source("default", "claims")
    assert len(claims_lower) == 3


@pytest.mark.unit
def test_list_live_by_source_isolates_membership(wh, reg: SuiteRegistry) -> None:
    seed_baselines(wh, client_id="default")
    mem = reg.list_live_by_source("default", "MEMBERSHIP")
    assert {s.suite_name for s in mem} == {
        "bronze_membership",
        "silver_membership",
        "gold_membership",
    }
    # every expectation in a membership suite must NOT reference claim_id
    for suite in mem:
        for exp in suite.expectations:
            col = exp.get("kwargs", {}).get("column", "")
            assert col != "claim_id", f"membership suite must not check claim_id, got {exp}"


# ----------------------------------------------------------------------------
# Baseline seeder
# ----------------------------------------------------------------------------


@pytest.mark.unit
def test_seed_baselines_creates_nine_per_source_plus_three_legacy(wh, reg: SuiteRegistry) -> None:
    """Phase 6: 9 per-(stage, source) suites + 3 legacy aggregate suites = 12 LIVE baselines."""
    n = seed_baselines(wh, client_id="default")
    assert n == 12
    # 9 per-source suites, each tagged with a source_type
    per_source = [
        ("bronze_claims", "CLAIMS"),
        ("bronze_membership", "MEMBERSHIP"),
        ("bronze_provider", "PROVIDER"),
        ("silver_claims", "CLAIMS"),
        ("silver_membership", "MEMBERSHIP"),
        ("silver_provider", "PROVIDER"),
        ("gold_claims", "CLAIMS"),
        ("gold_membership", "MEMBERSHIP"),
        ("gold_provider", "PROVIDER"),
    ]
    for suite_name, source_type in per_source:
        live = reg.get_live("default", suite_name)
        assert live is not None, f"missing LIVE suite {suite_name}"
        assert live.source is SuiteSource.BASELINE_PYTHON
        assert live.status is SuiteStatus.LIVE
        assert live.version == 1
        assert live.source_type == source_type
        assert len(live.expectations) > 0
        assert any(
            e.get("meta", {}).get("dq_dimension") in {d.value for d in DqDimension}
            for e in live.expectations
        )
    # 3 legacy aggregate suites (source_type IS NULL)
    for suite_name in ["bronze_structural", "silver_clinical", "gold_business"]:
        live = reg.get_live("default", suite_name)
        assert live is not None, f"missing legacy aggregate suite {suite_name}"
        assert live.source_type is None


@pytest.mark.unit
def test_seed_baselines_is_idempotent(wh) -> None:
    """12 suites seeded on first call, 0 on subsequent calls."""
    assert seed_baselines(wh) == 12
    assert seed_baselines(wh) == 0  # already seeded; no-op
    assert seed_baselines(wh) == 0


@pytest.mark.unit
def test_seed_all_real_clients_produces_84_suites(wh, reg: SuiteRegistry) -> None:
    """Phase 6: 7 clients (default + 6 real payer tenants) x 12 suites = 84.

    The 6 real tenants are AETNA / CARESOURCE / AFFINITY / COACCESS /
    DHMP / HCSC — the production payer list Jatin confirmed.
    """
    from datalink.quality.baseline_seeder import REAL_CLIENTS, seed_all_real_clients

    assert REAL_CLIENTS == ("aetna", "caresource", "affinity", "coaccess", "dhmp", "hcsc")

    result = seed_all_real_clients(wh)
    assert set(result.keys()) == {"default", *REAL_CLIENTS}
    assert all(n == 12 for n in result.values()), f"expected 12 per client, got {result}"

    # Every real tenant must have its full set of 9 per-source suites LIVE.
    for client in REAL_CLIENTS:
        for suite_name in [
            "bronze_claims",
            "bronze_membership",
            "bronze_provider",
            "silver_claims",
            "silver_membership",
            "silver_provider",
            "gold_claims",
            "gold_membership",
            "gold_provider",
        ]:
            live = reg.get_live(client, suite_name)
            assert live is not None, f"{client} / {suite_name} missing"
            assert live.status.value == "LIVE"


@pytest.mark.unit
def test_seed_all_real_clients_is_idempotent(wh) -> None:
    """Re-running the seeder returns 0 per client on the second call."""
    from datalink.quality.baseline_seeder import seed_all_real_clients

    first = seed_all_real_clients(wh)
    assert sum(first.values()) == 7 * 12  # 84 suites
    second = seed_all_real_clients(wh)
    assert all(n == 0 for n in second.values())


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
