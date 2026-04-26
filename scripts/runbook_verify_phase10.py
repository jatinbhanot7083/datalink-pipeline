"""Phase 10 verifier — Option C HITL approval policy end-to-end.

Mirrors ``scripts/runbook_verify.py`` from Phase 9: assertion-based,
PASS/FAIL output, exit 1 on any failure. Runs against a throwaway
DuckDB so it's safe on any developer laptop and in CI.

Three scenarios:

  1. Profiler-style suite (``SuiteSource.BASELINE_PYTHON``) seeded via
     ``submit_with_policy(mode=AUTO_APPROVE)`` → must land LIVE.
     Verifies that mechanical profiler-generated baselines still skip
     review (Option C: low-risk, high-volume → auto-approve).

  2. Agent-style suite (``SuiteSource.AGENT``) created via
     ``accept_proposal(..., mode=None)`` → must land PENDING_REVIEW.
     Verifies that NL-prompt-driven creative content forces HITL
     by default (Option C: novel → review).

  3. Edit of the LIVE profiler suite via ``fork_for_edit`` + per-expectation
     CRUD + ``submit_with_policy(HITL)`` → must land PENDING_REVIEW.
     Verifies the "edit of LIVE is forced through HITL" rule, regardless
     of the source suite's original auto-approve eligibility. The original
     LIVE row stays LIVE during the edit cycle (production gate
     uninterrupted) and atomically swaps to ARCHIVED on activate.

  Bonus: legacy ``target_status='LIVE'`` shim on ``accept_proposal`` still
  walks to LIVE — back-compat for any external code still passing strings.

Usage::

    source .venv/bin/activate
    python -m scripts.runbook_verify_phase10

Exit 0 on green, 1 on any FAIL.
"""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

os.environ["DL_ADAPTERS__WAREHOUSE__TYPE"] = "duckdb"
TMPDB = Path(tempfile.mkdtemp(prefix="phase10_final_")) / "test.duckdb"
os.environ["DL_ADAPTERS__WAREHOUSE__PATH"] = str(TMPDB)

from datalink.adapters.factory import build_adapters  # noqa: E402
from datalink.config.loader import load_settings  # noqa: E402
from datalink.quality.agent_authored import Proposal, accept_proposal  # noqa: E402
from datalink.quality.control import PipelineControl  # noqa: E402
from datalink.quality.registry import (  # noqa: E402
    ApprovalMode,
    SuiteDraft,
    SuiteRegistry,
    SuiteSource,
    SuiteStatus,
    default_approval_mode,
)


def main() -> int:
    settings = load_settings()
    adapters = build_adapters(settings)
    wh = adapters.warehouse
    PipelineControl(wh).ensure()
    reg = SuiteRegistry(wh)

    failures: list[str] = []

    def fail(msg: str) -> None:
        failures.append(msg)
        print(f"   ❌ {msg}")

    def ok(msg: str) -> None:
        print(f"   ✅ {msg}")

    print()
    print("=== Scenario 1: Profiler (BASELINE_PYTHON) → AUTO_APPROVE → LIVE ===")

    profiler_draft = SuiteDraft(
        client_id="acme",
        suite_name="bronze_membership",
        expectations=[
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": "member_id"},
                "meta": {"dq_dimension": "Completeness", "severity": "HIGH"},
            }
        ],
        created_by="system:profiler",
        source=SuiteSource.BASELINE_PYTHON,
        dq_dimensions=["Completeness"],
    )
    sid = reg.create_draft(profiler_draft)
    policy_mode = default_approval_mode(SuiteSource.BASELINE_PYTHON)
    if policy_mode is not ApprovalMode.AUTO_APPROVE:
        fail(f"profiler default should be AUTO_APPROVE, got {policy_mode}")
    reg.submit_with_policy(sid, mode=policy_mode, actor="system:profiler")
    after = reg.get_by_id(sid)
    if after is None or after.status is not SuiteStatus.LIVE:
        fail(f"profiler suite should be LIVE, got {after.status if after else None}")
    else:
        ok(f"profiler v{after.version} → LIVE (auto-approved per policy)")

    profiler_live_id = sid

    print()
    print("=== Scenario 2: Agent (AGENT, mode=None) → HITL → PENDING_REVIEW ===")

    proposal = Proposal(
        expectation_type="expect_column_values_to_match_regex",
        kwargs={"column": "ssn", "regex": r"^\d{3}-\d{2}-\d{4}$"},
        meta={"dq_dimension": "Validity", "source_type": "MEMBERSHIP"},
        sql_preview="SELECT 1",
        rationale="SSN format check inferred from sample values.",
    )
    agent_id = accept_proposal(
        registry=reg,
        proposal=proposal,
        client_id="acme",
        suite_name="custom_membership_ssn",
        source_prompt="Make sure SSN follows xxx-xx-xxxx",
        author="user:bob",
        # mode=None → uses default_approval_mode(SuiteSource.AGENT) = HITL
    )
    after = reg.get_by_id(agent_id)
    if after is None or after.status is not SuiteStatus.PENDING_REVIEW:
        fail(f"agent suite should be PENDING_REVIEW, got {after.status if after else None}")
    else:
        ok(f"agent v{after.version} → PENDING_REVIEW (HITL per policy)")

    # Also verify legacy target_status="LIVE" still works (back-compat)
    legacy_id = accept_proposal(
        registry=reg,
        proposal=Proposal(
            expectation_type="expect_column_values_to_be_unique",
            kwargs={"column": "claim_id"},
            meta={"dq_dimension": "Uniqueness", "source_type": "CLAIMS"},
            sql_preview="SELECT 1",
            rationale="Claim ID uniqueness.",
        ),
        client_id="acme",
        suite_name="custom_claim_unique",
        source_prompt="Claim id should be unique",
        author="user:bob",
        target_status="LIVE",  # legacy alias → AUTO_APPROVE
    )
    after = reg.get_by_id(legacy_id)
    if after is None or after.status is not SuiteStatus.LIVE:
        fail(
            f"legacy target_status='LIVE' should AUTO_APPROVE → LIVE, got {after.status if after else None}"
        )
    else:
        ok("back-compat: legacy target_status='LIVE' still walks to LIVE")

    print()
    print("=== Scenario 3: Edit of LIVE → fork + CRUD + HITL ===")

    draft_id = reg.fork_for_edit(profiler_live_id, actor="user:alice")
    forked = reg.get_by_id(draft_id)
    if forked is None or forked.status is not SuiteStatus.DRAFT:
        fail(f"fork should DRAFT, got {forked.status if forked else None}")
    elif forked.version != 2:
        fail(f"fork version should be 2, got {forked.version}")
    else:
        ok(f"forked LIVE v1 → DRAFT v{forked.version}")

    reg.add_expectation(
        draft_id,
        {
            "expectation_type": "expect_column_values_to_be_unique",
            "kwargs": {"column": "member_id"},
            "meta": {"dq_dimension": "Uniqueness", "severity": "HIGH"},
        },
        actor="user:alice",
    )
    after = reg.get_by_id(draft_id)
    if after is None or len(after.expectations) != 2:
        fail(f"add_expectation: expected 2, got {len(after.expectations) if after else 0}")
    else:
        ok(f"added uniqueness check; draft now has {len(after.expectations)} expectations")

    # Edit-of-LIVE policy lookup
    edit_mode = default_approval_mode(SuiteSource.BASELINE_PYTHON, is_edit_of_live=True)
    if edit_mode is not ApprovalMode.HITL:
        fail(f"edit-of-LIVE should be HITL, got {edit_mode}")
    else:
        ok("policy: edit_of_live=True overrides BASELINE_PYTHON's AUTO_APPROVE → HITL")

    reg.submit_with_policy(draft_id, mode=ApprovalMode.HITL, actor="user:alice")
    after = reg.get_by_id(draft_id)
    if after is None or after.status is not SuiteStatus.PENDING_REVIEW:
        fail(f"edited draft should be PENDING_REVIEW, got {after.status if after else None}")
    else:
        ok("edited draft → PENDING_REVIEW")

    # Original LIVE must remain LIVE during this flow
    orig = reg.get_by_id(profiler_live_id)
    if orig is None or orig.status is not SuiteStatus.LIVE:
        fail(f"original LIVE should still be LIVE, got {orig.status if orig else None}")
    else:
        ok("original v1 still LIVE — production gate uninterrupted during edit cycle")

    # Reviewer approves + activates → new v2 takes over, v1 archives
    reg.approve(draft_id, actor="reviewer:carol", notes="LGTM")
    reg.activate(draft_id, actor="reviewer:carol")
    after = reg.get_by_id(draft_id)
    orig_after = reg.get_by_id(profiler_live_id)
    if after is None or after.status is not SuiteStatus.LIVE:
        fail(f"approved+activated draft should be LIVE, got {after.status if after else None}")
    elif orig_after is None or orig_after.status is not SuiteStatus.ARCHIVED:
        fail(f"prior LIVE should have archived, got {orig_after.status if orig_after else None}")
    else:
        ok("activate: v2 → LIVE, v1 → ARCHIVED (atomic swap)")

    print()
    if failures:
        print(f"❌ {len(failures)} FAIL")
        for f in failures:
            print(f"   - {f}")
        return 1
    print("✅ Phase 10 PASS — Option C end-to-end verified across all 3 scenarios")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    finally:
        try:
            TMPDB.unlink(missing_ok=True)
            TMPDB.parent.rmdir()
        except OSError:
            pass
