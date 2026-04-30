"""Phase 12 verifier — pipeline control policies + two-layer HITL.

Mirrors ``runbook_verify_phase{9,10,11}.py``: assertion-based PASS/FAIL,
exits 1 on any failure, runs against a throwaway DuckDB.

Three sections:

  1. **Storage + state machine** — DEFAULT_POLICY back-compat,
     PolicyDraft validation, full DRAFT → PENDING_REVIEW → APPROVED →
     LIVE walk, fork_for_edit + atomic LIVE swap, illegal transitions
     raise.

  2. **Two-layer HITL semantics** — auto_approve via ApprovalMode.AUTO_APPROVE
     walks all 3 hops in one call; HITL stops at PENDING_REVIEW; DRAFT
     stays parked.

  3. **Runtime check fires against active policy** — drives a synthetic
     CheckpointResult through the same branch logic as
     ``hooks.run_checkpoint_with_hooks``, asserts that PAUSE / ABORT /
     no-op fire correctly per the LIVE policy thresholds. We don't
     actually call run_checkpoint_with_hooks (it pulls in CrewAI / GX),
     just the policy-driven decision logic.

Usage::

    source .venv/bin/activate
    python -m scripts.runbook_verify_phase12

Exit 0 on green, 1 on any FAIL.
"""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

os.environ["DL_ADAPTERS__WAREHOUSE__TYPE"] = "duckdb"
TMPDB = Path(tempfile.mkdtemp(prefix="phase12_verify_")) / "test.duckdb"
os.environ["DL_ADAPTERS__WAREHOUSE__PATH"] = str(TMPDB)

from datalink.adapters.factory import build_adapters  # noqa: E402
from datalink.config.loader import load_settings  # noqa: E402
from datalink.pipeline.control_policy import (  # noqa: E402
    DEFAULT_POLICY,
    PipelineControlPolicyRegistry,
    PolicyDraft,
    PolicyStatus,
    ThresholdPolicy,
)
from datalink.quality.control import PipelineControl  # noqa: E402
from datalink.quality.registry import ApprovalMode  # noqa: E402


def _runtime_decision(fail_pct: float, policy: ThresholdPolicy) -> str:
    """Mirror the branch logic in hooks.run_checkpoint_with_hooks.

    Returns the action label that hooks would take on a BREACHED
    checkpoint with the given fail_pct against the given policy.
    """
    if fail_pct > policy.fail_rate_abort_pct:
        return "ABORT"
    if fail_pct > policy.fail_rate_pause_pct:
        return "PAUSE"
    return "NOOP"


def main() -> int:
    failures: list[str] = []

    def fail(msg: str) -> None:
        failures.append(msg)
        print(f"   ❌ {msg}")

    def ok(msg: str) -> None:
        print(f"   ✅ {msg}")

    settings = load_settings()
    adapters = build_adapters(settings)
    wh = adapters.warehouse
    PipelineControl(wh).ensure()
    reg = PipelineControlPolicyRegistry(wh)

    print()
    print("=== 1. DEFAULT_POLICY back-compat ===")

    # 1.1 DEFAULT values mirror pre-Phase-12 hardcode.
    if (
        DEFAULT_POLICY.fail_rate_pause_pct != 0.0
        or DEFAULT_POLICY.fail_rate_abort_pct != 25.0
        or DEFAULT_POLICY.schema_drift_action != "PAUSE"
    ):
        fail(
            f"DEFAULT_POLICY drifted from pre-Phase-12 hardcode: "
            f"pause={DEFAULT_POLICY.fail_rate_pause_pct}, "
            f"abort={DEFAULT_POLICY.fail_rate_abort_pct}, "
            f"drift={DEFAULT_POLICY.schema_drift_action}"
        )
    else:
        ok("DEFAULT_POLICY = pause>0% / abort>25% / drift=PAUSE (matches Phase 9.4 hardcode)")

    # 1.2 No-policy tenant gets DEFAULT.
    p = reg.get_or_default("acme", "bronze_ingest")
    if not p.is_default:
        fail(f"unregistered tuple should return DEFAULT, got version={p.version}")
    else:
        ok("get_or_default for unregistered tuple → DEFAULT_POLICY")

    print()
    print("=== 2. Validation ===")

    bad_cases = [
        (
            "pause > abort",
            PolicyDraft(
                client_id="acme",
                pipeline_id="bronze_ingest",
                fail_rate_pause_pct=50.0,
                fail_rate_abort_pct=25.0,
                schema_drift_action="PAUSE",
                row_count_drop_pct=50.0,
                created_by="t",
            ),
        ),
        (
            "abort > 100",
            PolicyDraft(
                client_id="acme",
                pipeline_id="bronze_ingest",
                fail_rate_pause_pct=0.0,
                fail_rate_abort_pct=200.0,
                schema_drift_action="PAUSE",
                row_count_drop_pct=50.0,
                created_by="t",
            ),
        ),
        (
            "invalid drift action",
            PolicyDraft(
                client_id="acme",
                pipeline_id="bronze_ingest",
                fail_rate_pause_pct=0.0,
                fail_rate_abort_pct=25.0,
                schema_drift_action="LOL",
                row_count_drop_pct=50.0,
                created_by="t",
            ),
        ),
    ]
    for label, bad in bad_cases:
        try:
            bad.validate()
            fail(f"{label}: expected ValueError, none raised")
        except ValueError:
            ok(f"validation rejected: {label}")

    print()
    print("=== 3. Full HITL walk: DRAFT → PENDING_REVIEW → APPROVED → LIVE ===")

    draft = PolicyDraft(
        client_id="acme",
        pipeline_id="bronze_ingest",
        fail_rate_pause_pct=2.0,
        fail_rate_abort_pct=15.0,  # stricter than DEFAULT's 25
        schema_drift_action="ABORT",  # stricter than DEFAULT's PAUSE
        row_count_drop_pct=30.0,
        created_by="user:alice",
        notes="aetna runs strict",
    )
    pid = reg.create_draft(draft)
    reg.submit_for_review(pid, actor="user:alice")
    reg.approve(pid, actor="reviewer:bob", notes="thresholds reasonable")
    reg.activate(pid, actor="reviewer:bob")
    p = reg.get_by_id(pid)
    if p is None or p.status is not PolicyStatus.LIVE:
        fail(f"full HITL walk didn't land LIVE: {p.status if p else None}")
    elif p.reviewed_by != "reviewer:bob":
        fail(f"reviewed_by not recorded: {p.reviewed_by}")
    elif p.activated_at is None:
        fail("activated_at not set")
    else:
        ok(
            "full HITL walk: DRAFT → PENDING_REVIEW → APPROVED → LIVE (v1, reviewed by reviewer:bob)"
        )

    # get_or_default now returns the LIVE custom policy.
    active = reg.get_or_default("acme", "bronze_ingest")
    if active.is_default:
        fail("get_or_default still returns DEFAULT after activate")
    elif active.fail_rate_abort_pct != 15.0:
        fail(f"active threshold wrong: {active.fail_rate_abort_pct}")
    else:
        ok("get_or_default('acme', 'bronze_ingest') now returns custom v1 (abort>15%)")

    print()
    print("=== 4. ApprovalMode.AUTO_APPROVE walks all 3 hops ===")

    auto_draft = PolicyDraft(
        client_id="caresource",
        pipeline_id="silver_build",
        fail_rate_pause_pct=5.0,
        fail_rate_abort_pct=30.0,
        schema_drift_action="PAUSE",
        row_count_drop_pct=40.0,
        created_by="user:alice",
    )
    apid = reg.create_draft(auto_draft)
    reg.submit_with_policy(
        apid,
        mode=ApprovalMode.AUTO_APPROVE,
        actor="user:alice",
        approval_notes="auto-approved by senior steward",
    )
    p = reg.get_by_id(apid)
    if p is None or p.status is not PolicyStatus.LIVE:
        fail(f"AUTO_APPROVE didn't reach LIVE: {p.status if p else None}")
    else:
        ok("submit_with_policy(AUTO_APPROVE) → LIVE in one call")

    # ApprovalMode.HITL stops at PENDING_REVIEW.
    hitl_draft = PolicyDraft(
        client_id="dhmp",
        pipeline_id="gold_egress",
        fail_rate_pause_pct=1.0,
        fail_rate_abort_pct=10.0,
        schema_drift_action="ABORT",
        row_count_drop_pct=25.0,
        created_by="user:alice",
    )
    hpid = reg.create_draft(hitl_draft)
    reg.submit_with_policy(hpid, mode=ApprovalMode.HITL, actor="user:alice")
    p = reg.get_by_id(hpid)
    if p is None or p.status is not PolicyStatus.PENDING_REVIEW:
        fail(f"HITL didn't stop at PENDING_REVIEW: {p.status if p else None}")
    else:
        ok("submit_with_policy(HITL) stops at PENDING_REVIEW (awaits reviewer)")

    # ApprovalMode.DRAFT keeps it parked.
    park_draft = PolicyDraft(
        client_id="dhmp",
        pipeline_id="bronze_ingest",
        fail_rate_pause_pct=1.0,
        fail_rate_abort_pct=10.0,
        schema_drift_action="PAUSE",
        row_count_drop_pct=25.0,
        created_by="user:alice",
    )
    parkid = reg.create_draft(park_draft)
    reg.submit_with_policy(parkid, mode=ApprovalMode.DRAFT, actor="user:alice")
    p = reg.get_by_id(parkid)
    if p is None or p.status is not PolicyStatus.DRAFT:
        fail(f"DRAFT mode didn't park: {p.status if p else None}")
    else:
        ok("submit_with_policy(DRAFT) keeps it parked")

    print()
    print("=== 5. Edit-of-LIVE forces fork + atomic swap ===")

    # Fork the acme bronze_ingest LIVE policy → new DRAFT v2.
    new_id = reg.fork_for_edit(pid, actor="user:alice")
    forked = reg.get_by_id(new_id)
    orig = reg.get_by_id(pid)
    if forked is None or forked.status is not PolicyStatus.DRAFT:
        fail(f"fork status wrong: {forked.status if forked else None}")
    elif forked.version != 2:
        fail(f"fork version: expected 2, got {forked.version}")
    elif orig is None or orig.status is not PolicyStatus.LIVE:
        fail(f"original mutated: {orig.status if orig else None}")
    else:
        ok("fork_for_edit: LIVE v1 unchanged, new DRAFT v2 cloned thresholds")

    # Loosen abort threshold → 20%, route through HITL.
    reg.update_draft(
        new_id,
        fail_rate_pause_pct=5.0,
        fail_rate_abort_pct=20.0,
        schema_drift_action="PAUSE",
        row_count_drop_pct=30.0,
        actor="user:alice",
        notes="loosen after operations review",
    )
    reg.submit_for_review(new_id, actor="user:alice")
    reg.approve(new_id, actor="reviewer:bob")
    reg.activate(new_id, actor="reviewer:bob")
    new_active = reg.get_or_default("acme", "bronze_ingest")
    archived = reg.get_by_id(pid)
    if new_active.policy_id != new_id:
        fail(f"new LIVE wrong: {new_active.policy_id}")
    elif archived is None or archived.status is not PolicyStatus.ARCHIVED:
        fail(f"prior LIVE not archived: {archived.status if archived else None}")
    elif new_active.fail_rate_abort_pct != 20.0:
        fail(f"new active threshold wrong: {new_active.fail_rate_abort_pct}")
    else:
        ok("activate v2: v1 → ARCHIVED, v2 → LIVE (atomic swap)")

    print()
    print("=== 6. Runtime decision logic fires against LIVE policy ===")

    # acme/bronze_ingest LIVE = v2 with pause>5, abort>20.
    cases = [
        (0.0, "NOOP"),  # 0% fail — below pause
        (3.0, "NOOP"),  # 3% — below pause
        (5.0, "NOOP"),  # at threshold — must STRICTLY exceed
        (5.5, "PAUSE"),  # above pause, below abort
        (15.0, "PAUSE"),  # well into PAUSE band
        (20.0, "PAUSE"),  # at abort threshold — must STRICTLY exceed
        (25.0, "ABORT"),  # above abort
        (75.0, "ABORT"),  # well into ABORT
    ]
    p = reg.get_or_default("acme", "bronze_ingest")
    for fail_pct, expected in cases:
        got = _runtime_decision(fail_pct, p)
        if got != expected:
            fail(f"runtime decision @{fail_pct}% expected {expected}, got {got}")
    ok(f"runtime decision logic correct for {len(cases)} fail_pct levels against custom v2")

    # Same checkpoint against DEFAULT_POLICY (pause>0, abort>25).
    cases_default = [
        (0.0, "NOOP"),  # exactly at pause threshold (0) — must STRICTLY exceed
        (0.1, "PAUSE"),  # > 0 → PAUSE
        (25.0, "PAUSE"),  # at abort threshold — STRICTLY exceed
        (25.1, "ABORT"),
    ]
    for fail_pct, expected in cases_default:
        got = _runtime_decision(fail_pct, DEFAULT_POLICY)
        if got != expected:
            fail(f"DEFAULT runtime @{fail_pct}% expected {expected}, got {got}")
    ok("DEFAULT_POLICY runtime logic matches pre-Phase-12 (any fail → PAUSE, > 25% → ABORT)")

    print()
    print("=== 7. Illegal operations raise ===")

    try:
        reg.update_draft(
            new_id,  # this is now LIVE
            fail_rate_pause_pct=99.0,
            fail_rate_abort_pct=99.0,
            schema_drift_action="CONTINUE",
            row_count_drop_pct=99.0,
            actor="user:alice",
        )
        fail("expected ValueError editing LIVE policy directly; got none")
    except ValueError:
        ok("update_draft on LIVE policy correctly rejected (must fork first)")

    try:
        reg._transition(  # type: ignore[attr-defined]
            parkid, PolicyStatus.LIVE, actor="t"
        )
        fail("expected ValueError on DRAFT → LIVE skip; got none")
    except ValueError:
        ok("illegal DRAFT → LIVE transition correctly rejected")

    print()
    print("=== 8. Listings ===")

    live = reg.list_all_active()
    if len(live) != 2:
        fail(
            f"expected 2 LIVE policies (acme/bronze_ingest, caresource/silver_build), got {len(live)}"
        )
    else:
        keys = sorted((p.client_id, p.pipeline_id) for p in live)
        ok(f"list_all_active → {keys}")

    pending = reg.list_pending_reviews()
    if len(pending) != 1:
        fail(f"expected 1 pending review (dhmp/gold_egress), got {len(pending)}")
    elif pending[0].pipeline_id != "gold_egress":
        fail(f"pending pipeline wrong: {pending[0].pipeline_id}")
    else:
        ok(f"list_pending_reviews → 1 ({pending[0].client_id}/{pending[0].pipeline_id})")

    print()
    print("=== 9. Phase 12.5 — per-source-type granularity + cascade ===")

    from datalink.pipeline.control_policy import GLOBAL_CLIENT

    # Author a CLAIMS-specific policy for acme/bronze_ingest with stricter
    # numbers than its pipeline-wide v2 (pause>5, abort>20). Per-source
    # should win over pipeline-wide for a CLAIMS query.
    claims_draft = PolicyDraft(
        client_id="acme",
        pipeline_id="bronze_ingest",
        source_type="CLAIMS",
        fail_rate_pause_pct=1.0,
        fail_rate_abort_pct=10.0,
        schema_drift_action="ABORT",
        row_count_drop_pct=20.0,
        created_by="user:alice",
        notes="acme claims runs hot — strict",
    )
    claims_pid = reg.create_draft(claims_draft)
    reg.submit_with_policy(claims_pid, mode=ApprovalMode.AUTO_APPROVE, actor="user:alice")
    p = reg.get_or_default("acme", "bronze_ingest", source_type="CLAIMS")
    if p.fail_rate_abort_pct != 10.0 or p.source_type != "CLAIMS":
        fail(f"CLAIMS-specific lookup wrong: source={p.source_type}, abort={p.fail_rate_abort_pct}")
    else:
        ok("cascade tier 1 — per-client + per-source CLAIMS wins (abort>10%)")

    p = reg.get_or_default("acme", "bronze_ingest", source_type="MEMBERSHIP")
    if p.fail_rate_abort_pct != 20.0 or p.source_type is not None:
        fail(
            f"MEMBERSHIP should fall back to pipeline-wide acme v2: got source={p.source_type}, abort={p.fail_rate_abort_pct}"
        )
    else:
        ok("cascade tier 2 — MEMBERSHIP falls back to acme pipeline-wide (abort>20%)")

    # Tier 3+4: a global policy for caresource/silver_build is already LIVE
    # from earlier in the test. No global default exists at the * tier.
    # Author a global * / silver_build / pipeline-wide policy.
    global_draft = PolicyDraft(
        client_id=GLOBAL_CLIENT,
        pipeline_id="silver_build",
        source_type=None,
        fail_rate_pause_pct=3.0,
        fail_rate_abort_pct=18.0,
        schema_drift_action="PAUSE",
        row_count_drop_pct=40.0,
        created_by="system:org_admin",
        notes="org-wide silver default",
    )
    global_pid = reg.create_draft(global_draft)
    reg.submit_with_policy(global_pid, mode=ApprovalMode.AUTO_APPROVE, actor="system:org_admin")

    # Lookup for a tenant that has NO silver_build override → should hit
    # global * tier.
    p = reg.get_or_default("dhmp", "silver_build", source_type="CLAIMS")
    if not p.is_global or p.fail_rate_abort_pct != 18.0:
        fail(
            f"global tier 4 should win for dhmp/silver_build: got client={p.client_id}, abort={p.fail_rate_abort_pct}"
        )
    else:
        ok("cascade tier 4 — dhmp/silver_build/CLAIMS falls back to global * (abort>18%)")

    # caresource HAS a silver_build LIVE (per-client, pipeline-wide). It
    # should win over the global *.
    p = reg.get_or_default("caresource", "silver_build", source_type="CLAIMS")
    if p.client_id != "caresource" or p.fail_rate_abort_pct != 30.0:
        fail(
            f"caresource silver_build should win over global: got client={p.client_id}, abort={p.fail_rate_abort_pct}"
        )
    else:
        ok("cascade priority — caresource per-client beats global * (abort>30%)")

    # No coverage at all for some tuple → DEFAULT_POLICY
    p = reg.get_or_default("affinity", "gold_egress", source_type="CLAIMS")
    if not p.is_default:
        fail(f"unregistered tuple should fall to DEFAULT, got version={p.version}")
    else:
        ok("cascade tier 5 — uncovered tuple falls to DEFAULT_POLICY")

    print()
    print("=== 10. Phase 12.5 — clone_to_client ===")

    # Clone the new acme/bronze_ingest/CLAIMS policy to caresource.
    cloned_id = reg.clone_to_client(claims_pid, "caresource", actor="user:alice")
    cloned = reg.get_by_id(cloned_id)
    if cloned is None or cloned.status is not PolicyStatus.DRAFT:
        fail(f"clone status wrong: {cloned.status if cloned else None}")
    elif cloned.client_id != "caresource":
        fail(f"clone client_id wrong: {cloned.client_id}")
    elif cloned.source_type != "CLAIMS":
        fail(f"clone source_type wrong: {cloned.source_type}")
    elif cloned.fail_rate_abort_pct != 10.0:
        fail(f"clone abort threshold wrong: {cloned.fail_rate_abort_pct}")
    else:
        ok(
            f"clone_to_client: caresource gets a DRAFT v{cloned.version} of CLAIMS policy "
            f"(thresholds copied verbatim, ready for HITL review)"
        )

    # Cloning to same client should refuse.
    try:
        reg.clone_to_client(claims_pid, "acme", actor="user:alice")
        fail("expected ValueError cloning to same client")
    except ValueError:
        ok("clone_to_client refuses same-client target (use fork_for_edit)")

    # Cloning when target already has a DRAFT should refuse.
    try:
        reg.clone_to_client(claims_pid, "caresource", actor="user:alice")
        fail("expected ValueError cloning to client with existing DRAFT")
    except ValueError:
        ok("clone_to_client refuses target with pre-existing DRAFT")

    print()
    if failures:
        print(f"❌ {len(failures)} FAIL")
        for f in failures:
            print(f"   - {f}")
        return 1
    print("✅ Phase 12 + 12.5 PASS — HITL pipeline control + cascade + clone verified")
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
