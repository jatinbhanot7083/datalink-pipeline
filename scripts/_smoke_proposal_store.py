"""Smoke test for datalink.proposals.store — exercises every public function
end-to-end against the live Snowflake. Cleans up after itself."""

from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

# Auto-load .env
_ENV = ROOT / ".env"
if _ENV.exists():
    for raw in _ENV.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        os.environ.setdefault(k.strip(), v.strip().strip("'").strip('"'))
os.environ.setdefault("DL_ENV", "dev")

from datalink.proposals import store  # noqa: E402
from datalink.proposals.store import ProposalStatus  # noqa: E402

SCOPE_TYPE = "smoke_test"
SCOPE_KEY = "phase16_smoke"


def main() -> int:
    print("=" * 60)
    print(" datalink.proposals.store smoke test")
    print("=" * 60)
    print()

    # 1. estimate_cost_usd — pure function, no DB
    print("[1/9] estimate_cost_usd")
    cost = store.estimate_cost_usd(
        model_id="claude-haiku-4.5", prompt_tokens=850, completion_tokens=191
    )
    expected = (850 / 1_000_000) * 1.00 + (191 / 1_000_000) * 5.00
    assert abs(cost - expected) < 1e-9, f"cost mismatch: got {cost} expected {expected}"
    print(f"      ✅ Haiku 850/191 → ${cost:.6f} (expected ${expected:.6f})")

    # 2. all_tags — system catalog should have 13 rows
    print("[2/9] all_tags (system catalog)")
    tags = store.all_tags()
    assert len(tags) >= 13, f"expected ≥13 system tags, got {len(tags)}"
    print(f"      ✅ catalog has {len(tags)} tags; sample: {[t['tag_name'] for t in tags[:3]]}")

    # 3. find_active — should be None initially
    print("[3/9] find_active (empty scope)")
    p = store.find_active(scope_type=SCOPE_TYPE, scope_key=SCOPE_KEY)
    assert p is None, f"expected None for empty scope, got {p}"
    print(f"      ✅ no proposal yet for {SCOPE_TYPE}:{SCOPE_KEY}")

    # 4. save_proposal — create v1
    print("[4/9] save_proposal v1")
    p1 = store.save_proposal(
        agent_type="pipeline_architect",
        scope_type=SCOPE_TYPE,
        scope_key=SCOPE_KEY,
        payload={"executive_summary": "first attempt", "tables": 13},
        agent_input={"prompt": "Propose a pipeline for membership"},
        prompt_tokens=850,
        completion_tokens=191,
        model_id="claude-haiku-4.5",
        latency_ms=4145,
        created_by="smoke:test",
    )
    assert p1.version == 1
    assert p1.status == ProposalStatus.DRAFT
    assert p1.proposal_payload["tables"] == 13
    assert p1.estimated_cost_usd > 0
    print(
        f"      ✅ saved {p1.proposal_id[:8]}... v1, "
        f"{p1.total_tokens} tokens, ${p1.estimated_cost_usd:.6f}"
    )

    # 5. save_proposal v2 — should auto-archive v1
    print("[5/9] save_proposal v2 (should auto-archive v1)")
    p2 = store.save_proposal(
        agent_type="pipeline_architect",
        scope_type=SCOPE_TYPE,
        scope_key=SCOPE_KEY,
        payload={"executive_summary": "second attempt", "tables": 14},
        agent_input={"prompt": "Re-propose"},
        prompt_tokens=900,
        completion_tokens=210,
        model_id="claude-haiku-4.5",
        latency_ms=5012,
        created_by="smoke:test",
    )
    assert p2.version == 2
    assert p2.supersedes_proposal_id == p1.proposal_id
    print(f"      ✅ saved {p2.proposal_id[:8]}... v2 supersedes v1")

    p1_after = store.get(p1.proposal_id)
    assert p1_after.status == ProposalStatus.ARCHIVED, (
        f"expected v1 ARCHIVED, got {p1_after.status}"
    )
    print(f"      ✅ v1 auto-archived (status={p1_after.status.value})")

    # 6. find_active — should now return v2
    print("[6/9] find_active returns v2")
    active = store.find_active(scope_type=SCOPE_TYPE, scope_key=SCOPE_KEY)
    assert active is not None and active.version == 2
    print(f"      ✅ active = v{active.version} ({active.proposal_id[:8]}...)")

    # 7. add_tag / list_tags / remove_tag
    print("[7/9] tag operations")
    store.add_tag(proposal_id=p2.proposal_id, tag_name="ai-only", assigned_by="smoke:test")
    store.add_tag(proposal_id=p2.proposal_id, tag_name="experimental", assigned_by="smoke:test")
    # idempotent re-add
    store.add_tag(proposal_id=p2.proposal_id, tag_name="ai-only", assigned_by="smoke:test")
    tags_on_p2 = store.list_tags(p2.proposal_id)
    assert set(tags_on_p2) == {"ai-only", "experimental"}, f"unexpected tags: {tags_on_p2}"
    print(f"      ✅ tags on p2: {tags_on_p2}")

    removed = store.remove_tag(
        proposal_id=p2.proposal_id, tag_name="experimental", removed_by="smoke:test"
    )
    assert removed is True
    tags_after = store.list_tags(p2.proposal_id)
    assert tags_after == ["ai-only"]
    print(f"      ✅ after remove: {tags_after}")

    # 8. approve — links to a fake artifact
    print("[8/9] approve")
    store.approve(
        p2.proposal_id,
        approved_by="smoke:test",
        linked_artifact_type="pipeline_instance",
        linked_artifact_id="fake-instance-abc-123",
    )
    p2_after = store.get(p2.proposal_id)
    assert p2_after.status == ProposalStatus.APPROVED
    assert p2_after.linked_artifact_id == "fake-instance-abc-123"
    print(
        f"      ✅ p2 status=APPROVED, linked to "
        f"{p2_after.linked_artifact_type}:{p2_after.linked_artifact_id}"
    )

    # 9. history — should show both v1 and v2
    print("[9/12] history")
    hist = store.history(scope_type=SCOPE_TYPE, scope_key=SCOPE_KEY)
    assert len(hist) == 2
    assert hist[0].version == 2 and hist[0].status == ProposalStatus.APPROVED
    assert hist[1].version == 1 and hist[1].status == ProposalStatus.ARCHIVED
    print("      ✅ history has 2 entries, newest first:")
    for h in hist:
        print(f"          v{h.version}  {h.status.value:9s}  tags={h.tags}  by={h.created_by}")

    # 10. Auto-archive of APPROVED — re-propose v3 should archive v2 (APPROVED).
    print("[10/12] save_proposal v3 — should auto-archive APPROVED v2")
    p3 = store.save_proposal(
        agent_type="pipeline_architect",
        scope_type=SCOPE_TYPE,
        scope_key=SCOPE_KEY,
        payload={"executive_summary": "third attempt", "tables": 15},
        agent_input={"prompt": "Re-propose for v3"},
        prompt_tokens=920,
        completion_tokens=205,
        model_id="claude-haiku-4.5",
        latency_ms=4500,
        created_by="smoke:test",
    )
    assert p3.version == 3
    p2_after_v3 = store.get(p2.proposal_id)
    assert p2_after_v3.status == ProposalStatus.ARCHIVED, (
        f"expected v2 ARCHIVED after v3 save, got {p2_after_v3.status}"
    )
    # The downstream artifact link is still preserved on v2 — only status changed
    assert p2_after_v3.linked_artifact_id == "fake-instance-abc-123"
    print("      ✅ v3 saved; v2 (was APPROVED) is now ARCHIVED")
    print(
        f"      ✅ v2's linked_artifact preserved: "
        f"{p2_after_v3.linked_artifact_type}:{p2_after_v3.linked_artifact_id}"
    )

    # 11. Pinned protection — pinning a version should keep it active across re-proposes
    print("[11/12] pin protection — pinned version survives re-propose")
    store.pin(p3.proposal_id)
    p4 = store.save_proposal(
        agent_type="pipeline_architect",
        scope_type=SCOPE_TYPE,
        scope_key=SCOPE_KEY,
        payload={"executive_summary": "fourth attempt", "tables": 16},
        agent_input={"prompt": "Re-propose for v4"},
        prompt_tokens=900,
        completion_tokens=200,
        model_id="claude-haiku-4.5",
        latency_ms=4000,
        created_by="smoke:test",
    )
    assert p4.version == 4
    p3_after = store.get(p3.proposal_id)
    assert p3_after.status == ProposalStatus.DRAFT, (
        f"pinned v3 should stay DRAFT, got {p3_after.status}"
    )
    assert p3_after.pinned is True
    print("      ✅ v3 (pinned, status=DRAFT) survived v4's auto-archive sweep")
    # Cleanup pin so v3 can be archived in cleanup phase
    store.unpin(p3.proposal_id)

    # 12. Role enforcement — currently permissive but log entries fire
    print("[12/12] role check — permissive today, audited")
    # Apply a role-gated tag — should succeed under stub (today all roles held)
    store.add_tag(proposal_id=p4.proposal_id, tag_name="hipaa-audited", assigned_by="smoke:test")
    tags_p4 = store.list_tags(p4.proposal_id)
    assert "hipaa-audited" in tags_p4
    print("      ✅ role-gated tag 'hipaa-audited' applied (stub permissive — Wave 3 enforces)")
    # Verify the catalog has the new compliance + client-scoped tags
    catalog = {t["tag_name"]: t for t in store.all_tags()}
    for required in [
        "soc2-compliant",
        "gdpr-compliant",
        "hitrust-validated",
        "aetna-only",
        "bcbs-only",
    ]:
        assert required in catalog, f"missing system tag: {required}"
    print(
        "      ✅ all 5 new tags present in catalog "
        "(soc2 + gdpr + hitrust + aetna-only + bcbs-only)"
    )

    # Cleanup — remove the smoke-test rows so we don't pollute prod
    print()
    print("Cleanup...")
    from datalink.adapters.factory import build_adapters
    from datalink.config.loader import load_settings
    from datalink.quality.control import CONTROL_SCHEMA

    wh = build_adapters(load_settings(env="dev")).warehouse
    wh.execute(
        f"DELETE FROM {CONTROL_SCHEMA}.proposal_tag_assignments "
        f"WHERE proposal_id IN (SELECT proposal_id FROM {CONTROL_SCHEMA}.agent_proposals "
        f"                       WHERE scope_type = %(st)s AND scope_key = %(sk)s)",
        {"st": SCOPE_TYPE, "sk": SCOPE_KEY},
    )
    wh.execute(
        f"DELETE FROM {CONTROL_SCHEMA}.agent_proposals "
        f"WHERE scope_type = %(st)s AND scope_key = %(sk)s",
        {"st": SCOPE_TYPE, "sk": SCOPE_KEY},
    )
    print(f"  ✅ cleaned up smoke-test rows for {SCOPE_TYPE}:{SCOPE_KEY}")

    print()
    print("🟢 ALL 12 CHECKS PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
