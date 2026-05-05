"""Saved-proposal infrastructure (Phase 16.1).

Public API:

    from datalink.proposals import store, Proposal, ProposalStatus

    # Save a fresh proposal (returns Proposal with version=N)
    p = store.save_proposal(
        agent_type="pipeline_architect",
        scope_type="client_dataset",
        scope_key="aetna:membership",
        payload={...},                # full agent output
        agent_input={...},             # prompt + RAG context (for replay)
        prompt_tokens=850, completion_tokens=191,
        model_id="claude-haiku-4.5",
        latency_ms=4145,
        created_by="ui:aetna",
    )

    # Find latest non-rejected/non-archived proposal for a scope
    active = store.find_active(scope_type="client_dataset", scope_key="aetna:membership")

    # Full history (every version, every status, newest first)
    versions = store.history(scope_type="client_dataset", scope_key="aetna:membership")

    # Approve (links to a downstream artifact)
    store.approve(p.proposal_id, approved_by="ui:aetna",
                  linked_artifact_type="pipeline_instance",
                  linked_artifact_id="abc-123")

    # Reject / Archive
    store.reject(p.proposal_id, rejected_by="ui:aetna")
    store.archive(p.proposal_id, archived_by="ui:aetna")

    # Tag operations
    store.add_tag(proposal_id="...", tag_name="known-good", assigned_by="ui:aetna")
    store.remove_tag(proposal_id="...", tag_name="known-good", removed_by="ui:aetna")
    store.list_tags(proposal_id="...")
"""

from datalink.proposals.store import (  # noqa: F401
    Proposal,
    ProposalStatus,
    TagRoleError,
    add_tag,
    all_tags,
    approve,
    archive,
    estimate_cost_usd,
    find_active,
    get,
    history,
    list_tags,
    pin,
    reject,
    remove_tag,
    save_proposal,
    unpin,
)
