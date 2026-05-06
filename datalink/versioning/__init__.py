"""Universal version registry — Phase 16.2 (Wave 2).

ONE table (CONTROL.version_history) tracks every version of every artifact:
Silver schemas, Gold schemas, pipeline instances, DQ suites, …

Public API:

    from datalink.versioning import store

    # Record a new version
    ev = store.record_version(
        artifact_type="silver_schema",
        artifact_id="silver-membership",
        artifact_scope_key="membership",
        snapshot={"tables": [...], "columns": [...]},
        change_kind="CREATE",
        change_reason="Initial AI-generated schema",
        created_by="ui:aetna",
    )

    # Get full history (all versions of an artifact, newest first)
    hist = store.history(artifact_type="silver_schema", artifact_id="silver-membership")

    # Diff two versions
    d = store.diff(version_event_id_a, version_event_id_b)

    # Restore a prior version (creates a new "current" with change_kind=RESTORE)
    new_ev = store.restore(version_event_id, restored_by="ui:aetna")

    # Pin a version (protects from auto-archive on future updates)
    store.pin(version_event_id)

    # Tag a version
    store.add_tag(version_event_id, tag_name="certified-prod", assigned_by="ui:aetna")

    # Cross-client clone — record the lineage
    new_ev = store.record_version(
        ...
        cloned_from_artifact_id="silver-membership-aetna",
        cloned_from_version=4,
        change_kind="CLONE",
    )

    # Upstream-update notifications
    store.notify_upstream_update(
        downstream_artifact_id="pipeline-aetna-membership",
        upstream_artifact_id="silver-membership",
        consumed_version=3,
        new_version=4,
    )
"""

# Phase 17.4 — specialized Gold semver + per-client subscription helpers
# (parallel to the generic store above, tailored for the BREAKING/ADDITIVE
# compatibility classification needed for downstream-product migrations).
from datalink.versioning import gold_schema  # noqa: F401
from datalink.versioning.store import (  # noqa: F401
    UpstreamNotification,
    VersionEvent,
    VersionRestoreError,
    add_tag,
    diff,
    get,
    history,
    list_open_notifications,
    notify_upstream_update,
    pin,
    record_version,
    remove_tag,
    restore,
    set_current,
    unpin,
)
