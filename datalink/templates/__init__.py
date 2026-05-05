"""Global Template Layer — Phase 16.7.

The platform maintains a CANONICAL global template per dataset (Bronze + Silver +
Gold + DAG + GX + routing). New clients clone from global by default — zero
LLM tokens, zero re-design effort. Per-client overrides layer on top of the
clone, with full lineage back to the global version they forked from.

Public API:

    from datalink.templates import store

    # Inventory
    store.list_globals()                  # all global templates (per dataset)
    store.has_global(dataset_code)        # True/False quick check
    store.get_global(dataset_code)        # full template (Silver + Gold + Pipeline)

    # Authoring (one-time per dataset)
    store.publish_silver_to_global(silver_dataset_id, by)
    store.publish_gold_to_global(gold_dataset_id, by)
    store.publish_pipeline_to_global(instance_id, by)

    # Cloning (the 70-80% case — no LLM tokens)
    store.clone_to_client(
        dataset_code='membership',
        target_client_id='bcbs',
        actor='ui:bcbs',
    )

    # Migration (when global changes, fan-out to clients)
    store.plan_migration(global_artifact_id, from_v, to_v, by)
    store.list_open_migrations()
"""

from datalink.templates.store import (  # noqa: F401
    GLOBAL_SCOPE,
    GlobalTemplateError,
    clone_to_client,
    get_global,
    has_global,
    list_globals,
    plan_migration,
    publish_gold_to_global,
    publish_pipeline_to_global,
    publish_silver_to_global,
)
