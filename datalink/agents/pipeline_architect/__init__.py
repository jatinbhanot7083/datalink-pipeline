"""Phase 15 + 15.7 — Pipeline Architect.

Materialises a per-client pipeline instance, top-down from the LIVE Gold
schema (Phase 15.6 designer output). When no LIVE Gold exists it falls
back to the original Phase 15 mode (Bronze catalog as Gold) and prompts
the operator to design Gold first.

Public API:

  * ``PipelineArchitectAgent`` — Anthropic-backed agent, deterministic
    artifacts + LLM narrative.
  * ``propose_pipeline``       — bridge entrypoint, looks up catalog +
    LIVE Gold + peer instances and returns a fully-resolved proposal.
  * ``persist_proposal``       — writes proposal as a DRAFT row in
    CONTROL.client_pipeline_instances.
  * ``approve_and_deploy``     — emits artifacts (Bronze DDL with
    _variant_overflow, Gold DDL, Silver DV2 dbt models, Gold dbt, Airflow
    DAG, GX suite) and flips status to LIVE.

Phase 15.7 additions:
  * ``fetch_live_gold_schema``       — Gold-LIVE detection helper
  * ``list_pending_overflow``        — overflow-column HITL queue
  * ``log_overflow_column``          — Bronze ingest hook
  * ``propose_greenfield_dataset``   — register a brand-new dataset
                                       from a sample file (chains
                                       Phase 14 ContractArchitect)
  * ``approve_greenfield``           — promote into Bronze catalog
  * ``list_greenfield_proposals``    — UI list helper
"""

from .agent import DecisionMode, PipelineArchitectAgent, PipelineProposal
from .bridge import (
    PipelinePrerequisitesError,
    approve_and_deploy,
    approve_greenfield,
    check_pipeline_prerequisites,
    clone_pipeline_to_client,
    fetch_live_gold_schema,
    list_client_instances,
    list_clonable_peer_pipelines,
    list_dataset_codes,
    list_greenfield_proposals,
    list_pending_overflow,
    log_overflow_column,
    persist_proposal,
    propose_greenfield_dataset,
    propose_pipeline,
)

__all__ = [
    "DecisionMode",
    "PipelineArchitectAgent",
    "PipelinePrerequisitesError",
    "PipelineProposal",
    "approve_and_deploy",
    "approve_greenfield",
    "check_pipeline_prerequisites",
    "clone_pipeline_to_client",
    "fetch_live_gold_schema",
    "list_client_instances",
    "list_clonable_peer_pipelines",
    "list_dataset_codes",
    "list_greenfield_proposals",
    "list_pending_overflow",
    "log_overflow_column",
    "persist_proposal",
    "propose_greenfield_dataset",
    "propose_pipeline",
]
