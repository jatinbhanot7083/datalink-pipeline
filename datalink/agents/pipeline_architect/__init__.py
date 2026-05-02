"""Phase 15 — Pipeline Architect.

Materialises a per-client pipeline instance from the Global Gold Catalog.

Public API:

  * ``PipelineArchitectAgent`` — Anthropic-backed agent, deterministic
    artifacts + LLM narrative.
  * ``propose_pipeline``       — bridge entrypoint, looks up catalog +
    peer instances and returns a fully-resolved proposal dict.
  * ``persist_proposal``       — writes proposal as a DRAFT row in
    CONTROL.client_pipeline_instances.
  * ``approve_and_deploy``     — emits 5 artifacts (Gold DDL, Silver dbt,
    Gold dbt, Airflow DAG, GX suite) and flips status to LIVE.
"""

from .agent import DecisionMode, PipelineArchitectAgent, PipelineProposal
from .bridge import (
    approve_and_deploy,
    list_client_instances,
    list_dataset_codes,
    persist_proposal,
    propose_pipeline,
)

__all__ = [
    "DecisionMode",
    "PipelineArchitectAgent",
    "PipelineProposal",
    "approve_and_deploy",
    "list_client_instances",
    "list_dataset_codes",
    "persist_proposal",
    "propose_pipeline",
]
