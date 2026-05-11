"""Phase 19.1 — DQ AI Architect (factory-pattern, suite-level).

Public API:
  * DqSuiteArchitectAgent — Anthropic-backed agent.  Suite-level proposer
    (vs the per-expectation DqProposerAgent in datalink.agents.dq_author).
  * propose_suite()       — bridge entrypoint: fetch schema + RAG + run.
  * persist_suite()       — write DRAFT row to CONTROL.dq_suites.
  * approve_and_promote() — DRAFT → LIVE; archives prior LIVE per
                            (dataset_code, layer) invariant.
"""

from .agent import DqSuiteArchitectAgent
from .bridge import approve_and_promote, persist_suite, propose_suite

__all__ = [
    "DqSuiteArchitectAgent",
    "approve_and_promote",
    "persist_suite",
    "propose_suite",
]
