"""CrewAI-style agent layer — plug-in quality intelligence.

Two independent crews per GX_Pipeline_Control_Agentic_AI_Architecture.docx §4:
  - Pre-Val Crew    Profiler → Expectation Author → Reviewer
  - Post-Val Crew   Root Cause → Remediation → Reporting

Non-negotiable design principles (from base doc §4.1):
  1. Fail-Safe: agents SUGGEST, never auto-execute pipeline changes.
  2. Crew Separation: each crew runs independently.
  3. Full Auditability: every agent invocation logged to
     CONTROL.agent_reasoning_log.
  4. PHI Guardrail enforced in code — every agent→LLM call goes through
     PhiRedactionLayer.assert_clean; violation raises PhiBoundaryViolationError
     before a byte leaves the process.

When features.agents.enabled = false, the crews are no-ops. The pipeline
runs end-to-end without quality intelligence — proven by
tests/plugout/test_plugout_gx_agents.py.

Imports are LAZY (PEP 562 ``__getattr__``) so importing one part of the
package — e.g. ``datalink.agents.llm_router`` — does not eagerly drag in
``post_validation.crew`` and through it the entire adapter factory + azure
SDK. The Streamlit UI runs in a slim container that has no ``azure`` package;
forcing it through this ``__init__`` was triggering ImportError on azure.core.
Re-exports here remain available — they just resolve on first access instead
of at module-import time.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    # IDEs + mypy see the full surface; runtime stays lazy.
    from datalink.agents.base import AgentBase, AgentResult, CrewBase
    from datalink.agents.llm_router import get_llm
    from datalink.agents.post_validation.crew import PostValidationCrew
    from datalink.agents.pre_validation.crew import PreValidationCrew

__all__ = [
    "AgentBase",
    "AgentResult",
    "CrewBase",
    "PostValidationCrew",
    "PreValidationCrew",
    "get_llm",
]


def __getattr__(name: str) -> Any:
    if name in ("AgentBase", "AgentResult", "CrewBase"):
        from datalink.agents import base as _base

        return getattr(_base, name)
    if name == "get_llm":
        from datalink.agents.llm_router import get_llm as _get_llm

        return _get_llm
    if name == "PostValidationCrew":
        from datalink.agents.post_validation.crew import (
            PostValidationCrew as _PostValidationCrew,
        )

        return _PostValidationCrew
    if name == "PreValidationCrew":
        from datalink.agents.pre_validation.crew import (
            PreValidationCrew as _PreValidationCrew,
        )

        return _PreValidationCrew
    raise AttributeError(f"module 'datalink.agents' has no attribute {name!r}")
