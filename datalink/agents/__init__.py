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
"""

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
