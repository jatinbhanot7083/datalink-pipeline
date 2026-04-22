"""Pre-Validation Crew orchestrator.

Triggered BEFORE a GX checkpoint runs. Sequence:
  Profiler → Expectation Author → Reviewer.
"""

from __future__ import annotations

from datalink.adapters.factory import AdapterSet
from datalink.agents.base import AgentResult, CrewBase
from datalink.agents.llm_router import get_llm
from datalink.agents.pre_validation.expectation_author import ExpectationAuthorAgent
from datalink.agents.pre_validation.profiler import ProfilerAgent
from datalink.agents.pre_validation.reviewer import ReviewerAgent
from datalink.config.loader import Settings


class PreValidationCrew(CrewBase):
    def __init__(self, adapters: AdapterSet, settings: Settings) -> None:
        llm = get_llm(settings)
        # Phase 6: thinking_mode from settings.features.agents.thinking_mode
        # controls extended-thinking on Anthropic calls. Stub ignores it.
        thinking = settings.features.agents.thinking_mode
        agents = [
            ProfilerAgent(llm, adapters.warehouse, thinking_mode=thinking),
            ExpectationAuthorAgent(llm, adapters.warehouse, thinking_mode=thinking),
            ReviewerAgent(llm, adapters.warehouse, thinking_mode=thinking),
        ]
        super().__init__(name="pre_validation", agents=agents)


def run_pre_validation(adapters: AdapterSet, settings: Settings, table: str) -> list[AgentResult]:
    """Convenience: instantiate and kick off. Returns agent results in order."""
    crew = PreValidationCrew(adapters, settings)
    return crew.kickoff({"table": table})
