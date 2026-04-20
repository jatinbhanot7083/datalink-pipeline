"""Post-Validation Crew orchestrator.

Triggered AFTER a GX checkpoint has FAILED. Sequence:
  Root Cause → Remediation → Reporting.

Nothing auto-executes. The pipeline state transition to PAUSED happens
separately (in the quality-layer integration — see datalink.pipeline.hooks).
"""

from __future__ import annotations

from datalink.adapters.factory import AdapterSet
from datalink.agents.base import AgentResult, CrewBase
from datalink.agents.llm_router import get_llm
from datalink.agents.post_validation.remediation import RemediationAgent
from datalink.agents.post_validation.reporting import ReportingAgent
from datalink.agents.post_validation.root_cause import RootCauseAgent
from datalink.config.loader import Settings


class PostValidationCrew(CrewBase):
    def __init__(self, adapters: AdapterSet, settings: Settings) -> None:
        llm = get_llm(settings)
        agents = [
            RootCauseAgent(llm, adapters.warehouse),
            RemediationAgent(llm, adapters.warehouse),
            ReportingAgent(llm, adapters.warehouse, adapters.notifier),
        ]
        super().__init__(name="post_validation", agents=agents)


def run_post_validation(
    adapters: AdapterSet, settings: Settings, run_id: str, checkpoint_name: str
) -> list[AgentResult]:
    crew = PostValidationCrew(adapters, settings)
    return crew.kickoff({"run_id": run_id, "checkpoint_name": checkpoint_name})
