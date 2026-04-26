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
        # Phase 6: adaptive thinking shines on diagnosis tasks — pass through.
        thinking = settings.features.agents.thinking_mode
        agents = [
            RootCauseAgent(llm, adapters.warehouse, thinking_mode=thinking),
            RemediationAgent(llm, adapters.warehouse, thinking_mode=thinking),
            ReportingAgent(llm, adapters.warehouse, adapters.notifier, thinking_mode=thinking),
        ]
        super().__init__(name="post_validation", agents=agents)


def run_post_validation(
    adapters: AdapterSet,
    settings: Settings,
    run_id: str,
    checkpoint_name: str,
    *,
    client_id: str | None = None,
    failure_summary: dict[str, object] | None = None,
) -> list[AgentResult]:
    """Run the Post-Validation crew with optional Tier B RAG grounding.

    Phase 8: when the optional ``failure_summary`` dict is provided, we
    query ``agent_memory.reasoning_embeddings`` for the top-3 most-
    similar past breaches (same client, ranked by cosine distance over
    a humanised summary of the failure). The hits are passed in the
    crew context so each downstream agent can reference them in its
    ``execute()`` (RootCause: "have we seen this before?"; Remediation:
    "what did we recommend last time?").

    Failure to retrieve is non-fatal — the crew runs ungrounded.
    """
    grounding: list[dict[str, object]] = []
    if failure_summary:
        try:
            from datalink.adapters.embeddings.router import get_embedder
            from datalink.memory import AgentMemoryStore
            from datalink.memory.store import reasoning_source_text

            memory = AgentMemoryStore(embedder=get_embedder())
            memory.ensure_schema()
            query_text = reasoning_source_text(failure_summary)
            hits = memory.query_similar_reasoning(
                query_text=query_text,
                client_id=client_id,
                k=3,
            )
            grounding = [
                {
                    "invocation_id": h.invocation_id,
                    "agent_name": h.agent_name,
                    "breach_class": h.breach_class,
                    "recommendation": h.recommendation,
                    "source_text": h.source_text,
                    "distance": h.distance,
                }
                for h in hits
            ]
        except Exception as e:
            # Memory unavailable — proceed ungrounded. Log for ops.
            from datalink.logging import get_logger as _gl

            _gl(__name__).warning("post_val.grounding_failed", error=str(e))
            grounding = []

    crew = PostValidationCrew(adapters, settings)
    return crew.kickoff(
        {
            "run_id": run_id,
            "checkpoint_name": checkpoint_name,
            "client_id": client_id,
            "grounding": grounding,
        }
    )
