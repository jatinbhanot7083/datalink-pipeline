"""Pre-Validation Crew orchestrator.

Triggered BEFORE a GX checkpoint runs. Sequence:
  Profiler → Expectation Author → Reviewer.

Phase 6 Commit 2 additions:
  * Schema-fingerprint cache — if a LIVE suite for (client, source_type)
    already exists with the same column layout, the crew skips entirely
    (zero LLM calls). Drift in columns/types invalidates the cache.
  * Auto-approve split — ExpectationAuthorAgent lands high-confidence rules
    directly as LIVE (no human). Only flagged rules enter PENDING_REVIEW.
"""

from __future__ import annotations

from datalink.adapters.factory import AdapterSet
from datalink.agents.base import AgentResult, CrewBase
from datalink.agents.llm_router import get_llm
from datalink.agents.pre_validation.expectation_author import ExpectationAuthorAgent
from datalink.agents.pre_validation.profiler import ProfilerAgent
from datalink.agents.pre_validation.reviewer import ReviewerAgent
from datalink.config.loader import Settings
from datalink.logging import get_logger
from datalink.quality.registry import SuiteRegistry
from datalink.quality.schema_fingerprint import compute_fingerprint

_log = get_logger(__name__)


class PreValidationCrew(CrewBase):
    def __init__(self, adapters: AdapterSet, settings: Settings) -> None:
        llm = get_llm(settings)
        thinking = settings.features.agents.thinking_mode
        agents = [
            ProfilerAgent(llm, adapters.warehouse, thinking_mode=thinking),
            ExpectationAuthorAgent(llm, adapters.warehouse, thinking_mode=thinking),
            ReviewerAgent(llm, adapters.warehouse, thinking_mode=thinking),
        ]
        super().__init__(name="pre_validation", agents=agents)


def _derive_suite_name(client_id: str, source_type: str | None) -> str | None:
    """Convention: LIVE auto-approved suite is `auto_{source_type}`.
    Returns None when source_type is missing (legacy aggregate path)."""
    if not source_type:
        return None
    return f"auto_{source_type.lower()}"


def run_pre_validation(
    adapters: AdapterSet,
    settings: Settings,
    table: str,
    client_id: str = "default",
    source_type: str | None = None,
    checkpoint_name: str | None = None,
    force_refresh: bool = False,
) -> list[AgentResult]:
    """Run the Pre-Val crew unless the schema is unchanged since last authoring.

    Skip path (no Claude calls, returns []):
        - force_refresh=False AND
        - a LIVE suite named auto_{source_type} exists for client_id AND
        - its schema_fingerprint matches the current table's fingerprint.

    Run path (3 agents fire, ~12-30 LLM calls with structured outputs):
        - new client, OR
        - new source_type for this client, OR
        - schema drift detected, OR
        - force_refresh=True explicitly.

    Returns the AgentResult list from the crew (empty list on skip).
    """
    # Phase 6 Commit 2 cache gate — short-circuit on unchanged schema.
    if not force_refresh and source_type:
        try:
            current_fp = compute_fingerprint(adapters.warehouse, table)
        except Exception as exc:
            _log.warning(
                "pre_val.fingerprint.unavailable",
                table=table,
                error=str(exc)[:200],
            )
            current_fp = None

        if current_fp:
            suite_name = _derive_suite_name(client_id, source_type)
            if suite_name:
                reg = SuiteRegistry(adapters.warehouse)
                live = reg.get_live(client_id, suite_name)
                if live and live.schema_fingerprint == current_fp:
                    _log.info(
                        "pre_val.skip.schema_unchanged",
                        client=client_id,
                        source_type=source_type,
                        table=table,
                        suite=suite_name,
                        suite_version=live.version,
                        fingerprint=current_fp,
                    )
                    return []

    # Not cached (or cache disabled) — run the full crew.
    _log.info(
        "pre_val.running",
        client=client_id,
        source_type=source_type,
        table=table,
        force_refresh=force_refresh,
    )
    crew = PreValidationCrew(adapters, settings)
    return crew.kickoff(
        {
            "table": table,
            "client_id": client_id,
            "source_type": source_type,
            "checkpoint_name": checkpoint_name,
        }
    )
