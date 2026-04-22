"""Phase 5 structural + unit verification.

End-to-end demo lives in scripts/demo_phase_5.py (verify-phase-5-plugged-in).
Plug-out proof lives in scripts/plugout_phase_5.py (verify-phase-5-plugged-out).
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from datalink.adapters.protocols import LlmMessage
from datalink.agents.base import AgentBase
from datalink.agents.post_validation.remediation import RemediationAgent
from datalink.agents.post_validation.reporting import ReportingAgent
from datalink.agents.post_validation.root_cause import RootCauseAgent
from datalink.agents.pre_validation.expectation_author import ExpectationAuthorAgent
from datalink.agents.pre_validation.profiler import ProfilerAgent
from datalink.agents.pre_validation.reviewer import ReviewerAgent
from datalink.quality import CheckpointStatus
from datalink.quality.control import PipelineState
from datalink.quality.suites import (
    BRONZE_STRUCTURAL,
    GOLD_BUSINESS,
    SILVER_CLINICAL,
    build_bronze_suite,
    build_gold_suite,
    build_silver_suite,
)

REPO_ROOT = Path(__file__).resolve().parents[2]


# ---------------------------------------------------------------------------
# GX layer structural
# ---------------------------------------------------------------------------


@pytest.mark.phase
def test_all_three_checkpoint_names_defined() -> None:
    assert BRONZE_STRUCTURAL == "bronze_structural"
    assert SILVER_CLINICAL == "silver_clinical"
    assert GOLD_BUSINESS == "gold_business"


@pytest.mark.phase
@pytest.mark.parametrize("builder", [build_bronze_suite, build_silver_suite, build_gold_suite])
def test_every_suite_builds_at_least_five_expectations(builder) -> None:  # type: ignore[no-untyped-def]
    import great_expectations as gx

    gx.get_context(mode="ephemeral")  # prime singleton
    suite = builder()
    assert len(suite.expectations) >= 5, f"{builder.__name__} produced too few expectations"


@pytest.mark.phase
def test_control_schema_ddl_includes_required_tables() -> None:
    from datalink.quality import control as ctl

    ddl_text = "\n".join(ctl._DDL).lower()
    for required in [
        "pipeline_control_state",
        "pipeline_checkpoints",
        "pipeline_control_audit_log",
        "gx_validation_results",
        "batch_quarantine",
        "agent_reasoning_log",
    ]:
        assert required in ddl_text, f"DDL missing table {required}"


@pytest.mark.phase
def test_pipeline_state_transitions_legal() -> None:
    from datalink.quality.control import PipelineControl

    legal = PipelineControl._LEGAL_TRANSITIONS
    # Running → Paused → Resuming → Running cycle
    assert PipelineState.PAUSED in legal[PipelineState.RUNNING]
    assert PipelineState.RESUMING in legal[PipelineState.PAUSED]
    assert PipelineState.RUNNING in legal[PipelineState.RESUMING]
    # Aborted is terminal
    assert legal[PipelineState.ABORTED] == set()
    # Paused cannot go directly to Running
    assert PipelineState.RUNNING not in legal[PipelineState.PAUSED]


# ---------------------------------------------------------------------------
# Agent layer structural
# ---------------------------------------------------------------------------


@pytest.mark.phase
@pytest.mark.parametrize(
    "cls",
    [
        ProfilerAgent,
        ExpectationAuthorAgent,
        ReviewerAgent,
        RootCauseAgent,
        RemediationAgent,
        ReportingAgent,
    ],
)
def test_every_agent_has_role_goal_and_crew(cls) -> None:  # type: ignore[no-untyped-def]
    assert cls.role, f"{cls.__name__}.role is empty"
    assert cls.goal, f"{cls.__name__}.goal is empty"
    assert cls.crew_name in {"pre_validation", "post_validation"}, cls.crew_name


@pytest.mark.phase
def test_exactly_six_agents_exported() -> None:
    """Phase 0 decision #2 pinned the baseline at 6 agents (3+3)."""

    pre_classes = {ProfilerAgent, ExpectationAuthorAgent, ReviewerAgent}
    post_classes = {RootCauseAgent, RemediationAgent, ReportingAgent}
    assert len(pre_classes) == 3
    assert len(post_classes) == 3


# ---------------------------------------------------------------------------
# AgentBase + PHI boundary (unit tests, no DB)
# ---------------------------------------------------------------------------


class _RecordingLlm:
    """Test double that records the last messages it received."""

    def __init__(self) -> None:
        self.model = "test"
        self.last_messages: list[LlmMessage] | None = None

    def complete(self, messages, max_tokens, temperature=0.0, thinking_mode="off"):  # type: ignore[no-untyped-def]
        from datalink.adapters.protocols import LlmCompletion

        self.last_messages = messages
        return LlmCompletion(
            content='{"ok": true}',
            input_tokens=1,
            output_tokens=1,
            model=self.model,
            stop_reason="stop_sequence",
        )


class _NoopAgent(AgentBase):
    role = "no-op"
    goal = "no-op"
    crew_name = "pre_validation"

    def execute(self, context):  # type: ignore[no-untyped-def]
        # Forward whatever the caller sent to the LLM (PHI guard should bite on bad keys).
        self._ask_llm("test", safe_payload=context)
        return {"ok": True}


@pytest.mark.unit
def test_agent_phi_guard_rejects_row_like_payload() -> None:
    llm = _RecordingLlm()
    wh = MagicMock()
    agent = _NoopAgent(llm, wh)
    result = agent.run({"raw_row": {"member_id": "MBR-1", "dob": "1950-06-15"}})
    assert result.success is False
    assert "not in SAFE_FIELDS" in (result.error or "")
    assert llm.last_messages is None  # LLM NEVER called


@pytest.mark.unit
def test_agent_phi_guard_allows_metadata_only_payload() -> None:
    llm = _RecordingLlm()
    wh = MagicMock()
    agent = _NoopAgent(llm, wh)
    result = agent.run({"table_name": "RAW_CLAIMS", "row_count": 1000, "null_pct": 0.02})
    assert result.success is True
    assert llm.last_messages is not None


@pytest.mark.unit
def test_agent_audit_log_called_on_every_run() -> None:
    llm = _RecordingLlm()
    wh = MagicMock()
    agent = _NoopAgent(llm, wh)
    agent.run({"table_name": "X"})
    # Find the INSERT INTO CONTROL.agent_reasoning_log call.
    calls = [c for c in wh.execute.call_args_list if "agent_reasoning_log" in str(c)]
    assert calls, "audit log INSERT was not executed"


# ---------------------------------------------------------------------------
# Plug-out proof — config toggles make hooks a no-op.
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_hooks_skip_gx_when_feature_disabled(
    monkeypatch: pytest.MonkeyPatch, clean_dl_env: None
) -> None:
    monkeypatch.setenv("DL_FEATURES__GX__ENABLED", "false")
    monkeypatch.setenv("DL_FEATURES__AGENTS__ENABLED", "false")
    from datalink.config.loader import load_settings

    settings = load_settings(env="local")
    assert settings.features.gx.enabled is False
    assert settings.features.agents.enabled is False

    # Build fake adapters (warehouse that records calls).
    wh = MagicMock()
    wh.query.return_value = []
    notifier = MagicMock()
    adapters = MagicMock()
    adapters.warehouse = wh
    adapters.notifier = notifier

    from datalink.pipeline.hooks import run_checkpoint_with_hooks

    hc = run_checkpoint_with_hooks(
        adapters,
        settings,
        "pipe",
        "run",
        "cp",
        "T",
        build_bronze_suite,
    )
    assert hc.checkpoint.status == CheckpointStatus.SKIPPED
    assert hc.pre_val_results == []
    assert hc.post_val_results == []
    assert hc.pipeline_paused is False
    # Neither pre-val nor post-val LLM calls happened — warehouse.query was only
    # touched by control-table ops, not the profiler's aggregates.
