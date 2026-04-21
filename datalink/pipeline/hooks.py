"""Pipeline hooks that integrate GX + CrewAI with the Bronze/Silver/Gold flow.

One public entry point — `run_checkpoint_with_hooks` — that wraps a single
GX checkpoint run with the Pre-Val (before) + Post-Val (after failure) crews.
Both sides are gated on config:

  features.gx.enabled = false      → checkpoint is SKIPPED (returns a marker
                                     CheckpointResult; pipeline continues)
  features.agents.enabled = false  → Pre-Val and Post-Val crews are no-ops

When both are false the pipeline runs exactly as in Phase 4 — proven by
tests/plugout/test_plugout_gx_agents.py.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from datalink.logging import get_logger
from datalink.quality import (
    CheckpointResult,
    CheckpointStatus,
    PipelineControl,
    PipelineState,
    run_checkpoint,
    seed_baselines,
)
from datalink.quality.checkpoint import skipped_result
from datalink.quality.control import Severity, StateTransition

if TYPE_CHECKING:
    from great_expectations.core.expectation_suite import ExpectationSuite

    from datalink.adapters.factory import AdapterSet
    from datalink.agents.base import AgentResult
    from datalink.config.loader import Settings

_log = get_logger(__name__)


@dataclass
class HookedCheckpoint:
    checkpoint: CheckpointResult
    pre_val_results: list[AgentResult] = field(default_factory=list)
    post_val_results: list[AgentResult] = field(default_factory=list)
    pipeline_paused: bool = False


def run_checkpoint_with_hooks(
    adapters: AdapterSet,
    settings: Settings,
    pipeline_id: str,
    run_id: str,
    checkpoint_name: str,
    qualified_table: str,
    suite_builder: Callable[[], ExpectationSuite] | None = None,
    client_id: str = "default",
    fail_threshold_pct: float | None = None,
) -> HookedCheckpoint:
    """Run Pre-Val crew → GX checkpoint → (on failure) Post-Val crew → pause pipeline.

    Phase 5.8: `client_id` routes to the DB-backed SuiteRegistry for the LIVE
    expectation suite. `suite_builder` remains as an optional legacy fallback
    so existing Phase-5 tests that pass a Python builder still work.
    """
    gx_enabled = settings.features.gx.enabled
    agents_enabled = settings.features.agents.enabled
    control = PipelineControl(adapters.warehouse)
    control.ensure()
    control.start(pipeline_id)
    # Idempotent seed: on very first run, populate client='default' with the
    # 3 LIVE baselines. No-op on subsequent runs. Only when GX is enabled —
    # no reason to seed if the whole quality layer is turned off.
    if gx_enabled:
        seed_baselines(adapters.warehouse)

    # ------------------------------------------------------------------
    # Pre-Val crew (if enabled)
    # ------------------------------------------------------------------
    pre_val_results: list[AgentResult] = []
    if agents_enabled:
        # Lazy import — keeps plug-out fast (no CrewAI / anthropic imports needed).
        from datalink.agents.pre_validation.crew import run_pre_validation

        _log.info("hooks.pre_validation.start", table=qualified_table)
        pre_val_results = run_pre_validation(adapters, settings, qualified_table)
    else:
        _log.info("hooks.pre_validation.disabled")

    # ------------------------------------------------------------------
    # GX checkpoint (if enabled)
    # ------------------------------------------------------------------
    if gx_enabled:
        threshold = (
            fail_threshold_pct
            if fail_threshold_pct is not None
            else (
                settings.features.gx.checkpoints.get(
                    checkpoint_name, type("P", (), {"fail_threshold_pct": 5.0})()
                ).fail_threshold_pct
            )
        )
        checkpoint = run_checkpoint(
            warehouse=adapters.warehouse,
            qualified_table=qualified_table,
            checkpoint_name=checkpoint_name,
            pipeline_id=pipeline_id,
            run_id=run_id,
            client_id=client_id,
            suite_builder=suite_builder,
            fail_threshold_pct=threshold,
        )
        control.record_checkpoint(
            run_id=run_id,
            pipeline_id=pipeline_id,
            checkpoint_name=checkpoint_name,
            row_count=checkpoint.row_count,
            fail_pct=checkpoint.fail_pct,
            status=checkpoint.status.value,
        )
    else:
        _log.info("hooks.gx.disabled")
        checkpoint = skipped_result(checkpoint_name, pipeline_id, run_id)

    # ------------------------------------------------------------------
    # Auto-pause on breach + Post-Val crew
    # ------------------------------------------------------------------
    post_val_results: list[AgentResult] = []
    paused = False
    if checkpoint.status == CheckpointStatus.BREACHED:
        if agents_enabled:
            from datalink.agents.post_validation.crew import run_post_validation

            _log.info("hooks.post_validation.start", checkpoint=checkpoint_name)
            post_val_results = run_post_validation(
                adapters, settings, run_id=run_id, checkpoint_name=checkpoint_name
            )
        # Transition pipeline state — always done on BREACH regardless of agents flag.
        try:
            control.transition(
                StateTransition(
                    pipeline_id=pipeline_id,
                    from_state=PipelineState.RUNNING,
                    to_state=PipelineState.PAUSED,
                    actor="gx-checkpoint-callback",
                    reason=(
                        f"checkpoint {checkpoint_name} breached: "
                        f"{checkpoint.failed_expectations}/{checkpoint.total_expectations} "
                        f"failed ({checkpoint.fail_pct}%)"
                    ),
                    severity=Severity.HIGH,
                )
            )
            paused = True
        except ValueError as exc:
            _log.warning("hooks.pause_skipped", reason=str(exc))

    return HookedCheckpoint(
        checkpoint=checkpoint,
        pre_val_results=pre_val_results,
        post_val_results=post_val_results,
        pipeline_paused=paused,
    )
