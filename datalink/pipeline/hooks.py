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
    run_checkpoint,
    seed_baselines,
)
from datalink.quality.checkpoint import skipped_result

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
    source_type: str | None = None,
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
    # Phase 9.4: per-client state. Each tenant gets its own row; aetna
    # being PAUSED never blocks cano_health.
    control.start(pipeline_id, client_id=client_id)
    # Phase 6: idempotently create the tenant's BRONZE / SILVER_silver /
    # SILVER_gold_um schemas before the pipeline touches them. No-op for
    # client_id=='default' on re-runs.
    from datalink.tenancy import ensure_tenant_schemas

    ensure_tenant_schemas(adapters.warehouse, client_id)

    # Idempotent seed: populate THIS client's 3 LIVE baselines on first run.
    # Phase 6: seed per-client so a new tenant gets the baseline automatically
    # on its very first DAG run (no human step required). No-op on subsequent
    # runs for the same tenant.
    if gx_enabled:
        seed_baselines(adapters.warehouse, client_id=client_id)

    # ------------------------------------------------------------------
    # Pre-Val crew (if enabled)
    # ------------------------------------------------------------------
    pre_val_results: list[AgentResult] = []
    if agents_enabled:
        # Lazy import — keeps plug-out fast (no CrewAI / anthropic imports needed).
        from datalink.agents.pre_validation.crew import run_pre_validation

        _log.info("hooks.pre_validation.start", table=qualified_table)
        # Phase 6 Commit 2: pass client_id + source_type so the crew can
        # skip authoring when the schema hasn't changed (no LLM calls on
        # cache hit). Without these args the crew falls back to always-run.
        pre_val_results = run_pre_validation(
            adapters,
            settings,
            qualified_table,
            client_id=client_id,
            source_type=source_type,
            checkpoint_name=checkpoint_name,
        )
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
            source_type=source_type,
        )
        control.record_checkpoint(
            run_id=run_id,
            pipeline_id=pipeline_id,
            checkpoint_name=checkpoint_name,
            row_count=checkpoint.row_count,
            fail_pct=checkpoint.fail_pct,
            status=checkpoint.status.value,
            client_id=client_id,
            source_type=source_type,
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
        # Phase 12.2 — runtime threshold check fires against the active
        # ``ThresholdPolicy`` (config-time HITL approved at edit). Three
        # outcomes drive the state machine:
        #
        #   fail_pct > abort_pct  → ABORT (terminal, force-resume to re-arm)
        #   fail_pct > pause_pct  → PAUSE (operator can Resume)
        #   else                  → continue silently — neither threshold
        #                           breached, the BREACH itself is the
        #                           sub-threshold signal
        #
        # No-policy tenants keep behaving as Phase 9.4: pause_pct=0,
        # abort_pct=25, so any failure pauses, > 25% aborts. Zero
        # behavioral change for clients that haven't authored a policy.
        from datalink.pipeline.control_policy import (
            PipelineControlPolicyRegistry,
        )

        policy_registry = PipelineControlPolicyRegistry(adapters.warehouse)
        # Phase 12.5 — pass source_type so the policy lookup can pick the
        # most-specific per-source override (e.g., aetna/bronze_ingest/CLAIMS
        # is stricter than the pipeline-wide aetna/bronze_ingest policy).
        # source_type may be None for cross-source pipelines; cascade
        # gracefully falls back through tenant-wide → global → DEFAULT.
        policy = policy_registry.get_or_default(client_id, pipeline_id, source_type=source_type)
        breach_reason = (
            f"checkpoint {checkpoint_name} breached: "
            f"{checkpoint.failed_expectations}/{checkpoint.total_expectations} "
            f"failed ({checkpoint.fail_pct}%)"
            f" [policy v{policy.version} "
            f"pause>{policy.fail_rate_pause_pct}% abort>{policy.fail_rate_abort_pct}%]"
        )
        try:
            if checkpoint.fail_pct > policy.fail_rate_abort_pct:
                control.abort(
                    pipeline_id,
                    client_id=client_id,
                    reason=breach_reason,
                    actor="gx-checkpoint-callback",
                )
                _log.error(
                    "hooks.pipeline_aborted",
                    pipeline_id=pipeline_id,
                    client_id=client_id,
                    fail_pct=checkpoint.fail_pct,
                    abort_threshold=policy.fail_rate_abort_pct,
                    policy_version=policy.version,
                )
                paused = True
            elif checkpoint.fail_pct > policy.fail_rate_pause_pct:
                control.pause(
                    pipeline_id,
                    client_id=client_id,
                    reason=breach_reason,
                    actor="gx-checkpoint-callback",
                )
                _log.warning(
                    "hooks.pipeline_paused",
                    pipeline_id=pipeline_id,
                    client_id=client_id,
                    fail_pct=checkpoint.fail_pct,
                    pause_threshold=policy.fail_rate_pause_pct,
                    policy_version=policy.version,
                )
                paused = True
            else:
                # Sub-threshold breach — log it but don't change state.
                # The BREACH was a soft signal; policy says don't act.
                _log.info(
                    "hooks.breach_below_pause_threshold",
                    pipeline_id=pipeline_id,
                    client_id=client_id,
                    fail_pct=checkpoint.fail_pct,
                    pause_threshold=policy.fail_rate_pause_pct,
                    policy_version=policy.version,
                )
        except ValueError as exc:
            _log.warning("hooks.pause_skipped", reason=str(exc))

    return HookedCheckpoint(
        checkpoint=checkpoint,
        pre_val_results=pre_val_results,
        post_val_results=post_val_results,
        pipeline_paused=paused,
    )
