"""Phase 5 plug-out proof.

Sets DL_FEATURES__GX__ENABLED=false + DL_FEATURES__AGENTS__ENABLED=false
and runs the same hooks. Asserts:
  - no GX checkpoint ran (all results are SKIPPED)
  - no agent was invoked (no new rows in agent_reasoning_log)
  - pipeline is still RUNNING (no pause fired)

This is the plug-in contract proof: GX + CrewAI are provably removable.
"""

from __future__ import annotations

import os
import sys

from datalink.adapters.factory import build_adapters
from datalink.config.loader import load_settings
from datalink.logging import configure_logging
from datalink.pipeline.hooks import run_checkpoint_with_hooks
from datalink.quality import create_control_tables
from datalink.quality.checkpoint import CheckpointStatus
from datalink.quality.control import CONTROL_SCHEMA
from datalink.quality.suites import (
    BRONZE_STRUCTURAL,
    build_bronze_suite,
)


def main() -> int:
    configure_logging(level="INFO", fmt="console")

    # Flip the feature flags BEFORE loading settings.
    os.environ["DL_FEATURES__GX__ENABLED"] = "false"
    os.environ["DL_FEATURES__AGENTS__ENABLED"] = "false"

    settings = load_settings(env="local")
    assert settings.features.gx.enabled is False
    assert settings.features.agents.enabled is False

    adapters = build_adapters(settings)
    create_control_tables(adapters.warehouse)

    # Baseline: count existing agent invocations.
    pre_count = int(
        adapters.warehouse.query(f"SELECT COUNT(*) AS n FROM {CONTROL_SCHEMA}.agent_reasoning_log")[
            0
        ]["n"]
    )

    print()
    print("=" * 70)
    print(" PHASE 5 PLUG-OUT PROOF")
    print(" features.gx.enabled    =", settings.features.gx.enabled)
    print(" features.agents.enabled=", settings.features.agents.enabled)
    print("=" * 70)

    hc = run_checkpoint_with_hooks(
        adapters,
        settings,
        "plugout-pipeline",
        "plugout-run",
        BRONZE_STRUCTURAL,
        "BRONZE.RAW_CLAIMS",
        build_bronze_suite,
    )

    failures: list[str] = []
    if hc.checkpoint.status != CheckpointStatus.SKIPPED:
        failures.append(
            f"GX was disabled but checkpoint status was {hc.checkpoint.status.value} "
            f"(expected SKIPPED)"
        )
    if hc.pre_val_results:
        failures.append(f"Pre-Val results returned with agents disabled: {hc.pre_val_results}")
    if hc.post_val_results:
        failures.append(f"Post-Val results returned with agents disabled: {hc.post_val_results}")
    if hc.pipeline_paused:
        failures.append("pipeline_paused=True with GX disabled — should not pause")

    post_count = int(
        adapters.warehouse.query(f"SELECT COUNT(*) AS n FROM {CONTROL_SCHEMA}.agent_reasoning_log")[
            0
        ]["n"]
    )
    if post_count != pre_count:
        failures.append(
            f"agent_reasoning_log grew from {pre_count} to {post_count} "
            "with agents disabled — agent invocation happened anyway"
        )

    # Cleanup env override.
    for k in ("DL_FEATURES__GX__ENABLED", "DL_FEATURES__AGENTS__ENABLED"):
        os.environ.pop(k, None)

    if failures:
        print("\n  FAIL:")
        for f in failures:
            print(f"   - {f}")
        return 1

    print(f"\n  checkpoint.status      = {hc.checkpoint.status.value}  ✓")
    print(f"  pre_val_results        = {hc.pre_val_results}  ✓")
    print(f"  post_val_results       = {hc.post_val_results}  ✓")
    print(f"  pipeline_paused        = {hc.pipeline_paused}  ✓")
    print(f"  agent_log unchanged    = {pre_count} → {post_count}  ✓")
    print()
    print("=" * 70)
    print(" PLUG-OUT PROOF GREEN — pipeline runs unchanged with GX + Agents disabled")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    sys.exit(main())
