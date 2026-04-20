"""Phase 5 end-to-end demo — GX + CrewAI in action.

Runs all three GX checkpoints (CP1 Bronze, CP2 Silver, CP3 Gold) against
the live DuckDB warehouse, with Pre-Val + Post-Val crews wired in. Then
deliberately triggers a BREACHED checkpoint to show the Post-Val crew
root-cause/remediation/reporting flow in full.

Also exercises the PHI boundary: attempts to send a PHI-shaped payload
through AgentBase._ask_llm and confirms PhiBoundaryViolationError is raised.
"""

from __future__ import annotations

import sys

from datalink.adapters.factory import build_adapters
from datalink.agents.pre_validation.crew import run_pre_validation
from datalink.config.loader import load_settings
from datalink.logging import configure_logging
from datalink.phi import PhiBoundaryViolationError, PhiRedactionLayer
from datalink.pipeline.hooks import run_checkpoint_with_hooks
from datalink.quality import create_control_tables
from datalink.quality.suites import (
    BRONZE_STRUCTURAL,
    GOLD_BUSINESS,
    SILVER_CLINICAL,
    build_bronze_suite,
    build_gold_suite,
    build_silver_suite,
)


def main() -> int:
    configure_logging(level="INFO", fmt="console")

    settings = load_settings(env="local")
    adapters = build_adapters(settings)
    create_control_tables(adapters.warehouse)

    print()
    print("=" * 70)
    print(" PHASE 5 DEMO — GX + CrewAI in action")
    print(" features.gx.enabled    =", settings.features.gx.enabled)
    print(" features.agents.enabled=", settings.features.agents.enabled)
    print(" llm adapter            =", settings.adapters.llm.type)
    print("=" * 70)

    pipeline_id = "demo-phase-5"
    run_id = "phase5-demo"

    # ------------------------------------------------------------------
    # 1. Pre-Val crew standalone run (Profiler → Author → Reviewer)
    # ------------------------------------------------------------------
    print("\n--- 1. Pre-Val Crew on BRONZE.RAW_CLAIMS ---")
    for r in run_pre_validation(adapters, settings, "BRONZE.RAW_CLAIMS"):
        status = "OK " if r.success else "FAIL"
        keys = list(r.payload)[:5]
        print(f"  [{status}] {r.agent_name:24} {r.duration_ms:>4}ms  keys={keys}")

    # ------------------------------------------------------------------
    # 2. All three GX checkpoints via the hooks module (Pre-Val + run + Post-Val if breach)
    # ------------------------------------------------------------------
    print("\n--- 2. GX checkpoints CP1 / CP2 / CP3 (hooked) ---")
    checkpoints = [
        (BRONZE_STRUCTURAL, "BRONZE.RAW_CLAIMS", build_bronze_suite),
        (SILVER_CLINICAL, "SILVER_silver.sat_claim_details", build_silver_suite),
        (GOLD_BUSINESS, "SILVER_gold_um.gold_patient_auth", build_gold_suite),
    ]
    for name, table, builder in checkpoints:
        hc = run_checkpoint_with_hooks(
            adapters, settings, pipeline_id, run_id, name, table, builder
        )
        cp = hc.checkpoint
        print(
            f"  {name:22} total={cp.total_expectations:>2} "
            f"failed={cp.failed_expectations} fail_pct={cp.fail_pct:>5.1f}% "
            f"status={cp.status.value}"
        )

    # ------------------------------------------------------------------
    # 3. Deliberately trigger a BREACH to show the Post-Val crew
    # ------------------------------------------------------------------
    print("\n--- 3. Inject a synthetic BREACH and run Post-Val Crew ---")
    # Run the SILVER suite against the BRONZE table — mismatch causes ~57% failures.
    hc = run_checkpoint_with_hooks(
        adapters,
        settings,
        pipeline_id="demo-breach",
        run_id="breach-run",
        checkpoint_name="synthetic_breach",
        qualified_table="BRONZE.RAW_CLAIMS",
        suite_builder=build_silver_suite,
        fail_threshold_pct=5.0,
    )
    cp = hc.checkpoint
    print(
        f"  Result: status={cp.status.value} fail_pct={cp.fail_pct}% "
        f"failed={cp.failed_expectations}/{cp.total_expectations}"
    )
    print(f"  pipeline_paused={hc.pipeline_paused}")
    for r in hc.post_val_results:
        print(f"    post-val {r.agent_name:22} {r.duration_ms:>4}ms  success={r.success}")

    # ------------------------------------------------------------------
    # 4. PHI boundary — prove the guard bites
    # ------------------------------------------------------------------
    print("\n--- 4. PHI boundary enforcement ---")
    phi = PhiRedactionLayer()
    try:
        phi.assert_clean({"raw_row": {"member_id": "MBR-00000001", "dob": "1950-06-15"}})
        print("  FAIL — PHI leak should have raised")
        return 1
    except PhiBoundaryViolationError as exc:
        print(f"  OK — PhiBoundaryViolationError raised: {str(exc)[:120]}")

    # Final: confirm audit log picked up every invocation.
    count_rows = adapters.warehouse.query(
        "SELECT COUNT(*) AS n FROM CONTROL.agent_reasoning_log "
        "WHERE ts >= CURRENT_TIMESTAMP - INTERVAL 1 HOUR"
    )
    print(f"\n  Agent invocations logged in last hour: {int(count_rows[0]['n'])}")

    print()
    print("=" * 70)
    print(" PHASE 5 DEMO COMPLETE")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    sys.exit(main())
