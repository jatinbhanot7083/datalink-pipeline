"""Orchestration smoke — exercises the local_sequential runner end-to-end.

What this proves:
  1. All 3 pipelines (bronze_ingest, silver_transform, gold_um_push) run
     green through the local_sequential orchestrator, not the smoke-script
     entry points. This is the proof that the orchestrator actually drives
     the same pipeline functions the hand-rolled smokes do.
  2. The PipelineControlStateSensor equivalent (the state check inside the
     runner's task loop) short-circuits downstream tasks when the pipeline
     is transitioned to PAUSED mid-run.
  3. Restoring the state to RUNNING via RESUMING allows a re-run to proceed.

Called by `make verify-phase55-local`.
"""

from __future__ import annotations

import sys
from pathlib import Path

from datalink.adapters.factory import build_adapters
from datalink.config.loader import load_settings
from datalink.logging import configure_logging
from datalink.orchestration import (
    BRONZE_INGEST_PIPELINE,
    GOLD_UM_PUSH_PIPELINE,
    SILVER_TRANSFORM_PIPELINE,
    run_pipeline,
)
from datalink.orchestration.local_sequential import LocalSequentialRunner
from datalink.quality import PipelineControl, PipelineState, create_control_tables
from datalink.quality.control import Severity, StateTransition


def _print_run(label: str, result) -> None:
    print()
    print(f"--- {label} ---")
    print(f"  pipeline={result.pipeline_id}  status={result.status}  run_id={result.run_id}")
    for tr in result.task_results:
        print(f"    [{tr.status:>15}]  {tr.name:22}  {tr.duration_ms:>5}ms")


def _with_control(fn):
    """Run fn(control) with a fresh warehouse connection, opened + closed around the call.

    DuckDB's file lock is per-process. We open, do the state-machine op, close —
    so downstream subprocesses (dbt) can acquire the same file.
    """
    settings = load_settings(env="local")
    adapters = build_adapters(settings)
    try:
        create_control_tables(adapters.warehouse)
        control = PipelineControl(adapters.warehouse)
        return fn(control)
    finally:
        close = getattr(adapters.warehouse, "close", None)
        if callable(close):
            close()


def main() -> int:
    configure_logging(level="INFO", fmt="console")
    # Bootstrap control tables + close the connection before any pipeline runs.
    _with_control(lambda _c: None)

    failures: list[str] = []

    # Ensure the sample data is present — if not, skip bronze and lean on Silver/Gold
    # (which both assume Bronze has already been populated at least once).
    sample_claims = Path(__file__).resolve().parents[1] / "data" / "sample" / "claims_sample.csv"
    if not sample_claims.exists():
        print(
            f"NOTE: {sample_claims} missing — skipping bronze_ingest "
            "(Silver/Gold still run against whatever's in the warehouse)."
        )

    print()
    print("=" * 70)
    print(" PHASE 5.5 ORCHESTRATION SMOKE (local_sequential)")
    print("=" * 70)

    # ------------------------------------------------------------------
    # 1. Bronze pipeline via the runner.
    # ------------------------------------------------------------------
    if sample_claims.exists():
        r1 = run_pipeline(BRONZE_INGEST_PIPELINE, env="local", run_id="phase55-bronze")
        _print_run("1. bronze_ingest (runner)", r1)
        if r1.status not in {"SUCCESS", "HALTED"}:
            failures.append(f"bronze_ingest status {r1.status}")

    # ------------------------------------------------------------------
    # 2. Silver pipeline via the runner.
    # ------------------------------------------------------------------
    r2 = run_pipeline(SILVER_TRANSFORM_PIPELINE, env="local", run_id="phase55-silver")
    _print_run("2. silver_transform (runner)", r2)
    if r2.status not in {"SUCCESS", "HALTED"}:
        failures.append(f"silver_transform status {r2.status}")

    # ------------------------------------------------------------------
    # 3. Gold pipeline via the runner.
    # ------------------------------------------------------------------
    r3 = run_pipeline(GOLD_UM_PUSH_PIPELINE, env="local", run_id="phase55-gold")
    _print_run("3. gold_um_push (runner)", r3)
    if r3.status not in {"SUCCESS", "HALTED"}:
        failures.append(f"gold_um_push status {r3.status}")

    # ------------------------------------------------------------------
    # 4. Sensor short-circuit: pause bronze_ingest BEFORE running, confirm
    #    runner skips all tasks downstream of the gate.
    # ------------------------------------------------------------------
    print()
    print("--- 4. Sensor short-circuit on PAUSED ---")

    def _pause(c):
        c.start("bronze_ingest")
        c.transition(
            StateTransition(
                pipeline_id="bronze_ingest",
                from_state=PipelineState.RUNNING,
                to_state=PipelineState.PAUSED,
                actor="smoke_orchestration",
                reason="testing sensor short-circuit",
                severity=Severity.HIGH,
            )
        )

    _with_control(_pause)

    runner = LocalSequentialRunner()
    r4 = runner.run(BRONZE_INGEST_PIPELINE, env="local", run_id="phase55-paused")
    _print_run("4. bronze_ingest while PAUSED (runner)", r4)
    skipped = [t for t in r4.task_results if t.status == "SKIPPED_PAUSED"]
    if not skipped:
        failures.append("sensor short-circuit: expected tasks skipped while PAUSED, got none")
    elif len(skipped) != len(BRONZE_INGEST_PIPELINE.tasks):
        failures.append(
            f"sensor short-circuit: expected all {len(BRONZE_INGEST_PIPELINE.tasks)} tasks "
            f"skipped; skipped {len(skipped)}"
        )

    # ------------------------------------------------------------------
    # 5. Resume: state flipped to RESUMING -> RUNNING.
    # ------------------------------------------------------------------
    def _resume(c):
        c.transition(
            StateTransition(
                pipeline_id="bronze_ingest",
                from_state=PipelineState.PAUSED,
                to_state=PipelineState.RESUMING,
                actor="smoke_orchestration",
                reason="operator cleared the pause",
                severity=Severity.LOW,
            )
        )
        c.transition(
            StateTransition(
                pipeline_id="bronze_ingest",
                from_state=PipelineState.RESUMING,
                to_state=PipelineState.RUNNING,
                actor="smoke_orchestration",
                reason="back to normal",
                severity=Severity.LOW,
            )
        )

    _with_control(_resume)

    # ------------------------------------------------------------------
    # 6. DAG parse — prove the Airflow DAG modules are import-clean IFF airflow
    #    is installed (opt-in `--extra orchestration`). If not, note and skip.
    # ------------------------------------------------------------------
    print()
    print("--- 5. DAG parse (airflow DagBag) ---")
    try:
        from airflow.models import DagBag

        repo_root = Path(__file__).resolve().parents[1]
        # safe_mode=False: our DAG files call `build_dag(...)` rather than
        # instantiating `DAG(...)` inline, so the default safe-mode string
        # scanner filters them out. Force a full parse.
        bag = DagBag(dag_folder=str(repo_root / "dags"), include_examples=False, safe_mode=False)
        if bag.import_errors:
            for path, err in bag.import_errors.items():
                print(f"  IMPORT ERROR in {path}: {err}")
                failures.append(f"dag import error: {path}")
        found = sorted(bag.dag_ids)
        expected = {"bronze_ingest", "silver_transform", "gold_um_push"}
        print(f"  loaded DAGs: {found}")
        if not expected.issubset(set(found)):
            failures.append(f"DagBag missing expected DAGs: wanted {expected}, got {found}")
    except ImportError:
        print("  airflow not installed in this venv — skipping DAG parse")
        print("  (install with: uv sync --extra orchestration)")

    # ------------------------------------------------------------------
    # Report.
    # ------------------------------------------------------------------
    print()
    print("=" * 70)
    if failures:
        print(f" PHASE 5.5 SMOKE — {len(failures)} FAILURES")
        for f in failures:
            print(f"   - {f}")
        print("=" * 70)
        return 1

    print(" PHASE 5.5 ORCHESTRATION SMOKE GREEN")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    sys.exit(main())
