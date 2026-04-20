"""Local sequential orchestrator — runs a Pipeline in-process, deterministically.

This is the default orchestrator (`settings.orchestrator = "local_sequential"`).
Every dev box can run it; no kind/Helm/kubectl required, no pod overhead.

Execution model:
  1. Topo-sort the Pipeline's Task graph.
  2. For each task (in order):
     a. Poll `CONTROL.pipeline_control_state` — if PAUSED / ABORTED,
        short-circuit the rest of the pipeline (mirrors Airflow's
        PipelineControlStateSensor behavior).
     b. Call the task's callable(ctx).
     c. Merge output into ctx.upstream for downstream tasks.

CLI:
    python -m datalink.orchestration.local_sequential run --pipeline bronze_ingest
    python -m datalink.orchestration.local_sequential run --pipeline silver_transform
    python -m datalink.orchestration.local_sequential run --pipeline gold_um_push --env local
    python -m datalink.orchestration.local_sequential list
"""

from __future__ import annotations

import argparse
import contextlib
import json
import sys
import time
import traceback
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from datalink.logging import configure_logging, get_logger
from datalink.orchestration.tasks import TaskContext, generate_run_id

if TYPE_CHECKING:
    from datalink.orchestration.pipelines import Pipeline, Task

_log = get_logger(__name__)


@dataclass
class TaskResult:
    name: str
    status: str  # "SUCCESS" | "FAILED" | "SKIPPED_PAUSED" | "SKIPPED_ABORTED"
    duration_ms: int
    output: dict[str, Any] = field(default_factory=dict)
    error: str | None = None


@dataclass
class RunResult:
    pipeline_id: str
    run_id: str
    status: str  # "SUCCESS" | "FAILED" | "HALTED"
    task_results: list[TaskResult] = field(default_factory=list)

    @property
    def all_green(self) -> bool:
        return self.status == "SUCCESS" and all(t.status == "SUCCESS" for t in self.task_results)


def run_pipeline(
    pipeline: Pipeline,
    *,
    env: str = "local",
    run_id: str | None = None,
    stop_on_error: bool = True,
) -> RunResult:
    """Run a Pipeline in-process, sequentially.

    Args:
      pipeline: the Pipeline dataclass from `datalink.orchestration.pipelines`.
      env: config environment name (load_settings(env=...)).
      run_id: stable identifier for this run. Generated if None.
      stop_on_error: if True, stop at the first failing task.
    """
    runner = LocalSequentialRunner(stop_on_error=stop_on_error)
    return runner.run(pipeline, env=env, run_id=run_id)


class LocalSequentialRunner:
    """Thin orchestrator: topo-sort, execute, honor pipeline_control_state."""

    def __init__(self, stop_on_error: bool = True) -> None:
        self.stop_on_error = stop_on_error

    def run(
        self,
        pipeline: Pipeline,
        *,
        env: str = "local",
        run_id: str | None = None,
    ) -> RunResult:
        run_id = run_id or generate_run_id()
        ctx = TaskContext(
            pipeline_id=pipeline.pipeline_id,
            run_id=run_id,
            env=env,
        )

        # Bootstrap control tables + initialise pipeline state ONCE, then
        # close the warehouse — dbt subprocesses need the file lock.
        # Every subsequent state check opens its own short-lived connection.
        self._bootstrap_control_state(env, pipeline.pipeline_id)

        result = RunResult(pipeline_id=pipeline.pipeline_id, run_id=run_id, status="SUCCESS")
        _log.info(
            "local_sequential.run.start",
            pipeline_id=pipeline.pipeline_id,
            run_id=run_id,
            env=env,
            task_count=len(pipeline.tasks),
        )

        halted = False
        for task in pipeline.topo_sorted():
            # Sensor check — identical semantics to Airflow's PipelineControlStateSensor.
            # Short-lived connection so dbt subprocesses can grab the file lock.
            state_value = self._check_state(env, pipeline.pipeline_id)
            if halted or state_value in {"PAUSED", "ABORTED"}:
                skip_status = "SKIPPED_ABORTED" if state_value == "ABORTED" else "SKIPPED_PAUSED"
                result.task_results.append(
                    TaskResult(name=task.name, status=skip_status, duration_ms=0)
                )
                _log.info(
                    "local_sequential.task.skipped",
                    task=task.name,
                    reason=skip_status,
                )
                continue

            tr = self._run_task(task, ctx)
            result.task_results.append(tr)
            if tr.status == "SUCCESS":
                ctx.upstream[task.name] = tr.output
            else:
                if self.stop_on_error:
                    halted = True
                    result.status = "FAILED"

        # If any task halted AND the pipeline itself got paused mid-flow (via
        # a GX BREACH), mark the run HALTED rather than FAILED.
        state_final = self._check_state(env, pipeline.pipeline_id)
        if state_final == "PAUSED" and result.status == "SUCCESS":
            result.status = "HALTED"

        # Close the warehouse connection so the DuckDB file lock is released.
        # DuckDB is single-writer per process-file-handle — if we leave the
        # connection open, a dbt subprocess in the NEXT pipeline run can't
        # acquire the lock. Idempotent if already None.
        if ctx._adapters is not None:
            close = getattr(ctx._adapters.warehouse, "close", None)
            if callable(close):
                with contextlib.suppress(Exception):
                    close()

        _log.info(
            "local_sequential.run.done",
            pipeline_id=pipeline.pipeline_id,
            run_id=run_id,
            status=result.status,
            task_count=len(result.task_results),
        )
        return result

    @staticmethod
    def _bootstrap_control_state(env: str, pipeline_id: str) -> None:
        """Open warehouse, ensure control tables + initial state, then close.

        Short-lived so dbt subprocesses can grab the DuckDB file lock afterward.
        """
        from datalink.adapters.factory import build_adapters
        from datalink.config.loader import load_settings
        from datalink.quality import PipelineControl, create_control_tables

        settings = load_settings(env=env)
        adapters = build_adapters(settings)
        try:
            create_control_tables(adapters.warehouse)
            control = PipelineControl(adapters.warehouse)
            # Only initialise on first run — do NOT reset PAUSED / ABORTED.
            if control.current(pipeline_id) is None:
                control.start(pipeline_id)
        finally:
            close = getattr(adapters.warehouse, "close", None)
            if callable(close):
                close()

    @staticmethod
    def _check_state(env: str, pipeline_id: str) -> str | None:
        """Open → query state → close. Returns the state's string value or None."""
        from datalink.adapters.factory import build_adapters
        from datalink.config.loader import load_settings
        from datalink.quality import PipelineControl

        settings = load_settings(env=env)
        adapters = build_adapters(settings)
        try:
            control = PipelineControl(adapters.warehouse)
            state = control.current(pipeline_id)
            return state.value if state is not None else None
        finally:
            close = getattr(adapters.warehouse, "close", None)
            if callable(close):
                close()

    def _run_task(self, task: Task, ctx: TaskContext) -> TaskResult:
        start = time.monotonic()
        try:
            out = task.callable(ctx) or {}
            duration_ms = int((time.monotonic() - start) * 1000)
            _log.info(
                "local_sequential.task.done",
                task=task.name,
                duration_ms=duration_ms,
            )
            return TaskResult(
                name=task.name,
                status="SUCCESS",
                duration_ms=duration_ms,
                output=out,
            )
        except Exception as exc:
            duration_ms = int((time.monotonic() - start) * 1000)
            _log.error(
                "local_sequential.task.failed",
                task=task.name,
                duration_ms=duration_ms,
                error=str(exc),
                trace=traceback.format_exc(limit=3),
            )
            return TaskResult(
                name=task.name,
                status="FAILED",
                duration_ms=duration_ms,
                error=str(exc),
            )


# ----------------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------------


def _cmd_run(args: argparse.Namespace) -> int:
    from datalink.orchestration.pipelines import get_pipeline

    configure_logging(level="INFO", fmt="console")
    pipeline = get_pipeline(args.pipeline)
    result = run_pipeline(pipeline, env=args.env, run_id=args.run_id)

    # Emit a compact summary block to stdout.
    print()
    print("=" * 70)
    print(f" LOCAL_SEQUENTIAL — pipeline={result.pipeline_id}  run_id={result.run_id}")
    print(f" status={result.status}")
    print("=" * 70)
    for tr in result.task_results:
        marker = "OK " if tr.status == "SUCCESS" else tr.status
        print(f"  [{marker:>14}]  {tr.name:24}  {tr.duration_ms:>5}ms")
    print("=" * 70)

    if args.json:
        print(
            json.dumps(
                {
                    "pipeline_id": result.pipeline_id,
                    "run_id": result.run_id,
                    "status": result.status,
                    "tasks": [
                        {
                            "name": t.name,
                            "status": t.status,
                            "duration_ms": t.duration_ms,
                            "error": t.error,
                        }
                        for t in result.task_results
                    ],
                },
                indent=2,
            )
        )

    return 0 if result.all_green or result.status == "HALTED" else 1


def _cmd_list(args: argparse.Namespace) -> int:
    from datalink.orchestration.pipelines import all_pipelines

    for p in all_pipelines():
        print(f"{p.pipeline_id:20}  {p.description}")
        for t in p.topo_sorted():
            deps = ",".join(t.upstream) if t.upstream else "-"
            print(f"    - {t.name:22}  upstream={deps:30}  {t.description}")
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="datalink.orchestration.local_sequential",
        description="Local sequential orchestrator for DataLink pipelines",
    )
    sub = ap.add_subparsers(dest="cmd", required=True)

    run_p = sub.add_parser("run", help="Run a named pipeline")
    run_p.add_argument(
        "--pipeline",
        required=True,
        help="pipeline id (bronze_ingest / silver_transform / gold_um_push)",
    )
    run_p.add_argument("--env", default="local", help="config env (default: local)")
    run_p.add_argument("--run-id", default=None, help="run id (generated if omitted)")
    run_p.add_argument("--json", action="store_true", help="also emit a JSON result block")
    run_p.set_defaults(func=_cmd_run)

    list_p = sub.add_parser("list", help="List available pipelines + their task graphs")
    list_p.set_defaults(func=_cmd_list)

    args = ap.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    sys.exit(main())
