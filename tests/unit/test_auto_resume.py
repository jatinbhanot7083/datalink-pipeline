"""Tests for Phase 6 auto-resume + fatal-error terminal state.

Validates:
  * TaskProgressTracker records/reads task completion correctly
  * LocalSequentialRunner skips SUCCESSFUL tasks on re-run with same run_id
  * FatalPipelineError transitions pipeline to ABORTED + halts downstream
  * Non-fatal Exception halts pipeline but leaves it resumable (PAUSED-able)
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from datalink.adapters.warehouse.duckdb_adapter import DuckDBWarehouse
from datalink.config.models import WarehouseConfig
from datalink.orchestration.local_sequential import LocalSequentialRunner
from datalink.orchestration.pipelines import Pipeline, Task
from datalink.orchestration.tasks import TaskContext
from datalink.quality import (
    FatalPipelineError,
    PipelineControl,
    PipelineState,
    TaskProgressTracker,
    create_control_tables,
)


@pytest.fixture
def wh():
    """In-memory DuckDB with control tables materialized."""
    w = DuckDBWarehouse(WarehouseConfig(type="duckdb", path=":memory:"))
    create_control_tables(w)
    yield w
    w.close()


# ----------------------------------------------------------------------------
# TaskProgressTracker unit tests
# ----------------------------------------------------------------------------


@pytest.mark.unit
def test_tracker_completed_tasks_empty_for_new_run(wh) -> None:
    tracker = TaskProgressTracker(wh)
    assert tracker.completed_tasks("never-seen-run") == {}


@pytest.mark.unit
def test_tracker_mark_done_records_output(wh) -> None:
    tracker = TaskProgressTracker(wh)
    tracker.mark_done(
        run_id="r-1",
        pipeline_id="bronze_ingest",
        task_name="ingest",
        output={"rows": 42},
        duration_ms=123,
    )
    completed = tracker.completed_tasks("r-1")
    assert set(completed) == {"ingest"}
    assert completed["ingest"].status == "SUCCESS"
    assert completed["ingest"].output == {"rows": 42}
    assert completed["ingest"].duration_ms == 123


@pytest.mark.unit
def test_tracker_failed_task_not_in_completed(wh) -> None:
    """Failed tasks must not appear in completed_tasks — resume re-runs them."""
    tracker = TaskProgressTracker(wh)
    tracker.mark_failed(
        run_id="r-1",
        pipeline_id="bronze_ingest",
        task_name="ingest",
        error="boom",
        duration_ms=50,
    )
    assert tracker.completed_tasks("r-1") == {}


@pytest.mark.unit
def test_tracker_runs_are_isolated_per_run_id(wh) -> None:
    tracker = TaskProgressTracker(wh)
    tracker.mark_done("r-1", "bronze_ingest", "t1", {}, 10)
    tracker.mark_done("r-2", "bronze_ingest", "t2", {}, 10)
    assert set(tracker.completed_tasks("r-1")) == {"t1"}
    assert set(tracker.completed_tasks("r-2")) == {"t2"}


@pytest.mark.unit
def test_tracker_overwrites_failed_on_successful_retry(wh) -> None:
    tracker = TaskProgressTracker(wh)
    tracker.mark_failed("r-1", "bronze_ingest", "t1", "oops", 10)
    tracker.mark_done("r-1", "bronze_ingest", "t1", {"rows": 1}, 10)
    # After successful retry, the task is in completed_tasks with SUCCESS status
    completed = tracker.completed_tasks("r-1")
    assert "t1" in completed
    assert completed["t1"].status == "SUCCESS"


# ----------------------------------------------------------------------------
# LocalSequentialRunner auto-resume
# ----------------------------------------------------------------------------


def _write_local_yaml(tmp_path: Path, db_path: Path) -> Path:
    """Create a minimal config/environments/local.yaml pointing at tmp db."""
    config_root = tmp_path / "config"
    (config_root / "environments").mkdir(parents=True)
    (config_root / "features").mkdir(parents=True)
    (config_root / "base.yaml").write_text("project_name: test\n")
    (config_root / "environments" / "local.yaml").write_text(
        f"env: local\n"
        f"adapters:\n"
        f"  warehouse:\n"
        f"    type: duckdb\n"
        f"    path: {db_path.as_posix()}\n"
        f"features:\n"
        f"  gx: {{enabled: false}}\n"
        f"  agents: {{enabled: false}}\n"
    )
    return config_root


@pytest.mark.unit
def test_runner_skips_successful_tasks_on_second_run(tmp_path, monkeypatch) -> None:
    """Re-invoking run() with same run_id skips tasks that already succeeded."""
    db = tmp_path / "wh.duckdb"
    config_root = _write_local_yaml(tmp_path, db)
    monkeypatch.setenv("DL_ENV", "local")
    # Point load_settings at our isolated config/ root.
    monkeypatch.setattr("datalink.config.loader._default_config_root", lambda: config_root)

    call_counts: dict[str, int] = {"a": 0, "b": 0}

    def task_a(ctx: TaskContext) -> dict[str, Any]:
        call_counts["a"] += 1
        return {"a_ran": True}

    def task_b(ctx: TaskContext) -> dict[str, Any]:
        call_counts["b"] += 1
        return {"b_ran": True}

    pipeline = Pipeline(
        pipeline_id="test_resume",
        description="resume test",
        tasks=[
            Task(name="a", callable=task_a, description="a"),
            Task(name="b", callable=task_b, description="b", upstream=["a"]),
        ],
    )
    runner = LocalSequentialRunner()

    # First run — both tasks should execute.
    r1 = runner.run(pipeline, env="local", run_id="run-42")
    assert r1.status == "SUCCESS"
    assert call_counts == {"a": 1, "b": 1}

    # Second run with SAME run_id — both tasks are skipped as RESUMED.
    r2 = runner.run(pipeline, env="local", run_id="run-42")
    assert r2.status == "SUCCESS"
    assert call_counts == {"a": 1, "b": 1}  # no new executions
    statuses = [t.status for t in r2.task_results]
    assert statuses == ["SKIPPED_RESUMED", "SKIPPED_RESUMED"]


@pytest.mark.unit
def test_runner_resumes_from_failure_point(tmp_path, monkeypatch) -> None:
    """After a mid-pipeline failure, re-running runs ONLY the failed + downstream tasks."""
    db = tmp_path / "wh.duckdb"
    config_root = _write_local_yaml(tmp_path, db)
    monkeypatch.setenv("DL_ENV", "local")
    monkeypatch.setattr("datalink.config.loader._default_config_root", lambda: config_root)

    call_counts: dict[str, int] = {"a": 0, "b": 0, "c": 0}
    should_fail_b = {"value": True}  # toggled between runs

    def task_a(ctx: TaskContext) -> dict[str, Any]:
        call_counts["a"] += 1
        return {}

    def task_b(ctx: TaskContext) -> dict[str, Any]:
        call_counts["b"] += 1
        if should_fail_b["value"]:
            raise RuntimeError("simulated transient failure")
        return {}

    def task_c(ctx: TaskContext) -> dict[str, Any]:
        call_counts["c"] += 1
        return {}

    pipeline = Pipeline(
        pipeline_id="test_resume_fail",
        description="fail-then-resume",
        tasks=[
            Task(name="a", callable=task_a, description="a"),
            Task(name="b", callable=task_b, description="b", upstream=["a"]),
            Task(name="c", callable=task_c, description="c", upstream=["b"]),
        ],
    )
    runner = LocalSequentialRunner()

    # Run 1: a succeeds, b fails, c skipped.
    r1 = runner.run(pipeline, env="local", run_id="run-42")
    assert r1.status == "FAILED"
    assert call_counts == {"a": 1, "b": 1, "c": 0}

    # Fix the transient fault and re-run with same run_id.
    should_fail_b["value"] = False
    r2 = runner.run(pipeline, env="local", run_id="run-42")
    assert r2.status == "SUCCESS"
    # a was already successful -> skipped; b + c execute fresh.
    assert call_counts == {"a": 1, "b": 2, "c": 1}
    statuses = {t.name: t.status for t in r2.task_results}
    assert statuses == {"a": "SKIPPED_RESUMED", "b": "SUCCESS", "c": "SUCCESS"}


# ----------------------------------------------------------------------------
# FatalPipelineError -> ABORTED state
# ----------------------------------------------------------------------------


@pytest.mark.unit
def test_fatal_error_aborts_pipeline(tmp_path, monkeypatch) -> None:
    """FatalPipelineError must transition state to ABORTED and skip downstream."""
    db = tmp_path / "wh.duckdb"
    config_root = _write_local_yaml(tmp_path, db)
    monkeypatch.setenv("DL_ENV", "local")
    monkeypatch.setattr("datalink.config.loader._default_config_root", lambda: config_root)

    def task_a(ctx: TaskContext) -> dict[str, Any]:
        return {}

    def task_b(ctx: TaskContext) -> dict[str, Any]:
        raise FatalPipelineError("schema drift on claim_id", severity="CRITICAL")

    def task_c(ctx: TaskContext) -> dict[str, Any]:
        return {"c_ran": True}  # should never execute

    pipeline = Pipeline(
        pipeline_id="test_fatal",
        description="fatal",
        tasks=[
            Task(name="a", callable=task_a, description="a"),
            Task(name="b", callable=task_b, description="b", upstream=["a"]),
            Task(name="c", callable=task_c, description="c", upstream=["b"]),
        ],
    )
    runner = LocalSequentialRunner()
    r = runner.run(pipeline, env="local", run_id="run-fatal")

    assert r.status == "FAILED"
    # Task b marked FATAL; c skipped with SKIPPED_ABORTED.
    statuses = {t.name: t.status for t in r.task_results}
    assert statuses["a"] == "SUCCESS"
    assert statuses["b"] == "FATAL"
    assert statuses["c"] == "SKIPPED_ABORTED"

    # Pipeline state is now ABORTED (terminal).
    wh = DuckDBWarehouse(WarehouseConfig(type="duckdb", path=str(db)))
    try:
        assert PipelineControl(wh).current("test_fatal") is PipelineState.ABORTED
    finally:
        wh.close()


@pytest.mark.unit
def test_aborted_pipeline_cannot_resume(tmp_path, monkeypatch) -> None:
    """Re-running an ABORTED pipeline must NOT execute any task — it's terminal."""
    db = tmp_path / "wh.duckdb"
    config_root = _write_local_yaml(tmp_path, db)
    monkeypatch.setenv("DL_ENV", "local")
    monkeypatch.setattr("datalink.config.loader._default_config_root", lambda: config_root)

    calls = {"n": 0}

    def task_fatal(ctx: TaskContext) -> dict[str, Any]:
        calls["n"] += 1
        raise FatalPipelineError("unrecoverable")

    pipeline = Pipeline(
        pipeline_id="test_terminal",
        description="terminal",
        tasks=[Task(name="fatal_task", callable=task_fatal, description="f")],
    )
    runner = LocalSequentialRunner()
    r1 = runner.run(pipeline, env="local", run_id="run-terminal")
    assert r1.status == "FAILED"
    assert calls["n"] == 1

    # Attempt to resume with same run_id — state is ABORTED, so the sensor
    # short-circuits every task to SKIPPED_ABORTED and we don't re-execute.
    r2 = runner.run(pipeline, env="local", run_id="run-terminal")
    assert calls["n"] == 1  # unchanged
    # The one task recorded FAILED last time, so it's NOT in completed_tasks.
    # But the pipeline is ABORTED, so the sensor skips it with SKIPPED_ABORTED.
    assert r2.task_results[0].status == "SKIPPED_ABORTED"
