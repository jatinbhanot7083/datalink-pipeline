"""Phase 5.5 structural + unit verification.

End-to-end live smoke lives in scripts/smoke_orchestration.py
(verify-phase-5.5-local). Airflow DAG parse lives in the `dag-parse` Makefile
target / verify-phase-5.5-airflow (opt-in, requires --extra orchestration).
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from datalink.orchestration import (
    BRONZE_INGEST_PIPELINE,
    GOLD_UM_PUSH_PIPELINE,
    SILVER_TRANSFORM_PIPELINE,
    Pipeline,
    Task,
    get_pipeline,
)
from datalink.orchestration.local_sequential import LocalSequentialRunner
from datalink.orchestration.tasks import TaskContext

REPO_ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.phase
def test_orchestration_package_structure() -> None:
    """The orchestration package must expose the documented entry points."""
    orch_root = REPO_ROOT / "datalink" / "orchestration"
    for fn in (
        "__init__.py",
        "tasks.py",
        "pipelines.py",
        "local_sequential.py",
        "airflow_sensors.py",
    ):
        assert (orch_root / fn).exists(), f"missing {fn}"


@pytest.mark.phase
def test_three_dag_files_exist() -> None:
    """The 3 DAG files documented in docs/architecture.md §7 must exist."""
    dags = REPO_ROOT / "dags"
    for fn in ("bronze_ingest_dag.py", "silver_transform_dag.py", "gold_um_push_dag.py"):
        p = dags / fn
        assert p.exists(), f"missing DAG file: {p}"
        txt = p.read_text()
        # Every DAG must import build_dag from _dag_builder and reference a Pipeline.
        assert "build_dag(" in txt, f"{fn} does not call build_dag()"
        assert "_PIPELINE" in txt, f"{fn} does not reference a pipeline dataclass"


@pytest.mark.phase
def test_all_three_pipelines_registered() -> None:
    assert get_pipeline("bronze_ingest") is BRONZE_INGEST_PIPELINE
    assert get_pipeline("silver_transform") is SILVER_TRANSFORM_PIPELINE
    assert get_pipeline("gold_um_push") is GOLD_UM_PUSH_PIPELINE


@pytest.mark.phase
def test_pipeline_topo_sort_is_deterministic() -> None:
    """Two topo-sorts of the same Pipeline must produce the identical order."""
    for p in (BRONZE_INGEST_PIPELINE, SILVER_TRANSFORM_PIPELINE, GOLD_UM_PUSH_PIPELINE):
        assert [t.name for t in p.topo_sorted()] == [t.name for t in p.topo_sorted()]


@pytest.mark.phase
def test_pipeline_task_dependencies_valid() -> None:
    """Every upstream referenced by a Task must exist as another Task in the same pipeline."""
    for p in (BRONZE_INGEST_PIPELINE, SILVER_TRANSFORM_PIPELINE, GOLD_UM_PUSH_PIPELINE):
        names = {t.name for t in p.tasks}
        for t in p.tasks:
            for u in t.upstream:
                assert u in names, f"{p.pipeline_id}: task {t.name} has unknown upstream {u!r}"


@pytest.mark.phase
def test_every_pipeline_has_a_gx_checkpoint_task() -> None:
    """Each of the 3 medallion pipelines must run at least one GX checkpoint."""
    for p in (BRONZE_INGEST_PIPELINE, SILVER_TRANSFORM_PIPELINE, GOLD_UM_PUSH_PIPELINE):
        assert p.gx_checkpoint_task_names, f"{p.pipeline_id} has no GX checkpoint tasks"
        for cp_name in p.gx_checkpoint_task_names:
            assert any(
                t.name == cp_name for t in p.tasks
            ), f"{p.pipeline_id} references {cp_name!r} but no such task"


@pytest.mark.phase
def test_pipelines_are_acyclic() -> None:
    """A cycle would blow up topo_sorted() — verify each real pipeline is fine."""
    for p in (BRONZE_INGEST_PIPELINE, SILVER_TRANSFORM_PIPELINE, GOLD_UM_PUSH_PIPELINE):
        p.topo_sorted()  # raises if cyclic


@pytest.mark.phase
def test_cycle_detection_raises() -> None:
    """Sanity check: a Pipeline WITH a cycle must raise from topo_sorted()."""
    bad = Pipeline(
        pipeline_id="bad",
        description="cyclic",
        tasks=(
            Task(name="a", callable=lambda _c: {}, upstream=("b",)),
            Task(name="b", callable=lambda _c: {}, upstream=("a",)),
        ),
    )
    with pytest.raises(ValueError, match="cycle"):
        bad.topo_sorted()


@pytest.mark.phase
def test_airflow_sensors_module_imports_without_airflow() -> None:
    """The sensor module must be importable on boxes without airflow installed —
    so that package-level imports from datalink.orchestration never fail just
    because the orchestration extra isn't synced."""
    # Just importing it is the test.
    import datalink.orchestration.airflow_sensors  # noqa: F401


@pytest.mark.phase
def test_get_pipeline_rejects_unknown_id() -> None:
    with pytest.raises(KeyError):
        get_pipeline("not-a-real-pipeline")


# ----------------------------------------------------------------------------
# Unit tests — runner exercise with synthetic tasks (no warehouse needed).
# ----------------------------------------------------------------------------


@pytest.mark.unit
def test_runner_executes_tasks_in_topological_order() -> None:
    calls: list[str] = []

    def t1(_ctx):
        calls.append("t1")
        return {"k": 1}

    def t2(_ctx):
        calls.append("t2")
        return {"k": 2}

    def t3(_ctx):
        calls.append("t3")
        return {"k": 3}

    p = Pipeline(
        pipeline_id="synth",
        description="fan-out",
        tasks=(
            Task(name="t1", callable=t1),
            Task(name="t2", callable=t2, upstream=("t1",)),
            Task(name="t3", callable=t3, upstream=("t2",)),
        ),
    )

    runner = LocalSequentialRunner()
    # Mock adapters so the runner's create_control_tables + pipeline_control_state
    # path doesn't need a real warehouse.
    _run_with_stubbed_control(runner, p)
    assert calls == ["t1", "t2", "t3"]


@pytest.mark.unit
def test_runner_halts_on_failing_task_when_stop_on_error_true() -> None:
    calls: list[str] = []

    def good(_ctx):
        calls.append("good")
        return {"ok": True}

    def boom(_ctx):
        calls.append("boom")
        raise RuntimeError("synthetic boom")

    def never(_ctx):
        calls.append("never")
        return {}

    p = Pipeline(
        pipeline_id="synth_fail",
        description="fail-in-middle",
        tasks=(
            Task(name="a", callable=good),
            Task(name="b", callable=boom, upstream=("a",)),
            Task(name="c", callable=never, upstream=("b",)),
        ),
    )
    result = _run_with_stubbed_control(LocalSequentialRunner(stop_on_error=True), p)
    assert calls == ["good", "boom"]
    assert result.status == "FAILED"
    by_name = {t.name: t for t in result.task_results}
    assert by_name["a"].status == "SUCCESS"
    assert by_name["b"].status == "FAILED"
    assert by_name["c"].status == "SKIPPED_PAUSED" or by_name["c"].status in {"SKIPPED_ABORTED"}


@pytest.mark.unit
def test_runner_skips_downstream_when_state_is_paused(monkeypatch: pytest.MonkeyPatch) -> None:
    """If the pipeline is PAUSED when the runner starts, every task is skipped."""

    def should_not_run(_ctx):
        raise AssertionError("task ran while PAUSED — sensor did not short-circuit")

    p = Pipeline(
        pipeline_id="synth_paused",
        description="sensor test",
        tasks=(Task(name="only", callable=should_not_run),),
    )

    # Build a fake PipelineControl whose `.current()` returns PAUSED.
    from datalink.quality.control import PipelineState

    fake_control = MagicMock()
    fake_control.current.return_value = PipelineState.PAUSED

    runner = LocalSequentialRunner()
    result = _run_with_stubbed_control(runner, p, control_override=fake_control)

    assert len(result.task_results) == 1
    assert result.task_results[0].status == "SKIPPED_PAUSED"


@pytest.mark.unit
def test_task_context_lazy_loads_settings_and_adapters(monkeypatch: pytest.MonkeyPatch) -> None:
    """TaskContext should not touch config/adapters until .settings/.adapters accessed."""
    ctx = TaskContext(pipeline_id="x", run_id="y", env="local")
    # Nothing loaded yet.
    assert ctx._settings is None
    assert ctx._adapters is None

    # Stub the loader so we don't need a real config dir.
    fake_settings = MagicMock(name="Settings")
    fake_adapters = MagicMock(name="AdapterSet")
    monkeypatch.setattr(
        "datalink.orchestration.tasks.load_settings", lambda env=None: fake_settings
    )
    # build_adapters is imported inside the property; patch its symbol *there*.
    monkeypatch.setattr("datalink.adapters.factory.build_adapters", lambda _s: fake_adapters)
    assert ctx.settings is fake_settings
    assert ctx.adapters is fake_adapters


# ----------------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------------


def _run_with_stubbed_control(
    runner: LocalSequentialRunner, pipeline: Pipeline, *, control_override=None
):
    """Invoke runner.run() with PipelineControl + create_control_tables stubbed out.

    We don't want to spin up a real DuckDB for every synthetic-pipeline unit test.
    """
    from unittest.mock import patch

    fake_control = control_override or MagicMock()
    if control_override is None:
        fake_control.current.return_value = None  # first-run equivalent

    fake_adapters = MagicMock(name="AdapterSet")

    # The runner opens short-lived warehouse connections via:
    #   - datalink.adapters.factory.build_adapters(settings)
    #   - datalink.config.loader.load_settings(env=...)
    #   - datalink.quality.create_control_tables / PipelineControl (deferred imports)
    # Patch each at the source so nothing touches a real config/warehouse.
    with (
        patch("datalink.adapters.factory.build_adapters", return_value=fake_adapters),
        patch("datalink.config.loader.load_settings", return_value=MagicMock(name="Settings")),
        patch("datalink.quality.create_control_tables"),
        patch("datalink.quality.PipelineControl", return_value=fake_control),
        # Also patch the TaskContext accessors so task callables that touch
        # ctx.adapters/ctx.settings see mocks (not real ones).
        patch.object(
            TaskContext,
            "adapters",
            property(lambda self: fake_adapters),
        ),
        patch.object(
            TaskContext,
            "settings",
            property(lambda self: MagicMock(name="Settings")),
        ),
    ):
        return runner.run(pipeline, env="local", run_id="unit-test")
