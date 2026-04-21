"""Shared DAG-builder helper — turns a `datalink.orchestration.Pipeline`
into a real Airflow DAG with:

  * a `PipelineControlStateSensor` gate at the top
  * one `PythonOperator` per `Task`
  * the same upstream/downstream graph as the Pipeline dataclass

Keeps every DAG file to ~15 lines of actual config — the pipeline graph is
the source of truth, not the DAG module.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from airflow.operators.python import PythonOperator  # noqa: F401

from datalink.orchestration.airflow_sensors import PipelineControlStateSensor
from datalink.orchestration.pipelines import Pipeline
from datalink.orchestration.tasks import TaskContext


def build_dag(
    pipeline: Pipeline,
    *,
    env: str = "local",
    schedule: str | None = None,
    start_date: datetime | None = None,
    owner: str = "team-datalink",
) -> Any:
    """Convert a Pipeline into a live Airflow DAG object."""
    # Deferred imports — Airflow is an opt-in extra via `uv sync --extra orchestration`.
    from airflow import DAG
    from airflow.operators.python import PythonOperator

    default_args = {
        "owner": owner,
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    }

    dag = DAG(
        dag_id=pipeline.pipeline_id,
        description=pipeline.description,
        default_args=default_args,
        schedule=schedule,
        start_date=start_date or datetime(2026, 1, 1),
        catchup=False,
        tags=["datalink", "medallion", env],
    )

    with dag:
        gate = PipelineControlStateSensor(
            task_id="pipeline_control_gate",
            pipeline_id=pipeline.pipeline_id,
            env=env,
        )

        op_by_name: dict[str, Any] = {}
        for task in pipeline.topo_sorted():
            # Airflow passes **context to `python_callable` when `provide_context`
            # is set via `op_kwargs`. We wrap the task so the callable sees a
            # TaskContext, not Airflow's template dict.
            op = PythonOperator(
                task_id=task.name,
                python_callable=_make_airflow_wrapper(pipeline.pipeline_id, task.callable, env),
            )
            op_by_name[task.name] = op

        # Wire upstream deps — Pipeline → Airflow.
        for task in pipeline.tasks:
            op = op_by_name[task.name]
            if not task.upstream:
                gate >> op
            for upstream_name in task.upstream:
                op_by_name[upstream_name] >> op

    return dag


def _make_airflow_wrapper(pipeline_id: str, task_callable, env: str):
    """Create a PythonOperator callable that builds a TaskContext and forwards."""

    def _wrapper(**airflow_context):
        dag_run = airflow_context.get("dag_run")
        run_id = airflow_context.get("run_id") or (dag_run.run_id if dag_run else "unknown")
        # Phase 5.8: pull client_id from the DAG-run conf (passed by the
        # Control Tower "▶ Run ..." button). Defaults to 'default' for
        # scheduled runs or manual triggers without a client set.
        conf: dict = {}
        if dag_run is not None:
            conf = getattr(dag_run, "conf", None) or {}
        client_id = str(conf.get("client_id", "default"))
        ctx = TaskContext(pipeline_id=pipeline_id, run_id=run_id, env=env, client_id=client_id)
        return task_callable(ctx)

    _wrapper.__name__ = task_callable.__name__
    return _wrapper
