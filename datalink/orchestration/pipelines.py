"""Pipeline graphs — three pipelines, shared by both orchestrators.

`Pipeline` + `Task` are intentionally minimal dataclasses (not Airflow
`DAG`/`Operator` subclasses) — this file is the source of truth that both
the local_sequential runner and the Airflow DAG files consult. Airflow DAGs
map each `Task` to a `PythonOperator`; the local runner walks the same
dependency graph in-process.

The task callables live in `datalink.orchestration.tasks` — one function
per step. If you need a new step, add it there and reference its name here.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from datalink.orchestration import tasks as _tasks


@dataclass(frozen=True)
class Task:
    name: str
    callable: Callable[[Any], dict[str, Any]]  # (TaskContext) -> output dict
    upstream: tuple[str, ...] = ()  # names of tasks that must finish first
    description: str = ""


@dataclass(frozen=True)
class Pipeline:
    pipeline_id: str
    description: str
    tasks: tuple[Task, ...]
    # Which GX checkpoint tasks are in this pipeline — used by the control-state
    # sensor to know what to short-circuit on a PAUSED / ABORTED state.
    gx_checkpoint_task_names: tuple[str, ...] = field(default_factory=tuple)

    def task(self, name: str) -> Task:
        for t in self.tasks:
            if t.name == name:
                return t
        raise KeyError(f"Task {name!r} not in pipeline {self.pipeline_id!r}")

    def topo_sorted(self) -> list[Task]:
        """Kahn's algorithm — deterministic topological ordering."""
        by_name = {t.name: t for t in self.tasks}
        indeg = {t.name: len(t.upstream) for t in self.tasks}
        ready = [n for n, d in indeg.items() if d == 0]
        ready.sort()  # deterministic tie-break
        order: list[Task] = []
        while ready:
            current = ready.pop(0)
            order.append(by_name[current])
            for t in self.tasks:
                if current in t.upstream:
                    indeg[t.name] -= 1
                    if indeg[t.name] == 0:
                        ready.append(t.name)
                        ready.sort()
        if len(order) != len(self.tasks):
            raise ValueError(f"cycle in pipeline {self.pipeline_id!r}")
        return order


# ----------------------------------------------------------------------------
# The three pipelines
# ----------------------------------------------------------------------------


BRONZE_INGEST_PIPELINE = Pipeline(
    pipeline_id="bronze_ingest",
    description="SFTP → ObjectStore → DuckDB Bronze MERGE → CP1 structural checkpoint",
    tasks=(
        Task(
            name="bronze_ingest",
            callable=_tasks.task_bronze_ingest,
            description="Ingest all 3 sample CSVs from SFTP into BRONZE.*",
        ),
        Task(
            name="bronze_checkpoint",
            callable=_tasks.task_bronze_checkpoint,
            upstream=("bronze_ingest",),
            description="CP1 — nulls, row counts, claim_id uniqueness",
        ),
    ),
    gx_checkpoint_task_names=("bronze_checkpoint",),
)


SILVER_TRANSFORM_PIPELINE = Pipeline(
    pipeline_id="silver_transform",
    description="dbt Silver DV 2.0 build + dbt tests + CP2 clinical checkpoint",
    tasks=(
        Task(
            name="dbt_run_silver",
            callable=_tasks.task_dbt_run_silver,
            description="dbt run --select silver — 10 DV 2.0 models",
        ),
        Task(
            name="dbt_test_silver",
            callable=_tasks.task_dbt_test_silver,
            upstream=("dbt_run_silver",),
            description="dbt test --select silver — 60 tests",
        ),
        Task(
            name="silver_checkpoint",
            callable=_tasks.task_silver_checkpoint,
            upstream=("dbt_test_silver",),
            description="CP2 — CPT/ICD, positive paid amounts, date ordering",
        ),
    ),
    gx_checkpoint_task_names=("silver_checkpoint",),
)


GOLD_UM_PUSH_PIPELINE = Pipeline(
    pipeline_id="gold_um_push",
    description="dbt Gold UM build + CP3 + router fan-out to operational DBs",
    tasks=(
        Task(
            name="dbt_seed_gold",
            callable=_tasks.task_dbt_seed_gold,
            description="dbt seed — Lu* CSVs",
        ),
        Task(
            name="dbt_run_gold",
            callable=_tasks.task_dbt_run_gold,
            upstream=("dbt_seed_gold",),
            description="dbt run --select gold — 5 UM models",
        ),
        Task(
            name="dbt_test_gold",
            callable=_tasks.task_dbt_test_gold,
            upstream=("dbt_run_gold",),
            description="dbt test --select gold",
        ),
        Task(
            name="gold_checkpoint",
            callable=_tasks.task_gold_checkpoint,
            upstream=("dbt_test_gold",),
            description="CP3 — auth_due_date >= auth_from_date, status value-sets",
        ),
        Task(
            name="router_push",
            callable=_tasks.task_router_push,
            upstream=("gold_checkpoint",),
            description="Fan Gold UM out to every enabled OperationalDb target",
        ),
    ),
    gx_checkpoint_task_names=("gold_checkpoint",),
)


_ALL: dict[str, Pipeline] = {
    BRONZE_INGEST_PIPELINE.pipeline_id: BRONZE_INGEST_PIPELINE,
    SILVER_TRANSFORM_PIPELINE.pipeline_id: SILVER_TRANSFORM_PIPELINE,
    GOLD_UM_PUSH_PIPELINE.pipeline_id: GOLD_UM_PUSH_PIPELINE,
}


def get_pipeline(pipeline_id: str) -> Pipeline:
    if pipeline_id not in _ALL:
        raise KeyError(f"Unknown pipeline {pipeline_id!r}; known: {sorted(_ALL.keys())}")
    return _ALL[pipeline_id]


def all_pipelines() -> list[Pipeline]:
    return list(_ALL.values())
