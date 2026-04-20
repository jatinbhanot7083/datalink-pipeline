"""Pipeline orchestration — Airflow DAGs + local_sequential runner.

The same set of task callables (`datalink.orchestration.tasks`) powers BOTH
orchestrators. This is the single source of truth for "what are the steps
in Bronze / Silver / Gold" — see `datalink.orchestration.pipelines`.

Orchestrator selection is config-driven: `settings.orchestrator` is either
`local_sequential` (default, in-process, runs on any dev box) or
`airflow_kind` (opt-in, kind + Helm, prod parity with AKS).

Neither path has hardcoded paths or credentials. Promotion to prod = flip
`DL_ENV` + `DL_ORCHESTRATOR`; no code changes.
"""

from datalink.orchestration.local_sequential import (
    LocalSequentialRunner,
    RunResult,
    TaskResult,
    run_pipeline,
)
from datalink.orchestration.pipelines import (
    BRONZE_INGEST_PIPELINE,
    GOLD_UM_PUSH_PIPELINE,
    SILVER_TRANSFORM_PIPELINE,
    Pipeline,
    Task,
    get_pipeline,
)

__all__ = [
    "BRONZE_INGEST_PIPELINE",
    "GOLD_UM_PUSH_PIPELINE",
    "SILVER_TRANSFORM_PIPELINE",
    "LocalSequentialRunner",
    "Pipeline",
    "RunResult",
    "Task",
    "TaskResult",
    "get_pipeline",
    "run_pipeline",
]
