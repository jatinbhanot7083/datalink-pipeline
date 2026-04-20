"""bronze_ingest_dag — SFTP → ObjectStore → Bronze DuckDB MERGE + CP1 checkpoint.

Pipeline graph:
    pipeline_control_gate  (PipelineControlStateSensor)
        └── bronze_ingest           (PythonOperator)
                └── bronze_checkpoint   (CP1 GX + Pre-Val/Post-Val crews)
"""

from __future__ import annotations

from dags._dag_builder import build_dag
from datalink.orchestration.pipelines import BRONZE_INGEST_PIPELINE

dag = build_dag(BRONZE_INGEST_PIPELINE, env="local", schedule=None)
