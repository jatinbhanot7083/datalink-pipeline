"""silver_transform_dag — dbt Silver DV 2.0 build + CP2 clinical checkpoint.

Pipeline graph:
    pipeline_control_gate
        └── dbt_run_silver   (dbt run --select silver)
                └── dbt_test_silver   (dbt test --select silver)
                        └── silver_checkpoint   (CP2 GX + crews)
"""

from __future__ import annotations

from dags._dag_builder import build_dag
from datalink.orchestration.pipelines import SILVER_TRANSFORM_PIPELINE

dag = build_dag(SILVER_TRANSFORM_PIPELINE, env="local", schedule=None)
