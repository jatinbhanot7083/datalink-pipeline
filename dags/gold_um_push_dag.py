"""gold_um_push_dag — dbt Gold UM build + CP3 + router fan-out.

Pipeline graph:
    pipeline_control_gate
        └── dbt_seed_gold       (Lu* seed CSVs)
                └── dbt_run_gold       (dbt run --select gold)
                        └── dbt_test_gold       (dbt test --select gold)
                                └── gold_checkpoint     (CP3)
                                        └── router_push (fan-out to operational DBs)
"""

from __future__ import annotations

from dags._dag_builder import build_dag
from datalink.orchestration.pipelines import GOLD_UM_PUSH_PIPELINE

dag = build_dag(GOLD_UM_PUSH_PIPELINE, env="local", schedule=None)
