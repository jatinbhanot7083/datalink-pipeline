"""Auto-generated Airflow DAG — Phase 15 Pipeline Architect.

  client_id        : aetna
  dataset_code     : membership
  bronze_anchor    : FLAT_FILE  (Pipe-delimited or CSV vendor extract)
  schedule         : 0 4 * * *
  catalog_version  : 1
  onprem routing   : CC → postgres, E360 → snowflake_share, EC → sqlserver, ESV → snowflake_share, RBN → postgres

DO NOT EDIT BY HAND. Re-generate via `make rebuild-dag CLIENT=aetna DATASET=membership`
or the Pipeline Architect UI. Manual edits are clobbered on re-deploy.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from datalink.orchestration.tasks import (
    bronze_land_task,
    bronze_validate_task,
    gold_dbt_task,
    onprem_push_task,
    silver_dbt_task,
)

DEFAULT_ARGS = {
    "owner": "datalink",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
    "email_on_failure": False,
}


with DAG(
    dag_id="aetna_membership_pipeline",
    description="Phase 15 — aetna / membership (Bronze→Silver→Gold→OnPrem)",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule="0 4 * * *",
    catchup=False,
    tags=["datalink", "phase15", "aetna", "membership", "flat_file"],
    max_active_runs=1,
) as dag:
    start = EmptyOperator(task_id="start")

    bronze_land = PythonOperator(
        task_id="bronze_land",
        python_callable=bronze_land_task,
        op_kwargs={
            "client_id": "aetna",
            "dataset_code": "membership",
            "bronze_anchor": "FLAT_FILE",
        },
    )

    bronze_validate = PythonOperator(
        task_id="bronze_validate",
        python_callable=bronze_validate_task,
        op_kwargs={
            "client_id": "aetna",
            "dataset_code": "membership",
        },
    )

    silver_dbt = PythonOperator(
        task_id="silver_dbt",
        python_callable=silver_dbt_task,
        op_kwargs={
            "client_id": "aetna",
            "dataset_code": "membership",
        },
    )

    gold_dbt = PythonOperator(
        task_id="gold_dbt",
        python_callable=gold_dbt_task,
        op_kwargs={
            "client_id": "aetna",
            "dataset_code": "membership",
        },
    )

    onprem_push = PythonOperator(
        task_id="onprem_push",
        python_callable=onprem_push_task,
        op_kwargs={
            "client_id": "aetna",
            "dataset_code": "membership",
            "targets": [
                {
                    "downstream_product": "CC",
                    "target_system": "postgres",
                    "target_uri": "postgres://care_compass.onprem",
                    "action": "ENABLE",
                },
                {
                    "downstream_product": "E360",
                    "target_system": "snowflake_share",
                    "target_uri": "snowflake://e360_share",
                    "action": "ENABLE",
                },
                {
                    "downstream_product": "EC",
                    "target_system": "sqlserver",
                    "target_uri": "sqlserver://evokeconnect.onprem",
                    "action": "ENABLE",
                },
                {
                    "downstream_product": "ESV",
                    "target_system": "snowflake_share",
                    "target_uri": "snowflake://esv_share",
                    "action": "ENABLE",
                },
                {
                    "downstream_product": "RBN",
                    "target_system": "postgres",
                    "target_uri": "postgres://rbn.onprem",
                    "action": "ENABLE",
                },
            ],
        },
    )

    end = EmptyOperator(task_id="end")

    start >> bronze_land >> bronze_validate >> silver_dbt >> gold_dbt >> onprem_push >> end
