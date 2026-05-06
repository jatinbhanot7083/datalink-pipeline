"""Shared helpers for dataset-scoped DAG factories — Phase 17.3.

Each ``dags/_factory_<dataset>.py`` calls ``build_dag_for_instance()`` per
LIVE row in CONTROL.client_pipeline_instances. The result is one Airflow DAG
per (client, dataset) tuple, all generated dynamically at scheduler load
time. No hand-written DAG files going forward.

Why one factory per dataset (not one mega-factory):
  * Each dataset has different orchestration logic (Membership is simple
    Bronze→Silver→Gold; Claims will need adjudication-status fan-out;
    Provider needs NPPES enrichment; etc.).
  * Per-dataset slicing keeps the file count manageable as datasets are
    added.
  * Adding a new client to an existing dataset is a metadata-only change
    (insert into client_pipeline_instances) — zero code edits.

Why query Snowflake at module load:
  * Airflow's scheduler re-imports DAG files when their mtime changes.
    Pipeline Architect's deploy step touches the relevant factory file
    after inserting the new instance row, forcing a re-import within
    ~30 seconds.
  * If Snowflake is unreachable at parse time, the factory returns an
    empty list + logs a warning — the scheduler won't crash, it just
    won't have those DAGs until the next successful parse.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

_log = logging.getLogger(__name__)


def list_live_instances(dataset_code: str) -> list[dict[str, Any]]:
    """Return every LIVE pipeline instance row for a given dataset.

    Robust to: stale ``BRONZE_SCHEMA`` / ``SILVER_SCHEMA`` / ``GOLD_SCHEMA``
    columns (the factory recomputes from client_id), Snowflake outages
    (returns empty list + warning log), schema drift on the metadata
    table (uses SELECT * and falls back to known field names).
    """
    try:
        import snowflake.connector
    except ImportError:
        _log.warning("dag_factory.no_snowflake_connector_in_env")
        return []
    try:
        conn = snowflake.connector.connect(
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            user=os.environ["SNOWFLAKE_USER"],
            password=os.environ["SNOWFLAKE_PASSWORD"],
            warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
            database=os.environ.get("SNOWFLAKE_DATABASE", "DATALINK_DEV"),
            role=os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        )
    except Exception as exc:
        _log.warning("dag_factory.snowflake_connect_failed err=%s", str(exc)[:200])
        return []
    try:
        cur = conn.cursor()
        cur.execute(
            f"SELECT * FROM CONTROL.client_pipeline_instances "
            f"WHERE LOWER(dataset_code) = LOWER('{dataset_code}') "
            f"AND status = 'LIVE'"
        )
        cols = [c.name.lower() for c in cur.description]
        rows = [dict(zip(cols, r, strict=False)) for r in cur.fetchall()]
        cur.close()
        conn.close()
        return rows
    except Exception as exc:
        _log.warning("dag_factory.query_failed err=%s", str(exc)[:200])
        try:
            conn.close()
        except Exception:
            pass
        return []


def _safe_routing_targets(row: dict[str, Any]) -> list[dict[str, Any]]:
    """Pull routing targets from AI_PROPOSAL_JSON or return empty.

    Default behavior when no targets configured: push_onprem becomes a
    no-op (returns immediately). Pipeline still completes green.
    """
    raw = row.get("ai_proposal_json") or "{}"
    try:
        parsed = json.loads(raw) if isinstance(raw, str) else raw
    except json.JSONDecodeError:
        return []
    targets = parsed.get("downstream_routing", []) or parsed.get("routing_targets", [])
    if not isinstance(targets, list):
        return []
    return targets


def build_dag_for_instance(
    *,
    instance: dict[str, Any],
    dataset_code: str,
    default_args: dict[str, Any] | None = None,
) -> DAG | None:
    """Construct one Airflow DAG for a single client_pipeline_instance row.

    Returns None if the row is malformed (logged + skipped — the factory
    keeps generating the rest).
    """
    # Lazy import — avoids loading datalink.* at module-parse time of the
    # factory file (Airflow's DagBag caches go faster).
    from datalink.orchestration.tasks import (
        bronze_land_task,
        bronze_validate_task,
        gold_dbt_task,
        onprem_push_task,
        silver_dbt_task,
    )

    client_id = str(instance.get("client_id") or "").strip()
    if not client_id:
        _log.warning("dag_factory.skip_row_no_client_id row=%s", instance.get("instance_id"))
        return None

    schedule = str(instance.get("schedule_cron") or "0 4 * * *")
    bronze_anchor = str(instance.get("bronze_anchor") or "FLAT_FILE")
    routing_targets = _safe_routing_targets(instance)

    dag_id = f"{client_id.lower()}_{dataset_code.lower()}_pipeline"
    tags = ["datalink", "phase17", client_id.lower(), dataset_code.lower(), bronze_anchor.lower()]

    da = {
        "owner": "datalink",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "depends_on_past": False,
        "email_on_failure": False,
    }
    if default_args:
        da.update(default_args)

    dag = DAG(
        dag_id=dag_id,
        description=(
            f"Phase 17 — {client_id} / {dataset_code} "
            f"(Bronze→Silver→Gold→OnPrem) — auto-generated"
        ),
        default_args=da,
        start_date=datetime(2026, 1, 1),
        schedule=schedule,
        catchup=False,
        tags=tags,
        max_active_runs=1,
    )

    # `with dag:` would also work but using explicit dag= keeps the factory
    # composable when invoked from another factory.
    start = EmptyOperator(task_id="start", dag=dag)

    bronze_land = PythonOperator(
        task_id="bronze_land",
        python_callable=bronze_land_task,
        op_kwargs={
            "client_id": client_id,
            "dataset_code": dataset_code,
            "bronze_anchor": bronze_anchor,
        },
        dag=dag,
    )

    bronze_validate = PythonOperator(
        task_id="bronze_validate",
        python_callable=bronze_validate_task,
        op_kwargs={"client_id": client_id, "dataset_code": dataset_code},
        dag=dag,
    )

    silver_dbt = PythonOperator(
        task_id="silver_dbt",
        python_callable=silver_dbt_task,
        op_kwargs={"client_id": client_id, "dataset_code": dataset_code},
        dag=dag,
    )

    gold_dbt = PythonOperator(
        task_id="gold_dbt",
        python_callable=gold_dbt_task,
        op_kwargs={"client_id": client_id, "dataset_code": dataset_code},
        dag=dag,
    )

    onprem_push = PythonOperator(
        task_id="onprem_push",
        python_callable=onprem_push_task,
        op_kwargs={
            "client_id": client_id,
            "dataset_code": dataset_code,
            "targets": routing_targets,
        },
        dag=dag,
    )

    end = EmptyOperator(task_id="end", dag=dag)

    start >> bronze_land >> bronze_validate >> silver_dbt >> gold_dbt >> onprem_push >> end

    return dag


def register_dags_for_dataset(dataset_code: str, module_globals: dict[str, Any]) -> int:
    """Generate DAGs for every LIVE instance of `dataset_code` and inject them
    into the factory file's globals so Airflow discovers them.

    Returns the number of DAGs registered. Call from each factory file:

        DATASET = "membership"
        n = register_dags_for_dataset(DATASET, globals())
    """
    instances = list_live_instances(dataset_code)
    n = 0
    for inst in instances:
        dag = build_dag_for_instance(instance=inst, dataset_code=dataset_code)
        if dag is None:
            continue
        # Airflow's DagBag scans module globals — adding the dag to globals()
        # is the canonical way to register dynamic DAGs.
        module_globals[dag.dag_id] = dag
        n += 1
    _log.info("dag_factory.registered dataset=%s n=%s", dataset_code, n)
    return n
