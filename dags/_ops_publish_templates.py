"""Global template publish DAG — Phase 17.7 (the single GLOBAL_CORP-only ops DAG).

GLOBAL_CORP doesn't ingest data — it owns canonical *templates* (Silver/Gold
schemas, dbt model bodies, GX expectation suites, DDL).  Real clients clone
from these templates via the Cloning Center.  Whenever a Global schema
flips to LIVE in Data Model Designer, those templates need to be:

  1. Frozen at this version
  2. Materialized into deployable artifacts (rendered dbt project, GX YAMLs,
     CREATE TABLE DDLs)
  3. Snapshotted into ``CONTROL.global_artifact_blobs`` (or equivalent)
     so client clones reference an immutable bundle, not the live edit.
  4. Tagged as a release version that the Compatibility Advisor can diff.

This DAG is the "publish" half of the contract.  It runs on:

  * **Manual trigger** from Pipeline Architect Section 4 (Phase 17.8 UI).
  * Optionally a daily safety-net schedule that re-publishes if anything
    drifted (off by default — uncomment the schedule below to enable).

It is the ONLY DAG that has scope_owner=GLOBAL_CORP.  All data DAGs are
scoped to a real client_id via the per-dataset factories.

Tasks (sequential):
  validate_global_live → render_dbt → render_gx → render_ddl → snapshot_blobs → tag_release → notify
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

try:
    from airflow import DAG
    from airflow.operators.empty import EmptyOperator
    from airflow.operators.python import PythonOperator
except ImportError:  # pragma: no cover — local dev without Airflow
    DAG = None  # type: ignore[assignment]
    EmptyOperator = None  # type: ignore[assignment]
    PythonOperator = None  # type: ignore[assignment]

_log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Task callables — kept thin; real implementation lands in Phase 17.8
# (Section 4 of Pipeline Architect).  For 17.7 they're stubs that log and
# return a structured payload, so a manual trigger of the DAG completes
# end-to-end without touching live artifacts.
# ---------------------------------------------------------------------------
def validate_global_live(**_: Any) -> dict[str, Any]:
    """Confirm at least one GLOBAL_CORP Silver+Gold pair is LIVE.

    Returns a manifest of what's about to be published.  Phase 17.8 will
    add hard-fail conditions (e.g. dbt model present for every LIVE Gold).
    """
    _log.info("ops.publish.validate_global_live.start")
    # Placeholder — Phase 17.8 replaces with a real Snowflake query.
    manifest = {"silver_live_count": 0, "gold_live_count": 0, "ready": True}
    _log.info("ops.publish.validate_global_live.done manifest=%s", manifest)
    return manifest


def render_dbt(**_: Any) -> dict[str, Any]:
    """Render dbt project YAML/SQL for every LIVE Global dataset.  STUB."""
    _log.info("ops.publish.render_dbt.start")
    return {"models_rendered": 0}


def render_gx(**_: Any) -> dict[str, Any]:
    """Render GX expectation suites for every LIVE Global dataset.  STUB."""
    _log.info("ops.publish.render_gx.start")
    return {"suites_rendered": 0}


def render_ddl(**_: Any) -> dict[str, Any]:
    """Render CREATE TABLE DDL for every LIVE Global Silver+Gold.  STUB."""
    _log.info("ops.publish.render_ddl.start")
    return {"ddl_rendered": 0}


def snapshot_blobs(**_: Any) -> dict[str, Any]:
    """Persist rendered artifacts into CONTROL.global_artifact_blobs.  STUB."""
    _log.info("ops.publish.snapshot_blobs.start")
    return {"blobs_written": 0}


def tag_release(**_: Any) -> dict[str, Any]:
    """Bump GLOBAL_CORP release tag.  STUB.

    Phase 17.8 will write a row to a new ``global_release_tags`` table
    capturing (release_id, semver, included_datasets, created_at,
    created_by).  Clients that clone after this point reference this tag.
    """
    _log.info("ops.publish.tag_release.start")
    return {"release_tag": "v0.0.0-stub"}


def notify(**_: Any) -> dict[str, Any]:
    """Emit a notification when publish completes.  STUB."""
    _log.info("ops.publish.notify.start")
    return {"notified": True}


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
if DAG is not None:  # only register when Airflow is importable
    default_args = {
        "owner": "datalink-ops",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
        "depends_on_past": False,
        "email_on_failure": False,
    }

    dag = DAG(
        dag_id="global_publish_templates",
        description=(
            "GLOBAL_CORP template publish — renders dbt/GX/DDL artifacts "
            "from LIVE Global schemas and snapshots them as a release bundle. "
            "Triggered manually from Pipeline Architect (Phase 17.8). "
            "Auto-generated, do not hand-edit."
        ),
        default_args=default_args,
        start_date=datetime(2026, 1, 1),
        schedule=None,  # manual trigger only; flip to "0 6 * * *" for daily safety net
        catchup=False,
        tags=["datalink", "phase17", "ops", "global_corp", "publish"],
        max_active_runs=1,
    )

    with dag:
        start = EmptyOperator(task_id="start")
        t_validate = PythonOperator(
            task_id="validate_global_live", python_callable=validate_global_live
        )
        t_dbt = PythonOperator(task_id="render_dbt", python_callable=render_dbt)
        t_gx = PythonOperator(task_id="render_gx", python_callable=render_gx)
        t_ddl = PythonOperator(task_id="render_ddl", python_callable=render_ddl)
        t_snap = PythonOperator(task_id="snapshot_blobs", python_callable=snapshot_blobs)
        t_tag = PythonOperator(task_id="tag_release", python_callable=tag_release)
        t_notify = PythonOperator(task_id="notify", python_callable=notify)
        end = EmptyOperator(task_id="end")

        start >> t_validate >> [t_dbt, t_gx, t_ddl]  # render in parallel
        [t_dbt, t_gx, t_ddl] >> t_snap >> t_tag >> t_notify >> end

    # No globals() injection — DAG variable name `dag` is what Airflow scans.
