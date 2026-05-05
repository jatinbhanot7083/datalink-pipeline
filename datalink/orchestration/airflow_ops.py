"""Airflow REST API helpers — Phase 16.1 (Wave 1 Item 3).

Wraps the airflow-webserver REST API (auth: basic, default ``airflow / airflow``)
so Streamlit pages can trigger / pause / unpause / inspect DAGs without
making the operator switch to a separate browser tab.

Public functions:

  * ``trigger_dag(dag_id, conf=None)``        — POST /api/v1/dags/<id>/dagRuns
  * ``unpause_dag(dag_id)``                   — PATCH ``is_paused=false``
  * ``pause_dag(dag_id)``                     — PATCH ``is_paused=true``
  * ``get_latest_run(dag_id)``                — most-recent DAG run state
  * ``list_runs(dag_id, limit=10)``           — recent runs (newest first)
  * ``get_task_states(dag_id, run_id)``       — per-task state for a run
  * ``get_task_log_url(dag_id, run_id, ti)``  — deep-link to Airflow UI for the task log
"""

from __future__ import annotations

import os
from datetime import UTC, datetime
from typing import Any

import httpx

from datalink.logging import get_logger

_log = get_logger(__name__)


def _airflow_base_url() -> str:
    """Inside docker network use the service hostname; from host use localhost."""
    return os.environ.get("AIRFLOW_API_URL", "http://airflow_webserver:8080/api/v1")


def _airflow_ui_url() -> str:
    """Browser-facing URL for log deep-links."""
    return os.environ.get("AIRFLOW_UI_URL", "http://localhost:8088")


def _auth() -> tuple[str, str]:
    user = os.environ.get("AIRFLOW_API_USER", "airflow")
    pw = os.environ.get("AIRFLOW_API_PASSWORD", "airflow")
    return (user, pw)


def _client() -> httpx.Client:
    return httpx.Client(base_url=_airflow_base_url(), auth=_auth(), timeout=10.0)


def trigger_dag(
    dag_id: str, *, conf: dict[str, Any] | None = None, logical_date: datetime | None = None
) -> dict[str, Any]:
    """Trigger a manual DAG run. Returns the dag_run JSON."""
    payload: dict[str, Any] = {"conf": conf or {}}
    if logical_date is not None:
        payload["logical_date"] = logical_date.astimezone(UTC).isoformat()
    with _client() as c:
        r = c.post(f"/dags/{dag_id}/dagRuns", json=payload)
        if r.status_code >= 400:
            raise RuntimeError(
                f"Airflow trigger_dag {dag_id} failed: {r.status_code} {r.text[:200]}"
            )
        return r.json()


def unpause_dag(dag_id: str) -> bool:
    with _client() as c:
        r = c.patch(f"/dags/{dag_id}", json={"is_paused": False})
        return r.status_code < 300


def pause_dag(dag_id: str) -> bool:
    with _client() as c:
        r = c.patch(f"/dags/{dag_id}", json={"is_paused": True})
        return r.status_code < 300


def get_latest_run(dag_id: str) -> dict[str, Any] | None:
    """Return the most-recent DagRun row, or None if no runs."""
    runs = list_runs(dag_id, limit=1)
    return runs[0] if runs else None


def list_runs(dag_id: str, limit: int = 10) -> list[dict[str, Any]]:
    """Return the N most-recent DAG runs (newest first)."""
    with _client() as c:
        r = c.get(
            f"/dags/{dag_id}/dagRuns",
            params={"order_by": "-execution_date", "limit": limit},
        )
        if r.status_code == 404:
            return []
        if r.status_code >= 400:
            raise RuntimeError(f"Airflow list_runs {dag_id} failed: {r.status_code} {r.text[:200]}")
        return r.json().get("dag_runs", [])


def get_task_states(dag_id: str, run_id: str) -> list[dict[str, Any]]:
    """Per-task state for a specific dag_run, ordered by task_id."""
    with _client() as c:
        r = c.get(f"/dags/{dag_id}/dagRuns/{run_id}/taskInstances")
        if r.status_code >= 400:
            return []
        rows = r.json().get("task_instances", [])
        return sorted(rows, key=lambda t: t.get("task_id") or "")


def get_task_log_url(dag_id: str, run_id: str, task_id: str, try_number: int = 1) -> str:
    """Deep-link to the Airflow UI's task log page."""
    base = _airflow_ui_url().rstrip("/")
    # URL-encode the run_id (often has timestamps + colons)
    from urllib.parse import quote_plus

    return (
        f"{base}/dags/{dag_id}/grid?dag_run_id={quote_plus(run_id)}"
        f"&task_id={task_id}&tab=logs&try_number={try_number}"
    )


def is_dag_known(dag_id: str) -> bool:
    """True iff the scheduler has registered this DAG (even if paused)."""
    with _client() as c:
        r = c.get(f"/dags/{dag_id}")
        return r.status_code == 200
