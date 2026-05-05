"""Anomaly Detector — Phase 16.5 (Wave 5 #2).

Computes per-(client x dataset x metric) statistical baselines and detects
deviations on each Bronze validate run. Three-sigma events fire SMART_PAUSE
which the silver_dbt_task respects (skip materialization if any OPEN
CRITICAL anomaly).

Metrics tracked:
  * row_count           — total rows in BRONZE_<CLIENT>.raw_<dataset>
  * null_rate           — per business-key column
  * cardinality         — distinct values for high-cardinality cols

Usage from bronze_validate_task::

    from datalink.quality.anomaly_detector import (
        check_and_record_anomalies,
        has_open_smart_pause,
    )

    events = check_and_record_anomalies(
        warehouse=wh, client_id=client_id, dataset_code=dataset_code,
    )

    if has_open_smart_pause(wh, client_id, dataset_code):
        raise RuntimeError("Smart pause active — anomaly events open. Ack via UI.")
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from datalink.adapters.protocols import Warehouse
from datalink.logging import get_logger
from datalink.quality.control import CONTROL_SCHEMA

_log = get_logger(__name__)

SIGMA_WARNING = 2.0
SIGMA_CRITICAL = 3.0
MIN_BASELINE_SAMPLES = 5  # need at least N runs to compute mean/stddev
BASELINE_WINDOW_DAYS = 30


@dataclass
class AnomalyEvent:
    event_id: str
    metric: str
    column_name: str | None
    observed_value: float
    baseline_mean: float
    baseline_stddev: float
    sigma: float
    severity: str  # WARNING | CRITICAL
    action_taken: str  # LOGGED | SMART_PAUSE


def _now_utc() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def _bronze_table(client_id: str, dataset_code: str) -> str:
    return f"BRONZE_{client_id.upper()}.raw_{dataset_code.lower()}"


def _table_exists(wh: Warehouse, schema: str, table: str) -> bool:
    try:
        rows = list(
            wh.query(
                "SELECT COUNT(*) AS c FROM INFORMATION_SCHEMA.TABLES "
                "WHERE TABLE_SCHEMA = %(s)s AND TABLE_NAME = %(t)s",
                {"s": schema.upper(), "t": table.upper()},
            )
        )
        return bool(rows) and int(rows[0]["c"]) > 0
    except Exception:
        return False


def _row_count(wh: Warehouse, fq: str) -> int:
    rows = list(wh.query(f"SELECT COUNT(*) AS c FROM {fq}"))
    return int(rows[0]["c"]) if rows else 0


def _null_rate(wh: Warehouse, fq: str, column: str) -> float:
    rows = list(
        wh.query(f"SELECT AVG(CASE WHEN {column} IS NULL THEN 1.0 ELSE 0.0 END) AS r FROM {fq}")
    )
    return float(rows[0]["r"] or 0) if rows else 0.0


def _list_business_keys(wh: Warehouse, dataset_code: str) -> list[str]:
    """Pull business-key columns from the catalog so we know which to baseline."""
    try:
        rows = list(
            wh.query(
                f"SELECT bronze_column_name "
                f"FROM {CONTROL_SCHEMA}.global_bronze_catalog_fields "
                f"WHERE dataset_code = %(ds)s AND is_business_key = TRUE",
                {"ds": dataset_code},
            )
        )
        return [str(r["bronze_column_name"]) for r in rows]
    except Exception:
        return []


def _get_or_compute_baseline(
    wh: Warehouse,
    *,
    client_id: str,
    dataset_code: str,
    metric: str,
    column_name: str | None,
) -> dict[str, Any] | None:
    """Return baseline {mean, stddev, n} or None if insufficient samples."""
    # Pull historical observations from anomaly_events (we use the event log
    # itself as the time series for stddev calc — every observed_value is a
    # data point).
    where = (
        "client_id = %(c)s AND dataset_code = %(ds)s AND metric = %(m)s "
        "AND event_at > DATEADD(day, -%(w)s, CURRENT_TIMESTAMP())"
    )
    params: dict[str, Any] = {
        "c": client_id,
        "ds": dataset_code,
        "m": metric,
        "w": BASELINE_WINDOW_DAYS,
    }
    if column_name is None:
        where += " AND column_name IS NULL"
    else:
        where += " AND column_name = %(col)s"
        params["col"] = column_name

    try:
        rows = list(
            wh.query(
                f"SELECT AVG(observed_value) AS m, STDDEV(observed_value) AS s, "
                f"       COUNT(*) AS n "
                f"FROM {CONTROL_SCHEMA}.anomaly_events "
                f"WHERE {where}",
                params,
            )
        )
    except Exception:
        return None
    if not rows or not rows[0]["n"] or int(rows[0]["n"]) < MIN_BASELINE_SAMPLES:
        return None
    return {
        "mean": float(rows[0]["m"] or 0),
        "stddev": float(rows[0]["s"] or 0),
        "n": int(rows[0]["n"]),
    }


def _record_event(
    wh: Warehouse,
    *,
    client_id: str,
    dataset_code: str,
    metric: str,
    column_name: str | None,
    observed: float,
    baseline: dict[str, Any] | None,
) -> AnomalyEvent | None:
    """Persist an anomaly_events row. Always records the observation (so it
    becomes part of the baseline for the NEXT run). Severity = WARNING /
    CRITICAL only fires when |sigma| crosses thresholds."""
    if baseline:
        std = baseline["stddev"] or 1e-9
        sigma = (observed - baseline["mean"]) / std if std else 0.0
        if abs(sigma) >= SIGMA_CRITICAL:
            severity = "CRITICAL"
            action = "SMART_PAUSE"
        elif abs(sigma) >= SIGMA_WARNING:
            severity = "WARNING"
            action = "LOGGED"
        else:
            severity = "INFO"
            action = "LOGGED"
        mean_v = baseline["mean"]
        std_v = baseline["stddev"]
    else:
        sigma = 0.0
        severity = "INFO"  # bootstrap — not enough samples
        action = "LOGGED"
        mean_v = observed
        std_v = 0.0

    eid = str(uuid.uuid4())
    try:
        wh.execute(
            f"INSERT INTO {CONTROL_SCHEMA}.anomaly_events "
            f"(event_id, client_id, dataset_code, metric, column_name, "
            f" observed_value, baseline_mean, baseline_stddev, sigma, "
            f" severity, action_taken, status, event_at) "
            f"SELECT %(e)s, %(c)s, %(ds)s, %(m)s, %(col)s, %(o)s, %(mn)s, "
            f"       %(sd)s, %(sg)s, %(sv)s, %(act)s, "
            f"       CASE WHEN %(sv)s IN ('WARNING','CRITICAL') THEN 'OPEN' "
            f"            ELSE 'RESOLVED' END, "
            f"       %(ts)s",
            {
                "e": eid,
                "c": client_id,
                "ds": dataset_code,
                "m": metric,
                "col": column_name,
                "o": observed,
                "mn": mean_v,
                "sd": std_v,
                "sg": sigma,
                "sv": severity,
                "act": action,
                "ts": _now_utc(),
            },
        )
    except Exception as exc:
        _log.warning("anomaly.record_failed", err=str(exc)[:200])
        return None

    if severity in ("WARNING", "CRITICAL"):
        return AnomalyEvent(
            event_id=eid,
            metric=metric,
            column_name=column_name,
            observed_value=observed,
            baseline_mean=mean_v,
            baseline_stddev=std_v,
            sigma=sigma,
            severity=severity,
            action_taken=action,
        )
    return None


def check_and_record_anomalies(
    *,
    warehouse: Warehouse,
    client_id: str,
    dataset_code: str,
) -> list[AnomalyEvent]:
    """Compute current observations, compare to baselines, record events.

    Returns the list of WARNING/CRITICAL events fired (if any). INFO events
    are recorded silently for baseline accumulation.
    """
    fq = _bronze_table(client_id, dataset_code)
    schema, _, table = fq.partition(".")
    if not _table_exists(warehouse, schema, table):
        _log.info("anomaly.skip_no_table", fq=fq)
        return []

    fired: list[AnomalyEvent] = []

    # 1. row_count
    try:
        rc = _row_count(warehouse, fq)
        baseline = _get_or_compute_baseline(
            warehouse,
            client_id=client_id,
            dataset_code=dataset_code,
            metric="row_count",
            column_name=None,
        )
        ev = _record_event(
            warehouse,
            client_id=client_id,
            dataset_code=dataset_code,
            metric="row_count",
            column_name=None,
            observed=float(rc),
            baseline=baseline,
        )
        if ev:
            fired.append(ev)
    except Exception as exc:
        _log.warning("anomaly.rowcount_failed", err=str(exc)[:200])

    # 2. null_rate per business-key column
    bks = _list_business_keys(warehouse, dataset_code)
    for col in bks:
        try:
            nr = _null_rate(warehouse, fq, col)
            baseline = _get_or_compute_baseline(
                warehouse,
                client_id=client_id,
                dataset_code=dataset_code,
                metric="null_rate",
                column_name=col,
            )
            ev = _record_event(
                warehouse,
                client_id=client_id,
                dataset_code=dataset_code,
                metric="null_rate",
                column_name=col,
                observed=nr,
                baseline=baseline,
            )
            if ev:
                fired.append(ev)
        except Exception as exc:
            _log.warning("anomaly.nullrate_failed", col=col, err=str(exc)[:200])

    if fired:
        _log.warning(
            "anomaly.events_fired",
            client_id=client_id,
            dataset_code=dataset_code,
            n_warning=sum(1 for e in fired if e.severity == "WARNING"),
            n_critical=sum(1 for e in fired if e.severity == "CRITICAL"),
        )
    return fired


def has_open_smart_pause(
    warehouse: Warehouse,
    *,
    client_id: str,
    dataset_code: str,
) -> bool:
    """True if there's an OPEN CRITICAL anomaly event for this (client, dataset).

    The silver_dbt_task should call this BEFORE materializing — if True, raise
    so the operator must ack via the Anomalies UI page first.
    """
    try:
        rows = list(
            warehouse.query(
                f"SELECT COUNT(*) AS c FROM {CONTROL_SCHEMA}.anomaly_events "
                f"WHERE client_id = %(c)s AND dataset_code = %(ds)s "
                f"  AND severity = 'CRITICAL' AND action_taken = 'SMART_PAUSE' "
                f"  AND status = 'OPEN'",
                {"c": client_id, "ds": dataset_code},
            )
        )
    except Exception:
        return False
    return bool(rows) and int(rows[0]["c"]) > 0


def ensure_anomaly_tables(warehouse: Warehouse) -> None:
    """Idempotent — same DDL as 35_Anomalies.py page. Called from
    bronze_validate_task on first run."""
    warehouse.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.anomaly_events (
            event_id        VARCHAR(36) NOT NULL,
            client_id       VARCHAR(64) NOT NULL,
            dataset_code    VARCHAR(64) NOT NULL,
            metric          VARCHAR(48) NOT NULL,
            column_name     VARCHAR(128),
            observed_value  FLOAT NOT NULL,
            baseline_mean   FLOAT NOT NULL,
            baseline_stddev FLOAT NOT NULL,
            sigma           FLOAT NOT NULL,
            severity        VARCHAR(16) NOT NULL,
            action_taken    VARCHAR(48) NOT NULL,
            status          VARCHAR(16) NOT NULL,
            event_at        TIMESTAMP_NTZ NOT NULL,
            acked_at        TIMESTAMP_NTZ,
            acked_by        VARCHAR(128),
            notes           VARCHAR,
            PRIMARY KEY (event_id)
        )
        """
    )
