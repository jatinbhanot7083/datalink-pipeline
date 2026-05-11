"""DataLink metrics — Phase 20.1 observability emission.

Exports OpenTelemetry metrics via OTLP gRPC to the local OTEL collector
(``otel-collector:4317`` inside Docker; ``localhost:4317`` for local dev).
The collector re-exposes them on a Prometheus endpoint at ``:8889``,
which Prometheus scrapes per ``docker/prometheus/prometheus.yml``.

Metric inventory:

  Counters (monotonic, sum across the universe):
    pipeline_runs_total        {dataset, layer, status}
    dq_checks_total            {dataset, layer, dimension, severity, result}
    bronze_rows_loaded_total   {dataset, client}
    silver_rows_total          {dataset, client}
    gold_rows_total            {dataset, client}
    ai_tokens_total            {agent}
    ai_proposals_total         {agent, outcome}
    schema_drift_events_total  {dataset, drift_type, severity, action}

  Gauges (instantaneous):
    pipelines_live             {}
    dq_pass_rate               {dataset, layer}

  Histograms (latency):
    task_duration_seconds      {task, dataset, client, status}
    ai_latency_seconds         {agent}

Idempotent init: ``configure_metrics()`` is safe to call repeatedly; the
first call wins and subsequent calls are no-ops.  All public ``record_*``
helpers degrade to silent no-ops if the SDK / collector is unavailable —
metric emission MUST NOT fail the pipeline.
"""

from __future__ import annotations

import logging
import os
import threading
from typing import Any

_log = logging.getLogger(__name__)

# Module-level state; populated by configure_metrics() on first call.
_LOCK = threading.Lock()
_INITIALISED = False
_METER: Any = None  # actual type: opentelemetry.metrics.Meter | None
_INSTRUMENTS: dict[str, Any] = {}


def _resolve_endpoint() -> str:
    """Pick the OTLP gRPC endpoint based on env.

    In Docker, the Streamlit / Airflow worker containers reach the
    collector at ``otel-collector:4317`` (compose network alias).
    For dev outside Docker, ``localhost:4317`` is right.
    """
    return os.environ.get(
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "http://otel-collector:4317"
        if os.environ.get("DOCKER_NETWORK") == "datalink"
        else "http://localhost:4317",
    )


def configure_metrics(
    *,
    service_name: str = "datalink",
    service_namespace: str = "datalink",
    deployment_env: str = "local",
) -> bool:
    """Initialise the OTLP metrics pipeline.  Returns True on success,
    False if anything went wrong (caller can keep going either way —
    record_* helpers no-op when not initialised)."""
    global _INITIALISED, _METER, _INSTRUMENTS

    with _LOCK:
        if _INITIALISED:
            return _METER is not None
        try:
            from opentelemetry import metrics
            from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import (
                OTLPMetricExporter,
            )
            from opentelemetry.sdk.metrics import MeterProvider
            from opentelemetry.sdk.metrics.export import (
                PeriodicExportingMetricReader,
            )
            from opentelemetry.sdk.resources import Resource

            endpoint = _resolve_endpoint()
            resource = Resource.create(
                {
                    "service.name": service_name,
                    "service.namespace": service_namespace,
                    "deployment.environment": deployment_env,
                }
            )
            exporter = OTLPMetricExporter(endpoint=endpoint, insecure=True)
            reader = PeriodicExportingMetricReader(
                exporter,
                export_interval_millis=10_000,  # push every 10s
                export_timeout_millis=8_000,
            )
            provider = MeterProvider(resource=resource, metric_readers=[reader])
            metrics.set_meter_provider(provider)
            _METER = metrics.get_meter(service_name, "1.0")

            # Counters
            _INSTRUMENTS["pipeline_runs_total"] = _METER.create_counter(
                name="datalink.pipeline_runs.total",
                description="Number of pipeline runs by dataset/layer/status",
                unit="1",
            )
            _INSTRUMENTS["dq_checks_total"] = _METER.create_counter(
                name="datalink.dq_checks.total",
                description="DQ expectation evaluations",
                unit="1",
            )
            _INSTRUMENTS["bronze_rows_total"] = _METER.create_counter(
                name="datalink.bronze_rows_loaded.total",
                description="Rows loaded into Bronze across runs",
                unit="rows",
            )
            _INSTRUMENTS["silver_rows_total"] = _METER.create_counter(
                name="datalink.silver_rows.total",
                description="Rows materialized into Silver",
                unit="rows",
            )
            _INSTRUMENTS["gold_rows_total"] = _METER.create_counter(
                name="datalink.gold_rows.total",
                description="Rows materialized into Gold",
                unit="rows",
            )
            _INSTRUMENTS["ai_tokens_total"] = _METER.create_counter(
                name="datalink.ai_tokens.total",
                description="LLM tokens spent by agent",
                unit="tokens",
            )
            _INSTRUMENTS["ai_proposals_total"] = _METER.create_counter(
                name="datalink.ai_proposals.total",
                description="AI proposals by agent and outcome",
                unit="1",
            )
            _INSTRUMENTS["schema_drift_events_total"] = _METER.create_counter(
                name="datalink.schema_drift_events.total",
                description="Schema drift events detected",
                unit="1",
            )

            # Histograms
            _INSTRUMENTS["task_duration"] = _METER.create_histogram(
                name="datalink.task_duration_seconds",
                description="Task execution duration",
                unit="s",
            )
            _INSTRUMENTS["ai_latency"] = _METER.create_histogram(
                name="datalink.ai_latency_seconds",
                description="AI proposal latency",
                unit="s",
            )

            _INITIALISED = True
            _log.info(
                "metrics.configured endpoint=%s service=%s",
                endpoint,
                service_name,
            )
            return True
        except Exception as exc:  # pragma: no cover — defensive
            _log.warning("metrics.configure_failed err=%s", str(exc)[:300])
            _INITIALISED = True  # Mark as tried — don't retry per call
            _METER = None
            return False


# ---------------------------------------------------------------------------
# Recording helpers — every one is a silent no-op if not configured.
# ---------------------------------------------------------------------------
def _safe_record(instrument_name: str, fn_name: str, value: Any, attrs: dict) -> None:
    inst = _INSTRUMENTS.get(instrument_name)
    if inst is None:
        return
    try:
        getattr(inst, fn_name)(value, attributes=attrs)
    except Exception:  # pragma: no cover — defensive
        pass


def record_pipeline_run(*, dataset: str, client: str, status: str, duration_s: float) -> None:
    """Called at the end of run_now / Airflow DAG run."""
    if not _INITIALISED:
        configure_metrics()
    attrs = {"dataset": dataset, "client": client, "status": status}
    _safe_record("pipeline_runs_total", "add", 1, attrs)
    _safe_record(
        "task_duration",
        "record",
        max(0.0, float(duration_s)),
        {**attrs, "task": "pipeline"},
    )


def record_task_duration(
    *,
    task: str,
    dataset: str,
    client: str,
    status: str,
    duration_s: float,
) -> None:
    """Called per-task inside run_now."""
    if not _INITIALISED:
        configure_metrics()
    _safe_record(
        "task_duration",
        "record",
        max(0.0, float(duration_s)),
        {"task": task, "dataset": dataset, "client": client, "status": status},
    )


def record_layer_rows(*, layer: str, dataset: str, client: str, rows: int) -> None:
    """Bronze/Silver/Gold row counts post-task."""
    if not _INITIALISED:
        configure_metrics()
    if rows is None or rows <= 0:
        return
    inst_name = {
        "BRONZE": "bronze_rows_total",
        "SILVER": "silver_rows_total",
        "GOLD": "gold_rows_total",
    }.get(str(layer).upper())
    if not inst_name:
        return
    _safe_record(inst_name, "add", int(rows), {"dataset": dataset, "client": client})


def record_dq_check_batch(
    *,
    dataset: str,
    layer: str,
    passed: int,
    failed: int,
    skipped: int = 0,
) -> None:
    """One emission per validate-task completion — counters by result.
    Suite-level (no per-expectation labels)."""
    if not _INITIALISED:
        configure_metrics()
    base = {"dataset": dataset, "layer": layer}
    if passed:
        _safe_record("dq_checks_total", "add", passed, {**base, "result": "pass"})
    if failed:
        _safe_record("dq_checks_total", "add", failed, {**base, "result": "fail"})
    if skipped:
        _safe_record("dq_checks_total", "add", skipped, {**base, "result": "skip"})


def record_ai_proposal(
    *,
    agent: str,
    tokens: int,
    duration_s: float,
    outcome: str = "success",
) -> None:
    """Called by AI architect agents after each proposal completes."""
    if not _INITIALISED:
        configure_metrics()
    _safe_record("ai_tokens_total", "add", int(tokens or 0), {"agent": agent})
    _safe_record("ai_proposals_total", "add", 1, {"agent": agent, "outcome": outcome})
    _safe_record(
        "ai_latency",
        "record",
        max(0.0, float(duration_s)),
        {"agent": agent, "outcome": outcome},
    )


def record_schema_drift(*, dataset: str, drift_type: str, severity: str, action: str) -> None:
    if not _INITIALISED:
        configure_metrics()
    _safe_record(
        "schema_drift_events_total",
        "add",
        1,
        {
            "dataset": dataset,
            "drift_type": drift_type,
            "severity": severity,
            "action": action,
        },
    )


def shutdown() -> None:
    """Flush + shut down the OTLP exporter.  Call from app teardown."""
    if not _INITIALISED:
        return
    try:
        from opentelemetry import metrics

        provider = metrics.get_meter_provider()
        if hasattr(provider, "shutdown"):
            provider.shutdown()
    except Exception:  # pragma: no cover
        pass
