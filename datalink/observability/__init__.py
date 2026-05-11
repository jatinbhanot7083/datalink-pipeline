"""DataLink observability — Phase 20.1.

OTEL-based metric emission to the local collector.  Public API:

    from datalink.observability.metrics import (
        configure_metrics,
        record_pipeline_run,
        record_task_duration,
        record_layer_rows,
        record_dq_check_batch,
        record_ai_proposal,
        record_schema_drift,
    )

All ``record_*`` helpers are idempotent + silent-no-op safe.  Metric
emission MUST NEVER fail the pipeline.
"""

from .metrics import (
    configure_metrics,
    record_ai_proposal,
    record_dq_check_batch,
    record_layer_rows,
    record_pipeline_run,
    record_schema_drift,
    record_task_duration,
    shutdown,
)

__all__ = [
    "configure_metrics",
    "record_ai_proposal",
    "record_dq_check_batch",
    "record_layer_rows",
    "record_pipeline_run",
    "record_schema_drift",
    "record_task_duration",
    "shutdown",
]
