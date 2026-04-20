"""PipelineControlStateSensor — Airflow sensor that polls CONTROL.pipeline_control_state.

Per GX base doc §3.1: "Airflow `PipelineControlStateSensor` polls state and
halts DAG execution." This is that sensor.

Behavior:
  - state = RUNNING | RESUMING | None (first run)  → `return True` (proceed)
  - state = PAUSED                                  → `return False` (keep poking)
  - state = ABORTED                                 → raise AirflowException (fail task)

The `airflow` import is intentionally deferred inside the class body so that
`datalink.orchestration.airflow_sensors` can be imported on dev boxes WITHOUT
Airflow installed — which matters because the local_sequential runner (the
default) will touch this module during package-level imports via
`datalink.orchestration`. We only pay the airflow import cost if a DAG
actually instantiates the sensor.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from datalink.logging import get_logger

if TYPE_CHECKING:
    from airflow.sensors.base import BaseSensorOperator
else:
    try:
        from airflow.sensors.base import BaseSensorOperator
    except ImportError:  # pragma: no cover — only hit without the orchestration extra
        BaseSensorOperator = object  # type: ignore[misc,assignment]

_log = get_logger(__name__)


class PipelineControlStateSensor(BaseSensorOperator):  # type: ignore[misc]
    """Poll CONTROL.pipeline_control_state for the given pipeline_id.

    Usage in a DAG:

        from datalink.orchestration.airflow_sensors import PipelineControlStateSensor

        gate = PipelineControlStateSensor(
            task_id="pipeline_gate",
            pipeline_id="bronze_ingest",
            env="local",
            poke_interval=10,
            timeout=3600,
        )
        gate >> ingest_task
    """

    template_fields = ("pipeline_id", "env")

    def __init__(
        self,
        *,
        pipeline_id: str,
        env: str = "local",
        **kwargs: Any,
    ) -> None:
        # Safe default: poke every 10 s, fail after 1 h.
        kwargs.setdefault("poke_interval", 10)
        kwargs.setdefault("timeout", 3600)
        kwargs.setdefault("mode", "poke")
        super().__init__(**kwargs)
        self.pipeline_id = pipeline_id
        self.env = env

    def poke(self, context: dict[str, Any]) -> bool:  # pragma: no cover — Airflow runtime
        from airflow.exceptions import AirflowException

        from datalink.adapters.factory import build_adapters
        from datalink.config.loader import load_settings
        from datalink.quality import PipelineControl

        settings = load_settings(env=self.env)
        adapters = build_adapters(settings)
        control = PipelineControl(adapters.warehouse)
        control.ensure()
        state = control.current(self.pipeline_id)

        if state is None:
            _log.info(
                "sensor.pipeline_control.first_run",
                pipeline_id=self.pipeline_id,
            )
            return True

        state_value = state.value
        _log.info(
            "sensor.pipeline_control.poke",
            pipeline_id=self.pipeline_id,
            state=state_value,
        )
        if state_value == "ABORTED":
            raise AirflowException(
                f"Pipeline {self.pipeline_id!r} is ABORTED — manual intervention required"
            )
        # state is RUNNING or RESUMING → proceed; PAUSED → keep poking.
        return state_value != "PAUSED"
