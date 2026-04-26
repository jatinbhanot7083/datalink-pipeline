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

    template_fields = ("pipeline_id", "env", "client_id")

    def __init__(
        self,
        *,
        pipeline_id: str,
        env: str = "local",
        client_id: str | None = None,
        **kwargs: Any,
    ) -> None:
        # Phase 9.4 defaults: poke every 10s, hard-stop after 4 hours.
        # 4h is enough for an operator to be paged + intervene via the
        # Control Tower; longer than that and the DAG should fail loudly
        # so it shows up in the on-call queue rather than polling silently.
        kwargs.setdefault("poke_interval", 10)
        kwargs.setdefault("timeout", 4 * 3600)
        kwargs.setdefault("mode", "poke")
        super().__init__(**kwargs)
        self.pipeline_id = pipeline_id
        self.env = env
        # client_id is optional; templated from `{{ dag_run.conf.client_id }}`
        # by callers. Falls back to 'default' so legacy DAGs that don't pass
        # it through still work — same shape PipelineControl uses.
        self.client_id = client_id or "default"

    def poke(self, context: dict[str, Any]) -> bool:  # pragma: no cover — Airflow runtime
        from airflow.exceptions import AirflowException

        from datalink.adapters.factory import build_adapters
        from datalink.config.loader import load_settings
        from datalink.quality import PipelineControl

        # Phase 9.4: read client_id from the DAG run config when present.
        # This handles operators triggering with `{ "client_id": "aetna" }`
        # — the DAG already templates that into task_callable kwargs;
        # mirroring it here keeps the gate scoped to the right tenant.
        dag_run = context.get("dag_run")
        run_conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
        effective_client = run_conf.get("client_id") or self.client_id

        settings = load_settings(env=self.env)
        adapters = build_adapters(settings)
        control = PipelineControl(adapters.warehouse)
        control.ensure()
        state = control.current(self.pipeline_id, client_id=effective_client)

        if state is None:
            _log.info(
                "sensor.pipeline_control.first_run",
                pipeline_id=self.pipeline_id,
                client_id=effective_client,
            )
            return True

        state_value = state.value
        _log.info(
            "sensor.pipeline_control.poke",
            pipeline_id=self.pipeline_id,
            client_id=effective_client,
            state=state_value,
        )
        if state_value == "ABORTED":
            raise AirflowException(
                f"Pipeline {self.pipeline_id!r} (client={effective_client!r}) is "
                "ABORTED — operator must Force Resume in Control Tower"
            )
        # RUNNING / RESUMING → proceed; PAUSED → keep poking up to 4h.
        return state_value != "PAUSED"
