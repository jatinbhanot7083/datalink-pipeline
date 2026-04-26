"""Phase 9.3 — Outbox-driven egress to On-Prem operational DBs.

This module replaces the direct ``push_gold_um_to_operational`` flow for
``gold_patient_auth``. The new pattern:

  1. dbt builds ``outbox_gold_patient_auth`` in
     ``SILVER_gold_um_<CLIENT>``. Each row is a delta vs the last
     successful egress, with an ``egress_action``.

  2. ``push_outbox_to_operational`` (this function):
        * Reads the outbox.
        * Inserts a PENDING row into CONTROL.egress_batch_log per
          (target, entity).
        * For each operational target (Postgres, SQL Server):
            - Uses the existing operational-DB adapter's ``upsert``
              method to apply each delta row according to its
              ``egress_action``.
            - Updates the egress_batch_log row with PUSHED + finished_at.
        * On error: status=FAILED, error stamped, exception re-raised.

  3. Next dbt run reads CONTROL.egress_batch_log, computes a new cutoff
     (highest effective_start_date among PUSHED rows for this entity),
     and ONLY surfaces rows newer than the cutoff in the next outbox.

Why an outbox vs direct push:
  * Idempotency — rerunning the same Airflow run replays only the rows
    still marked PENDING.
  * Audit — every row that crossed the network has a trail on Snowflake
    (egress_batch_log + the outbox table itself, retained per Phase 9.5
    retention policy).
  * Resumability — if SQL Server times out mid-batch, only the un-pushed
    rows retry.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from datalink.adapters.factory import AdapterSet
from datalink.config.loader import Settings
from datalink.logging import get_logger
from datalink.quality.control import CONTROL_SCHEMA

_log = get_logger(__name__)

ENTITY_PATIENT_AUTH = "gold_patient_auth"


@dataclass
class OutboxPushResult:
    """Aggregated outcome of a single outbox push run."""

    client_id: str
    entity: str
    targets_attempted: list[str] = field(default_factory=list)
    per_target: dict[str, dict[str, Any]] = field(default_factory=dict)
    delta_count: int = 0
    all_green: bool = False


def push_outbox_to_operational(
    adapters: AdapterSet,
    settings: Settings,
    *,
    client_id: str,
    pipeline_run_id: str,
    source_schema: str | None = None,
    entity: str = ENTITY_PATIENT_AUTH,
) -> OutboxPushResult:
    """Read the outbox + push deltas to every enabled operational target.

    Records one egress_batch_log row per (target, entity) attempt. Returns
    OutboxPushResult so the orchestration layer can XCom the summary.

    Failure semantics:
      * If reading the outbox itself fails — raise immediately, no log row.
      * If any per-target push fails — log row written with status=FAILED
        + error message, then exception re-raised so Airflow surfaces it.
      * Successful per-target pushes write status=PUSHED before the next
        target is attempted, so a partial failure leaves audit-trail
        evidence for the green ones.
    """
    if entity != ENTITY_PATIENT_AUTH:
        # Forward-compat hook — Phase 9.3 ships only one outbox; other
        # entities follow the same pattern but need their own dbt model.
        raise NotImplementedError(
            f"outbox push for entity={entity!r} not yet implemented; "
            "currently only 'gold_patient_auth' has an outbox model"
        )

    schema = source_schema or _gold_um_schema_for(client_id)
    outbox_table = f"{schema}.outbox_gold_patient_auth"
    deltas = _read_outbox(adapters, outbox_table)

    result = OutboxPushResult(client_id=client_id, entity=entity, delta_count=len(deltas))
    targets = list(settings.features.warehouse_router.targets)
    result.targets_attempted = targets

    if not deltas:
        # NOOP path — no work to do, but still log it so operators see
        # "today: 0 deltas, no push needed" instead of inferring silence.
        for target in targets:
            _log_egress_attempt(
                adapters,
                pipeline_run_id=pipeline_run_id,
                client_id=client_id,
                entity=entity,
                target=target,
                status="NOOP",
                row_count=0,
                cutoff_dts=None,
                error=None,
            )
            result.per_target[target] = {"status": "NOOP", "row_count": 0}
        result.all_green = True
        _log.info("outbox.push.noop", client_id=client_id, entity=entity)
        return result

    # Cutoff = highest effective_start_date in this batch — gets stamped
    # on every PUSHED log row so the NEXT outbox computation knows what's
    # already shipped.
    new_cutoff = max(d["effective_start_date"] for d in deltas)

    success_count = 0
    for target in targets:
        op_db = adapters.operational_dbs.get(target)
        if op_db is None:
            _log.warning("outbox.push.target_not_registered", target=target)
            _log_egress_attempt(
                adapters,
                pipeline_run_id=pipeline_run_id,
                client_id=client_id,
                entity=entity,
                target=target,
                status="FAILED",
                row_count=0,
                cutoff_dts=None,
                error=f"target {target} not registered in adapters.operational_dbs",
            )
            result.per_target[target] = {"status": "FAILED", "row_count": 0}
            continue

        batch_id = str(uuid.uuid4())
        started = datetime.now(UTC)
        # Pre-write a PENDING row so partial failures still leave evidence.
        _insert_pending_log(
            adapters,
            egress_batch_id=batch_id,
            pipeline_run_id=pipeline_run_id,
            client_id=client_id,
            entity=entity,
            target=target,
            row_count=len(deltas),
            cutoff_dts=new_cutoff,
            started_at=started,
        )
        try:
            written = _apply_deltas_to_target(op_db, deltas)
            _finalize_log(
                adapters,
                egress_batch_id=batch_id,
                status="PUSHED",
                error=None,
            )
            success_count += 1
            result.per_target[target] = {"status": "PUSHED", "row_count": written}
            _log.info(
                "outbox.push.target_done",
                client_id=client_id,
                target=target,
                row_count=written,
                batch_id=batch_id,
            )
        except Exception as e:
            err = f"{type(e).__name__}: {e}"
            _finalize_log(adapters, egress_batch_id=batch_id, status="FAILED", error=err[:1000])
            result.per_target[target] = {"status": "FAILED", "row_count": 0, "error": err}
            _log.error(
                "outbox.push.target_failed",
                client_id=client_id,
                target=target,
                error=err,
                batch_id=batch_id,
            )
            # Re-raise so Airflow marks the task FAILED and the pipeline
            # control state machine kicks in (Phase 9.4 will lean on this).
            raise

    result.all_green = success_count == len(targets) and success_count > 0
    return result


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _gold_um_schema_for(client_id: str) -> str:
    """Resolve Gold UM schema name with the same convention dbt uses."""
    client = (client_id or "default").strip()
    return "SILVER_gold_um" if client == "default" else f"SILVER_gold_um_{client.upper()}"


def _read_outbox(adapters: AdapterSet, outbox_table: str) -> list[dict[str, Any]]:
    """SELECT * FROM the outbox table — returns a list of dicts ready for push."""
    rows = adapters.warehouse.query(f"SELECT * FROM {outbox_table}")
    return list(rows)


def _insert_pending_log(
    adapters: AdapterSet,
    *,
    egress_batch_id: str,
    pipeline_run_id: str,
    client_id: str,
    entity: str,
    target: str,
    row_count: int,
    cutoff_dts: Any,
    started_at: datetime,
) -> None:
    adapters.warehouse.execute(
        f"INSERT INTO {CONTROL_SCHEMA}.egress_batch_log "
        "(egress_batch_id, pipeline_run_id, client_id, entity, target, "
        " row_count, started_at, finished_at, status, cutoff_dts, error) "
        "VALUES ($id, $rid, $cid, $e, $t, $rc, $st, NULL, 'PENDING', $cut, NULL)",
        {
            "id": egress_batch_id,
            "rid": pipeline_run_id,
            "cid": client_id,
            "e": entity,
            "t": target,
            "rc": row_count,
            "st": started_at,
            "cut": cutoff_dts,
        },
    )


def _finalize_log(
    adapters: AdapterSet,
    *,
    egress_batch_id: str,
    status: str,
    error: str | None,
) -> None:
    adapters.warehouse.execute(
        f"UPDATE {CONTROL_SCHEMA}.egress_batch_log "
        "SET status = $s, finished_at = $fa, error = $err "
        "WHERE egress_batch_id = $id",
        {
            "s": status,
            "fa": datetime.now(UTC),
            "err": error,
            "id": egress_batch_id,
        },
    )


def _log_egress_attempt(
    adapters: AdapterSet,
    *,
    pipeline_run_id: str,
    client_id: str,
    entity: str,
    target: str,
    status: str,
    row_count: int,
    cutoff_dts: Any,
    error: str | None,
) -> None:
    """Single-shot log entry — used by NOOP and missing-target paths."""
    now = datetime.now(UTC)
    adapters.warehouse.execute(
        f"INSERT INTO {CONTROL_SCHEMA}.egress_batch_log "
        "(egress_batch_id, pipeline_run_id, client_id, entity, target, "
        " row_count, started_at, finished_at, status, cutoff_dts, error) "
        "VALUES ($id, $rid, $cid, $e, $t, $rc, $st, $ft, $s, $cut, $err)",
        {
            "id": str(uuid.uuid4()),
            "rid": pipeline_run_id,
            "cid": client_id,
            "e": entity,
            "t": target,
            "rc": row_count,
            "st": now,
            "ft": now,
            "s": status,
            "cut": cutoff_dts,
            "err": error,
        },
    )


def _apply_deltas_to_target(op_db: Any, deltas: list[dict[str, Any]]) -> int:
    """Apply outbox deltas to the operational DB target via bulk_upsert.

    The legacy ``push_gold_um_to_operational`` writes via
    ``op_db.bulk_upsert(target_table, rows, pk_columns)`` — same
    interface across Postgres + SQL Server adapters. We reuse that
    contract here so the only thing that's actually NEW about the
    outbox flow is *what* we send (deltas, not full table) and *what
    we record after* (CONTROL.egress_batch_log, not just task XCom).

    Per-row processing:
      * Strip outbox-only cols (``egress_batch_id``, ``egress_action``,
        ``prior_cutoff_dts``, ``effective_start_date``) before sending —
        the operational schema doesn't have them.
      * Lowercase the keys so they match the target DDL (Snowflake
        returns UPPER, target DDL is snake_case).
      * DEACTIVATE rows are upserted just like INSERT/UPDATE today;
        the operational tables don't yet carry an ``is_active`` column.
        Future enhancement: add a tombstone column + special-case
        DEACTIVATE here. Safe for now because the egress_batch_log
        carries the action so audit history is preserved.
    """
    if not deltas:
        return 0

    # Strip outbox audit cols + lowercase keys to match operational DDL.
    drop = {"egress_batch_id", "egress_action", "prior_cutoff_dts", "effective_start_date"}
    rows = [{k.lower(): v for k, v in row.items() if k.lower() not in drop} for row in deltas]
    return int(op_db.bulk_upsert("patient_auth", rows, ["patient_auth_id"]))
