"""Bridge between PipelineArchitectAgent and the rest of the platform.

Three responsibilities:

  1. ``propose_pipeline()``     — looks up catalog rows + peer instances,
     invokes the agent, returns a fully-resolved proposal dict.
  2. ``persist_proposal()``     — writes DRAFT row to
     CONTROL.client_pipeline_instances + audit log entry.
  3. ``approve_and_deploy()``   — terminal step. Emits 5 artifacts:
       a. Gold DDL file        — datalink/pipeline/gold/ddl/<client>_<dataset>.sql
       b. Silver dbt model     — dbt/models/silver/<client>/<dataset>_clean.sql
       c. Gold dbt model       — dbt/models/gold/<client>/<dataset>.sql
       d. Airflow DAG          — dags/<client>_<dataset>_pipeline.py
       e. GX expectation suite — CONTROL.dq_suites row (LIVE)
     Then flips status to LIVE.

The agent layer never writes to Snowflake or files directly. Persistence
happens here, AFTER the agent has produced a proposal. PHI boundary
remains clean.
"""

from __future__ import annotations

import json
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from datalink.adapters.protocols import LlmProvider, Warehouse
from datalink.agents.pipeline_architect.agent import (
    PipelineArchitectAgent,
)
from datalink.logging import get_logger
from datalink.quality.control import CONTROL_SCHEMA

_log = get_logger(__name__)


# =============================================================================
# Read helpers — UI dropdowns + agent context
# =============================================================================


def list_dataset_codes(warehouse: Warehouse) -> list[dict[str, Any]]:
    """Return [{dataset_code, display_name, category, used_by, total_fields, ...}]
    for every active dataset in the global catalog."""
    rows = warehouse.query(
        f"""
        SELECT dataset_id, dataset_code, display_name, category,
               default_frequency, used_by, total_fields, required_fields,
               optional_fields, catalog_version, is_active
          FROM {CONTROL_SCHEMA}.global_bronze_catalog_datasets
         WHERE is_active = TRUE
         ORDER BY display_name
        """
    )
    return list(rows)


def list_client_instances(
    warehouse: Warehouse,
    *,
    client_id: str | None = None,
    dataset_code: str | None = None,
    status: str | None = None,
) -> list[dict[str, Any]]:
    """Return rows from CONTROL.client_pipeline_instances optionally
    filtered by client / dataset / status."""
    where_clauses: list[str] = []
    params: dict[str, Any] = {}
    if client_id:
        where_clauses.append("client_id = $cid")
        params["cid"] = client_id
    if dataset_code:
        where_clauses.append("dataset_code = $ds")
        params["ds"] = dataset_code
    if status:
        where_clauses.append("status = $st")
        params["st"] = status
    where = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""
    rows = warehouse.query(
        f"""
        SELECT instance_id, client_id, dataset_code, template_id,
               cloned_from_instance, status, bronze_anchor,
               bronze_schema, bronze_table, silver_schema, silver_table,
               gold_schema, gold_table, schedule_cron,
               contract_id, gx_suite_id, dag_uri, dbt_models_uri,
               deviation_count, ai_token_count, ai_latency_ms,
               created_at, created_by, deployed_at, deployed_by, paused_at,
               archived_at, notes
          FROM {CONTROL_SCHEMA}.client_pipeline_instances
        {where}
        ORDER BY created_at DESC
        """,
        params if params else None,
    )
    return list(rows)


def _fetch_catalog_for_dataset(
    warehouse: Warehouse, dataset_code: str
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Get the dataset master row + ordered field rows from the catalog."""
    ds_rows = warehouse.query(
        f"""
        SELECT dataset_id, dataset_code, display_name, category,
               default_frequency, used_by, total_fields, required_fields,
               optional_fields, catalog_version, is_active, notes,
               source_doc_uri
          FROM {CONTROL_SCHEMA}.global_bronze_catalog_datasets
         WHERE dataset_code = $ds AND is_active = TRUE
        """,
        {"ds": dataset_code},
    )
    if not ds_rows:
        raise ValueError(
            f"No active dataset with dataset_code={dataset_code!r} in "
            f"CONTROL.global_bronze_catalog_datasets. Did the loader run?"
        )
    dataset = dict(ds_rows[0])
    field_rows = warehouse.query(
        f"""
        SELECT field_id, dataset_id, dataset_code, field_order,
               field_display_name,
               bronze_column_name AS gold_column_name,  -- alias preserved for Phase-15 in-memory contract
               requirement,
               logical_type, description, additional_notes, example,
               is_pii, is_phi, is_business_key, catalog_version
          FROM {CONTROL_SCHEMA}.global_bronze_catalog_fields
         WHERE dataset_code = $ds
         ORDER BY field_order
        """,
        {"ds": dataset_code},
    )
    return dataset, list(field_rows)


def _fetch_overrides(
    warehouse: Warehouse, *, client_id: str, dataset_code: str
) -> list[dict[str, Any]]:
    """Return all stored overrides for (client_id, dataset_code)."""
    rows = warehouse.query(
        f"""
        SELECT override_id, instance_id, client_id, dataset_code,
               gold_column_name, override_kind, original_value,
               override_value, rationale, created_by, created_at
          FROM {CONTROL_SCHEMA}.client_field_overrides
         WHERE client_id = $cid AND dataset_code = $ds
         ORDER BY created_at
        """,
        {"cid": client_id, "ds": dataset_code},
    )
    return list(rows)


def _fetch_default_routing(warehouse: Warehouse, dataset_code: str) -> list[dict[str, Any]]:
    rows = warehouse.query(
        f"""
        SELECT rule_id, dataset_code, downstream_product, target_system,
               target_uri, is_default, is_active, notes
          FROM {CONTROL_SCHEMA}.onprem_routing_rules
         WHERE dataset_code = $ds AND is_active = TRUE
         ORDER BY downstream_product
        """,
        {"ds": dataset_code},
    )
    return list(rows)


def _fetch_routing_overrides(
    warehouse: Warehouse, *, client_id: str, dataset_code: str
) -> list[dict[str, Any]]:
    rows = warehouse.query(
        f"""
        SELECT override_id, client_id, dataset_code, downstream_product,
               action, target_system, target_uri, rationale,
               created_at, created_by
          FROM {CONTROL_SCHEMA}.client_routing_overrides
         WHERE client_id = $cid AND dataset_code = $ds
         ORDER BY downstream_product
        """,
        {"cid": client_id, "ds": dataset_code},
    )
    return list(rows)


# =============================================================================
# Public API
# =============================================================================


def propose_pipeline(
    *,
    llm: LlmProvider,
    warehouse: Warehouse,
    client_id: str,
    dataset_code: str,
    bronze_anchor: str,
    decision_mode: str = "AUTO",
    schedule_cron: str | None = None,
    temperature: float = 0.0,
    actor: str = "operator",
) -> dict[str, Any]:
    """End-to-end: look up catalog + peers, run agent, return a proposal.

    Does NOT persist — the UI shows the proposal, lets the operator add
    overrides + edits, then calls ``persist_proposal()``.
    """
    dataset, catalog_fields = _fetch_catalog_for_dataset(warehouse, dataset_code)
    overrides = _fetch_overrides(warehouse, client_id=client_id, dataset_code=dataset_code)
    default_routing = _fetch_default_routing(warehouse, dataset_code)
    client_routing_overrides = _fetch_routing_overrides(
        warehouse, client_id=client_id, dataset_code=dataset_code
    )

    # Peer instances — anyone OTHER than this client with the same dataset.
    all_instances = list_client_instances(warehouse, dataset_code=dataset_code)
    existing_instances = [i for i in all_instances if i.get("client_id") != client_id]

    agent = PipelineArchitectAgent(llm=llm, warehouse=warehouse)
    started = time.time()
    result = agent.run(
        {
            "client_id": client_id,
            "dataset_code": dataset_code,
            "dataset_display_name": dataset.get("display_name") or dataset_code,
            "bronze_anchor": bronze_anchor,
            "catalog_fields": catalog_fields,
            "catalog_dataset": dataset,
            "existing_instances": existing_instances,
            "client_overrides": overrides,
            "default_routing": default_routing,
            "client_routing_overrides": client_routing_overrides,
            "decision_mode": decision_mode,
            "schedule_cron": schedule_cron,
            "temperature": temperature,
        }
    )
    duration_ms = int((time.time() - started) * 1000)

    if not result.success:
        raise RuntimeError(f"PipelineArchitectAgent failed: {result.error or '(no detail)'}")

    proposal = dict(result.payload)
    proposal["tokens_used"] = int(result.tokens_used)
    proposal["duration_ms"] = duration_ms
    return proposal


def persist_proposal(
    *,
    warehouse: Warehouse,
    proposal: dict[str, Any],
    actor: str,
    notes: str = "",
) -> str:
    """Insert a DRAFT row in client_pipeline_instances + audit log.

    Returns the new instance_id. The UI uses this to track + later
    approve_and_deploy().
    """
    instance_id = str(uuid.uuid4())
    client_id = str(proposal["client_id"])
    dataset_code = str(proposal["dataset_code"])
    decision = str(proposal.get("decision_mode") or "BUILD")

    overrides_json = json.dumps(proposal.get("deviations", []), default=str)
    deviation_count = int(proposal.get("override_count", 0))
    ai_proposal_json = json.dumps(
        {
            "executive_summary": proposal.get("executive_summary"),
            "clone_recommendation": proposal.get("clone_recommendation"),
            "override_review": proposal.get("override_review"),
            "rationale": proposal.get("rationale"),
            "decision_mode": decision,
            "cloned_from_instance_id": proposal.get("cloned_from_instance_id"),
            "routing_plan": proposal.get("routing_plan", []),
            "resolved_field_count": proposal.get("resolved_field_count", 0),
        },
        default=str,
    )

    warehouse.execute(
        f"""
        INSERT INTO {CONTROL_SCHEMA}.client_pipeline_instances
          (instance_id, client_id, dataset_code, template_id, cloned_from_instance,
           status, bronze_anchor, bronze_schema, bronze_table,
           silver_schema, silver_table, gold_schema, gold_table,
           schedule_cron, contract_id, gx_suite_id, dag_uri, dbt_models_uri,
           overrides_json, deviation_count, ai_proposal_json, ai_reasoning,
           ai_token_count, ai_latency_ms, created_at, created_by, notes)
        VALUES ($id, $cid, $ds, NULL, $clone,
                'PENDING_REVIEW', $anchor, $bsch, $btab,
                $ssch, $stab, $gsch, $gtab,
                $cron, NULL, NULL, NULL, NULL,
                $oj, $dc, $aj, $ar,
                $tok, $lat, $ts, $by, $notes)
        """,
        {
            "id": instance_id,
            "cid": client_id,
            "ds": dataset_code,
            "clone": proposal.get("cloned_from_instance_id"),
            "anchor": str(proposal.get("bronze_anchor", "FLAT_FILE")),
            "bsch": str(proposal["bronze_schema"]),
            "btab": str(proposal["bronze_table"]),
            "ssch": str(proposal["silver_schema"]),
            "stab": str(proposal["silver_table"]),
            "gsch": str(proposal["gold_schema"]),
            "gtab": str(proposal["gold_table"]),
            "cron": str(proposal.get("schedule_cron") or "0 4 * * *"),
            "oj": overrides_json,
            "dc": deviation_count,
            "aj": ai_proposal_json,
            "ar": str(proposal.get("rationale") or "")[:4000],
            "tok": int(proposal.get("tokens_used", 0)),
            "lat": int(proposal.get("duration_ms", 0)),
            "ts": datetime.now(UTC),
            "by": actor,
            "notes": notes,
        },
    )

    _audit_log(
        warehouse=warehouse,
        instance_id=instance_id,
        client_id=client_id,
        dataset_code=dataset_code,
        action="PROPOSED" if decision == "BUILD" else "CLONED",
        actor=actor,
        from_status=None,
        to_status="PENDING_REVIEW",
        diff_summary={
            "decision": decision,
            "cloned_from": proposal.get("cloned_from_instance_id"),
            "deviation_count": deviation_count,
            "resolved_field_count": proposal.get("resolved_field_count", 0),
        },
        ai_reasoning=str(proposal.get("rationale") or "")[:4000],
        token_count=int(proposal.get("tokens_used", 0)),
        latency_ms=int(proposal.get("duration_ms", 0)),
        notes=notes,
    )

    return instance_id


def approve_and_deploy(
    *,
    warehouse: Warehouse,
    instance_id: str,
    proposal: dict[str, Any],
    actor: str,
    notes: str = "",
    repo_root: Path | None = None,
) -> dict[str, Any]:
    """Approve a proposal and emit all 5 artifacts.

    Idempotent: re-deploying the same instance overwrites artifacts in
    place + bumps audit log.
    """
    rows = warehouse.query(
        f"SELECT * FROM {CONTROL_SCHEMA}.client_pipeline_instances WHERE instance_id = $id",
        {"id": instance_id},
    )
    if not rows:
        raise ValueError(f"Instance {instance_id} not found.")
    cur = rows[0]
    if cur.get("status") in ("ARCHIVED",):
        raise ValueError(f"Instance {instance_id} is ARCHIVED — cannot deploy.")

    repo_root = repo_root or _detect_repo_root()
    client_id = str(cur.get("client_id"))
    dataset_code = str(cur.get("dataset_code"))

    artifact_paths: dict[str, str] = {}

    # --- 1. Gold DDL committed file -----------------------------------------
    gold_ddl_path = (
        repo_root
        / "datalink"
        / "pipeline"
        / "gold"
        / "ddl"
        / f"{client_id.lower()}_{dataset_code}.sql"
    )
    gold_ddl_path.parent.mkdir(parents=True, exist_ok=True)
    gold_ddl_path.write_text(
        _format_artifact_header(
            kind="Gold DDL",
            client_id=client_id,
            dataset_code=dataset_code,
            instance_id=instance_id,
        )
        + str(proposal.get("gold_ddl") or ""),
        encoding="utf-8",
    )
    _chown_to_host_user(gold_ddl_path)
    artifact_paths["gold_ddl"] = str(gold_ddl_path.relative_to(repo_root))

    # --- 2. Silver dbt model ------------------------------------------------
    silver_dbt_path = (
        repo_root / "dbt" / "models" / "silver" / client_id.lower() / f"{dataset_code}_clean.sql"
    )
    silver_dbt_path.parent.mkdir(parents=True, exist_ok=True)
    silver_dbt_path.write_text(
        str(proposal.get("silver_dbt_sql") or ""),
        encoding="utf-8",
    )
    _chown_to_host_user(silver_dbt_path)
    artifact_paths["silver_dbt"] = str(silver_dbt_path.relative_to(repo_root))

    # --- 3. Gold dbt model --------------------------------------------------
    gold_dbt_path = (
        repo_root / "dbt" / "models" / "gold" / client_id.lower() / f"{dataset_code}.sql"
    )
    gold_dbt_path.parent.mkdir(parents=True, exist_ok=True)
    gold_dbt_path.write_text(
        str(proposal.get("gold_dbt_sql") or ""),
        encoding="utf-8",
    )
    _chown_to_host_user(gold_dbt_path)
    artifact_paths["gold_dbt"] = str(gold_dbt_path.relative_to(repo_root))

    # --- 4. Airflow DAG -----------------------------------------------------
    dag_filename = f"{client_id.lower()}_{dataset_code}_pipeline.py"
    dag_path = repo_root / "dags" / dag_filename
    dag_path.parent.mkdir(parents=True, exist_ok=True)
    dag_path.write_text(
        str(proposal.get("airflow_dag_py") or ""),
        encoding="utf-8",
    )
    _chown_to_host_user(dag_path)
    artifact_paths["airflow_dag"] = str(dag_path.relative_to(repo_root))

    # --- 5. GX expectation suite (CONTROL.dq_suites row) --------------------
    gx_suite_obj = proposal.get("gx_suite") or {}
    gx_suite_id = _register_gx_suite(
        warehouse=warehouse,
        client_id=client_id,
        dataset_code=dataset_code,
        gx_suite_obj=gx_suite_obj,
        actor=actor,
    )

    # --- Update instance row with artifact pointers + status -----------------
    warehouse.execute(
        f"""
        UPDATE {CONTROL_SCHEMA}.client_pipeline_instances
           SET status = 'LIVE',
               approved_at = $ts,
               approved_by = $actor,
               deployed_at = $ts,
               deployed_by = $actor,
               gx_suite_id = $sid,
               dag_uri = $dag,
               dbt_models_uri = $dbt,
               notes = COALESCE(notes, '') || $notes
         WHERE instance_id = $id
        """,
        {
            "id": instance_id,
            "ts": datetime.now(UTC),
            "actor": actor,
            "sid": gx_suite_id,
            "dag": artifact_paths["airflow_dag"],
            "dbt": str(silver_dbt_path.parent.relative_to(repo_root)),
            "notes": f"\n[deployed] {notes}" if notes else "",
        },
    )

    _audit_log(
        warehouse=warehouse,
        instance_id=instance_id,
        client_id=client_id,
        dataset_code=dataset_code,
        action="DEPLOYED",
        actor=actor,
        from_status=str(cur.get("status", "")),
        to_status="LIVE",
        diff_summary={"artifacts": artifact_paths, "gx_suite_id": gx_suite_id},
        ai_reasoning=None,
        token_count=None,
        latency_ms=None,
        notes=notes,
    )

    return {
        "instance_id": instance_id,
        "status": "LIVE",
        "deployed_by": actor,
        "client_id": client_id,
        "dataset_code": dataset_code,
        "gx_suite_id": gx_suite_id,
        "artifact_paths": artifact_paths,
    }


# =============================================================================
# Internals
# =============================================================================


def _detect_repo_root() -> Path:
    """Walk up from this file until pyproject.toml is found. Fallback to
    /opt/datalink (the in-container path)."""
    here = Path(__file__).resolve()
    for parent in [here, *here.parents]:
        if (parent / "pyproject.toml").exists():
            return parent
    return Path("/opt/datalink")


def _chown_to_host_user(path: Path) -> None:
    """Match the Phase 14 helper — when running as root in the container,
    set ownership to the host user (UID/GID 1000) so git can stage files.
    No-op on permission errors or when not applicable."""
    import contextlib
    import os

    with contextlib.suppress(PermissionError, FileNotFoundError, OSError):
        os.chown(str(path), 1000, 1000)


def _format_artifact_header(
    *, kind: str, client_id: str, dataset_code: str, instance_id: str
) -> str:
    return (
        f"-- AUTO-GENERATED by Pipeline Architect (Phase 15)\n"
        f"-- {kind} for {client_id} / {dataset_code}\n"
        f"-- instance_id : {instance_id}\n"
        f"-- generated   : {datetime.now(UTC).isoformat()}\n"
        f"-- DO NOT EDIT BY HAND. Re-deploy from the UI to regenerate.\n"
        f"\n"
    )


def _register_gx_suite(
    *,
    warehouse: Warehouse,
    client_id: str,
    dataset_code: str,
    gx_suite_obj: dict[str, Any],
    actor: str,
) -> str:
    """Insert a fresh row in CONTROL.dq_suites carrying the auto-anchored
    expectation suite. Bumps version on each re-deploy."""
    suite_id = str(uuid.uuid4())
    suite_name = gx_suite_obj.get("suite_name") or f"gold_{client_id.lower()}_{dataset_code}"
    expectations = gx_suite_obj.get("expectations") or []
    dimensions = gx_suite_obj.get("dq_dimensions") or []

    # Look up next version
    rows = warehouse.query(
        f"SELECT MAX(version) AS v FROM {CONTROL_SCHEMA}.dq_suites "
        f"WHERE client_id = $c AND suite_name = $n",
        {"c": client_id, "n": suite_name},
    )
    next_version = 1
    if rows and rows[0].get("v") is not None:
        next_version = int(rows[0]["v"]) + 1

    # Mark prior LIVE row as ARCHIVED
    warehouse.execute(
        f"UPDATE {CONTROL_SCHEMA}.dq_suites SET status = 'ARCHIVED', archived_at = $ts "
        f"WHERE client_id = $c AND suite_name = $n AND status = 'LIVE'",
        {"c": client_id, "n": suite_name, "ts": datetime.now(UTC)},
    )

    warehouse.execute(
        f"""
        INSERT INTO {CONTROL_SCHEMA}.dq_suites
          (suite_id, client_id, suite_name, version, status, expectations,
           dq_dimensions, source_type, source, created_by, created_at,
           activated_at, schema_fingerprint)
        VALUES ($sid, $c, $n, $v, 'LIVE', $exp,
                $dim, $stp, 'PIPELINE_ARCHITECT', $actor, $ts,
                $ts, NULL)
        """,
        {
            "sid": suite_id,
            "c": client_id,
            "n": suite_name,
            "v": next_version,
            "exp": json.dumps(expectations),
            "dim": json.dumps(dimensions),
            "stp": dataset_code.upper(),
            "actor": actor,
            "ts": datetime.now(UTC),
        },
    )
    return suite_id


def _audit_log(
    *,
    warehouse: Warehouse,
    instance_id: str,
    client_id: str,
    dataset_code: str,
    action: str,
    actor: str,
    from_status: str | None,
    to_status: str | None,
    diff_summary: dict[str, Any] | None,
    ai_reasoning: str | None,
    token_count: int | None,
    latency_ms: int | None,
    notes: str,
) -> None:
    warehouse.execute(
        f"""
        INSERT INTO {CONTROL_SCHEMA}.pipeline_instance_audit_log
          (audit_id, instance_id, client_id, dataset_code, action, actor,
           from_status, to_status, diff_summary, ai_reasoning,
           token_count, latency_ms, ts, notes)
        VALUES ($aid, $iid, $cid, $ds, $act, $actor,
                $fs, $tos, $diff, $ar,
                $tok, $lat, $ts, $notes)
        """,
        {
            "aid": str(uuid.uuid4()),
            "iid": instance_id,
            "cid": client_id,
            "ds": dataset_code,
            "act": action,
            "actor": actor,
            "fs": from_status,
            "tos": to_status,
            "diff": json.dumps(diff_summary, default=str) if diff_summary else None,
            "ar": (ai_reasoning or "")[:4000] or None,
            "tok": token_count,
            "lat": latency_ms,
            "ts": datetime.now(UTC),
            "notes": notes,
        },
    )
