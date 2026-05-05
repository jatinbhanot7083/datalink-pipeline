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
from datalink.agents.pipeline_architect.dv2_silver_builder import (
    GoldColumnSpec,
    MappingSpec,
    build_bronze_ddl_with_overflow,
    build_dv2_silver_models,
)
from datalink.logging import get_logger
from datalink.quality.control import CONTROL_SCHEMA

_log = get_logger(__name__)


# =============================================================================
# Gold-LIVE detection + Greenfield support
# =============================================================================


# =============================================================================
# Phase 16.2 (Wave 2 Item 9) — Cross-client pipeline cloning
# =============================================================================


def list_clonable_peer_pipelines(
    warehouse: Warehouse,
    *,
    dataset_code: str,
    exclude_client: str | None = None,
) -> list[dict[str, Any]]:
    """Return LIVE pipeline instances for ``dataset_code`` belonging to OTHER
    clients — these are clone candidates."""
    where = ["status = 'LIVE'", "dataset_code = $ds"]
    params: dict[str, Any] = {"ds": dataset_code}
    if exclude_client:
        where.append("client_id <> $cid")
        params["cid"] = exclude_client
    rows = warehouse.query(
        f"""
        SELECT instance_id, client_id, dataset_code, bronze_anchor,
               schedule_cron, gx_suite_id, deployed_at, deployed_by,
               cloned_from_instance, deviation_count
          FROM {CONTROL_SCHEMA}.client_pipeline_instances
         WHERE {" AND ".join(where)}
         ORDER BY deployed_at DESC
        """,
        params,
    )
    return list(rows)


def clone_pipeline_to_client(
    *,
    warehouse: Warehouse,
    source_instance_id: str,
    target_client_id: str,
    actor: str,
    notes: str = "",
) -> dict[str, Any]:
    """Clone a LIVE pipeline from one client to another.

    Reads the source instance row, copies its proposal-derived fields, sets
    the target client_id + cloned_from_instance lineage, and creates a new
    DRAFT instance row. Returns the new instance_id (caller still has to
    call approve_and_deploy() to materialize).
    """
    src_rows = list(
        warehouse.query(
            f"SELECT * FROM {CONTROL_SCHEMA}.client_pipeline_instances WHERE instance_id = $sid",
            {"sid": source_instance_id},
        )
    )
    if not src_rows:
        raise ValueError(f"Source instance {source_instance_id} not found")
    src = dict(src_rows[0])

    new_id = str(uuid.uuid4())
    bronze_schema = f"BRONZE_{target_client_id.upper()}"
    silver_schema = f"SILVER_{target_client_id.upper()}"
    gold_schema = f"GOLD_{target_client_id.upper()}"

    warehouse.execute(
        f"""
        INSERT INTO {CONTROL_SCHEMA}.client_pipeline_instances
          (instance_id, client_id, dataset_code, status,
           bronze_anchor, bronze_schema, bronze_table,
           silver_schema, silver_table, gold_schema, gold_table,
           schedule_cron, gx_suite_id, dag_uri,
           cloned_from_instance, deviation_count,
           ai_proposal_json, ai_reasoning, ai_token_count, ai_latency_ms,
           created_at, created_by, notes)
        SELECT $iid, $cid, $ds, 'DRAFT',
               $ba, $bs, $bt, $ss, $st, $gs, $gt,
               $sc, $gxs, $dag,
               $clone_from, 0,
               $ai_p, $ai_r, $ai_tk, $ai_la,
               $ts, $by, $notes
        """,
        {
            "iid": new_id,
            "cid": target_client_id,
            "ds": src["dataset_code"],
            "ba": src["bronze_anchor"],
            "bs": bronze_schema,
            "bt": src["bronze_table"],  # raw_<dataset> stays the same
            "ss": silver_schema,
            "st": src["silver_table"],
            "gs": gold_schema,
            "gt": src["gold_table"],
            "sc": src["schedule_cron"],
            "gxs": src.get("gx_suite_id"),
            "dag": src.get("dag_uri"),
            "clone_from": source_instance_id,
            "ai_p": src.get("ai_proposal_json"),
            "ai_r": src.get("ai_reasoning"),
            "ai_tk": src.get("ai_token_count"),
            "ai_la": src.get("ai_latency_ms"),
            "ts": datetime.now(UTC).replace(tzinfo=None),
            "by": actor,
            "notes": (
                f"[CLONE] from {source_instance_id} "
                f"({src.get('client_id')}/{src['dataset_code']}). {notes}"
            ),
        },
    )
    _log.info(
        "pipeline_architect.cloned",
        new_instance_id=new_id,
        source=source_instance_id,
        target_client=target_client_id,
        dataset=src["dataset_code"],
    )

    # Record in version_history for cross-client lineage
    try:
        from datalink.versioning import store as v_store

        v_store.record_version(
            artifact_type="pipeline_instance",
            artifact_id=new_id,
            artifact_scope_key=f"{target_client_id}:{src['dataset_code']}",
            snapshot={
                "client_id": target_client_id,
                "dataset_code": src["dataset_code"],
                "bronze_anchor": src["bronze_anchor"],
                "schedule_cron": src["schedule_cron"],
                "cloned_from": source_instance_id,
            },
            change_kind="CLONE",
            change_reason=f"Cloned from {src.get('client_id')}/{src['dataset_code']}",
            cloned_from_artifact_id=str(source_instance_id),
            cloned_from_version=None,
            created_by=actor,
            notes=notes or None,
        )
    except Exception as exc:
        _log.warning("clone.version_record_failed", err=str(exc)[:120])

    return {
        "new_instance_id": new_id,
        "target_client_id": target_client_id,
        "dataset_code": src["dataset_code"],
        "source_instance_id": source_instance_id,
        "source_client_id": src.get("client_id"),
    }


# =============================================================================
# Phase 16.1 — Workflow gate: Pipeline Architect REQUIRES LIVE Silver+Gold.
# Architectural fix: Pipeline Architect is a CONSUMER of Data Model Designer's
# LIVE schemas. The Bronze catalog is the source for Data Model Designer, NOT
# for Pipeline Architect. The latter only sees the LIVE designs.
# =============================================================================


class PipelinePrerequisitesError(Exception):
    """Raised when Pipeline Architect can't propose because the upstream
    Silver and/or Gold schemas aren't LIVE in the registry yet."""

    def __init__(self, *, dataset_code: str, missing: list[str], detail: str = "") -> None:
        super().__init__(detail or f"missing for {dataset_code}: {', '.join(missing)}")
        self.dataset_code = dataset_code
        self.missing = missing


def check_pipeline_prerequisites(warehouse: Warehouse, dataset_code: str) -> dict[str, Any]:
    """Return the upstream-readiness state for a dataset.

    Output shape:
        {
          "dataset_code": str,
          "silver_live": bool,
          "gold_live": bool,
          "silver_schema_id": str | None,
          "silver_version": int | None,
          "silver_pattern": str | None,
          "gold_dataset_id": str | None,
          "gold_version": int | None,
          "ready_for_propose": bool,             # True only when BOTH live
          "ready_for_silver_stop": bool,         # True when Silver-LIVE only (Gold-stop allowed)
          "missing": list[str],                  # ['silver'] | ['gold'] | ['silver','gold']
          "next_steps": list[str],               # human-readable guidance strings
        }

    Used by the UI to render the readiness banner and gate the Propose
    button. Used by ``propose_pipeline`` to refuse the LLM call when
    upstream design isn't ready.
    """
    # Local import to avoid module-load cycles (silver_schema_designer
    # transitively imports adapters).
    from datalink.agents.silver_schema_designer.bridge import fetch_live_silver

    silver = fetch_live_silver(warehouse, dataset_code)
    gold = fetch_live_gold_schema(warehouse, dataset_code)

    missing: list[str] = []
    if silver is None:
        missing.append("silver")
    if gold is None:
        missing.append("gold")

    next_steps: list[str] = []
    if "silver" in missing and "gold" in missing:
        next_steps.append(
            f"Open Data Model Designer → pick `{dataset_code}` → switch to "
            f"🥈 Silver layer → AI Construct (or Manual / Import) → Approve to LIVE."
        )
        next_steps.append(
            "Then in Data Model Designer → switch to 🥇 Gold layer → "
            "AI Construct → Approve to LIVE."
        )
    elif "silver" in missing:
        next_steps.append(
            f"Gold is LIVE but Silver isn't. Open Data Model Designer → "
            f"`{dataset_code}` → 🥈 Silver layer → Approve to LIVE. "
            f"Pipeline Architect refuses to propose without Silver."
        )
    elif "gold" in missing:
        next_steps.append(
            f"Silver is LIVE but Gold isn't. Open Data Model Designer → "
            f"`{dataset_code}` → 🥇 Gold layer → Approve to LIVE for full "
            f"Bronze→Silver→Gold. To deploy Silver-only ('Silver-stop'), "
            f"use the explicit option in the form below."
        )

    return {
        "dataset_code": dataset_code,
        "silver_live": silver is not None,
        "gold_live": gold is not None,
        "silver_schema_id": (silver or {}).get("silver_dataset_id"),
        "silver_version": (silver or {}).get("version"),
        "silver_pattern": (silver or {}).get("pattern"),
        "gold_dataset_id": ((gold or {}).get("header") or {}).get("gold_dataset_id"),
        "gold_version": ((gold or {}).get("header") or {}).get("version"),
        "ready_for_propose": (silver is not None and gold is not None),
        "ready_for_silver_stop": (silver is not None),
        "missing": missing,
        "next_steps": next_steps,
        # Hand back the full payloads so callers don't re-fetch.
        "live_silver": silver,
        "live_gold": gold,
    }


def fetch_live_gold_schema(warehouse: Warehouse, dataset_code: str) -> dict[str, Any] | None:
    """Return the LIVE Gold schema for a dataset (or None when no Gold yet).

    Output shape:
        {
          "header": <global_gold_schema_datasets row>,
          "columns": [<global_gold_schema_fields rows, ordered>],
          "mappings": [<bronze_to_gold_mappings rows>],
          "silver_pattern_recommendation": <row | None>,
        }
    """
    headers = list(
        warehouse.query(
            f"SELECT * FROM {CONTROL_SCHEMA}.global_gold_schema_datasets "
            f"WHERE dataset_code = $ds AND status = 'LIVE'",
            {"ds": dataset_code},
        )
    )
    if not headers:
        return None
    header = dict(headers[0])
    cols = list(
        warehouse.query(
            f"SELECT * FROM {CONTROL_SCHEMA}.global_gold_schema_fields "
            f"WHERE gold_dataset_id = $g ORDER BY column_order",
            {"g": header["gold_dataset_id"]},
        )
    )
    mappings = list(
        warehouse.query(
            f"SELECT * FROM {CONTROL_SCHEMA}.bronze_to_gold_mappings WHERE gold_dataset_id = $g",
            {"g": header["gold_dataset_id"]},
        )
    )
    rec_rows = list(
        warehouse.query(
            f"SELECT * FROM {CONTROL_SCHEMA}.silver_pattern_recommendations "
            f"WHERE dataset_code = $ds AND status = 'LIVE'",
            {"ds": dataset_code},
        )
    )
    return {
        "header": header,
        "columns": cols,
        "mappings": mappings,
        "silver_pattern_recommendation": rec_rows[0] if rec_rows else None,
    }


def list_pending_overflow(
    warehouse: Warehouse,
    *,
    client_id: str | None = None,
    dataset_code: str | None = None,
) -> list[dict[str, Any]]:
    """Return rows from bronze_overflow_log with status = PENDING_REVIEW."""
    where: list[str] = ["status = 'PENDING_REVIEW'"]
    params: dict[str, Any] = {}
    if client_id:
        where.append("client_id = $cid")
        params["cid"] = client_id
    if dataset_code:
        where.append("dataset_code = $ds")
        params["ds"] = dataset_code
    rows = warehouse.query(
        f"SELECT * FROM {CONTROL_SCHEMA}.bronze_overflow_log "
        f"WHERE {' AND '.join(where)} ORDER BY first_seen_at DESC",
        params if params else None,
    )
    return list(rows)


def list_greenfield_proposals(
    warehouse: Warehouse, *, status: str | None = None
) -> list[dict[str, Any]]:
    """Return rows from greenfield_dataset_proposals."""
    where = ""
    params: dict[str, Any] = {}
    if status:
        where = "WHERE status = $st"
        params["st"] = status
    return list(
        warehouse.query(
            f"SELECT * FROM {CONTROL_SCHEMA}.greenfield_dataset_proposals "
            f"{where} ORDER BY created_at DESC",
            params if params else None,
        )
    )


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
    allow_silver_stop: bool = False,
) -> dict[str, Any]:
    """End-to-end: enforce upstream prerequisites, run the agent, return a proposal.

    Phase 16.1 architectural fix: Pipeline Architect is a CONSUMER of
    Data Model Designer's LIVE Silver and Gold schemas. The Bronze catalog
    is the source for Data Model Designer, NOT for Pipeline Architect.

    Refuses to propose unless:
      * BOTH Silver-LIVE AND Gold-LIVE exist for ``dataset_code``, OR
      * ``allow_silver_stop=True`` AND Silver-LIVE exists (Gold may be missing —
        emits a Silver-stop pipeline, no Gold materialization).

    Raises :class:`PipelinePrerequisitesError` when prerequisites aren't met.
    The UI catches this and surfaces a clear "Author X first in Data Model
    Designer" guidance with deep-link.

    Does NOT persist — the UI shows the proposal, lets the operator add
    overrides + edits, then calls ``persist_proposal()``.
    """
    # ---- Phase 16.1 prerequisite gate ----
    prereqs = check_pipeline_prerequisites(warehouse, dataset_code)
    if not prereqs["ready_for_propose"]:
        if allow_silver_stop and prereqs["ready_for_silver_stop"]:
            _log.info(
                "pipeline_architect.silver_stop_explicit",
                dataset_code=dataset_code,
                client_id=client_id,
            )
            # Allow it — caller knows what they're doing.
        else:
            raise PipelinePrerequisitesError(
                dataset_code=dataset_code,
                missing=prereqs["missing"],
                detail=(
                    f"Cannot propose pipeline for {dataset_code!r}: "
                    f"upstream Silver/Gold not LIVE. "
                    f"Missing: {prereqs['missing']}. "
                    f"Next steps: {' | '.join(prereqs['next_steps'])}"
                ),
            )

    dataset, catalog_fields = _fetch_catalog_for_dataset(warehouse, dataset_code)
    overrides = _fetch_overrides(warehouse, client_id=client_id, dataset_code=dataset_code)
    default_routing = _fetch_default_routing(warehouse, dataset_code)
    client_routing_overrides = _fetch_routing_overrides(
        warehouse, client_id=client_id, dataset_code=dataset_code
    )

    # Phase 15.7 — pick up LIVE Gold schema (now confirmed present per gate above
    # unless explicit Silver-stop was permitted)
    live_gold = prereqs["live_gold"]
    live_silver = prereqs["live_silver"]

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
            "live_gold": live_gold,
        }
    )
    duration_ms = int((time.time() - started) * 1000)

    if not result.success:
        raise RuntimeError(f"PipelineArchitectAgent failed: {result.error or '(no detail)'}")

    proposal = dict(result.payload)
    proposal["tokens_used"] = int(result.tokens_used)
    proposal["duration_ms"] = duration_ms

    # Phase 15.7 — when Gold LIVE, override the Phase 15 Silver/Gold/Bronze
    # artifacts with Gold-driven DV2 versions. The agent's Phase 15
    # builders still ran (for narrative compat) but the structural truth
    # comes from the Gold registry now.
    if live_gold:
        gold_cols_raw = list(live_gold["columns"])
        mappings_raw = list(live_gold["mappings"])
        rec = live_gold.get("silver_pattern_recommendation") or {}
        silver_pattern = str(rec.get("recommended_pattern") or "HUB_SAT_LINK")
        proposed_shape = {}
        if rec.get("proposed_silver_shape"):
            try:
                proposed_shape = json.loads(rec["proposed_silver_shape"])
            except (TypeError, json.JSONDecodeError):
                proposed_shape = {}

        gold_specs = [
            GoldColumnSpec(
                gold_column_name=str(c["gold_column_name"]),
                logical_type=str(c.get("logical_type") or "TEXT"),
                nullable=bool(c.get("nullable", True)),
                is_business_key=bool(c.get("is_business_key", False)),
                is_pii=bool(c.get("is_pii", False)),
                is_phi=bool(c.get("is_phi", False)),
                description=str(c.get("description") or ""),
            )
            for c in gold_cols_raw
        ]
        mapping_specs: list[MappingSpec] = []
        for m in mappings_raw:
            srcs = m.get("bronze_source_columns")
            if isinstance(srcs, str):
                try:
                    srcs = json.loads(srcs)
                except json.JSONDecodeError:
                    srcs = []
            mapping_specs.append(
                MappingSpec(
                    gold_column_name=str(m["gold_column_name"]),
                    transform_kind=str(m["transform_kind"]),
                    transform_sql=str(m["transform_sql"]),
                    bronze_source_columns=list(srcs or []),
                )
            )
        bronze_col_names = [
            str(f.get("bronze_column_name") or f.get("gold_column_name") or "")
            for f in catalog_fields
        ]
        # DV2 Silver dbt models (multiple files)
        silver_dbt_models = build_dv2_silver_models(
            client_id=client_id,
            dataset_code=dataset_code,
            gold_columns=gold_specs,
            mappings=mapping_specs,
            bronze_columns=bronze_col_names,
            silver_pattern=silver_pattern,
            proposed_silver_shape=proposed_shape,
        )
        # Bronze DDL with _variant_overflow
        bronze_ddl = build_bronze_ddl_with_overflow(
            client_id=client_id,
            dataset_code=dataset_code,
            bronze_fields=catalog_fields,
        )
        proposal["live_gold_dataset_id"] = live_gold["header"]["gold_dataset_id"]
        proposal["live_gold_table_name"] = live_gold["header"]["gold_table_name"]
        proposal["live_gold_anchor"] = live_gold["header"]["gold_anchor"]
        proposal["live_gold_version"] = live_gold["header"].get("version")
        # Phase 16.1 — lineage to upstream Silver design (used for "upstream
        # update available" notifications when Silver version bumps).
        if live_silver is not None:
            proposal["live_silver_dataset_id"] = live_silver.get("silver_dataset_id")
            proposal["live_silver_version"] = live_silver.get("version")
            proposal["live_silver_pattern"] = live_silver.get("pattern")
        proposal["silver_pattern"] = silver_pattern
        proposal["silver_pattern_is_overkill"] = bool(rec.get("is_overkill_flag", False))
        proposal["silver_dbt_models"] = silver_dbt_models  # dict {filename: sql}
        proposal["bronze_ddl_overflow"] = bronze_ddl
        proposal["gold_columns_count"] = len(gold_cols_raw)
        proposal["bronze_to_gold_mapping_count"] = len(mappings_raw)
        proposal["gold_schema_status"] = "LIVE"
        # Replace the legacy Phase 15 silver_dbt_sql (the 1:1 cast) so the
        # UI/preview reflects the new DV2 reality. Keep the legacy gold_ddl
        # as-is until we generate one from the Gold registry too.
        if silver_dbt_models:
            # Use first DV2 file as the headline preview
            proposal["silver_dbt_sql"] = next(iter(silver_dbt_models.values()))
    else:
        proposal["gold_schema_status"] = "MISSING_RECOMMEND_DESIGN"
        proposal["silver_pattern"] = "HUB_SAT_LINK"
        proposal["silver_dbt_models"] = {}

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

    # --- 0. Bronze DDL with _variant_overflow (Phase 15.7) ------------------
    bronze_ddl_text = proposal.get("bronze_ddl_overflow")
    if bronze_ddl_text:
        bronze_ddl_path = (
            repo_root
            / "datalink"
            / "pipeline"
            / "bronze"
            / "ddl"
            / f"{client_id.lower()}_{dataset_code}.sql"
        )
        bronze_ddl_path.parent.mkdir(parents=True, exist_ok=True)
        bronze_ddl_path.write_text(
            _format_artifact_header(
                kind="Bronze DDL (with _variant_overflow)",
                client_id=client_id,
                dataset_code=dataset_code,
                instance_id=instance_id,
            )
            + str(bronze_ddl_text),
            encoding="utf-8",
        )
        _chown_to_host_user(bronze_ddl_path)
        artifact_paths["bronze_ddl"] = str(bronze_ddl_path.relative_to(repo_root))

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

    # --- 2. Silver dbt models ----------------------------------------------
    # Phase 15.7: when Gold-LIVE drives the pipeline, this is a DV2 set
    # (multiple Hub/Sat/Link models). Otherwise it's a single 1:1 cast.
    silver_dir = repo_root / "dbt" / "models" / "silver" / client_id.lower() / dataset_code
    silver_dir.mkdir(parents=True, exist_ok=True)
    silver_dbt_models = proposal.get("silver_dbt_models") or {}
    if silver_dbt_models:
        # Clean prior auto-generated files in this dir to avoid stale Hubs/Sats
        for old in silver_dir.glob("*.sql"):
            with __import__("contextlib").suppress(OSError):
                old.unlink()
        silver_files: list[str] = []
        for filename, sql in silver_dbt_models.items():
            p = silver_dir / filename
            p.write_text(sql, encoding="utf-8")
            _chown_to_host_user(p)
            silver_files.append(str(p.relative_to(repo_root)))
        artifact_paths["silver_dbt"] = ", ".join(silver_files)
        silver_dbt_path = silver_dir / next(iter(silver_dbt_models.keys()))
    else:
        # Phase-15-style single-file fallback (Bronze-as-Gold legacy path)
        silver_dbt_path = silver_dir / f"{dataset_code}_clean.sql"
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

    # --- 4.5 (Phase 16.1, Wave 1 Item 2) — Execute DDLs against Snowflake ---
    # Old behavior: deploy emitted SQL files but never ran them. Operator had
    # to invoke ``scripts/_run_pipeline_ddls.py`` manually from the terminal.
    # New behavior: deploy = emit + execute. Zero terminal commands.
    physical_artifacts: dict[str, str] = {}
    bronze_schema_name = f"BRONZE_{client_id.upper()}"
    silver_schema_name = f"SILVER_{client_id.upper()}"
    gold_schema_name = f"GOLD_{client_id.upper()}"
    for sch in (bronze_schema_name, silver_schema_name, gold_schema_name):
        try:
            warehouse.execute(f"CREATE SCHEMA IF NOT EXISTS {sch}")
            physical_artifacts[f"schema_{sch.lower()}"] = "CREATED"
        except Exception as exc:
            _log.warning("deploy.schema_create_failed", schema=sch, err=str(exc)[:120])
            physical_artifacts[f"schema_{sch.lower()}"] = f"FAILED: {str(exc)[:80]}"

    # Bronze table — only when we have the overflow-aware DDL (Phase 15.7+).
    if bronze_ddl_text:
        try:
            warehouse.execute(str(bronze_ddl_text))
            physical_artifacts["bronze_table"] = (
                f"{bronze_schema_name}.raw_{dataset_code.lower()} CREATED"
            )
        except Exception as exc:
            _log.warning("deploy.bronze_ddl_exec_failed", err=str(exc)[:200])
            physical_artifacts["bronze_table"] = f"FAILED: {str(exc)[:120]}"

    # Gold table.
    gold_ddl_text = proposal.get("gold_ddl") or ""
    if gold_ddl_text:
        try:
            warehouse.execute(str(gold_ddl_text))
            physical_artifacts["gold_table"] = f"{gold_schema_name}.{dataset_code.lower()} CREATED"
        except Exception as exc:
            _log.warning("deploy.gold_ddl_exec_failed", err=str(exc)[:200])
            physical_artifacts["gold_table"] = f"FAILED: {str(exc)[:120]}"

    # Silver tables come from dbt at DAG runtime — skip explicit DDL here.

    # Surface the physical-execution result alongside file artifacts so the
    # UI's deploy success card can show "✅ Bronze table created in Snowflake"
    # rather than just "Bronze DDL written to <path>".
    artifact_paths["physical"] = " · ".join(f"{k}={v}" for k, v in physical_artifacts.items())

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
# Phase 15.7 — Greenfield dataset registration
# =============================================================================


def propose_greenfield_dataset(
    *,
    llm: LlmProvider,
    warehouse: Warehouse,
    client_id: str | None,
    sample_file_name: str,
    headers: list[str],
    sample_rows: list[list[str]],
    proposed_dataset_code: str,
    proposed_display_name: str,
    proposed_category: str | None = None,
    actor: str = "operator",
) -> dict[str, Any]:
    """Profile a brand-new vendor file and propose a Bronze catalog entry.

    Uses the existing Phase 14 ContractArchitect machinery to AI-propose
    a Bronze schema; persists as DRAFT in greenfield_dataset_proposals.
    The operator approves → ``approve_greenfield()`` promotes it into
    global_bronze_catalog_*. Then the normal Gold Schema Designer flow
    can run against it, and Pipeline Architect can build pipelines.

    Returns the new greenfield_id and the AI's proposed column list.
    """
    # Phase 14 ContractArchitect knows how to read a sample header + rows
    # and propose a Bronze contract. We borrow that machinery here.
    from datalink.agents.contract_architect import (
        propose_contract,
    )

    proposal = propose_contract(
        llm=llm,
        warehouse=warehouse,
        memory=None,  # No RAG for greenfield (no anchor selected yet)
        client_id=client_id or "global",
        source_type=proposed_dataset_code.upper(),
        mode="FILE_DRIVEN",
        anchored_standards=[],  # CATALOG_ANCHOR — no industry standard selected
        payload={
            "file_name": sample_file_name,
            "headers": headers,
            "sample_values": sample_rows,
            "detected_format": "CSV",
        },
        temperature=0.0,
        grounding_k=0,
        strictness=0.3,
        actor=actor,
    )

    # Convert the ContractArchitect proposal into greenfield columns
    proposed_cols = []
    for c in proposal.proposed_columns:
        proposed_cols.append(
            {
                "bronze_column_name": c.name,
                "logical_type": _logical_type_from_contract_type(c.type),
                "requirement": "Required" if not c.nullable else "Optional",
                "description": c.rationale,
                "is_pii": False,
                "is_phi": False,
                "is_business_key": "id" in c.name.lower() or "npi" in c.name.lower(),
            }
        )

    greenfield_id = str(uuid.uuid4())
    required_count = sum(1 for c in proposed_cols if c["requirement"] == "Required")
    optional_count = len(proposed_cols) - required_count

    warehouse.execute(
        f"""
        INSERT INTO {CONTROL_SCHEMA}.greenfield_dataset_proposals
          (greenfield_id, proposed_dataset_code, proposed_display_name,
           proposed_category, proposed_default_anchor,
           client_id, source_sample_uri, sample_file_name,
           proposed_bronze_columns, proposed_field_count,
           proposed_required_count, proposed_optional_count,
           ai_token_count, ai_latency_ms, ai_rationale,
           status, created_by, submitted_at)
        VALUES ($id, $code, $name,
                $cat, 'CATALOG_ANCHOR',
                $cid, NULL, $sfn,
                $cols, $fc,
                $rc, $oc,
                $tok, $lat, $rat,
                'PENDING_REVIEW', $by, $ts)
        """,
        {
            "id": greenfield_id,
            "code": proposed_dataset_code,
            "name": proposed_display_name,
            "cat": proposed_category,
            "cid": client_id,
            "sfn": sample_file_name,
            "cols": json.dumps(proposed_cols),
            "fc": len(proposed_cols),
            "rc": required_count,
            "oc": optional_count,
            "tok": int(proposal.tokens_used),
            "lat": int(proposal.duration_ms),
            "rat": (proposal.rationale or "")[:4000],
            "by": actor,
            "ts": datetime.now(UTC),
        },
    )
    return {
        "greenfield_id": greenfield_id,
        "proposed_dataset_code": proposed_dataset_code,
        "proposed_display_name": proposed_display_name,
        "proposed_columns": proposed_cols,
        "field_count": len(proposed_cols),
        "required_count": required_count,
        "tokens_used": int(proposal.tokens_used),
        "duration_ms": int(proposal.duration_ms),
    }


def approve_greenfield(
    *,
    warehouse: Warehouse,
    greenfield_id: str,
    actor: str,
    notes: str = "",
) -> dict[str, Any]:
    """Promote a PENDING_REVIEW greenfield proposal into the global Bronze
    catalog tables. After this returns, Gold Schema Designer can be opened
    against the new dataset_code."""
    rows = list(
        warehouse.query(
            f"SELECT * FROM {CONTROL_SCHEMA}.greenfield_dataset_proposals WHERE greenfield_id = $g",
            {"g": greenfield_id},
        )
    )
    if not rows:
        raise ValueError(f"Greenfield proposal {greenfield_id} not found.")
    cur = rows[0]
    if cur["status"] != "PENDING_REVIEW":
        raise ValueError(f"Greenfield proposal status is {cur['status']} — must be PENDING_REVIEW.")

    dataset_code = str(cur["proposed_dataset_code"])
    display_name = str(cur["proposed_display_name"])
    category = cur.get("proposed_category")
    default_anchor = str(cur.get("proposed_default_anchor") or "CATALOG_ANCHOR")

    # Check for collision with existing Bronze catalog
    existing = list(
        warehouse.query(
            f"SELECT dataset_id FROM {CONTROL_SCHEMA}.global_bronze_catalog_datasets "
            f"WHERE dataset_code = $ds",
            {"ds": dataset_code},
        )
    )
    if existing:
        raise ValueError(
            f"Dataset code {dataset_code!r} already exists in the Bronze catalog. "
            f"Reject or rename the greenfield proposal."
        )

    # Insert into global_bronze_catalog_datasets
    dataset_id = str(uuid.uuid4())
    warehouse.execute(
        f"""
        INSERT INTO {CONTROL_SCHEMA}.global_bronze_catalog_datasets
          (dataset_id, dataset_code, display_name, category, default_anchor,
           total_fields, required_fields, optional_fields,
           is_active, catalog_version, source_doc_uri, registered_by, notes)
        VALUES ($id, $code, $name, $cat, $anchor,
                $tf, $rf, $of,
                TRUE, 1, $src, $by, 'Greenfield-promoted from sample file')
        """,
        {
            "id": dataset_id,
            "code": dataset_code,
            "name": display_name,
            "cat": category,
            "anchor": default_anchor,
            "tf": int(cur.get("proposed_field_count") or 0),
            "rf": int(cur.get("proposed_required_count") or 0),
            "of": int(cur.get("proposed_optional_count") or 0),
            "src": cur.get("sample_file_name") or "greenfield",
            "by": actor,
        },
    )

    # Insert each proposed column into global_bronze_catalog_fields
    cols = json.loads(cur["proposed_bronze_columns"])
    for i, c in enumerate(cols, start=1):
        warehouse.execute(
            f"""
            INSERT INTO {CONTROL_SCHEMA}.global_bronze_catalog_fields
              (field_id, dataset_id, dataset_code, field_order,
               field_display_name, bronze_column_name, requirement,
               logical_type, description, is_pii, is_phi, is_business_key,
               catalog_version)
            VALUES ($id, $dsid, $ds, $ord,
                    $name, $bcn, $req,
                    $type, $desc, $pii, $phi, $bk, 1)
            """,
            {
                "id": str(uuid.uuid4()),
                "dsid": dataset_id,
                "ds": dataset_code,
                "ord": i,
                "name": c["bronze_column_name"],
                "bcn": c["bronze_column_name"],
                "req": c["requirement"],
                "type": c["logical_type"],
                "desc": c.get("description") or "",
                "pii": bool(c.get("is_pii", False)),
                "phi": bool(c.get("is_phi", False)),
                "bk": bool(c.get("is_business_key", False)),
            },
        )

    # Flip greenfield proposal to APPROVED_PROMOTED
    warehouse.execute(
        f"""
        UPDATE {CONTROL_SCHEMA}.greenfield_dataset_proposals
           SET status = 'APPROVED_PROMOTED',
               promoted_dataset_id = $did,
               approved_by = $by,
               approved_at = $ts,
               notes = COALESCE(notes, '') || $notes
         WHERE greenfield_id = $g
        """,
        {
            "did": dataset_id,
            "by": actor,
            "ts": datetime.now(UTC),
            "g": greenfield_id,
            "notes": f"\n[approved] {notes}" if notes else "",
        },
    )

    return {
        "greenfield_id": greenfield_id,
        "dataset_code": dataset_code,
        "dataset_id": dataset_id,
        "field_count": len(cols),
        "status": "APPROVED_PROMOTED",
    }


def _logical_type_from_contract_type(contract_type: str) -> str:
    """Convert a Phase 14 ContractArchitect SQL type to a logical type."""
    t = contract_type.split("(", 1)[0].strip().upper()
    if t in {"INTEGER", "BIGINT", "SMALLINT", "TINYINT"}:
        return "INTEGER"
    if t in {"DECIMAL", "NUMERIC", "FLOAT", "DOUBLE", "REAL", "NUMBER"}:
        return "DECIMAL"
    if t == "DATE":
        return "DATE"
    if t in {"TIMESTAMP", "DATETIME"}:
        return "TIMESTAMP"
    if t == "BOOLEAN":
        return "BOOLEAN"
    return "TEXT"


# =============================================================================
# Phase 15.7 — Overflow column logging (called by Bronze ingest task)
# =============================================================================


def log_overflow_column(
    *,
    warehouse: Warehouse,
    client_id: str,
    dataset_code: str,
    column_name: str,
    batch_id: str,
    source_file: str | None = None,
    sample_values: list[Any] | None = None,
    inferred_type: str = "TEXT",
) -> None:
    """Record an unexpected column observed during Bronze ingestion.

    Idempotent on (client_id, dataset_code, column_name): if a row already
    exists with status != EXCLUDED_PERMANENTLY, increment occurrence_count
    and update last_seen_at. Otherwise INSERT a new PENDING_REVIEW row.
    """
    existing = list(
        warehouse.query(
            f"SELECT overflow_id, status, occurrence_count "
            f"FROM {CONTROL_SCHEMA}.bronze_overflow_log "
            f"WHERE client_id = $c AND dataset_code = $d AND column_name = $col",
            {"c": client_id, "d": dataset_code, "col": column_name},
        )
    )
    if existing and existing[0]["status"] != "EXCLUDED_PERMANENTLY":
        warehouse.execute(
            f"UPDATE {CONTROL_SCHEMA}.bronze_overflow_log "
            f"SET occurrence_count = occurrence_count + 1, last_seen_at = $ts "
            f"WHERE overflow_id = $oid",
            {"ts": datetime.now(UTC), "oid": existing[0]["overflow_id"]},
        )
        return

    warehouse.execute(
        f"""
        INSERT INTO {CONTROL_SCHEMA}.bronze_overflow_log
          (overflow_id, client_id, dataset_code, column_name,
           first_seen_batch_id, first_seen_source_file,
           first_seen_at, last_seen_at, occurrence_count,
           sample_values, inferred_logical_type, status)
        VALUES ($id, $c, $d, $col,
                $bid, $src,
                $ts, $ts, 1,
                $sv, $type, 'PENDING_REVIEW')
        """,
        {
            "id": str(uuid.uuid4()),
            "c": client_id,
            "d": dataset_code,
            "col": column_name,
            "bid": batch_id,
            "src": source_file,
            "ts": datetime.now(UTC),
            "sv": json.dumps([str(v) for v in (sample_values or [])][:5]),
            "type": inferred_type,
        },
    )


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
