"""DQ AI Architect bridge — Phase 19.1.

Three responsibilities (mirrors pipeline_architect.bridge shape):

  1. ``propose_suite()``    — gather schema + standards RAG + run agent;
                              return a fully-resolved proposal dict.
  2. ``persist_suite()``    — write a DRAFT row to CONTROL.dq_suites
                              (status='DRAFT', suite_name=<dataset>_<layer>).
  3. ``approve_and_promote()`` — flip DRAFT → LIVE.  Archives any prior
                              LIVE suite for the same (dataset, layer)
                              tuple so there's exactly ONE LIVE per pair.
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from typing import Any

from datalink.adapters.protocols import LlmProvider, Warehouse
from datalink.agents.dq_ai_architect.agent import DqSuiteArchitectAgent
from datalink.logging import get_logger
from datalink.quality.control import CONTROL_SCHEMA

_log = get_logger(__name__)


def _fetch_schema_for_layer(wh: Warehouse, *, dataset_code: str, layer: str) -> dict[str, Any]:
    """Pull the canonical column shape for (dataset_code, layer) from the
    appropriate Phase-17 metadata table:

      BRONZE  → CONTROL.global_bronze_catalog_fields
      SILVER  → CONTROL.global_silver_schema_columns (latest LIVE GLOBAL_CORP)
      GOLD    → CONTROL.global_gold_schema_fields    (latest LIVE GLOBAL_CORP)
    """
    layer = layer.upper()
    if layer == "BRONZE":
        rows = wh.query(
            f"""
            SELECT bronze_column_name AS name,
                   logical_type, requirement, is_pii, is_phi, is_business_key,
                   description
              FROM {CONTROL_SCHEMA}.global_bronze_catalog_fields
             WHERE LOWER(dataset_code) = LOWER($ds)
             ORDER BY field_order
            """,
            {"ds": dataset_code},
        )
        cols = []
        for r in rows:
            cols.append(
                {
                    "name": r.get("name") or r.get("bronze_column_name"),
                    "logical_type": r.get("logical_type"),
                    "nullable": str(r.get("requirement") or "").upper() != "REQUIRED",
                    "is_business_key": bool(r.get("is_business_key")),
                    "is_pii": bool(r.get("is_pii")),
                    "is_phi": bool(r.get("is_phi")),
                    "description": r.get("description") or "",
                }
            )
        return {"columns": cols, "source_table": "global_bronze_catalog_fields"}

    if layer == "SILVER":
        rows = wh.query(
            f"""
            SELECT c.column_name AS name, c.logical_type, c.nullable,
                   c.is_business_key, c.is_pii, c.is_phi, c.description
              FROM {CONTROL_SCHEMA}.global_silver_schema_columns c
              JOIN {CONTROL_SCHEMA}.global_silver_schema_datasets d
                ON d.silver_dataset_id = c.silver_dataset_id
             WHERE LOWER(d.dataset_code) = LOWER($ds)
               AND d.scope_owner = 'GLOBAL_CORP'
               AND d.status = 'LIVE'
             ORDER BY c.column_order
            """,
            {"ds": dataset_code},
        )
        cols = [
            {
                "name": r.get("name") or r.get("column_name"),
                "logical_type": r.get("logical_type"),
                "nullable": bool(r.get("nullable")),
                "is_business_key": bool(r.get("is_business_key")),
                "is_pii": bool(r.get("is_pii")),
                "is_phi": bool(r.get("is_phi")),
                "description": r.get("description") or "",
            }
            for r in rows
        ]
        return {"columns": cols, "source_table": "global_silver_schema_columns"}

    if layer == "GOLD":
        rows = wh.query(
            f"""
            SELECT f.gold_column_name AS name, f.logical_type, f.nullable,
                   f.is_business_key, f.is_pii, f.is_phi, f.description
              FROM {CONTROL_SCHEMA}.global_gold_schema_fields f
              JOIN {CONTROL_SCHEMA}.global_gold_schema_datasets d
                ON d.gold_dataset_id = f.gold_dataset_id
             WHERE LOWER(d.dataset_code) = LOWER($ds)
               AND d.scope_owner = 'GLOBAL_CORP'
               AND d.status = 'LIVE'
             ORDER BY f.column_order
            """,
            {"ds": dataset_code},
        )
        cols = [
            {
                "name": r.get("name") or r.get("gold_column_name"),
                "logical_type": r.get("logical_type"),
                "nullable": bool(r.get("nullable")),
                "is_business_key": bool(r.get("is_business_key")),
                "is_pii": bool(r.get("is_pii")),
                "is_phi": bool(r.get("is_phi")),
                "description": r.get("description") or "",
            }
            for r in rows
        ]
        return {"columns": cols, "source_table": "global_gold_schema_fields"}

    raise ValueError(f"Unknown layer {layer!r} (must be BRONZE / SILVER / GOLD)")


def _fetch_standards_rag(
    wh: Warehouse, *, dataset_code: str, layer: str, k: int = 5
) -> list[dict[str, Any]]:
    """Best-effort RAG hits from CONTROL.standard_registry.  No-op if the
    table doesn't exist or has no relevant standards."""
    try:
        rows = wh.query(
            f"""
            SELECT standard_name, snippet, relevance
              FROM {CONTROL_SCHEMA}.standard_registry
             WHERE LOWER(applies_to_dataset) = LOWER($ds)
                OR applies_to_dataset IS NULL
             ORDER BY relevance DESC NULLS LAST
             LIMIT {int(k)}
            """,
            {"ds": dataset_code},
        )
        return list(rows)
    except Exception:
        return []


def propose_suite(
    *,
    llm: LlmProvider,
    warehouse: Warehouse,
    dataset_code: str,
    dataset_display: str | None,
    layer: str,
    max_expectations: int = 30,
    temperature: float = 0.0,
    grounding_mode: str = "off",
    actor: str = "ui:dq_ai_architect",
) -> dict[str, Any]:
    """End-to-end: fetch schema + RAG, run agent, return proposal dict."""
    schema = _fetch_schema_for_layer(warehouse, dataset_code=dataset_code, layer=layer)
    cols = schema["columns"]
    if not cols:
        raise ValueError(
            f"No canonical columns found for {dataset_code}/{layer}.  "
            f"Source table: {schema['source_table']}.  "
            f"Author the schema in Data Model Designer first."
        )
    business_keys = [c["name"] for c in cols if c.get("is_business_key")]
    pii_columns = [c["name"] for c in cols if c.get("is_pii")]
    phi_columns = [c["name"] for c in cols if c.get("is_phi")]

    standards_hits: list[dict[str, Any]] = []
    if grounding_mode != "off":
        standards_hits = _fetch_standards_rag(warehouse, dataset_code=dataset_code, layer=layer)

    agent = DqSuiteArchitectAgent(llm=llm, warehouse=warehouse)
    result = agent.run(
        {
            "dataset_code": dataset_code,
            "dataset_display": dataset_display or dataset_code,
            "layer": layer,
            "columns": cols,
            "business_keys": business_keys,
            "pii_columns": pii_columns,
            "phi_columns": phi_columns,
            "standards_hits": standards_hits,
            "max_expectations": max_expectations,
            "temperature": temperature,
            "grounding_mode": grounding_mode,
        }
    )
    if not result.success:
        raise RuntimeError(f"DqSuiteArchitectAgent failed: {result.error or '(no detail)'}")
    proposal = dict(result.payload)
    proposal["tokens_used"] = int(result.tokens_used)
    proposal["duration_ms"] = int(result.duration_ms)
    proposal["proposed_at"] = datetime.now(UTC).isoformat()
    proposal["actor"] = actor
    proposal["column_count"] = len(cols)
    proposal["business_keys"] = business_keys
    proposal["pii_columns"] = pii_columns
    proposal["phi_columns"] = phi_columns

    # Phase 20.1 — emit metrics on every AI proposal (silent no-op if
    # collector unreachable).  Powers the AI Economics Grafana dashboard
    # + Executive Dashboard's spend tile.
    try:
        from datalink.observability.metrics import record_ai_proposal

        record_ai_proposal(
            agent="dq_ai_architect",
            tokens=int(result.tokens_used or 0),
            duration_s=float(result.duration_ms or 0) / 1000.0,
            outcome="success",
        )
    except Exception:
        pass

    return proposal


def persist_suite(
    *,
    warehouse: Warehouse,
    proposal: dict[str, Any],
    actor: str = "ui:dq_ai_architect",
    notes: str = "",
) -> str:
    """Write the proposal as a DRAFT row in CONTROL.dq_suites.  Returns
    the new suite_id.

    Suite naming: ``<dataset_code>_<layer>`` (lowercase).  Version starts
    at 1; ``approve_and_promote()`` bumps version per-promotion if a prior
    LIVE exists.
    """
    suite_id = str(uuid.uuid4())
    dataset_code = str(proposal["dataset_code"]).lower()
    layer = str(proposal["layer"]).upper()
    suite_name = proposal.get("suite_name") or f"{dataset_code}_{layer.lower()}"
    expectations = proposal.get("expectations") or []
    dimension_breakdown = proposal.get("dimension_breakdown") or {}
    rationale = str(proposal.get("rationale") or "")[:8000]

    # Look up the current max version for this (dataset, layer) so a re-
    # proposal lands as v(N+1) DRAFT rather than colliding on v1.
    rows = warehouse.query(
        f"SELECT COALESCE(MAX(version), 0) AS v "
        f"  FROM {CONTROL_SCHEMA}.dq_suites "
        f" WHERE LOWER(dataset_code) = LOWER($ds) "
        f"   AND UPPER(layer) = UPPER($lyr)",
        {"ds": dataset_code, "lyr": layer},
    )
    next_version = int((rows[0] if rows else {}).get("v") or 0) + 1

    warehouse.execute(
        f"""
        INSERT INTO {CONTROL_SCHEMA}.dq_suites
          (suite_id, client_id, suite_name, version, status, expectations,
           dq_dimensions, source_type, source, schema_fingerprint,
           dataset_code, layer,
           ai_proposal_json, ai_rationale, ai_token_count, ai_latency_ms,
           temperature, grounding_mode,
           created_by, created_at)
        VALUES
          ($id, NULL, $name, $ver, 'DRAFT', $exps,
           $dims, $src_type, 'ai_authored', NULL,
           $ds, $lyr,
           $aj, $ar, $tok, $lat,
           $temp, $ground,
           $by, CURRENT_TIMESTAMP())
        """,
        {
            "id": suite_id,
            "name": suite_name,
            "ver": next_version,
            "exps": json.dumps(expectations, default=str),
            "dims": json.dumps(dimension_breakdown, default=str),
            "src_type": dataset_code,  # kept populated for legacy queries
            "ds": dataset_code,
            "lyr": layer,
            "aj": json.dumps(
                {
                    "rationale": rationale,
                    "dimension_breakdown": dimension_breakdown,
                    "column_count": proposal.get("column_count"),
                    "business_keys": proposal.get("business_keys"),
                    "pii_columns": proposal.get("pii_columns"),
                    "phi_columns": proposal.get("phi_columns"),
                    "tokens_used": proposal.get("tokens_used"),
                    "duration_ms": proposal.get("duration_ms"),
                    "proposed_at": proposal.get("proposed_at"),
                },
                default=str,
            ),
            "ar": rationale,
            "tok": int(proposal.get("tokens_used") or 0),
            "lat": int(proposal.get("duration_ms") or 0),
            "temp": float(proposal.get("temperature") or 0.0),
            "ground": str(proposal.get("grounding_mode") or "off"),
            "by": actor,
        },
    )

    # Audit log entry
    warehouse.execute(
        f"""
        INSERT INTO {CONTROL_SCHEMA}.dq_suite_audit_log
          (audit_id, suite_id, from_status, to_status, actor, notes)
        VALUES ($aid, $sid, NULL, 'DRAFT', $by, $notes)
        """,
        {
            "aid": str(uuid.uuid4()),
            "sid": suite_id,
            "by": actor,
            "notes": notes or f"Proposed by AI Architect for {dataset_code}/{layer}",
        },
    )
    return suite_id


def approve_and_promote(
    *,
    warehouse: Warehouse,
    suite_id: str,
    actor: str = "ui:dq_ai_architect",
    notes: str = "",
) -> dict[str, Any]:
    """Flip the suite DRAFT → LIVE.  Archive any prior LIVE suite for the
    same (dataset_code, layer) tuple — invariant: exactly one LIVE per
    (dataset, layer)."""
    rows = warehouse.query(
        f"SELECT suite_id, dataset_code, layer, status, version "
        f"  FROM {CONTROL_SCHEMA}.dq_suites WHERE suite_id = $id",
        {"id": suite_id},
    )
    if not rows:
        raise ValueError(f"Suite {suite_id!r} not found.")
    target = rows[0]
    if str(target.get("status")) == "LIVE":
        return {"already_live": True, "suite_id": suite_id}
    if str(target.get("status")) == "ARCHIVED":
        raise ValueError(f"Suite {suite_id!r} is ARCHIVED.")

    ds_code = target.get("dataset_code")
    layer = target.get("layer")

    # Archive prior LIVE for this (dataset, layer).
    warehouse.execute(
        f"""
        UPDATE {CONTROL_SCHEMA}.dq_suites
           SET status = 'ARCHIVED', archived_at = CURRENT_TIMESTAMP()
         WHERE LOWER(dataset_code) = LOWER($ds)
           AND UPPER(layer) = UPPER($lyr)
           AND status = 'LIVE'
        """,
        {"ds": ds_code, "lyr": layer},
    )

    # Promote this one.
    warehouse.execute(
        f"""
        UPDATE {CONTROL_SCHEMA}.dq_suites
           SET status = 'LIVE',
               activated_at = CURRENT_TIMESTAMP(),
               reviewed_by = $by, reviewed_at = CURRENT_TIMESTAMP(),
               review_notes = $notes
         WHERE suite_id = $id
        """,
        {"id": suite_id, "by": actor, "notes": notes},
    )

    warehouse.execute(
        f"""
        INSERT INTO {CONTROL_SCHEMA}.dq_suite_audit_log
          (audit_id, suite_id, from_status, to_status, actor, notes)
        VALUES ($aid, $sid, $from, 'LIVE', $by, $notes)
        """,
        {
            "aid": str(uuid.uuid4()),
            "sid": suite_id,
            "from": str(target.get("status") or "DRAFT"),
            "by": actor,
            "notes": notes or f"Promoted to LIVE for {ds_code}/{layer}",
        },
    )
    _log.info(
        "dq_suite.promoted",
        suite_id=suite_id,
        dataset_code=ds_code,
        layer=layer,
        actor=actor,
    )
    return {
        "promoted": True,
        "suite_id": suite_id,
        "dataset_code": ds_code,
        "layer": layer,
        "version": target.get("version"),
    }
