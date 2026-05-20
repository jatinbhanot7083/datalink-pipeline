"""Canonical Extension Proposals — Phase 23.5.

Feedback loop: when a Client Onboarding proposal surfaces an unmapped
client field, the operator can promote it to a Canonical Extension
Proposal here.  The AI does the heavy lifting:

  * Suggests a canonical display_name (English-ish, not snake_case)
  * Suggests a slug (bronze_column_name)
  * Suggests a logical_type from the sample values
  * Suggests a description that captures intent
  * Flags PII/PHI candidacy
  * Suggests product-line scope (used_by)

Operator (acting as DBA per the role toggle) reviews + approves.  On
approve, a row lands in CONTROL.global_bronze_catalog_fields and the
extension is marked PROMOTED.

Public API:
  * :func:`propose_canonical_extension` — write a PENDING_REVIEW row
  * :func:`approve_canonical_extension` — promote to canonical
  * :func:`reject_canonical_extension`  — mark REJECTED with notes
  * :func:`list_extension_proposals`    — list for the DBA review surface
  * :func:`get_extension_proposal`      — single detail row
"""

from __future__ import annotations

import json
import re
import uuid
from datetime import UTC, datetime
from typing import Any

from datalink.adapters.protocols import LlmMessage, LlmProvider, Warehouse
from datalink.logging import get_logger
from datalink.quality.control import CONTROL_SCHEMA

_log = get_logger(__name__)


# ---------------------------------------------------------------------------
# AI helper — suggest the canonical shape for an unmapped client field
# ---------------------------------------------------------------------------


def ai_suggest_canonical_shape(
    *,
    llm: LlmProvider,
    dataset_code: str,
    client_field_name: str,
    client_field_path: str | None,
    sample_values: list[str],
    inferred_type: str,
    client_id: str | None = None,
) -> dict[str, Any]:
    """Ask the LLM to propose a canonical-field shape given an unmapped
    client field's name + sample values.  Returns a dict shaped for
    direct INSERT into ``bronze_canonical_extension_proposals``.

    Cheap LLM call (~1k tokens).  Caller persists the result.
    """
    samples = [str(v)[:120] for v in (sample_values or [])][:5]
    prompt = f"""You are a Senior Healthcare Data Modeler proposing an addition to a canonical Bronze schema.

CONTEXT:
  Canonical dataset    : {dataset_code}
  Client field name    : {client_field_name}
  Client field path    : {client_field_path or "(top level)"}
  Inferred type        : {inferred_type}
  Sample values        : {json.dumps(samples)}

TASK: Propose a CANONICAL field for this concept.  Return JSON:

{{
  "proposed_display_name"   : "<human-readable, Title Case, e.g. 'Member Race Code'>",
  "proposed_column_name"    : "<snake_case bronze_column_name>",
  "proposed_logical_type"   : "TEXT | NUMBER | BOOLEAN | DATE | TIMESTAMP_NTZ | VARIANT",
  "proposed_requirement"    : "Required | Recommended | Optional | Conditional",
  "proposed_description"    : "<one sentence that captures intent + value range — no PHI in description>",
  "proposed_is_pii"         : true/false,
  "proposed_is_phi"         : true/false,
  "proposed_used_by"        : ["E360"|"CC"|"RBN"|"EC"|"ESV"|"CC_CM"|"CC_UM"|"CC_AG"|"CC_PROV"|"CC_MBR"],
  "ai_rationale"            : "<2 sentences — why canonical · how broadly applicable>",
  "ai_confidence"           : 0.0
}}

RULES:
1. Pick the MOST SPECIFIC + MOST PORTABLE name — avoid client-specific jargon.
2. Mark PHI if the value clearly identifies a real person (full names, DOB, SSN, MRN); mark PII for non-clinical PII (email, phone, address, postal_code).
3. requirement defaults to Optional UNLESS the field is a clear identifier (set Required) or a documented standard reference (set Recommended).
4. logical_type — infer from samples; default TEXT if ambiguous.
5. confidence — 0.9+ = obvious canonical addition · 0.7+ = useful candidate · <0.5 = client-specific, may not generalize.

Return ONLY the JSON object.  No prose, no markdown fences.
""".strip()

    messages = [
        LlmMessage(role="system", content="You design canonical healthcare data schemas."),
        LlmMessage(role="user", content=prompt),
    ]
    import time

    t0 = time.perf_counter()
    completion = llm.complete(messages, max_tokens=800, temperature=0.0)
    duration_ms = int((time.perf_counter() - t0) * 1000)
    tokens = int(
        (getattr(completion, "input_tokens", 0) or 0)
        + (getattr(completion, "output_tokens", 0) or 0)
    )
    text = str(getattr(completion, "content", "") or "")

    # Reuse the same robust JSON parser as the main agent (avoid circular import)
    from datalink.agents.inbound_mapping.agent import InboundMappingAgent

    parsed = InboundMappingAgent._safe_parse_json(text)
    if not parsed:
        # Defensive fallback — produce a minimum-viable shape so the operator
        # can still see + edit before approval.
        slug = re.sub(r"[^a-z0-9]+", "_", client_field_name.lower()).strip("_") or "new_field"
        parsed = {
            "proposed_display_name": client_field_name.replace("_", " ").title(),
            "proposed_column_name": slug,
            "proposed_logical_type": inferred_type or "TEXT",
            "proposed_requirement": "Optional",
            "proposed_description": f"Inbound field '{client_field_name}' from client {client_id or '?'}",
            "proposed_is_pii": False,
            "proposed_is_phi": False,
            "proposed_used_by": [],
            "ai_rationale": "Fallback shape — LLM JSON parse failed; operator should review.",
            "ai_confidence": 0.0,
        }
    parsed["_ai_tokens"] = tokens
    parsed["_ai_latency_ms"] = duration_ms
    return parsed


# ---------------------------------------------------------------------------
# Bridge — propose / approve / reject
# ---------------------------------------------------------------------------


def propose_canonical_extension(
    *,
    wh: Warehouse,
    llm: LlmProvider,
    origin_proposal_id: str,
    origin_client_id: str,
    origin_field_name: str,
    dataset_code: str,
    actor: str = "ui:co:reviewer",
    submit_immediately: bool = True,
) -> dict[str, Any]:
    """Promote an unmapped client field from a Client Onboarding
    proposal into a Canonical Extension Proposal.

    Fetches the field's sample values + inferred type from the parent
    inbound-mapping proposal, asks the AI for a canonical shape, writes
    the row.  Returns the new extension_id + AI's suggested shape so the
    UI can show "✓ proposed as `member_race_code`" right away.
    """
    # 1. Pull the source proposal so we have sample values + inferred type
    inbound_rows = list(
        wh.query(
            f"SELECT source_schema_json FROM {CONTROL_SCHEMA}.bronze_inbound_mapping_proposals "
            f"WHERE proposal_id = $pid",
            {"pid": origin_proposal_id},
        )
    )
    if not inbound_rows:
        raise ValueError(f"origin proposal {origin_proposal_id} not found")

    raw_schema = inbound_rows[0].get("source_schema_json")
    if isinstance(raw_schema, str):
        try:
            schema = json.loads(raw_schema)
        except json.JSONDecodeError:
            schema = {}
    else:
        schema = raw_schema or {}

    target_field: dict[str, Any] = {}
    for f in schema.get("fields") or []:
        if str(f.get("name") or "") == origin_field_name:
            target_field = f
            break

    sample_values = list(target_field.get("sample_values") or [])
    inferred_type = str(target_field.get("inferred_type") or "TEXT")
    client_field_path = target_field.get("path")

    # 2. Ask the LLM for the canonical shape
    shape = ai_suggest_canonical_shape(
        llm=llm,
        dataset_code=dataset_code,
        client_field_name=origin_field_name,
        client_field_path=client_field_path,
        sample_values=sample_values,
        inferred_type=inferred_type,
        client_id=origin_client_id,
    )

    # 3. INSERT the extension proposal
    ext_id = str(uuid.uuid4())
    status = "PENDING_REVIEW" if submit_immediately else "DRAFT"
    used_by_json = json.dumps(shape.get("proposed_used_by") or [])
    samples_json = json.dumps(sample_values, default=str)
    wh.execute(
        f"INSERT INTO {CONTROL_SCHEMA}.bronze_canonical_extension_proposals "
        f"(extension_id, dataset_code, status, "
        f" proposed_display_name, proposed_column_name, proposed_logical_type, "
        f" proposed_requirement, proposed_description, "
        f" proposed_is_pii, proposed_is_phi, proposed_used_by, "
        f" origin_proposal_id, origin_client_id, origin_field_name, "
        f" origin_sample_values, ai_rationale, ai_confidence, "
        f" ai_tokens, ai_latency_ms, "
        f" created_by, created_at, submitted_at) "
        f"SELECT $eid, $ds, $st, "
        f"       $dn, $cn, $lt, "
        f"       $req, $desc, "
        f"       $pii, $phi, PARSE_JSON($used), "
        f"       $opid, $ocid, $ofn, "
        f"       PARSE_JSON($samples), $rat, $conf, "
        f"       $tok, $lat, "
        f"       $by, $now, $sub_ts",
        {
            "eid": ext_id,
            "ds": dataset_code,
            "st": status,
            "dn": str(shape.get("proposed_display_name") or "")[:256],
            "cn": str(shape.get("proposed_column_name") or "")[:64],
            "lt": str(shape.get("proposed_logical_type") or "TEXT")[:64],
            "req": str(shape.get("proposed_requirement") or "Optional"),
            "desc": str(shape.get("proposed_description") or "")[:2000],
            "pii": bool(shape.get("proposed_is_pii") or False),
            "phi": bool(shape.get("proposed_is_phi") or False),
            "used": used_by_json,
            "opid": origin_proposal_id,
            "ocid": origin_client_id,
            "ofn": origin_field_name[:256],
            "samples": samples_json,
            "rat": str(shape.get("ai_rationale") or "")[:2000],
            "conf": float(shape.get("ai_confidence") or 0),
            "tok": int(shape.get("_ai_tokens") or 0),
            "lat": int(shape.get("_ai_latency_ms") or 0),
            "by": actor,
            "now": _ts(),
            "sub_ts": _ts() if submit_immediately else None,
        },
    )
    return {"extension_id": ext_id, "status": status, **shape}


def approve_canonical_extension(
    *,
    wh: Warehouse,
    extension_id: str,
    actor: str,
    reviewer_notes: str = "",
) -> str:
    """Promote a PENDING_REVIEW extension into ``global_bronze_catalog_fields``.

    Returns the new field_id.
    """
    rows = list(
        wh.query(
            f"SELECT * FROM {CONTROL_SCHEMA}.bronze_canonical_extension_proposals "
            f"WHERE extension_id = $eid",
            {"eid": extension_id},
        )
    )
    if not rows:
        raise ValueError(f"extension_id '{extension_id}' not found")
    ext = dict(rows[0])
    if ext["status"] not in ("DRAFT", "PENDING_REVIEW"):
        raise ValueError(f"cannot approve from status '{ext['status']}'")

    dataset_code = str(ext["dataset_code"])

    # Resolve dataset_id
    ds_rows = list(
        wh.query(
            f"SELECT dataset_id, total_fields FROM {CONTROL_SCHEMA}.global_bronze_catalog_datasets "
            f"WHERE dataset_code = $ds LIMIT 1",
            {"ds": dataset_code},
        )
    )
    if not ds_rows:
        raise ValueError(f"canonical dataset '{dataset_code}' missing from catalog")
    dataset_id = str(ds_rows[0]["dataset_id"])

    # Refuse duplicates by bronze_column_name
    dup_check = list(
        wh.query(
            f"SELECT 1 FROM {CONTROL_SCHEMA}.global_bronze_catalog_fields "
            f"WHERE dataset_code = $ds AND bronze_column_name = $col",
            {"ds": dataset_code, "col": str(ext["proposed_column_name"])},
        )
    )
    if dup_check:
        raise ValueError(
            f"a canonical field with bronze_column_name='{ext['proposed_column_name']}' "
            f"already exists in dataset '{dataset_code}'.  "
            f"Edit the proposed_column_name first."
        )

    # Next field_order
    order_rows = list(
        wh.query(
            f"SELECT COALESCE(MAX(field_order),0) m "
            f"FROM {CONTROL_SCHEMA}.global_bronze_catalog_fields "
            f"WHERE dataset_code = $ds",
            {"ds": dataset_code},
        )
    )
    next_order = int(order_rows[0]["m"]) + 1

    # Compute catalog_version explicitly — Snowflake rejects scalar
    # subqueries inside parameterized VALUES clauses.
    cv_rows = list(
        wh.query(
            f"SELECT COALESCE(MAX(catalog_version), 1) v "
            f"FROM {CONTROL_SCHEMA}.global_bronze_catalog_datasets"
        )
    )
    catalog_version = int(cv_rows[0]["v"]) if cv_rows else 1

    field_id = str(uuid.uuid4())
    wh.execute(
        f"INSERT INTO {CONTROL_SCHEMA}.global_bronze_catalog_fields "
        f"(field_id, dataset_id, dataset_code, field_order, "
        f" field_display_name, bronze_column_name, requirement, logical_type, "
        f" description, additional_notes, example, "
        f" is_pii, is_phi, is_business_key, catalog_version, registered_at) "
        f"VALUES ($id, $ds_id, $ds, $ord, "
        f"        $name, $col, $req, $lt, "
        f"        $desc, $notes, $ex, "
        f"        $pii, $phi, FALSE, $cv, $ts)",
        {
            "id": field_id,
            "ds_id": dataset_id,
            "ds": dataset_code,
            "ord": next_order,
            "name": str(ext["proposed_display_name"]),
            "col": str(ext["proposed_column_name"]),
            "req": str(ext.get("proposed_requirement") or "Optional"),
            "lt": str(ext.get("proposed_logical_type") or "TEXT"),
            "desc": str(ext.get("proposed_description") or ""),
            "notes": (
                f"Promoted from canonical extension proposal {extension_id} · "
                f"origin: {ext.get('origin_client_id')}/{ext.get('origin_field_name')}"
            )[:1000],
            "ex": "",
            "pii": bool(ext.get("proposed_is_pii") or False),
            "phi": bool(ext.get("proposed_is_phi") or False),
            "cv": catalog_version,
            "ts": _ts(),
        },
    )

    # Bump total_fields on the dataset row
    wh.execute(
        f"UPDATE {CONTROL_SCHEMA}.global_bronze_catalog_datasets "
        f"SET total_fields = COALESCE(total_fields, 0) + 1 "
        f"WHERE dataset_code = $ds",
        {"ds": dataset_code},
    )

    # Mark extension PROMOTED
    wh.execute(
        f"UPDATE {CONTROL_SCHEMA}.bronze_canonical_extension_proposals "
        f"SET status = 'PROMOTED', reviewed_by = $by, reviewed_at = $ts, "
        f"    reviewer_notes = $notes, promoted_to_field_id = $fid, "
        f"    promoted_at = $ts "
        f"WHERE extension_id = $eid",
        {
            "eid": extension_id,
            "by": actor,
            "ts": _ts(),
            "notes": (reviewer_notes or "")[:4000],
            "fid": field_id,
        },
    )

    # Audit log entry — reuse the inbound-mapping audit table since this is
    # part of the same lifecycle family.
    try:
        wh.execute(
            f"INSERT INTO {CONTROL_SCHEMA}.bronze_inbound_mapping_audit "
            f"(audit_id, proposal_id, client_id, dataset_code, "
            f" action, actor, from_status, to_status, notes, ts) "
            f"SELECT $aid, $opid, $ocid, $ds, "
            f"       'promote_canonical_field', $by, 'PENDING_REVIEW', 'PROMOTED', "
            f"       $note, $ts",
            {
                "aid": str(uuid.uuid4()),
                "opid": ext.get("origin_proposal_id"),
                "ocid": ext.get("origin_client_id"),
                "ds": dataset_code,
                "by": actor,
                "note": f"extension_id={extension_id} field_id={field_id} "
                f"col='{ext.get('proposed_column_name')}' "
                f"display='{ext.get('proposed_display_name')}'  notes={reviewer_notes[:500]}",
                "ts": _ts(),
            },
        )
    except Exception as exc:
        _log.warning("ext.audit_failed", err=str(exc)[:200])

    return field_id


def reject_canonical_extension(
    *,
    wh: Warehouse,
    extension_id: str,
    actor: str,
    reviewer_notes: str,
) -> None:
    """Mark a canonical extension proposal REJECTED.  Notes required."""
    if not (reviewer_notes or "").strip():
        raise ValueError("reviewer_notes are required to reject a canonical extension")
    wh.execute(
        f"UPDATE {CONTROL_SCHEMA}.bronze_canonical_extension_proposals "
        f"SET status = 'REJECTED', reviewed_by = $by, reviewed_at = $ts, "
        f"    reviewer_notes = $notes "
        f"WHERE extension_id = $eid",
        {"eid": extension_id, "by": actor, "ts": _ts(), "notes": reviewer_notes[:4000]},
    )


def list_extension_proposals(
    *,
    wh: Warehouse,
    status: str | list[str] | None = None,
    dataset_code: str | None = None,
    limit: int = 200,
) -> list[dict[str, Any]]:
    where: list[str] = []
    params: dict[str, Any] = {}
    if isinstance(status, str):
        where.append("status = $st")
        params["st"] = status
    elif isinstance(status, list) and status:
        safe = [s for s in status if isinstance(s, str) and s.replace("_", "").isalpha()]
        if safe:
            where.append("status IN (" + ",".join(f"'{s}'" for s in safe) + ")")
    if dataset_code:
        where.append("dataset_code = $ds")
        params["ds"] = dataset_code
    clause = ("WHERE " + " AND ".join(where)) if where else ""
    return list(
        wh.query(
            f"SELECT extension_id, dataset_code, status, "
            f"       proposed_display_name, proposed_column_name, proposed_logical_type, "
            f"       proposed_requirement, proposed_is_pii, proposed_is_phi, "
            f"       origin_client_id, origin_field_name, ai_confidence, ai_tokens, "
            f"       created_by, created_at, submitted_at, "
            f"       reviewed_by, reviewed_at, reviewer_notes, "
            f"       promoted_to_field_id, promoted_at "
            f"FROM {CONTROL_SCHEMA}.bronze_canonical_extension_proposals "
            f"{clause} "
            f"ORDER BY created_at DESC LIMIT {int(limit)}",
            params,
        )
    )


def get_extension_proposal(*, wh: Warehouse, extension_id: str) -> dict[str, Any]:
    rows = list(
        wh.query(
            f"SELECT * FROM {CONTROL_SCHEMA}.bronze_canonical_extension_proposals "
            f"WHERE extension_id = $eid",
            {"eid": extension_id},
        )
    )
    if not rows:
        raise ValueError(f"extension_id '{extension_id}' not found")
    return dict(rows[0])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _ts() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)
