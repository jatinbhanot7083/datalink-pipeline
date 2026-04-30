"""Bridge between the ContractArchitectAgent and the rest of the platform.

Three responsibilities:

  1. ``propose_contract()`` — pulls top-k chunks from the RAG store,
     invokes the agent, returns a ContractProposal record the UI
     renders verbatim.

  2. ``persist_design()`` — upserts a row in
     ``CONTROL.contract_designs`` with status DRAFT/PENDING_REVIEW.

  3. ``approve_design()`` — terminal flow. Generates the 5 artifacts
     (Bronze DDL committed file, contract row, GX suite scaffold,
     dbt scaffold, vendor-spec markdown) and flips status to APPROVED.

The agent layer never touches Snowflake or pgvector directly — that
happens here, AFTER the LLM has produced a proposal. Keeps PHI
boundary clean: row data never travels through the LLM.
"""

from __future__ import annotations

import json
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from datalink.adapters.protocols import LlmProvider, Warehouse
from datalink.agents.contract_architect.agent import (
    ContractArchitectAgent,
    ContractMode,
    ContractProposal,
    ProposedColumn,
)
from datalink.logging import get_logger
from datalink.memory import AgentMemoryStore
from datalink.quality.control import CONTROL_SCHEMA

_log = get_logger(__name__)


# ============================================================================
# Public API
# ============================================================================


def propose_contract(
    *,
    llm: LlmProvider,
    warehouse: Warehouse,
    memory: AgentMemoryStore,
    client_id: str,
    source_type: str,
    mode: str,
    anchored_standards: list[str],
    payload: dict[str, Any],
    temperature: float = 0.0,
    grounding_k: int = 3,
    strictness: float = 0.5,
    actor: str = "operator",
) -> ContractProposal:
    """End-to-end: retrieve grounding, invoke agent, return reviewable
    proposal.

    Does NOT persist — the UI shows the proposal first; the operator
    edits and clicks Approve, which calls ``persist_design()`` then
    ``approve_design()``.
    """
    # 1. Build the RAG query text from the input
    mode_enum = ContractMode(mode.upper())
    query_text = _build_rag_query(mode_enum, payload, source_type)
    grounding_chunks: list[dict[str, Any]] = []
    grounding_meta: list[dict[str, Any]] = []
    if anchored_standards:
        try:
            hits = memory.query_similar_standard_chunks(
                query_text=query_text,
                anchored_codes=tuple(anchored_standards),
                k=grounding_k,
            )
            for h in hits:
                grounding_chunks.append(
                    {
                        "standard_code": h.standard_code,
                        "section_path": h.section_path,
                        "resource_type": h.resource_type,
                        "chunk_text": h.chunk_text,
                        "distance": h.distance,
                    }
                )
                grounding_meta.append(
                    {
                        "standard_code": h.standard_code,
                        "section_path": h.section_path,
                        "distance": float(h.distance),
                    }
                )
        except Exception as e:
            _log.warning(
                "contract_architect.rag_unavailable",
                error=str(e),
                anchored=anchored_standards,
            )

    # 2. Invoke the agent
    agent = ContractArchitectAgent(llm=llm, warehouse=warehouse)
    started = time.time()
    result = agent.run(
        {
            "client_id": client_id,
            "source_type": source_type,
            "mode": mode_enum.value,
            "anchored_standards": anchored_standards,
            "temperature": temperature,
            "grounding_k": grounding_k,
            "strictness": strictness,
            "payload": payload,
            "grounding_chunks": grounding_chunks,
        }
    )
    duration_ms = int((time.time() - started) * 1000)

    if not result.success:
        raise RuntimeError(f"ContractArchitectAgent failed: {result.error or '(no detail)'}")

    obj = result.payload

    # 3. Hydrate ContractProposal
    columns = [
        ProposedColumn(
            name=c["name"],
            type=c["type"],
            nullable=bool(c["nullable"]),
            rationale=c["rationale"],
            matches_standard=c.get("matches_standard"),
            vendor_field=c.get("vendor_field"),
            deviation=c.get("deviation"),
        )
        for c in obj["proposed_columns"]
    ]

    proposer_model = getattr(llm, "model", "")
    embedding_model = getattr(memory, "_embedder", None)
    embedding_model_name = getattr(embedding_model, "model", "") if embedding_model else ""

    return ContractProposal(
        proposed_table_name=obj["proposed_table_name"],
        proposed_columns=columns,
        proposed_ddl=obj["proposed_ddl"],
        standard_match_scores={
            k: float(v) for k, v in obj.get("standard_match_scores", {}).items()
        },
        rationale=obj.get("rationale", ""),
        deviation_log=obj.get("deviation_log", []),
        gx_scaffold=obj.get("gx_scaffold", []),
        dbt_scaffold=obj.get("dbt_scaffold", ""),
        vendor_spec_md=obj.get("vendor_spec_md", ""),
        grounding=grounding_meta,
        tokens_used=int(result.tokens_used),
        duration_ms=duration_ms,
        proposer_model=proposer_model,
        embedding_model=embedding_model_name,
        temperature=temperature,
        strictness=strictness,
        mode=mode_enum,
    )


def persist_design(
    *,
    warehouse: Warehouse,
    proposal: ContractProposal,
    client_id: str,
    source_type: str,
    anchored_standards: list[str],
    payload: dict[str, Any],
    edited_columns: list[dict[str, Any]] | None = None,
    edited_ddl: str | None = None,
    actor: str,
    approval_mode: str = "HITL",
    notes: str = "",
) -> str:
    """Insert / refresh a row in CONTROL.contract_designs. Returns design_id."""
    design_id = str(uuid.uuid4())
    status = "DRAFT" if approval_mode == "DRAFT" else "PENDING_REVIEW"

    sample_file_uri = (
        payload.get("file_name") if proposal.mode == ContractMode.FILE_DRIVEN else None
    )
    nl_description = (
        payload.get("nl_description") if proposal.mode == ContractMode.CONTRACT_FIRST else None
    )

    proposed_columns_json = json.dumps(
        [
            {
                "name": c.name,
                "type": c.type,
                "nullable": c.nullable,
                "rationale": c.rationale,
                "matches_standard": c.matches_standard,
                "vendor_field": c.vendor_field,
                "deviation": c.deviation,
            }
            for c in proposal.proposed_columns
        ]
    )

    warehouse.execute(
        f"""
        INSERT INTO {CONTROL_SCHEMA}.contract_designs
          (design_id, client_id, source_type, status, version, mode,
           anchored_standards, temperature, grounding_k, strictness,
           sample_file_uri, nl_description,
           proposed_columns, edited_columns, proposed_ddl, final_ddl,
           deviation_log, per_column_reasoning, standard_match_scores,
           vendor_spec_md, approval_mode, created_at, created_by, notes)
        VALUES ($id, $cid, $stp, $st, 1, $mode,
                $anc, $temp, $k, $strict,
                $fileuri, $nl,
                $pcols, $ecols, $pddl, $fddl,
                $devs, $reason, $scores,
                $spec, $appm, $ts, $actor, $notes)
        """,
        {
            "id": design_id,
            "cid": client_id,
            "stp": source_type.upper(),
            "st": status,
            "mode": proposal.mode.value,
            "anc": json.dumps(anchored_standards),
            "temp": float(proposal.temperature),
            "k": len(proposal.grounding),
            "strict": float(proposal.strictness),
            "fileuri": sample_file_uri,
            "nl": nl_description,
            "pcols": proposed_columns_json,
            "ecols": json.dumps(edited_columns) if edited_columns else None,
            "pddl": proposal.proposed_ddl,
            "fddl": edited_ddl,
            "devs": json.dumps(proposal.deviation_log),
            "reason": json.dumps({c.name: c.rationale for c in proposal.proposed_columns}),
            "scores": json.dumps(proposal.standard_match_scores),
            "spec": proposal.vendor_spec_md,
            "appm": approval_mode,
            "ts": datetime.now(UTC),
            "actor": actor,
            "notes": notes,
        },
    )

    # Audit log
    _audit_log(
        warehouse=warehouse,
        design_id=design_id,
        client_id=client_id,
        source_type=source_type,
        action="PROPOSED",
        actor=actor,
        from_status=None,
        to_status=status,
        diff_summary={"columns": len(proposal.proposed_columns)},
        ai_reasoning=proposal.rationale[:500],
        token_count=int(proposal.tokens_used),
        latency_ms=int(proposal.duration_ms),
        notes=notes,
    )

    return design_id


def approve_design(
    *,
    warehouse: Warehouse,
    design_id: str,
    actor: str,
    notes: str = "",
    repo_root: Path | None = None,
) -> dict[str, Any]:
    """Terminal step. Approves the design + emits all 5 artifacts:

      1. ``datalink/pipeline/bronze/ddl/<client>_<source>.sql``
      2. ``CONTROL.SOURCE_SCHEMA_CONTRACTS`` row registered (drift detect)
      3. ``CONTROL.DQ_SUITES`` row scaffolded with GX expectations
      4. ``dbt/models/silver/<client>/<table>.sql`` model stub
      5. ``docs/specs/<client>_<source>_v1.md`` vendor data contract spec

    All emissions are idempotent — re-approving a design overwrites the
    artifacts in place + bumps a contract version where applicable.

    Returns a summary dict the UI shows.
    """
    rows = warehouse.query(
        f"SELECT * FROM {CONTROL_SCHEMA}.contract_designs WHERE design_id = $id",
        {"id": design_id},
    )
    if not rows:
        raise ValueError(f"Design {design_id} not found.")
    cur = rows[0]
    if cur.get("status") in ("APPROVED", "ARCHIVED"):
        raise ValueError(
            f"Design {design_id} status is {cur.get('status')} — cannot approve again."
        )

    repo_root = repo_root or _detect_repo_root()
    client_id = str(cur.get("client_id", "")).lower()
    source_type = str(cur.get("source_type", "")).upper()

    # Resolve final_columns + final_ddl with sensible fallbacks
    final_cols_json = cur.get("edited_columns") or cur.get("proposed_columns") or "[]"
    final_cols = (
        json.loads(final_cols_json) if isinstance(final_cols_json, str) else final_cols_json
    )
    final_ddl = str(cur.get("final_ddl") or cur.get("proposed_ddl") or "")
    table_name = _extract_table_name(final_ddl) or f"RAW_{source_type}"

    artifact_paths: dict[str, str] = {}

    # --- Artifact 1: Bronze DDL committed file ---------------------------
    ddl_path = (
        repo_root
        / "datalink"
        / "pipeline"
        / "bronze"
        / "ddl"
        / f"{client_id}_{source_type.lower()}_{table_name.lower()}.sql"
    )
    ddl_path.parent.mkdir(parents=True, exist_ok=True)
    ddl_path.write_text(_format_ddl_with_header(final_ddl, design_id, client_id), encoding="utf-8")
    _chown_to_host_user(ddl_path)
    artifact_paths["bronze_ddl"] = str(ddl_path.relative_to(repo_root))

    # --- Artifact 2: SOURCE_SCHEMA_CONTRACTS row ------------------------
    contract_id = _register_schema_contract(
        warehouse=warehouse,
        client_id=client_id,
        source_type=source_type,
        final_columns=final_cols,
        actor=actor,
    )

    # --- Artifact 3: DQ_SUITES row (GX scaffold) ------------------------
    suite_id = _register_gx_scaffold(
        warehouse=warehouse,
        design_id=design_id,
        client_id=client_id,
        source_type=source_type,
        table_name=table_name,
        final_columns=final_cols,
        actor=actor,
    )

    # --- Artifact 4: dbt model scaffold ---------------------------------
    dbt_path = (
        repo_root / "dbt" / "models" / "silver" / client_id / f"silver_{table_name.lower()}.sql"
    )
    dbt_path.parent.mkdir(parents=True, exist_ok=True)
    dbt_path.write_text(
        _build_dbt_silver_stub(
            client_id=client_id, source_type=source_type, table_name=table_name, columns=final_cols
        ),
        encoding="utf-8",
    )
    _chown_to_host_user(dbt_path)
    artifact_paths["dbt_silver"] = str(dbt_path.relative_to(repo_root))

    # --- Artifact 5: Vendor spec markdown -------------------------------
    spec_md = str(cur.get("vendor_spec_md") or "") or _build_default_vendor_spec_md(
        client_id=client_id, source_type=source_type, columns=final_cols
    )
    spec_path = repo_root / "docs" / "specs" / f"{client_id}_{source_type.lower()}_v1.md"
    spec_path.parent.mkdir(parents=True, exist_ok=True)
    spec_path.write_text(spec_md, encoding="utf-8")
    _chown_to_host_user(spec_path)
    artifact_paths["vendor_spec_md"] = str(spec_path.relative_to(repo_root))

    # --- Update the contract_designs row with artifact pointers + status -
    warehouse.execute(
        f"""
        UPDATE {CONTROL_SCHEMA}.contract_designs
           SET status = 'APPROVED',
               approved_at = $ts,
               approved_by = $actor,
               final_ddl   = $fddl,
               final_columns = $fcols,
               gx_suite_id = $sid,
               contract_id = $cid,
               dbt_scaffold_uri = $dbt,
               vendor_spec_pdf_uri = NULL,
               notes       = COALESCE(notes, '') || $notes
         WHERE design_id = $id
        """,
        {
            "id": design_id,
            "ts": datetime.now(UTC),
            "actor": actor,
            "fddl": final_ddl,
            "fcols": json.dumps(final_cols),
            "sid": suite_id,
            "cid": contract_id,
            "dbt": artifact_paths.get("dbt_silver"),
            "notes": f"\n[approved] {notes}" if notes else "",
        },
    )

    _audit_log(
        warehouse=warehouse,
        design_id=design_id,
        client_id=client_id,
        source_type=source_type,
        action="APPROVED",
        actor=actor,
        from_status=str(cur.get("status", "")),
        to_status="APPROVED",
        diff_summary={"artifacts_emitted": [*artifact_paths.keys(), "contract", "gx_suite"]},
        ai_reasoning=None,
        token_count=None,
        latency_ms=None,
        notes=notes,
    )

    return {
        "design_id": design_id,
        "status": "APPROVED",
        "approved_by": actor,
        "table_name": table_name,
        "contract_id": contract_id,
        "gx_suite_id": suite_id,
        "artifact_paths": artifact_paths,
    }


# ============================================================================
# Artifact emitters
# ============================================================================


def _detect_repo_root() -> Path:
    """Locate the repo root by walking up until pyproject.toml is found.
    Falls back to /opt/datalink (the in-container path) which is the
    bind-mount of the host repo."""
    here = Path(__file__).resolve()
    for parent in [here, *here.parents]:
        if (parent / "pyproject.toml").exists():
            return parent
    return Path("/opt/datalink")


def _chown_to_host_user(path: Path) -> None:
    """When writing files from inside the container (running as root),
    set ownership to the host user so git/pre-commit can stage them.
    UID/GID 1000 maps to the typical Linux dev user (jatin on this box).
    No-op when already owned by 1000 or when permission is denied."""
    import contextlib
    import os

    with contextlib.suppress(PermissionError, FileNotFoundError, OSError):
        os.chown(str(path), 1000, 1000)


def _extract_table_name(ddl: str) -> str | None:
    """Pull the bare table name out of a CREATE TABLE statement."""
    import re as _re

    m = _re.search(
        r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:\{schema\}\.)?(\w+)", ddl, _re.IGNORECASE
    )
    return m.group(1) if m else None


def _format_ddl_with_header(ddl: str, design_id: str, client_id: str) -> str:
    """Top-of-file comment block so any operator opening the file knows
    where it came from + how to re-emit."""
    header = (
        f"-- AUTO-GENERATED by Data Contract Architect (Phase 14)\n"
        f"-- design_id : {design_id}\n"
        f"-- client    : {client_id}\n"
        f"-- generated : {datetime.now(UTC).isoformat()}\n"
        f"-- DO NOT EDIT BY HAND. Re-approve the design from the UI to\n"
        f"-- regenerate this file. The runtime substitutes {{schema}} with\n"
        f"-- the per-tenant Bronze schema (e.g. BRONZE_AETNA).\n\n"
    )
    return header + ddl.rstrip() + ("\n" if not ddl.endswith("\n") else "")


def _register_schema_contract(
    *,
    warehouse: Warehouse,
    client_id: str,
    source_type: str,
    final_columns: list[dict[str, Any]],
    actor: str,
) -> str:
    """Insert / refresh a row in CONTROL.source_schema_contracts. Bumps
    contract_version on each re-approval."""
    contract_id = str(uuid.uuid4())
    columns_json = json.dumps(
        [
            {"name": c["name"], "type": c["type"], "nullable": bool(c.get("nullable", True))}
            for c in final_columns
            if not str(c.get("name", "")).startswith("_")  # exclude audit cols
        ]
    )

    # Find current max version for this (client, source_type) tuple
    rows = warehouse.query(
        f"SELECT MAX(contract_version) AS v FROM {CONTROL_SCHEMA}.source_schema_contracts "
        f"WHERE client_id = $c AND source_type = $s",
        {"c": client_id, "s": source_type},
    )
    next_version = 1
    if rows and rows[0].get("v") is not None:
        next_version = int(rows[0]["v"]) + 1

    # Mark prior version inactive
    warehouse.execute(
        f"UPDATE {CONTROL_SCHEMA}.source_schema_contracts "
        f"SET is_active = FALSE WHERE client_id = $c AND source_type = $s",
        {"c": client_id, "s": source_type},
    )

    warehouse.execute(
        f"""
        INSERT INTO {CONTROL_SCHEMA}.source_schema_contracts
          (contract_id, client_id, source_type, contract_version,
           columns_json, is_active, created_at, created_by, notes)
        VALUES ($id, $c, $s, $v, $cols, TRUE, $ts, $actor,
                'Generated by Data Contract Architect approval')
        """,
        {
            "id": contract_id,
            "c": client_id,
            "s": source_type,
            "v": next_version,
            "cols": columns_json,
            "ts": datetime.now(UTC),
            "actor": actor,
        },
    )
    return contract_id


def _register_gx_scaffold(
    *,
    warehouse: Warehouse,
    design_id: str,
    client_id: str,
    source_type: str,
    table_name: str,
    final_columns: list[dict[str, Any]],
    actor: str,
) -> str:
    """Insert a DQ_SUITES row carrying the agent's GX scaffold as
    a JSON expectation suite."""
    suite_id = str(uuid.uuid4())
    suite_name = f"contract_scaffold_{client_id}_{source_type.lower()}"

    # Build a minimal GX suite from the columns
    expectations: list[dict[str, Any]] = []
    for c in final_columns:
        name = c.get("name", "")
        if name.startswith("_"):
            continue
        nullable = bool(c.get("nullable", True))
        if not nullable:
            expectations.append(
                {
                    "expectation_type": "expect_column_values_to_not_be_null",
                    "kwargs": {"column": name},
                    "meta": {
                        "dq_dimension": "Completeness",
                        "severity": "HIGH",
                        "description": f"{name} must not be null per contract.",
                        "source": "contract_architect",
                    },
                }
            )
        # Format checks for known patterns
        if name.endswith("_npi") or name == "npi":
            expectations.append(
                {
                    "expectation_type": "expect_column_values_to_match_regex",
                    "kwargs": {"column": name, "regex": "^[0-9]{10}$"},
                    "meta": {
                        "dq_dimension": "Validity",
                        "severity": "HIGH",
                        "description": f"{name} must be a 10-digit NPI.",
                        "source": "contract_architect",
                    },
                }
            )
        if name.startswith("icd10"):
            expectations.append(
                {
                    "expectation_type": "expect_column_values_to_match_regex",
                    "kwargs": {
                        "column": name,
                        "regex": "^[A-TV-Z][0-9][0-9AB](\\.[0-9A-Z]{0,4})?$",
                    },
                    "meta": {
                        "dq_dimension": "Validity",
                        "severity": "MEDIUM",
                        "description": f"{name} must match ICD-10-CM format.",
                        "source": "contract_architect",
                    },
                }
            )
        if name == "cpt_code":
            expectations.append(
                {
                    "expectation_type": "expect_column_values_to_match_regex",
                    "kwargs": {"column": name, "regex": "^[0-9]{5}[A-Z]?$"},
                    "meta": {
                        "dq_dimension": "Validity",
                        "severity": "HIGH",
                        "description": "cpt_code must be 5 digits + optional alpha modifier.",
                        "source": "contract_architect",
                    },
                }
            )

    expectations_json = json.dumps(expectations)
    # Distinct DQ dimensions used across the expectations
    dimensions = sorted({e["meta"]["dq_dimension"] for e in expectations}) if expectations else []
    dq_dimensions_json = json.dumps(dimensions)

    warehouse.execute(
        f"""
        INSERT INTO {CONTROL_SCHEMA}.dq_suites
          (suite_id, client_id, suite_name, version, status, expectations,
           dq_dimensions, source_type, source, created_by, created_at,
           schema_fingerprint)
        VALUES ($sid, $c, $n, 1, 'PENDING_REVIEW', $exp,
                $dim, $stp, 'AGENT', $actor, $ts, NULL)
        """,
        {
            "sid": suite_id,
            "c": client_id,
            "n": suite_name,
            "exp": expectations_json,
            "dim": dq_dimensions_json,
            "stp": source_type,
            "actor": actor,
            "ts": datetime.now(UTC),
        },
    )
    return suite_id


def _build_dbt_silver_stub(
    *,
    client_id: str,
    source_type: str,
    table_name: str,
    columns: list[dict[str, Any]],
) -> str:
    """Generate a minimal dbt Silver model that selects + casts from Bronze.
    Operator extends with hub/sat/link logic — this is a starter."""
    bronze_schema = f"BRONZE_{client_id.upper()}"
    business_cols = [c for c in columns if not str(c.get("name", "")).startswith("_")]
    select_lines = [f"  cast({c['name']} as {c['type']}) as {c['name']}" for c in business_cols]
    return (
        f"-- AUTO-GENERATED by Data Contract Architect (Phase 14)\n"
        f"-- client     : {client_id}\n"
        f"-- source     : {source_type}\n"
        f"-- generated  : {datetime.now(UTC).isoformat()}\n"
        f"--\n"
        f"-- Starter Silver model. Extend with Data Vault hubs/sats/links\n"
        f"-- via Smart Mapper, or hand-roll your transformations here.\n"
        f"\n"
        f"{{{{ config(\n"
        f"    materialized='view',\n"
        f"    schema='silver_{client_id.lower()}',\n"
        f"    tags=['contract_architect', '{client_id}', '{source_type.lower()}']\n"
        f") }}}}\n"
        f"\n"
        f"select\n"
        + ",\n".join(select_lines)
        + ",\n  _load_dt,\n  _source_file,\n  _batch_id,\n  _record_source\n"
        + f"from {{{{ source('{bronze_schema.lower()}', '{table_name.lower()}') }}}}\n"
    )


def _build_default_vendor_spec_md(
    *,
    client_id: str,
    source_type: str,
    columns: list[dict[str, Any]],
) -> str:
    """Fallback markdown when the agent didn't generate a vendor_spec_md
    (rare, but defensive). Real version is produced by the agent itself."""
    rows = []
    for c in columns:
        if str(c.get("name", "")).startswith("_"):
            continue
        nullable = "Yes" if c.get("nullable") else "No"
        std = c.get("matches_standard") or ""
        rows.append(f"| `{c['name']}` | {c['type']} | {nullable} | {std} |")
    return (
        f"# Vendor Data Contract Spec — {client_id} {source_type}\n\n"
        f"Generated by DataLink Data Contract Architect. The vendor must adhere\n"
        f"to the structure below before submitting any data.\n\n"
        f"## Columns\n\n"
        f"| Column | Type | Nullable | Cited Standard |\n"
        f"|---|---|---|---|\n" + "\n".join(rows) + "\n\n"
        "## Audit Columns (auto-injected by DataLink Bronze ingest)\n\n"
        "`_load_dt`, `_source_file`, `_batch_id`, `_record_source`, "
        "`_load_type`, `_file_row_number`, `_record_hash`\n"
    )


# ============================================================================
# Internals
# ============================================================================


def _build_rag_query(mode: ContractMode, payload: dict[str, Any], source_type: str) -> str:
    """Build the natural-language query we feed to the RAG store.

    For FILE_DRIVEN: combine source_type + visible header columns.
    For CONTRACT_FIRST: use the operator's NL description directly.
    """
    if mode == ContractMode.FILE_DRIVEN:
        headers = list(payload.get("headers", []))
        return f"{source_type} feed with columns: {', '.join(headers[:20])}"
    nl = str(payload.get("nl_description", "")).strip()
    return nl or source_type


def _audit_log(
    *,
    warehouse: Warehouse,
    design_id: str,
    client_id: str,
    source_type: str,
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
        INSERT INTO {CONTROL_SCHEMA}.contract_design_audit_log
          (audit_id, design_id, client_id, source_type, action, actor,
           from_status, to_status, diff_summary, ai_reasoning,
           token_count, latency_ms, ts, notes)
        VALUES ($aid, $did, $cid, $stp, $act, $actor,
                $fs, $ts2, $diff, $reason,
                $tok, $lat, $ts, $notes)
        """,
        {
            "aid": str(uuid.uuid4()),
            "did": design_id,
            "cid": client_id,
            "stp": source_type.upper(),
            "act": action,
            "actor": actor,
            "fs": from_status,
            "ts2": to_status,
            "diff": json.dumps(diff_summary) if diff_summary else None,
            "reason": ai_reasoning,
            "tok": token_count,
            "lat": latency_ms,
            "ts": datetime.now(UTC),
            "notes": notes,
        },
    )
