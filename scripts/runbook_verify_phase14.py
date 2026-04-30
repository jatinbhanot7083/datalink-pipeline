"""Phase 14 verifier — Data Contract Architect end-to-end regression.

Assertion-based PASS/FAIL. Mirrors ``runbook_verify_phase{9,10,11,12}.py``.
Exits 1 on any failure.

Six sections:

  1. CONTROL DDL — all 4 Phase 14 tables exist with the right columns.
  2. Standards registry — at least the 6 industry standards loaded with
     non-zero chunk counts.
  3. RAG retrieval — a known query returns relevant chunks, distance
     monotonically increases.
  4. Agent contract validation — _extract_json + _validate accept good
     output and reject malformed.
  5. Audit-column injection — _inject_audit_columns adds 7 columns,
     idempotent on re-application.
  6. Approval flow artifacts — persist_design + approve_design produce
     all 5 artifacts (DDL file, contract row, GX suite, dbt stub,
     vendor spec MD + HTML).

Usage from inside the control_tower container::

    docker exec datalink-control-tower python3 scripts/runbook_verify_phase14.py

Exit 0 on green, 1 on any FAIL.
"""

from __future__ import annotations

import json
import os
import sys
import uuid
from pathlib import Path

# bootstrap path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from datalink.adapters.embeddings.router import get_embedder  # noqa: E402
from datalink.agents.contract_architect import (  # noqa: E402
    ContractArchitectAgent,
)
from datalink.memory import AgentMemoryStore  # noqa: E402
from datalink.quality.control import CONTROL_SCHEMA, create_control_tables  # noqa: E402
from datalink.ui._query import warehouse_ctx  # noqa: E402

PASS: list[str] = []
FAIL: list[str] = []


def passes(name: str) -> None:
    PASS.append(name)
    print(f"  PASS  {name}")


def fails(name: str, msg: str) -> None:
    FAIL.append(f"{name} — {msg}")
    print(f"  FAIL  {name} — {msg}")


def section(label: str) -> None:
    print()
    print(f"{'=' * 72}")
    print(f"  {label}")
    print(f"{'=' * 72}")


# ============================================================================
# 1. CONTROL DDL — all 4 Phase 14 tables present with right columns
# ============================================================================

section("1. CONTROL schema — Phase 14 DDL")

with warehouse_ctx(readonly=False) as wh:
    create_control_tables(wh)

    expected_tables = {
        "STANDARD_REGISTRY",
        "CUSTOM_STANDARDS_REGISTRY",
        "CONTRACT_DESIGNS",
        "CONTRACT_DESIGN_AUDIT_LOG",
    }
    rows = wh.query(
        f"SELECT table_name FROM information_schema.tables "
        f"WHERE table_schema = '{CONTROL_SCHEMA}' "
        f"AND table_name IN ('STANDARD_REGISTRY','CUSTOM_STANDARDS_REGISTRY',"
        f"'CONTRACT_DESIGNS','CONTRACT_DESIGN_AUDIT_LOG')"
    )
    actual = {r["table_name"] for r in rows}
    missing = expected_tables - actual
    if missing:
        fails("phase 14 tables exist", f"missing: {sorted(missing)}")
    else:
        passes("4 phase 14 CONTROL tables present")

    # Spot-check key columns
    cols = wh.query(
        f"SELECT column_name FROM information_schema.columns "
        f"WHERE table_schema='{CONTROL_SCHEMA}' AND table_name='CONTRACT_DESIGNS'"
    )
    contract_cols = {c["column_name"].upper() for c in cols}
    must_have = {
        "DESIGN_ID",
        "CLIENT_ID",
        "SOURCE_TYPE",
        "STATUS",
        "MODE",
        "ANCHORED_STANDARDS",
        "TEMPERATURE",
        "STRICTNESS",
        "PROPOSED_DDL",
        "FINAL_DDL",
        "GX_SUITE_ID",
        "CONTRACT_ID",
        "VENDOR_SPEC_MD",
    }
    missing_cols = must_have - contract_cols
    if missing_cols:
        fails("contract_designs schema", f"missing columns: {sorted(missing_cols)}")
    else:
        passes("contract_designs has all required columns")


# ============================================================================
# 2. Standards registry — at least 6 industry standards with chunks
# ============================================================================

section("2. Standards registry + corpora")

with warehouse_ctx(readonly=True) as wh:
    rows = wh.query(
        f"SELECT code, chunk_count FROM {CONTROL_SCHEMA}.standard_registry "
        f"WHERE is_industry = TRUE ORDER BY code"
    )
    expected_codes = {"fhir-r4", "x12", "ncpdp-d0", "cms", "dv2", "hedis"}
    actual_codes = {r["code"] for r in rows}
    missing_codes = expected_codes - actual_codes
    if missing_codes:
        fails(
            "6 industry standards loaded",
            f"missing: {sorted(missing_codes)} (run scripts/load_industry_standards.py)",
        )
    else:
        passes(f"all 6 industry standards registered: {sorted(actual_codes)}")

    zero_chunk = [r["code"] for r in rows if (r.get("chunk_count") or 0) == 0]
    if zero_chunk:
        fails("each standard has chunks", f"zero-chunk: {zero_chunk}")
    else:
        total_chunks = sum(r.get("chunk_count") or 0 for r in rows)
        passes(f"all standards have chunks (total={total_chunks})")


# ============================================================================
# 3. RAG retrieval — known query returns relevant chunks with monotonic distance
# ============================================================================

section("3. RAG retrieval against pgvector")

try:
    memory = AgentMemoryStore(embedder=get_embedder())
    hits = memory.query_similar_standard_chunks(
        query_text="claim must have a 10 digit NPI for the rendering provider",
        anchored_codes=("fhir-r4", "x12"),
        k=3,
    )
    if not hits:
        fails("RAG returns hits", "0 chunks retrieved")
    else:
        passes(f"retrieved {len(hits)} chunks for NPI/claim query")
        distances = [h.distance for h in hits]
        if distances == sorted(distances):
            passes("distances monotonically non-decreasing")
        else:
            fails(
                "distance ordering",
                f"distances should be sorted: got {distances}",
            )
        codes_seen = {h.standard_code for h in hits}
        if codes_seen.issubset({"fhir-r4", "x12"}):
            passes(f"results stay within anchored standards: {sorted(codes_seen)}")
        else:
            fails(
                "anchor scoping",
                f"got chunks from non-anchored standards: {codes_seen - {'fhir-r4', 'x12'}}",
            )
except Exception as e:
    fails("RAG retrieval", f"{type(e).__name__}: {e}")


# ============================================================================
# 4. Agent contract validation — JSON parse + schema enforcement
# ============================================================================

section("4. Agent JSON contract validation")

# Good JSON should pass
good = {
    "proposed_table_name": "RAW_TEST",
    "proposed_columns": [
        {
            "name": "id_col",
            "type": "VARCHAR",
            "nullable": False,
            "rationale": "primary key",
        }
    ],
    "proposed_ddl": "CREATE TABLE {schema}.RAW_TEST (id_col VARCHAR NOT NULL);",
    "standard_match_scores": {"fhir-r4": 0.9},
    "rationale": "test rationale",
}
try:
    ContractArchitectAgent._validate(good)
    passes("validate accepts well-formed proposal")
except Exception as e:
    fails("validate accepts good", str(e))

# Bad JSON — missing required key
bad_missing = {**good}
del bad_missing["proposed_ddl"]
try:
    ContractArchitectAgent._validate(bad_missing)
    fails("validate rejects missing keys", "should have raised but didn't")
except ValueError:
    passes("validate rejects missing required keys")

# Bad JSON — invalid type
bad_type = json.loads(json.dumps(good))
bad_type["proposed_columns"][0]["type"] = "VARCHAR2(NOT_A_TYPE)"
try:
    ContractArchitectAgent._validate(bad_type)
    fails("validate rejects bad SQL type", "should have raised but didn't")
except ValueError:
    passes("validate rejects unsupported SQL type")

# Bad DDL — missing {schema}
bad_ddl = json.loads(json.dumps(good))
bad_ddl["proposed_ddl"] = "CREATE TABLE BRONZE_AETNA.RAW_TEST (...)"
try:
    ContractArchitectAgent._validate(bad_ddl)
    fails("validate requires {schema} placeholder", "should have raised but didn't")
except ValueError:
    passes("validate requires {schema} placeholder in DDL")

# JSON extraction from markdown-fenced output
fenced = """```json
{"proposed_table_name": "X", "proposed_columns": [{"name":"a","type":"VARCHAR","nullable":true,"rationale":"r"}], "proposed_ddl":"CREATE TABLE {schema}.X (a VARCHAR);", "standard_match_scores":{}, "rationale":"r"}
```
"""
try:
    obj = ContractArchitectAgent._extract_json(fenced)
    if obj.get("proposed_table_name") == "X":
        passes("extract_json strips markdown fences")
    else:
        fails("extract_json fence stripping", "wrong content")
except Exception as e:
    fails("extract_json fence stripping", str(e))


# ============================================================================
# 5. Audit column injection — adds 7 cols, idempotent
# ============================================================================

section("5. Audit-column injection")

base_ddl = "CREATE TABLE {schema}.RAW_X (id VARCHAR, val DECIMAL);"
injected = ContractArchitectAgent._inject_audit_columns(base_ddl)
required_audit = [
    "_load_dt",
    "_source_file",
    "_batch_id",
    "_record_source",
    "_load_type",
    "_file_row_number",
    "_record_hash",
]
missing = [c for c in required_audit if c not in injected]
if missing:
    fails("injection adds 7 audit cols", f"missing: {missing}")
else:
    passes("injection adds all 7 audit columns")

# Idempotent — running again should not duplicate
twice = ContractArchitectAgent._inject_audit_columns(injected)
if twice.count("_load_dt") == injected.count("_load_dt"):
    passes("injection idempotent on re-application")
else:
    fails(
        "injection idempotent",
        f"_load_dt count went from {injected.count('_load_dt')} to {twice.count('_load_dt')}",
    )


# ============================================================================
# 6. Approval flow artifacts — design row + 5 artifacts
# ============================================================================

section("6. Approval flow artifacts (uses real LLM + RAG)")

# Skip if no Anthropic key (CI / offline)
if not os.environ.get("ANTHROPIC_API_KEY") or os.environ.get(
    "DL_ADAPTERS__LLM__TYPE", ""
).lower() not in ("anthropic",):
    print("  SKIP — section 6 requires DL_ADAPTERS__LLM__TYPE=anthropic + ANTHROPIC_API_KEY")
else:
    try:
        from datalink.agents.contract_architect import (
            approve_design,
            persist_design,
            propose_contract,
        )
        from datalink.agents.llm_router import get_llm
        from datalink.config.loader import load_settings

        llm = get_llm(load_settings())
        memory = AgentMemoryStore(embedder=get_embedder())
        verifier_client = f"phase14_verify_{uuid.uuid4().hex[:6]}"
        with warehouse_ctx(readonly=False) as wh:
            proposal = propose_contract(
                llm=llm,
                warehouse=wh,
                memory=memory,
                client_id=verifier_client,
                source_type="CLAIMS",
                mode="FILE_DRIVEN",
                anchored_standards=["fhir-r4"],
                payload={
                    "file_name": "verify_sample.csv",
                    "headers": ["claim_id", "member_id", "npi"],
                    "sample_values": [["C1", "M1", "1234567890"]],
                    "detected_format": "CSV",
                },
                temperature=0.0,
                grounding_k=2,
                strictness=0.5,
                actor="phase14_verifier",
            )
            passes(f"propose_contract returned {len(proposal.proposed_columns)} columns")

            design_id = persist_design(
                warehouse=wh,
                proposal=proposal,
                client_id=verifier_client,
                source_type="CLAIMS",
                anchored_standards=["fhir-r4"],
                payload={"file_name": "verify_sample.csv"},
                actor="phase14_verifier",
                approval_mode="AUTO_APPROVE",
            )
            passes(f"persist_design wrote row design_id={design_id[:8]}…")

            summary = approve_design(
                warehouse=wh,
                design_id=design_id,
                actor="phase14_verifier",
                notes="phase 14 verifier",
            )
            if summary["status"] != "APPROVED":
                fails("approve_design", f"status={summary['status']}")
            else:
                passes(f"approve_design status=APPROVED for {design_id[:8]}…")

            # All 5 artifacts present?
            paths = summary["artifact_paths"]
            for art_key in ("bronze_ddl", "dbt_silver", "vendor_spec_md", "vendor_spec_html"):
                rel = paths.get(art_key)
                if not rel:
                    fails(f"artifact:{art_key}", "not present in artifact_paths")
                    continue
                full = ROOT / rel
                if full.exists() and full.stat().st_size > 0:
                    passes(f"artifact:{art_key} -> {rel} ({full.stat().st_size} bytes)")
                else:
                    fails(f"artifact:{art_key}", f"file missing or empty: {full}")

            # Snowflake-side artifacts
            if summary.get("contract_id"):
                rows_c = wh.query(
                    f"SELECT contract_id FROM {CONTROL_SCHEMA}.source_schema_contracts "
                    f"WHERE contract_id = $cid",
                    {"cid": summary["contract_id"]},
                )
                if rows_c:
                    passes(f"source_schema_contracts row inserted ({summary['contract_id'][:8]}…)")
                else:
                    fails(
                        "source_schema_contracts row",
                        f"contract_id {summary['contract_id'][:8]} not in DB",
                    )

            if summary.get("gx_suite_id"):
                rows_s = wh.query(
                    f"SELECT suite_id, status FROM {CONTROL_SCHEMA}.dq_suites "
                    f"WHERE suite_id = $sid",
                    {"sid": summary["gx_suite_id"]},
                )
                if rows_s and rows_s[0].get("status") == "PENDING_REVIEW":
                    passes(f"dq_suites row PENDING_REVIEW ({summary['gx_suite_id'][:8]}…)")
                else:
                    fails(
                        "dq_suites row",
                        f"suite_id {summary['gx_suite_id'][:8]} not in DB or wrong status",
                    )

            # Audit log entries — should have PROPOSED + APPROVED
            audits = wh.query(
                f"SELECT action FROM {CONTROL_SCHEMA}.contract_design_audit_log "
                f"WHERE design_id = $d ORDER BY ts",
                {"d": design_id},
            )
            actions = [a["action"] for a in audits]
            if "PROPOSED" in actions and "APPROVED" in actions:
                passes(f"audit log: {actions}")
            else:
                fails("audit log entries", f"got: {actions}")
    except Exception as e:
        import traceback

        traceback.print_exc()
        fails("section 6 end-to-end", f"{type(e).__name__}: {e}")


# ============================================================================
# Tally + exit
# ============================================================================

print()
print("=" * 72)
print(f"  PHASE 14 VERIFIER — {len(PASS)} PASS / {len(FAIL)} FAIL")
print("=" * 72)
if FAIL:
    print("FAILURES:")
    for f in FAIL:
        print(f"  - {f}")
    sys.exit(1)
print("All assertions green ✅")
sys.exit(0)
