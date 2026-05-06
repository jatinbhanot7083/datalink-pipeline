"""Phase 15 + 15.5 verifier — Pipeline Architect end-to-end regression.

Assertion-based PASS/FAIL. Mirrors runbook_verify_phase{10,11,12,13,14}.py.
Exits 1 on any failure.

Sections:

  1. CONTROL DDL          — 13 Phase 15 + 15.5 tables exist (8 base + 5 new).
  2. Catalog seed         — 33 datasets + 943 fields + 61+ routing rules.
  3. Builder determinism  — same input → same output for DDL / dbt / DAG.
  4. Override semantics   — apply_overrides + diff_against_global correct.
  5. Agent end-to-end     — propose_pipeline returns a valid proposal
                            against live Snowflake + real Anthropic.
  6. Persist + deploy     — full proposal → persist → deploy emits 5
                            artifacts (4 files + 1 GX suite row).
  7. UI surface           — /Pipeline_Architect serves HTTP 200.
  8. DAG callables        — generated DAGs import the 5 task stubs.
  9. Phase 15.5 tables    — Gold registry, Bronze→Gold mappings,
                            Silver pattern recommendations exist + writable.

Usage::

    set -a && source .env && set +a
    python3 scripts/runbook_verify_phase15.py

Exit 0 on green, 1 on any FAIL.
"""

from __future__ import annotations

import importlib
import json
import py_compile
import re as _re
import sys
import urllib.request
import uuid as _uuid
from datetime import UTC
from datetime import datetime as _dt
from pathlib import Path

# bootstrap path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from datalink.adapters.factory import build_adapters  # noqa: E402
from datalink.agents.llm_router import get_llm  # noqa: E402
from datalink.agents.pipeline_architect import (  # noqa: E402
    approve_and_deploy,
    persist_proposal,
    propose_pipeline,
)
from datalink.agents.pipeline_architect.builders import (  # noqa: E402
    apply_overrides,
    build_airflow_dag,
    build_gold_ddl,
    build_gx_suite_scaffold,
    build_routing_plan,
    build_silver_dbt_sql,
    diff_against_global,
)
from datalink.config.loader import load_settings  # noqa: E402
from datalink.quality.control import CONTROL_SCHEMA, create_control_tables  # noqa: E402

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


# Build live infra once for all sections that need it.
settings = load_settings()
adapters = build_adapters(settings)
WH = adapters.warehouse

# ============================================================================
# 1. CONTROL DDL — 8 Phase 15 tables present with expected column counts
# ============================================================================

section("1. CONTROL schema — Phase 15 DDL")

create_control_tables(WH)

EXPECTED_TABLES: dict[str, int] = {
    # Phase 15 base — renamed in 15.5: gold_catalog → bronze_catalog
    "GLOBAL_BRONZE_CATALOG_DATASETS": 16,  # +1 default_anchor in 15.5
    "GLOBAL_BRONZE_CATALOG_FIELDS": 16,
    "PIPELINE_TEMPLATES": 17,
    "CLIENT_PIPELINE_INSTANCES": 34,
    "CLIENT_FIELD_OVERRIDES": 11,
    "ONPREM_ROUTING_RULES": 10,
    "CLIENT_ROUTING_OVERRIDES": 10,
    "PIPELINE_INSTANCE_AUDIT_LOG": 14,
    # Phase 15.5 additions — Gold schema registry + Silver pattern recs
    "GLOBAL_GOLD_SCHEMA_DATASETS": 19,
    "GLOBAL_GOLD_SCHEMA_FIELDS": 14,
    "BRONZE_TO_GOLD_MAPPINGS": 11,
    "GOLD_SCHEMA_AUDIT_LOG": 13,
    "SILVER_PATTERN_RECOMMENDATIONS": 17,
    # Phase 15.7 additions — Overflow safety + Greenfield ingestion
    "BRONZE_OVERFLOW_LOG": 16,
    "GREENFIELD_DATASET_PROPOSALS": 24,
}

for table, min_cols in EXPECTED_TABLES.items():
    rows = WH.query(
        "SELECT COUNT(*) AS c FROM INFORMATION_SCHEMA.COLUMNS "
        "WHERE TABLE_SCHEMA = $s AND TABLE_NAME = $t",
        {"s": CONTROL_SCHEMA, "t": table},
    )
    cnt = int(rows[0]["c"]) if rows else 0
    if cnt >= min_cols:
        passes(f"  {table:<35} cols >= {min_cols} (got {cnt})")
    else:
        fails(f"  {table:<35} cols", f"expected >= {min_cols}, got {cnt}")


# ============================================================================
# 2. Catalog seed — datasets + fields + routing rules
# ============================================================================

section("2. Global Gold Catalog — seed counts")

ds_count = int(
    WH.query(
        f"SELECT COUNT(*) AS c FROM {CONTROL_SCHEMA}.global_bronze_catalog_datasets WHERE is_active = TRUE"
    )[0]["c"]
)
if ds_count >= 30:
    passes(f"datasets seeded: {ds_count} (expected >= 30)")
else:
    fails("datasets seeded", f"got {ds_count}, expected >= 30")

field_count = int(
    WH.query(f"SELECT COUNT(*) AS c FROM {CONTROL_SCHEMA}.global_bronze_catalog_fields")[0]["c"]
)
if field_count >= 900:
    passes(f"fields seeded: {field_count} (expected >= 900)")
else:
    fails("fields seeded", f"got {field_count}, expected >= 900")

rules_count = int(
    WH.query(
        f"SELECT COUNT(*) AS c FROM {CONTROL_SCHEMA}.onprem_routing_rules WHERE is_default = TRUE AND is_active = TRUE"
    )[0]["c"]
)
if rules_count >= 50:
    passes(f"routing rules seeded: {rules_count} (expected >= 50)")
else:
    fails("routing rules seeded", f"got {rules_count}, expected >= 50")

# Membership has 41 fields per the catalog
membership_field_count = int(
    WH.query(
        f"SELECT COUNT(*) AS c FROM {CONTROL_SCHEMA}.global_bronze_catalog_fields WHERE dataset_code = 'membership'"
    )[0]["c"]
)
if membership_field_count == 41:
    passes(f"Membership has 41 fields (got {membership_field_count})")
else:
    fails("Membership field count", f"got {membership_field_count}, expected 41")

# Membership has Used-by={CC,E360,RBN,EC,ESV} — five routing rules
mem_rules = int(
    WH.query(
        f"SELECT COUNT(*) AS c FROM {CONTROL_SCHEMA}.onprem_routing_rules "
        f"WHERE dataset_code = 'membership' AND is_default = TRUE"
    )[0]["c"]
)
if mem_rules == 5:
    passes(f"Membership has 5 default routing rules (got {mem_rules})")
else:
    fails("Membership routing rule count", f"got {mem_rules}, expected 5")


# ============================================================================
# 3. Builder determinism — same input → byte-identical output
# ============================================================================

section("3. Builders — deterministic outputs")

ds_rows = WH.query(
    f"SELECT *, bronze_column_name AS gold_column_name "
    f"FROM {CONTROL_SCHEMA}.global_bronze_catalog_fields "
    f"WHERE dataset_code = 'membership' ORDER BY field_order"
)
catalog_fields = list(ds_rows)

resolved1 = apply_overrides(catalog_fields, [])
resolved2 = apply_overrides(catalog_fields, [])
if [r.gold_column_name for r in resolved1] == [r.gold_column_name for r in resolved2]:
    passes("apply_overrides([]) is order-stable")
else:
    fails("apply_overrides", "order changed across calls")

ddl1 = build_gold_ddl(client_id="aetna", dataset_code="membership", resolved_fields=resolved1)
ddl2 = build_gold_ddl(client_id="aetna", dataset_code="membership", resolved_fields=resolved1)


def _strip_ts(s: str) -> str:
    return _re.sub(r"-- Generated at: .+", "-- Generated at: <ts>", s)


if _strip_ts(ddl1) == _strip_ts(ddl2):
    passes("build_gold_ddl deterministic (same input → same output)")
else:
    fails("build_gold_ddl deterministic", "outputs differ")

if "GOLD_AETNA.membership" in ddl1 and "_load_dt" in ddl1:
    passes("Gold DDL contains expected schema/audit-col tokens")
else:
    fails("Gold DDL content", "missing GOLD_AETNA.membership or _load_dt audit col")

silver_sql = build_silver_dbt_sql(
    client_id="aetna",
    dataset_code="membership",
    resolved_fields=resolved1,
    bronze_anchor="FLAT_FILE",
)
if "TRY_CAST" in silver_sql and "BRONZE_AETNA.raw_membership" in silver_sql:
    passes("Silver dbt SQL contains TRY_CAST + bronze source")
else:
    fails("Silver dbt SQL", "missing TRY_CAST or bronze source ref")

dag_py = build_airflow_dag(
    client_id="aetna",
    dataset_code="membership",
    bronze_anchor="FLAT_FILE",
    schedule_cron="0 4 * * *",
    onprem_targets=[{"downstream_product": "CC", "target_system": "postgres"}],
    catalog_version=1,
)
if "aetna_membership_pipeline" in dag_py and "bronze_land_task" in dag_py:
    passes("Airflow DAG contains dag_id + task callables")
else:
    fails("Airflow DAG", "missing dag_id or task callable refs")

gx_suite = build_gx_suite_scaffold(
    client_id="aetna", dataset_code="membership", resolved_fields=resolved1
)
if int(gx_suite.get("expectation_count", 0)) >= 30:
    passes(f"GX suite auto-anchored {gx_suite['expectation_count']} expectations")
else:
    fails("GX expectation count", f"got {gx_suite.get('expectation_count')}, expected >= 30")

if {"Completeness", "Uniqueness", "Validity"}.issubset(set(gx_suite.get("dq_dimensions", []))):
    passes("GX suite covers Completeness + Uniqueness + Validity dimensions")
else:
    fails("GX dimensions", f"got {gx_suite.get('dq_dimensions')}")


# ============================================================================
# 4. Override semantics — diff against global, ADD_FIELD, RELAX_NULLABLE
# ============================================================================

section("4. Override semantics")

overrides = [
    {
        "gold_column_name": "member_phone",
        "override_kind": "RELAX_NULLABLE",
        "override_value": json.dumps({"requirement": "Optional"}),
        "rationale": "Aetna sends some members without phone",
    },
    {
        "gold_column_name": "aetna_segment_id",
        "override_kind": "ADD_FIELD",
        "override_value": json.dumps(
            {
                "gold_column_name": "aetna_segment_id",
                "requirement": "Required",
                "logical_type": "TEXT",
                "description": "Aetna-specific segment identifier",
            }
        ),
        "rationale": "Aetna's product spec adds this column",
    },
]

resolved_with_ov = apply_overrides(catalog_fields, overrides)
phone = next((r for r in resolved_with_ov if r.gold_column_name == "member_phone"), None)
if phone and phone.requirement == "Optional":
    passes("RELAX_NULLABLE flips Required → Optional")
else:
    fails("RELAX_NULLABLE", f"phone requirement = {phone.requirement if phone else 'MISSING'}")

added = next((r for r in resolved_with_ov if r.gold_column_name == "aetna_segment_id"), None)
if added and added.is_added_by_override:
    passes("ADD_FIELD appends override-only column")
else:
    fails("ADD_FIELD", "aetna_segment_id missing from resolved fields")

deviations = diff_against_global(catalog_fields, overrides)
if len(deviations) == 2 and {d.kind for d in deviations} == {"RELAX_NULLABLE", "ADD_FIELD"}:
    passes("diff_against_global produces 2 deviations of expected kinds")
else:
    fails("deviation diff", f"got {[d.kind for d in deviations]}")

# Routing plan + override interaction
default_routing = list(
    WH.query(
        f"SELECT * FROM {CONTROL_SCHEMA}.onprem_routing_rules "
        f"WHERE dataset_code = 'membership' AND is_default = TRUE"
    )
)
client_routing_overrides = [
    {
        "client_id": "aetna",
        "dataset_code": "membership",
        "downstream_product": "CC",
        "action": "DISABLE",
        "rationale": "Aetna doesn't license Care Compass",
    }
]
plan = build_routing_plan(
    client_id="aetna",
    dataset_code="membership",
    default_rules=default_routing,
    client_overrides=client_routing_overrides,
)
disabled_count = sum(1 for p in plan if p.action.startswith("DISABLED"))
if disabled_count == 1:
    passes("Routing override DISABLE applied correctly")
else:
    fails("Routing override", f"expected 1 DISABLED entry, got {disabled_count}")


# ============================================================================
# 5. Agent end-to-end — real Anthropic + real Snowflake
# ============================================================================

section("5. Agent — propose_pipeline against live infra")

llm = get_llm(settings)

proposal = propose_pipeline(
    llm=llm,
    warehouse=WH,
    client_id="phase15_verify",
    dataset_code="membership",
    bronze_anchor="FLAT_FILE",
    decision_mode="AUTO",
    actor="phase15_verifier",
)

required_keys = {
    "client_id",
    "dataset_code",
    "decision_mode",
    "bronze_schema",
    "silver_schema",
    "gold_schema",
    "gold_ddl",
    "silver_dbt_sql",
    "gold_dbt_sql",
    "airflow_dag_py",
    "gx_suite",
    "routing_plan",
    "executive_summary",
    "clone_recommendation",
    "rationale",
}
missing = required_keys - set(proposal.keys())
if not missing:
    passes("Proposal contains all required top-level keys")
else:
    fails("proposal keys", f"missing {sorted(missing)}")

if int(proposal.get("resolved_field_count", 0)) == 41:
    passes("Proposal resolved 41 Membership fields")
else:
    fails("resolved_field_count", f"got {proposal.get('resolved_field_count')}")

if int(proposal.get("tokens_used", 0)) > 0:
    passes(f"LLM was invoked (tokens_used={proposal['tokens_used']})")
else:
    fails("tokens_used", "LLM not invoked or stub mode (Phase 15 must use real LLM)")

if proposal.get("decision_mode") in {"CLONE", "BUILD"}:
    passes(f"decision_mode = {proposal['decision_mode']}")
else:
    fails("decision_mode", f"got {proposal.get('decision_mode')}")


# ============================================================================
# 6. Persist + deploy — full HITL flow
# ============================================================================

section("6. Persist + deploy")

instance_id = persist_proposal(
    warehouse=WH,
    proposal=proposal,
    actor="phase15_verifier",
    notes="phase15 verifier",
)
inst_rows = WH.query(
    f"SELECT status, deviation_count FROM {CONTROL_SCHEMA}.client_pipeline_instances WHERE instance_id = $i",
    {"i": instance_id},
)
if inst_rows and inst_rows[0]["status"] == "PENDING_REVIEW":
    passes("persist_proposal -> PENDING_REVIEW row created")
else:
    fails("persist_proposal status", f"got {inst_rows[0]['status'] if inst_rows else 'MISSING'}")

deploy_result = approve_and_deploy(
    warehouse=WH,
    instance_id=instance_id,
    proposal=proposal,
    actor="phase15_verifier",
    notes="phase15 verifier deploy",
)

artifact_paths = deploy_result.get("artifact_paths") or {}
expected_artifacts = {"gold_ddl", "silver_dbt", "gold_dbt", "airflow_dag"}
if expected_artifacts.issubset(set(artifact_paths.keys())):
    passes(f"approve_and_deploy emitted all 4 file artifacts: {sorted(artifact_paths.keys())}")
else:
    fails(
        "deploy artifacts",
        f"missing: {expected_artifacts - set(artifact_paths.keys())}",
    )

REPO = ROOT
all_present = True
for label, rel in artifact_paths.items():
    # Phase 15.7: silver_dbt may be a comma-separated list of paths when
    # Gold-LIVE drives DV2 multi-file generation. Split + verify each.
    rels = [r.strip() for r in rel.split(",")] if "," in rel else [rel]
    for r in rels:
        p = REPO / r
        if not p.exists() or p.stat().st_size <= 0:
            all_present = False
            fails(f"artifact file missing: {label}", str(p))
if all_present:
    file_count = sum(len(v.split(",")) if "," in v else 1 for v in artifact_paths.values())
    passes(
        f"All {file_count} artifact files (across {len(artifact_paths)} kinds) exist on disk with content"
    )

# DB-side: instance LIVE, gx_suite_id present
inst_rows = WH.query(
    f"SELECT status, gx_suite_id, dag_uri FROM {CONTROL_SCHEMA}.client_pipeline_instances WHERE instance_id = $i",
    {"i": instance_id},
)
if inst_rows and inst_rows[0]["status"] == "LIVE" and inst_rows[0]["gx_suite_id"]:
    passes(f"Instance flipped to LIVE with gx_suite_id = {inst_rows[0]['gx_suite_id'][:8]}…")
else:
    fails("Instance LIVE status", f"got {inst_rows[0] if inst_rows else 'MISSING'}")

# GX suite registered as LIVE
gx_id = inst_rows[0]["gx_suite_id"] if inst_rows else None
if gx_id:
    suite_rows = WH.query(
        f"SELECT status, version FROM {CONTROL_SCHEMA}.dq_suites WHERE suite_id = $s",
        {"s": gx_id},
    )
    if suite_rows and suite_rows[0]["status"] == "LIVE":
        passes(f"dq_suites row LIVE (version {suite_rows[0]['version']})")
    else:
        fails("dq_suites status", f"got {suite_rows[0] if suite_rows else 'MISSING'}")

# Audit log: PROPOSED + DEPLOYED
audit_rows = WH.query(
    f"SELECT action FROM {CONTROL_SCHEMA}.pipeline_instance_audit_log WHERE instance_id = $i ORDER BY ts",
    {"i": instance_id},
)
actions = [r["action"] for r in audit_rows]
if any(a in actions for a in ("PROPOSED", "CLONED")) and "DEPLOYED" in actions:
    passes(f"Audit log captured proposal + deploy events: {actions}")
else:
    fails("audit log entries", f"got {actions}")


WH.execute(
    f"UPDATE {CONTROL_SCHEMA}.client_pipeline_instances "
    f"SET status = 'ARCHIVED', archived_at = $ts WHERE instance_id = $i",
    {"i": instance_id, "ts": _dt.now(UTC)},
)
passes("Verifier instance archived (cleanup)")


# ============================================================================
# 7. UI surface — page serves HTTP 200
# ============================================================================

section("7. UI page — /Pipeline_Architect")


try:
    with urllib.request.urlopen("http://localhost:8000/Pipeline_Architect", timeout=10) as resp:
        if resp.status == 200:
            passes(f"/Pipeline_Architect serves {resp.status}")
        else:
            fails("/Pipeline_Architect status", f"got HTTP {resp.status}")
except Exception as e:
    fails("/Pipeline_Architect", f"unreachable: {e}")


# ============================================================================
# 8. DAG callables — generated DAG imports + compiles
# ============================================================================

section("8. Generated DAG — task callables present + DAG compiles")


try:
    tasks_mod = importlib.import_module("datalink.orchestration.tasks")
    expected_tasks = [
        "bronze_land_task",
        "bronze_validate_task",
        "silver_dbt_task",
        "gold_dbt_task",
        "onprem_push_task",
    ]
    missing_tasks = [t for t in expected_tasks if not hasattr(tasks_mod, t)]
    if not missing_tasks:
        passes(f"All 5 Phase 15 task callables importable: {expected_tasks}")
    else:
        fails("task callables", f"missing: {missing_tasks}")
except Exception as e:
    fails("orchestration.tasks import", str(e))

# Compile any Phase 15-generated DAG sitting in dags/
dags_dir = ROOT / "dags"
phase15_dags = [
    p
    for p in dags_dir.glob("*_pipeline.py")
    if p.read_text(encoding="utf-8", errors="ignore").startswith(
        '"""Auto-generated Airflow DAG — Phase 15'
    )
]
if phase15_dags:
    compiled = 0
    for d in phase15_dags:
        try:
            py_compile.compile(str(d), doraise=True)
            compiled += 1
        except py_compile.PyCompileError as e:
            fails(f"DAG compile {d.name}", str(e))
    if compiled == len(phase15_dags):
        passes(f"All {compiled} Phase 15-generated DAG(s) compile")
else:
    print("    (no Phase 15-generated DAGs in dags/ yet — skipping compile check)")


# ============================================================================
# 9. Phase 15.5 — Gold registry + Bronze→Gold mappings + Silver pattern recs
# ============================================================================

section("9. Phase 15.5 — Gold registry round-trip")

# 9a. Bronze datasets carry default_anchor populated post-migration
default_anchors = WH.query(
    f"SELECT default_anchor, COUNT(*) AS c "
    f"FROM {CONTROL_SCHEMA}.global_bronze_catalog_datasets "
    f"GROUP BY default_anchor"
)
anchors_seen = {row["default_anchor"]: int(row["c"]) for row in default_anchors}
if anchors_seen.get("CATALOG_ANCHOR", 0) >= 30:
    passes(f"default_anchor column populated: {anchors_seen}")
else:
    fails("default_anchor column", f"got {anchors_seen}; expected CATALOG_ANCHOR on most datasets")

# 9b. Gold registry round-trip — insert + read + delete
verify_gold_id = str(_uuid.uuid4())
verify_field_id = str(_uuid.uuid4())
verify_mapping_id = str(_uuid.uuid4())
verify_audit_id = str(_uuid.uuid4())
verify_rec_id = str(_uuid.uuid4())
try:
    WH.execute(
        f"INSERT INTO {CONTROL_SCHEMA}.global_gold_schema_datasets "
        f"(gold_dataset_id, dataset_code, gold_table_name, version, status, "
        f" gold_anchor, source, created_by) "
        f"VALUES ($id, 'membership', 'member', 1, 'DRAFT', "
        f" 'FHIR_R4_ANCHOR', 'AI_CONSTRUCTED', 'phase15_5_verifier')",
        {"id": verify_gold_id},
    )
    rows = list(
        WH.query(
            f"SELECT status, gold_anchor, source FROM {CONTROL_SCHEMA}.global_gold_schema_datasets "
            f"WHERE gold_dataset_id = $id",
            {"id": verify_gold_id},
        )
    )
    if rows and rows[0]["status"] == "DRAFT" and rows[0]["gold_anchor"] == "FHIR_R4_ANCHOR":
        passes("global_gold_schema_datasets accepts INSERT + reads back correctly")
    else:
        fails("gold_schema_datasets", f"row not as expected: {rows}")

    WH.execute(
        f"INSERT INTO {CONTROL_SCHEMA}.global_gold_schema_fields "
        f"(gold_field_id, gold_dataset_id, dataset_code, column_order, "
        f" gold_column_name, logical_type, nullable, is_business_key) "
        f"VALUES ($f, $g, 'membership', 1, 'member_id', 'TEXT', FALSE, TRUE)",
        {"f": verify_field_id, "g": verify_gold_id},
    )
    field_rows = list(
        WH.query(
            f"SELECT gold_column_name, is_business_key "
            f"FROM {CONTROL_SCHEMA}.global_gold_schema_fields "
            f"WHERE gold_field_id = $f",
            {"f": verify_field_id},
        )
    )
    if field_rows and field_rows[0]["gold_column_name"] == "member_id":
        passes("global_gold_schema_fields accepts INSERT + reads back correctly")
    else:
        fails("gold_schema_fields", f"row not as expected: {field_rows}")

    WH.execute(
        f"INSERT INTO {CONTROL_SCHEMA}.bronze_to_gold_mappings "
        f"(mapping_id, gold_field_id, gold_dataset_id, gold_column_name, "
        f" bronze_source_columns, transform_kind, transform_sql, created_by) "
        f"VALUES ($m, $f, $g, 'member_id', "
        f" '[\"member_card_id\",\"member_medicare_id\"]', 'COALESCE', "
        f" 'COALESCE(member_card_id, member_medicare_id)', 'phase15_5_verifier')",
        {"m": verify_mapping_id, "f": verify_field_id, "g": verify_gold_id},
    )
    mapping_rows = list(
        WH.query(
            f"SELECT transform_kind, transform_sql "
            f"FROM {CONTROL_SCHEMA}.bronze_to_gold_mappings "
            f"WHERE mapping_id = $m",
            {"m": verify_mapping_id},
        )
    )
    if mapping_rows and mapping_rows[0]["transform_kind"] == "COALESCE":
        passes("bronze_to_gold_mappings accepts INSERT + reads back correctly")
    else:
        fails("bronze_to_gold_mappings", f"row not as expected: {mapping_rows}")

    WH.execute(
        f"INSERT INTO {CONTROL_SCHEMA}.gold_schema_audit_log "
        f"(audit_id, gold_dataset_id, dataset_code, action, actor, to_status) "
        f"VALUES ($a, $g, 'membership', 'PROPOSED', 'phase15_5_verifier', 'DRAFT')",
        {"a": verify_audit_id, "g": verify_gold_id},
    )
    audit_rows = list(
        WH.query(
            f"SELECT action, to_status FROM {CONTROL_SCHEMA}.gold_schema_audit_log "
            f"WHERE audit_id = $a",
            {"a": verify_audit_id},
        )
    )
    if audit_rows and audit_rows[0]["action"] == "PROPOSED":
        passes("gold_schema_audit_log accepts INSERT + reads back correctly")
    else:
        fails("gold_schema_audit_log", f"row not as expected: {audit_rows}")

    WH.execute(
        f"INSERT INTO {CONTROL_SCHEMA}.silver_pattern_recommendations "
        f"(recommendation_id, dataset_code, recommended_pattern, reasoning, "
        f" bronze_field_count, business_key_count, domain_count, "
        f" is_overkill_flag, status, created_by) "
        f"VALUES ($r, 'membership', 'HUB_SAT_LINK', "
        f" 'Member dataset has 41 fields across 4 domains (demo, address, eligibility, plan) "
        f"with one business key (member_card_id). DV2 Hub/Sat/Link is the right structural "
        f"choice; not overkill.', "
        f" 41, 1, 4, FALSE, 'DRAFT', 'phase15_5_verifier')",
        {"r": verify_rec_id},
    )
    rec_rows = list(
        WH.query(
            f"SELECT recommended_pattern, is_overkill_flag, status "
            f"FROM {CONTROL_SCHEMA}.silver_pattern_recommendations "
            f"WHERE recommendation_id = $r",
            {"r": verify_rec_id},
        )
    )
    if (
        rec_rows
        and rec_rows[0]["recommended_pattern"] == "HUB_SAT_LINK"
        and not rec_rows[0]["is_overkill_flag"]
    ):
        passes("silver_pattern_recommendations accepts INSERT + reads back correctly")
    else:
        fails("silver_pattern_recommendations", f"row not as expected: {rec_rows}")

    # Cleanup
    WH.execute(
        f"DELETE FROM {CONTROL_SCHEMA}.silver_pattern_recommendations WHERE recommendation_id = $r",
        {"r": verify_rec_id},
    )
    WH.execute(
        f"DELETE FROM {CONTROL_SCHEMA}.gold_schema_audit_log WHERE audit_id = $a",
        {"a": verify_audit_id},
    )
    WH.execute(
        f"DELETE FROM {CONTROL_SCHEMA}.bronze_to_gold_mappings WHERE mapping_id = $m",
        {"m": verify_mapping_id},
    )
    WH.execute(
        f"DELETE FROM {CONTROL_SCHEMA}.global_gold_schema_fields WHERE gold_field_id = $f",
        {"f": verify_field_id},
    )
    WH.execute(
        f"DELETE FROM {CONTROL_SCHEMA}.global_gold_schema_datasets WHERE gold_dataset_id = $g",
        {"g": verify_gold_id},
    )
    passes("Phase 15.5 verifier rows cleaned up")
except Exception as e:
    fails("Phase 15.5 round-trip", f"{type(e).__name__}: {e}")


# ============================================================================
# 10. Phase 15.7 — Gold-LIVE flow + DV2 Silver + Overflow + Greenfield
# ============================================================================

section("10. Phase 15.7 — Gold-LIVE flow + DV2 Silver builder")

from datalink.agents.pipeline_architect import (  # noqa: E402
    fetch_live_gold_schema,
    log_overflow_column,
)
from datalink.agents.pipeline_architect.dv2_silver_builder import (  # noqa: E402
    GoldColumnSpec,
    MappingSpec,
    build_bronze_ddl_with_overflow,
    build_dv2_silver_models,
)

# 10a. Bronze DDL with _extra
ddl = build_bronze_ddl_with_overflow(
    client_id="aetna",
    dataset_code="membership",
    bronze_fields=catalog_fields,
)
if "_extra" in ddl and "VARIANT" in ddl:
    passes("build_bronze_ddl_with_overflow includes _extra VARIANT")
else:
    fails("Bronze DDL overflow", "missing _extra column")

# 10b. DV2 Silver builder produces Hubs/Sats given a Gold column spec
test_gold_cols = [
    GoldColumnSpec("member_id", "TEXT", False, True, False, True),
    GoldColumnSpec("first_name", "TEXT", True, False, True, False),
    GoldColumnSpec("last_name", "TEXT", True, False, True, False),
    GoldColumnSpec("birth_date", "DATE", True, False, True, False),
    GoldColumnSpec("street_1", "TEXT", True, False, True, False),
    GoldColumnSpec("city", "TEXT", True, False, True, False),
]
test_mappings = [
    MappingSpec(
        "member_id",
        "COALESCE",
        "COALESCE(member_card_id, member_medicare_id)",
        ["member_card_id", "member_medicare_id"],
    ),
    MappingSpec("first_name", "DIRECT", "member_first_name", ["member_first_name"]),
    MappingSpec("last_name", "DIRECT", "member_last_name", ["member_last_name"]),
    MappingSpec("birth_date", "CAST", "TRY_CAST(member_birth_date AS DATE)", ["member_birth_date"]),
    MappingSpec(
        "street_1", "DIRECT", "member_street_address_line1", ["member_street_address_line1"]
    ),
    MappingSpec("city", "DIRECT", "member_city", ["member_city"]),
]
dv2_files = build_dv2_silver_models(
    client_id="aetna",
    dataset_code="membership",
    gold_columns=test_gold_cols,
    mappings=test_mappings,
    bronze_columns=[
        "member_card_id",
        "member_medicare_id",
        "member_first_name",
        "member_last_name",
        "member_birth_date",
        "member_street_address_line1",
        "member_city",
    ],
    silver_pattern="HUB_SAT_LINK",
)
hub_count = sum(1 for f in dv2_files if f.startswith("hub_"))
sat_count = sum(1 for f in dv2_files if f.startswith("sat_"))
if hub_count >= 1 and sat_count >= 1:
    passes(f"DV2 builder produced {hub_count} Hub(s) + {sat_count} Sat(s) from 6-col spec")
else:
    fails("DV2 builder", f"expected >=1 Hub + >=1 Sat; got {dv2_files}")

# 10c. NORMALIZED fallback when DV2 flagged overkill
norm_files = build_dv2_silver_models(
    client_id="aetna",
    dataset_code="membership",
    gold_columns=test_gold_cols,
    mappings=test_mappings,
    bronze_columns=["member_card_id", "member_first_name"],
    silver_pattern="NORMALIZED",
)
if norm_files and any("TRY_CAST" in v for v in norm_files.values()):
    passes("NORMALIZED Silver fallback emits TRY_CAST single-file model")
else:
    fails("NORMALIZED fallback", f"got {list(norm_files.keys())}")

# 10d. fetch_live_gold_schema returns dict when LIVE / None when missing
live_or_none = fetch_live_gold_schema(WH, "no_such_dataset_zzz")
if live_or_none is None:
    passes("fetch_live_gold_schema returns None for unknown dataset")
else:
    fails("fetch_live_gold_schema unknown", "should return None")

# 10e. Overflow log INSERT + UPDATE roundtrip
test_overflow_col = "phase15_7_verifier_unexpected_col"
log_overflow_column(
    warehouse=WH,
    client_id="phase15_7_verifier",
    dataset_code="membership",
    column_name=test_overflow_col,
    batch_id="verifier-batch-001",
    sample_values=["A", "B", "C"],
)
o_rows = list(
    WH.query(
        f"SELECT status, occurrence_count FROM {CONTROL_SCHEMA}.bronze_overflow_log "
        f"WHERE column_name = $c",
        {"c": test_overflow_col},
    )
)
if o_rows and o_rows[0]["status"] == "PENDING_REVIEW" and o_rows[0]["occurrence_count"] == 1:
    passes("log_overflow_column INSERT creates PENDING_REVIEW row")
else:
    fails("overflow log INSERT", f"got {o_rows}")

log_overflow_column(
    warehouse=WH,
    client_id="phase15_7_verifier",
    dataset_code="membership",
    column_name=test_overflow_col,
    batch_id="verifier-batch-002",
)
o_rows = list(
    WH.query(
        f"SELECT occurrence_count FROM {CONTROL_SCHEMA}.bronze_overflow_log WHERE column_name = $c",
        {"c": test_overflow_col},
    )
)
if o_rows and o_rows[0]["occurrence_count"] == 2:
    passes("log_overflow_column UPDATE bumps occurrence_count idempotently")
else:
    fails("overflow log UPDATE", f"got {o_rows}")

# 10f. Greenfield proposal table writable
greenfield_test_id = str(_uuid.uuid4())
WH.execute(
    f"""
    INSERT INTO {CONTROL_SCHEMA}.greenfield_dataset_proposals
      (greenfield_id, proposed_dataset_code, proposed_display_name,
       proposed_default_anchor, proposed_bronze_columns,
       proposed_field_count, status, created_by)
    VALUES ($id, 'phase15_7_verifier_test_ds', 'Verifier Test Dataset',
            'CATALOG_ANCHOR', '[{{"bronze_column_name":"col_a","logical_type":"TEXT"}}]',
            1, 'DRAFT', 'phase15_7_verifier')
    """,
    {"id": greenfield_test_id},
)
g_rows = list(
    WH.query(
        f"SELECT proposed_dataset_code, status FROM {CONTROL_SCHEMA}.greenfield_dataset_proposals "
        f"WHERE greenfield_id = $g",
        {"g": greenfield_test_id},
    )
)
if g_rows and g_rows[0]["status"] == "DRAFT":
    passes("greenfield_dataset_proposals accepts INSERT")
else:
    fails("greenfield insert", f"got {g_rows}")

# Cleanup
WH.execute(
    f"DELETE FROM {CONTROL_SCHEMA}.bronze_overflow_log WHERE column_name = $c",
    {"c": test_overflow_col},
)
WH.execute(
    f"DELETE FROM {CONTROL_SCHEMA}.greenfield_dataset_proposals WHERE greenfield_id = $g",
    {"g": greenfield_test_id},
)
passes("Phase 15.7 verifier artifacts cleaned up")


# ============================================================================
# 11. Phase 15.7 — UI surface for new pages
# ============================================================================

section("11. Phase 15.7 UI surfaces")

for page_path, page_name in [
    ("/Pipeline_Architect", "Pipeline Architect"),
    ("/Data_Model_Designer", "Data Model Designer"),
]:
    try:
        with urllib.request.urlopen(f"http://localhost:8000{page_path}", timeout=10) as resp:
            if resp.status == 200:
                passes(f"{page_name} page serves 200")
            else:
                fails(f"{page_name} page", f"HTTP {resp.status}")
    except Exception as e:
        fails(f"{page_name} page", f"unreachable: {e}")


# ============================================================================
# Final tally
# ============================================================================

section("Result")
print(f"  PASS: {len(PASS)}")
print(f"  FAIL: {len(FAIL)}")
if FAIL:
    print()
    print("  Failed assertions:")
    for f in FAIL:
        print(f"    - {f}")
    sys.exit(1)
print()
print("  PHASE 15 — ALL ASSERTIONS PASS ✅")
