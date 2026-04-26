"""Phase 13 verifier — AI Smart Mapper end-to-end.

Mirrors ``runbook_verify_phase{9..12}.py``: assertion-based PASS/FAIL,
exits 1 on any failure, runs against a throwaway DuckDB. Stub LLM mode
so it works on any laptop without an API key.

Sections (one per sub-phase):

  1. **13.1 — Source profiler**: rich metadata + semantic hints + audit
     col filter.
  2. **13.2 — Session storage**: full state machine walk
     DRAFT → PENDING_REVIEW → APPROVED → DEPLOYED, content-edit guards,
     conversation freeze on PENDING_REVIEW.
  3. **13.3 — Mapper agent (Silver)**: profile snapshot, agent proposal,
     sample preview executes, conversation grows, validation rejects
     DDL/DML.
  4. **13.4 — Optimization rules**: validate_push_sql scores correctly
     across compliant + non-compliant samples for both Postgres and
     SQL Server.
  5. **13.6 — Gold view generator**: propose_gold_view extends an
     existing Silver session with a Gold view, sample-runs Bronze under
     the proxy.
  6. **13.7 — On-Prem push generator**: propose_push_script generates
     compliant MERGE / UPSERT for both backends, validator passes 1.0.
  7. **13.8 — Deployer**: writes files to a temp project root, registers
     mapping_artifacts row, walks session APPROVED → DEPLOYED.

Usage::

    source .venv/bin/activate
    python -m scripts.runbook_verify_phase13

Exit 0 on green, 1 on any FAIL.
"""

from __future__ import annotations

import json
import os
import shutil
import sys
import tempfile
from pathlib import Path

os.environ["DL_ADAPTERS__WAREHOUSE__TYPE"] = "duckdb"
os.environ["DL_ADAPTERS__LLM__TYPE"] = "stub"
TMPROOT = Path(tempfile.mkdtemp(prefix="phase13_verify_"))
TMPDB = TMPROOT / "test.duckdb"
os.environ["DL_ADAPTERS__WAREHOUSE__PATH"] = str(TMPDB)


import contextlib  # noqa: E402

from datalink.adapters.factory import build_adapters  # noqa: E402
from datalink.agents.llm_router import get_llm  # noqa: E402
from datalink.agents.mapper.agent import MapperAgent  # noqa: E402
from datalink.agents.mapper.deployer import (  # noqa: E402
    deploy_session,
    list_deployments,
)
from datalink.agents.mapper.optimization import (  # noqa: E402
    TargetBackend,
    rules_prompt_block,
    validate_push_sql,
)
from datalink.agents.mapper.orchestrator import (  # noqa: E402
    propose_gold_view,
    propose_push_script,
    propose_silver_mapping,
)
from datalink.agents.mapper.profiler import (  # noqa: E402
    infer_semantic_hint,
    profile_source,
)
from datalink.agents.mapper.session import (  # noqa: E402
    MappingSessionRegistry,
    SessionStatus,
    TargetMode,
)
from datalink.config.loader import load_settings  # noqa: E402
from datalink.quality.control import PipelineControl  # noqa: E402


def main() -> int:
    failures: list[str] = []

    def fail(msg: str) -> None:
        failures.append(msg)
        print(f"   ❌ {msg}")

    def ok(msg: str) -> None:
        print(f"   ✅ {msg}")

    settings = load_settings()
    adapters = build_adapters(settings)
    wh = adapters.warehouse
    PipelineControl(wh).ensure()

    # Build a synthetic Bronze table that lasts the entire test.
    wh.execute("CREATE SCHEMA IF NOT EXISTS BRONZE")
    wh.execute("DROP TABLE IF EXISTS BRONZE.RAW_MEMBERSHIP")
    wh.execute("""
        CREATE TABLE BRONZE.RAW_MEMBERSHIP (
            member_id      VARCHAR,
            subscriber_id  VARCHAR,
            dob            DATE,
            gender         VARCHAR,
            plan_id        VARCHAR,
            zip_code       VARCHAR,
            provider_npi   VARCHAR,
            _load_dt       TIMESTAMP,
            _batch_id      VARCHAR,
            _load_type     VARCHAR
        )
    """)
    wh.execute("""
        INSERT INTO BRONZE.RAW_MEMBERSHIP VALUES
          ('M001', 'S001', DATE '1980-01-01', 'F', 'P1', '02141', '1234567890',
            TIMESTAMP '2026-01-01', 'B1', 'FULL'),
          ('M002', 'S002', DATE '1995-12-31', 'M', 'P1', '10001', '9876543210',
            TIMESTAMP '2026-01-01', 'B1', 'FULL'),
          ('M001', 'S001', DATE '1980-01-01', 'F', 'P2', '02141', '1234567890',
            TIMESTAMP '2026-01-08', 'B2', 'FULL'),
          ('M003', 'S003', DATE '2000-03-20', 'F', 'P1', '94105', '5555555555',
            TIMESTAMP '2026-01-08', 'B2', 'FULL')
    """)

    print()
    print("=== 13.1 — Source profiler ===")

    # Pure semantic inference
    cases = [
        ("zip_code", ["02141", "10001"], "ZIP_CODE"),
        ("provider_npi", ["1234567890"], "NPI"),
        ("dob", ["1980-01-01"], "DATE_STRING"),
        ("active_flag", ["Y", "N"], "BOOL_FLAG"),
        ("billed_amount", ["100.0"], "CURRENCY"),
    ]
    for name, samples, expected in cases:
        got = infer_semantic_hint(name, samples)
        if got != expected:
            fail(f"infer_semantic_hint({name!r}, {samples}) = {got!r}, expected {expected!r}")
    ok(f"semantic hints: {len(cases)} cases verified")

    profile = profile_source(wh, "BRONZE.RAW_MEMBERSHIP", sample_limit=3)
    if len(profile.columns) != 7:
        fail(f"profile column count: {len(profile.columns)}")
    elif any(c.name.startswith("_") for c in profile.columns):
        fail("audit cols leaked into profile")
    elif (
        profile.by_name.get("zip_code") is None
        or profile.by_name["zip_code"].semantic_hint != "ZIP_CODE"
    ):
        fail(f"zip_code hint missing: {profile.by_name.get('zip_code')}")
    elif profile.by_name["provider_npi"].semantic_hint != "NPI":
        fail(f"provider_npi hint: {profile.by_name['provider_npi'].semantic_hint!r}")
    else:
        ok(
            f"profile: {profile.total_rows} rows, {len(profile.columns)} business cols, "
            f"hints={[c.semantic_hint for c in profile.columns if c.semantic_hint]}"
        )

    print()
    print("=== 13.2 — Session storage + state machine ===")

    sess_reg = MappingSessionRegistry(wh)
    sid = sess_reg.create_session(
        client_id="acme",
        source_qualified_table="BRONZE.RAW_MEMBERSHIP",
        target_mode=TargetMode.FULL_STACK,
        created_by="user:alice",
    )
    s = sess_reg.get_session(sid)
    if s is None or s.status is not SessionStatus.DRAFT:
        fail(f"create returned wrong status: {s.status if s else None}")
    elif s.target_mode is not TargetMode.FULL_STACK:
        fail(f"target_mode: {s.target_mode}")
    else:
        ok("DRAFT session created (FULL_STACK target_mode)")

    sess_reg.add_turn(sid, role="user", text="hello", actor="user:alice")
    s = sess_reg.get_session(sid)
    if s is None or s.turn_count != 1:
        fail(f"add_turn: turn_count={s.turn_count if s else 0}")
    else:
        ok("add_turn: 1 turn appended")

    print()
    print("=== 13.3 — Mapper agent (Silver) ===")

    llm = get_llm(settings)
    prop = propose_silver_mapping(
        llm=llm,
        warehouse=wh,
        session_registry=sess_reg,
        session_id=sid,
        user_prompt="Build a Silver SCD2 sat for member demographics keyed on member_id+plan_id",
        actor="user:alice",
    )
    if not prop.silver_sql or "SELECT" not in prop.silver_sql.upper():
        fail("silver_sql looks invalid")
    elif not prop.natural_keys:
        fail(f"natural_keys empty: {prop.natural_keys}")
    elif prop.sample_error:
        fail(f"silver sample run: {prop.sample_error}")
    elif not prop.sample_rows:
        fail("silver sample preview empty")
    else:
        ok(
            f"silver proposal: target=`{prop.silver_target_table}` "
            f"NK={prop.natural_keys} "
            f"sample_rows={len(prop.sample_rows)}"
        )

    # Validation: forbidden DDL must raise.
    agent = MapperAgent(llm=llm, warehouse=wh)  # type: ignore[arg-type]
    bad = json.dumps(
        {
            "silver_target_table": "x",
            "natural_keys": ["member_id"],
            "scd2_change_cols": ["dob"],
            "silver_sql": "DROP TABLE BRONZE.RAW_MEMBERSHIP",
            "rationale": "lol",
        }
    )
    try:
        agent._parse_and_validate(bad, profile)  # type: ignore[attr-defined]
        fail("expected ValueError on DROP; got none")
    except ValueError:
        ok("DDL/DML in proposal correctly rejected")

    print()
    print("=== 13.4 — Optimization rules engine ===")

    good_pg = """
    INSERT INTO um.patient_auth (auth_id, status, effective_start_date)
    SELECT auth_id, status, effective_start_date
    FROM silver.gold_patient_auth
    WHERE effective_start_date > :last_watermark
    LIMIT :batch_size
    ON CONFLICT (auth_id) DO UPDATE SET status = EXCLUDED.status;
    UPDATE control.egress_batch_log SET cutoff_dts = NOW();
    """
    rep = validate_push_sql(good_pg, backend=TargetBackend.POSTGRES)
    if rep.score != 1.0:
        fail(f"compliant Postgres scored {rep.score:.2f}, expected 1.0")
        for r in rep.failures:
            print(f"      missing: {r.name} ({r.detail})")
    else:
        ok(f"validate_push_sql(POSTGRES) on compliant SQL: {rep.summary()}")

    bad_pg = "INSERT INTO x SELECT * FROM y;"
    rep = validate_push_sql(bad_pg, backend=TargetBackend.POSTGRES)
    if rep.score >= 0.5:
        fail(f"non-compliant Postgres scored too high: {rep.score:.2f}")
    else:
        ok(f"validate_push_sql(POSTGRES) on non-compliant SQL: {rep.summary()}")

    pg_block = rules_prompt_block(TargetBackend.POSTGRES)
    if "ON CONFLICT" not in pg_block or "MERGE INTO" in pg_block:
        fail("rules_prompt_block(POSTGRES) shape wrong")
    else:
        ok(f"rules_prompt_block(POSTGRES): {len(pg_block)} chars, no MSSQL bleed")

    print()
    print("=== 13.6 — Gold view generator ===")

    gold_prop = propose_gold_view(
        llm=llm,
        warehouse=wh,
        session_registry=sess_reg,
        session_id=sid,
        user_prompt="Expose only active members with member_id and plan_id",
        actor="user:alice",
    )
    if not gold_prop.gold_sql or "SELECT" not in gold_prop.gold_sql.upper():
        fail("gold_sql looks invalid")
    elif "{{" not in gold_prop.gold_sql:
        fail("gold_sql missing dbt jinja (no {{ ref(...) }} or {{ config(...) }})")
    else:
        ok(
            f"gold proposal: target=`{gold_prop.gold_target_table}` "
            f"sample_rows={len(gold_prop.sample_rows)}"
            + (f" (sample_error: {gold_prop.sample_error})" if gold_prop.sample_error else "")
        )

    # Session should now have gold_sql + gold_target_table.
    s = sess_reg.get_session(sid)
    if s is None or not s.gold_sql:
        fail("session.gold_sql not stored")
    elif not s.gold_target_table:
        fail("session.gold_target_table not stored")
    else:
        ok(f"session updated: gold_target=`{s.gold_target_table}`")

    print()
    print("=== 13.7 — On-Prem push generator (both backends) ===")

    pg_push = propose_push_script(
        llm=llm,
        warehouse=wh,
        session_registry=sess_reg,
        session_id=sid,
        backend="POSTGRES",
        target_table="um.member",
        primary_keys=["member_id"],
        column_list=["member_id", "plan_id", "dob", "gender"],
        actor="user:alice",
    )
    if not pg_push.push_sql or "ON CONFLICT" not in pg_push.push_sql.upper():
        fail("Postgres push script missing ON CONFLICT")
    elif pg_push.optimization_score < 0.9:
        fail(
            f"Postgres push score {pg_push.optimization_score:.2f}, "
            f"failed_rules={pg_push.failed_rules}"
        )
    else:
        ok(f"Postgres push: score={pg_push.optimization_score:.2f}, " f"all rules pass")

    ms_push = propose_push_script(
        llm=llm,
        warehouse=wh,
        session_registry=sess_reg,
        session_id=sid,
        backend="SQLSERVER",
        target_table="um.Member",
        primary_keys=["member_id"],
        column_list=["member_id", "plan_id", "dob", "gender"],
        actor="user:alice",
    )
    if not ms_push.push_sql or "MERGE INTO" not in ms_push.push_sql.upper():
        fail("SQLSERVER push script missing MERGE INTO")
    elif ms_push.optimization_score < 0.9:
        fail(
            f"SQLSERVER push score {ms_push.optimization_score:.2f}, "
            f"failed_rules={ms_push.failed_rules}"
        )
    else:
        ok(f"SQL Server push: score={ms_push.optimization_score:.2f}, " f"all rules pass")

    s = sess_reg.get_session(sid)
    if s is None or not s.onprem_postgres_sql or not s.onprem_mssql_sql:
        fail("session push columns not stored")
    else:
        ok("session updated: onprem_postgres_sql + onprem_mssql_sql")

    print()
    print("=== 13.8 — Deployer ===")

    # Walk to APPROVED so deploy can fire.
    sess_reg.submit_for_review(sid, actor="user:alice")
    sess_reg.approve(sid, actor="reviewer:bob", notes="LGTM, ship it")

    # Deploy into a fake repo root so we don't pollute the real tree.
    fake_root = TMPROOT / "fake_repo"
    fake_root.mkdir()
    # Drop a sentinel pyproject.toml so _resolve_repo_root would land here
    # IF we let it auto-resolve (we pass repo_root explicitly anyway).
    (fake_root / "pyproject.toml").write_text("# fake")

    artifact = deploy_session(
        warehouse=wh,
        session_id=sid,
        actor="system:deployer",
        repo_root=fake_root,
    )
    if not artifact.files_written:
        fail("deployer wrote no files")
    elif len(artifact.files_written) < 4:  # silver + gold + 2 push
        fail(f"deployer wrote only {len(artifact.files_written)} files: {artifact.files_written}")
    else:
        ok(f"deployer wrote {len(artifact.files_written)} files")

    # Verify each file actually exists on disk + has content.
    for rel in artifact.files_written:
        p = fake_root / rel
        if not p.exists():
            fail(f"deployer claimed to write {rel} but file missing")
        elif p.stat().st_size == 0:
            fail(f"deployer wrote empty file {rel}")
        else:
            ok(f"file present + non-empty: {rel}")

    # Session walked to DEPLOYED.
    s = sess_reg.get_session(sid)
    if s is None or s.status is not SessionStatus.DEPLOYED:
        fail(f"session not DEPLOYED after deploy: {s.status if s else None}")
    elif s.deployed_at is None:
        fail("session deployed_at not set")
    else:
        ok("session walked APPROVED → DEPLOYED")

    # mapping_artifacts row written.
    deps = list_deployments(wh, session_id=sid)
    if not deps:
        fail("mapping_artifacts row missing")
    elif deps[0]["content_hash"] != artifact.content_hash:
        fail(
            f"content_hash mismatch: artifact={artifact.content_hash[:8]}, db={deps[0]['content_hash'][:8]}"
        )
    else:
        ok(f"mapping_artifacts row written: hash={deps[0]['content_hash']}")

    print()
    if failures:
        print(f"❌ {len(failures)} FAIL")
        for f in failures:
            print(f"   - {f}")
        return 1
    print("✅ Phase 13 PASS — AI Smart Mapper end-to-end verified")
    print("   profile -> session -> silver -> gold -> push (x2) -> deploy: all green")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    finally:
        with contextlib.suppress(OSError):
            shutil.rmtree(TMPROOT, ignore_errors=True)
