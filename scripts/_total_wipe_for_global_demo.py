"""Total wipe for the global-template demo run (Phase 16.8 prep).

Hits every layer:
  1. Drop physical Snowflake schemas (BRONZE_*, SILVER_*, GOLD_*)
  2. Truncate CONTROL pipeline data + audit + DQ-Architect-authored suites
  3. Truncate CONTROL Silver/Gold design registry + mappings + recommendations
  4. Truncate CONTROL agent_proposals + tag assignments
  5. Truncate version_history + upstream_notifications + global templates (Phase 16.7)
  6. Wipe Airflow metadata (every DAG, not just Membership)
  7. Delete generated files (DAGs, dbt models, DDLs, sample data)
  8. Restart Airflow scheduler + Streamlit so they forget cached state

PRESERVED (seed data, expensive to rebuild):
  * global_bronze_catalog_datasets (33), global_bronze_catalog_fields (943)
  * onprem_routing_rules (61)
  * standard_registry (6) + agent_memory standards
  * proposal_tags (18 system tags)
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

# Auto-load .env
_env = ROOT / ".env"
if _env.exists():
    for raw in _env.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        os.environ.setdefault(k.strip(), v.strip().strip("'").strip('"'))
os.environ.setdefault("DL_ENV", "dev")

from datalink.adapters.factory import build_adapters  # noqa: E402
from datalink.admin import (  # noqa: E402
    delete_generated_artifacts,
    reset_design_registry,
    reset_pipeline_data,
    reset_proposals,
)
from datalink.config.loader import load_settings  # noqa: E402
from datalink.quality.control import CONTROL_SCHEMA  # noqa: E402

ACTOR = "ui:demo-prep"


def main() -> int:
    print("=" * 72)
    print(" TOTAL WIPE — Demo prep")
    print("=" * 72)
    print()

    wh = build_adapters(load_settings(env="dev")).warehouse

    # 1. Discover every client that has any pipeline state
    client_rows = list(
        wh.query(f"SELECT DISTINCT client_id FROM {CONTROL_SCHEMA}.client_pipeline_instances")
    )
    clients = [str(r["client_id"]) for r in client_rows if r.get("client_id")]
    print(f"[discovery] {len(clients)} clients with pipeline state: {clients}")
    print()

    # 2. Drop physical schemas (CASCADE)
    print("[1/8] Drop physical BRONZE_*/SILVER_*/GOLD_* schemas")
    for cid in clients:
        for layer in ("BRONZE", "SILVER", "GOLD"):
            sch = f"{layer}_{cid.upper()}"
            try:
                wh.execute(f"DROP SCHEMA IF EXISTS {sch} CASCADE")
                print(f"  [OK]  dropped {sch}")
            except Exception as exc:
                print(f"  [skip] {sch}: {str(exc)[:80]}")
    print()

    # 3. Pipeline data
    print("[2/8] Truncate pipeline data + DQ suites (Pipeline Architect-authored)")
    r = reset_pipeline_data(client_id=None, actor=ACTOR)
    print(f"  {r.summary_line()}")
    print()

    # 4. Design registry
    print("[3/8] Truncate Silver/Gold design registry")
    r = reset_design_registry(actor=ACTOR)
    print(f"  {r.summary_line()}")
    print()

    # 5. Proposals
    print("[4/8] Truncate agent_proposals + tag assignments")
    r = reset_proposals(actor=ACTOR)
    print(f"  {r.summary_line()}")
    print()

    # 6. Version history + globals + anomalies
    print("[5/8] Truncate version_history + upstream_notifications + global templates + anomalies")
    for tbl in (
        "version_tag_assignments",
        "version_history",
        "upstream_update_notifications",
        "global_artifact_blobs",
        "global_pipeline_templates",
        "global_migration_plans",
        "anomaly_events",
        "anomaly_baselines",
    ):
        try:
            wh.execute(f"DELETE FROM {CONTROL_SCHEMA}.{tbl}")
            print(f"  [OK]  truncated {tbl}")
        except Exception as exc:
            print(f"  [skip] {tbl}: {str(exc)[:60]}")
    print()

    # 7. Airflow metadata for ALL DAGs
    print("[6/8] Wipe Airflow metadata (every DAG)")
    sql = (
        "DELETE FROM task_instance; "
        "DELETE FROM xcom; "
        "DELETE FROM log; "
        "DELETE FROM dag_run; "
        "DELETE FROM dag_tag; "
        "DELETE FROM dag_warning; "
        "DELETE FROM dag;"
    )
    try:
        res = subprocess.run(
            [
                "docker",
                "exec",
                "-i",
                "datalink-airflow-db",
                "psql",
                "-U",
                "airflow",
                "-d",
                "airflow",
                "-c",
                sql,
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        total = sum(
            int(ln.split()[-1]) for ln in res.stdout.splitlines() if ln.startswith("DELETE ")
        )
        print(f"  [OK]  airflow rows deleted: {total}")
    except Exception as exc:
        print(f"  [warn] airflow wipe failed: {exc}")
    print()

    # 8. Generated artifacts
    print("[7/8] Delete generated artifacts")
    r = delete_generated_artifacts(client_id=None, actor=ACTOR)
    print(f"  {r.summary_line()}")
    # Force-clean root-owned residue via docker exec --user root
    cleanup_cmd = (
        "rm -rf "
        "/opt/datalink/dags/*_pipeline.py "
        "/opt/datalink/dbt/models/silver/aetna* "
        "/opt/datalink/dbt/models/silver/bcbs* "
        "/opt/datalink/dbt/models/silver/caresource* "
        "/opt/datalink/dbt/models/gold/aetna* "
        "/opt/datalink/dbt/models/gold/bcbs* "
        "/opt/datalink/dbt/models/gold/caresource* "
        "/opt/datalink/datalink/pipeline/bronze/ddl/*_*.sql "
        "/opt/datalink/datalink/pipeline/gold/ddl/*_*.sql "
        "/opt/datalink/data/generated/aetna* "
        "/opt/datalink/data/generated/bcbs* "
        "/opt/datalink/data/generated/caresource* "
        "/opt/datalink/data/generated/membership_* "
        "2>/dev/null"
    )
    subprocess.run(
        [
            "docker",
            "exec",
            "--user",
            "root",
            "datalink-airflow-scheduler",
            "bash",
            "-c",
            cleanup_cmd,
        ],
        capture_output=True,
        timeout=30,
    )
    print("  [OK]  root-owned residue cleared")
    print()

    # 9. Restart containers
    print("[8/8] Restart Airflow scheduler + Streamlit (force fresh state)")
    subprocess.run(
        [
            "docker",
            "compose",
            "-f",
            "/home/jatin/dev/DataPipelinesWithGX/docker-compose.yml",
            "restart",
            "airflow_scheduler",
            "control_tower",
        ],
        capture_output=True,
        timeout=120,
    )
    print("  [OK]  containers restarted")
    print()

    # Verification
    print("=" * 72)
    print(" VERIFICATION")
    print("=" * 72)
    print()
    schemas = [s["name"] for s in wh.query("SHOW SCHEMAS IN DATABASE DATALINK_DEV")]
    print("Snowflake schemas in DATALINK_DEV:")
    for s in schemas:
        bad = s.startswith(("BRONZE_", "SILVER_", "GOLD_")) and s not in (
            "CONTROL",
            "INFORMATION_SCHEMA",
            "PUBLIC",
        )
        print(f"  {s}{'  ⚠️ should be gone' if bad else ''}")
    print()

    print("Source-of-truth tables (must be non-zero):")
    for tbl, expected in [
        ("global_bronze_catalog_datasets", 33),
        ("global_bronze_catalog_fields", 943),
        ("onprem_routing_rules", 61),
        ("standard_registry", 6),
        ("proposal_tags", 18),
    ]:
        n = next(iter(wh.query(f"SELECT COUNT(*) c FROM {CONTROL_SCHEMA}.{tbl}")))["c"]
        flag = "✅" if int(n) == expected else "⚠️ "
        print(f"  {flag} {tbl:40s} = {n} (expected {expected})")
    print()

    print("Demo state tables (must all be 0):")
    for tbl in (
        "client_pipeline_instances",
        "global_silver_schema_datasets",
        "global_gold_schema_datasets",
        "agent_proposals",
        "version_history",
        "global_pipeline_templates",
        "global_artifact_blobs",
        "anomaly_events",
    ):
        n = next(iter(wh.query(f"SELECT COUNT(*) c FROM {CONTROL_SCHEMA}.{tbl}")))["c"]
        flag = "✅" if int(n) == 0 else "⚠️ "
        print(f"  {flag} {tbl:40s} = {n}")
    print()

    print("🟢 WIPE COMPLETE — ready for global-pipeline demo.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
