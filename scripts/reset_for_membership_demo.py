"""Reset Phase 15 test data for a clean Membership-only demo run.

Truncates ALL Silver / Gold / instance / overflow / greenfield data + deletes
generated dbt models / DAG files / Bronze-Gold DDL files, so the operator
starts from a known empty state.

KEEPS (the source of truth):
  - global_bronze_catalog_datasets   (33 datasets from product catalog xlsx)
  - global_bronze_catalog_fields     (943 fields)
  - onprem_routing_rules             (61 routing rules)
  - standard_registry + agent_memory.standard_references (88 RAG chunks)

TRUNCATES:
  - Silver registry (datasets + tables + columns + mappings + audit + silver_to_gold)
  - Gold registry (datasets + fields + bronze_to_gold + audit)
  - silver_pattern_recommendations
  - Pipeline instances + overrides + routing overrides + audit
  - Bronze overflow log + greenfield dataset proposals
  - Pipeline templates
  - DQ suites with source='PIPELINE_ARCHITECT' (auto-emitted)

DELETES on disk:
  - dbt/models/silver/<client>/    (auto-generated dbt files)
  - dbt/models/gold/<client>/
  - dags/<client>_*_pipeline.py    (auto-generated DAG files)
  - datalink/pipeline/bronze/ddl/<client>_*.sql
  - datalink/pipeline/gold/ddl/<client>_*.sql
  - data/generated/<client>/phase15/ markers
"""

from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from datalink.adapters.factory import build_adapters  # noqa: E402
from datalink.config.loader import load_settings  # noqa: E402
from datalink.quality.control import CONTROL_SCHEMA  # noqa: E402

TRUNCATE_TABLES = [
    # Silver registry
    "bronze_to_silver_mappings",
    "global_silver_schema_columns",
    "global_silver_schema_tables",
    "global_silver_schema_audit_log",
    "silver_schema_audit_log",
    "silver_to_gold_mappings",
    "global_silver_schema_datasets",
    # Gold registry
    "bronze_to_gold_mappings",
    "global_gold_schema_fields",
    "gold_schema_audit_log",
    "global_gold_schema_datasets",
    "silver_pattern_recommendations",
    # Pipeline / overrides
    "client_field_overrides",
    "client_routing_overrides",
    "pipeline_instance_audit_log",
    "client_pipeline_instances",
    "pipeline_templates",
    # Overflow / greenfield
    "bronze_overflow_log",
    "greenfield_dataset_proposals",
]


def _delete_artifacts() -> None:
    """Delete generated dbt models, DAG files, DDLs, markers (filesystem cleanup)."""
    print()
    print("=" * 72)
    print(" Deleting auto-generated artifact files")
    print("=" * 72)
    for path in [
        ROOT / "dbt" / "models" / "silver",
        ROOT / "dbt" / "models" / "gold",
        ROOT / "datalink" / "pipeline" / "bronze" / "ddl",
        ROOT / "datalink" / "pipeline" / "gold" / "ddl",
        ROOT / "data" / "generated",
    ]:
        if path.exists():
            for child in path.iterdir():
                if child.name.startswith(("aetna", "caresource", "phase15_verify")):
                    if child.is_dir():
                        shutil.rmtree(child, ignore_errors=True)
                    else:
                        child.unlink(missing_ok=True)
                    print(f"  [OK]  removed {child.relative_to(ROOT)}")
    dag_dir = ROOT / "dags"
    if dag_dir.exists():
        for f in dag_dir.glob("*_pipeline.py"):
            f.unlink(missing_ok=True)
            print(f"  [OK]  removed dags/{f.name}")


def main() -> None:
    env_name = os.environ.get("DL_ENV", "local")
    settings = load_settings(env=env_name)
    wh = build_adapters(settings).warehouse
    print(f"DL_ENV={env_name}  Warehouse: {type(wh).__name__}")
    print()
    print("=" * 72)
    print(" Truncating test data tables (keeping Bronze catalog + standards)")
    print("=" * 72)

    for t in TRUNCATE_TABLES:
        try:
            before = list(wh.query(f"SELECT COUNT(*) AS c FROM {CONTROL_SCHEMA}.{t}"))
            cnt_before = int(before[0]["c"]) if before else 0
            wh.execute(f"DELETE FROM {CONTROL_SCHEMA}.{t}")
            print(f"  [OK]  {t:<40} deleted {cnt_before} rows")
        except Exception as e:
            msg = str(e)[:80]
            print(f"  [skip] {t:<40} ({msg})")

    # Auto-generated DQ suites by Pipeline Architect
    try:
        wh.execute(f"DELETE FROM {CONTROL_SCHEMA}.dq_suites WHERE source = 'PIPELINE_ARCHITECT'")
        print("  [OK]  dq_suites WHERE source='PIPELINE_ARCHITECT' deleted")
    except Exception as e:
        print(f"  [skip] dq_suites cleanup: {e}")

    print()
    print("=" * 72)
    print(" Dropping physical demo schemas (BRONZE_*, SILVER_*, GOLD_*)")
    print("=" * 72)
    for schema in ("BRONZE_AETNA", "SILVER_AETNA", "GOLD_AETNA"):
        try:
            # Snowflake supports CASCADE drop
            wh.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
            print(f"  [OK]  dropped {schema}")
        except Exception as e:
            msg = str(e)[:80]
            print(f"  [skip] {schema}: {msg}")

    print()
    print("=" * 72)
    print(" Verifying counts of KEPT tables (source-of-truth)")
    print("=" * 72)
    for t in [
        "global_bronze_catalog_datasets",
        "global_bronze_catalog_fields",
        "onprem_routing_rules",
        "standard_registry",
    ]:
        try:
            rows = list(wh.query(f"SELECT COUNT(*) AS c FROM {CONTROL_SCHEMA}.{t}"))
            cnt = int(rows[0]["c"]) if rows else 0
            print(f"  {t:<40} rows={cnt}")
        except Exception as e:
            print(f"  {t:<40} (missing: {str(e)[:60]})")

    _delete_artifacts()

    print()
    print("RESET COMPLETE ✅ — clean state, Bronze catalog preserved.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n[!] main() raised: {e}\n[!] Running filesystem cleanup anyway...")
        _delete_artifacts()
        raise
