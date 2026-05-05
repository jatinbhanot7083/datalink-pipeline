#!/usr/bin/env bash
# ============================================================
#  DataLink — Master Reset for Membership Demo
#  Run from repo root:  bash scripts/master_reset.sh
#
#  KEEPS:
#    - global_bronze_catalog_datasets   (33 datasets)
#    - global_bronze_catalog_fields     (943 fields)
#    - onprem_routing_rules             (61 rules)
#    - standard_registry + RAG chunks   (88 chunks)
#
#  WIPES:
#    - Silver / Gold registry + mappings + audit
#    - Pipeline instances + overrides + DQ suites (auto-emitted)
#    - Physical Snowflake schemas BRONZE_AETNA / SILVER_AETNA / GOLD_AETNA
#    - Generated dbt models (silver/aetna, gold/aetna)
#    - Generated DAG files (dags/*_pipeline.py)
#    - Airflow metadata for aetna_membership_pipeline
# ============================================================

set -u  # fail on unset vars; do NOT set -e because we tolerate missing containers

# Resolve repo root from script location
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

DAG_ID="aetna_membership_pipeline"

# Target Snowflake (dev env) by default. Override with DL_ENV=local for DuckDB.
export DL_ENV="${DL_ENV:-dev}"

# Load credentials from .env (SNOWFLAKE_*, ANTHROPIC_API_KEY, etc.)
if [ -f "${REPO_ROOT}/.env" ]; then
    set -a
    # shellcheck disable=SC1091
    source "${REPO_ROOT}/.env"
    set +a
    echo "[env] Loaded ${REPO_ROOT}/.env"
else
    echo "[env] ⚠️  ${REPO_ROOT}/.env not found — Snowflake ops will skip"
fi

echo ""
echo "======================================================================"
echo " DataLink Master Reset — repo: ${REPO_ROOT}"
echo "======================================================================"

# ---- 1. Pause the DAG so scheduler stops queueing while we wipe ----
echo ""
echo "[1/6] Pausing DAG ${DAG_ID} ..."
docker exec datalink-airflow-scheduler airflow dags pause "${DAG_ID}" 2>/dev/null \
  || echo "      (DAG not registered yet — skip)"

# ---- 2a. Clean root-owned generated dirs via docker exec (container runs as root) ----
echo ""
echo "[2a/6] Removing root-owned generated artifacts via airflow container ..."
docker exec --user root datalink-airflow-scheduler bash -c '
    rm -rf /opt/datalink/dbt/models/silver/aetna \
           /opt/datalink/dbt/models/silver/aetna_* \
           /opt/datalink/dbt/models/silver/caresource \
           /opt/datalink/dbt/models/silver/phase14_verify_* \
           /opt/datalink/dbt/models/silver/phase15_verify_* \
           /opt/datalink/dbt/models/silver/hubs \
           /opt/datalink/dbt/models/silver/links \
           /opt/datalink/dbt/models/silver/satellites \
           /opt/datalink/dbt/models/gold/aetna \
           /opt/datalink/dbt/models/gold/caresource \
           /opt/datalink/datalink/pipeline/bronze/ddl/aetna_*.sql \
           /opt/datalink/datalink/pipeline/gold/ddl/aetna_*.sql \
           /opt/datalink/data/generated/aetna \
           /opt/datalink/data/generated/caresource \
           /opt/datalink/data/generated/phase15_verify 2>/dev/null
' && echo "      ✅ root-owned artifacts cleared" || echo "      ⚠️  docker cleanup had issues"

# ---- 2b. Truncate CONTROL tables + drop physical Snowflake schemas ----
echo ""
echo "[2b/6] Running reset_for_membership_demo.py (Snowflake side) ..."
"${REPO_ROOT}/.venv/bin/python3" scripts/reset_for_membership_demo.py

# ---- 3. Wipe Airflow metadata for this DAG ----
echo ""
echo "[3/6] Wiping Airflow metadata for ${DAG_ID} ..."
docker exec -i datalink-airflow-db psql -U airflow -d airflow <<SQL
DELETE FROM task_instance WHERE dag_id = '${DAG_ID}';
DELETE FROM xcom          WHERE dag_id = '${DAG_ID}';
DELETE FROM log           WHERE dag_id = '${DAG_ID}';
DELETE FROM dag_run       WHERE dag_id = '${DAG_ID}';
DELETE FROM dag           WHERE dag_id = '${DAG_ID}';
SQL

# ---- 4. Wipe defensive marker fallback dir ----
echo ""
echo "[4/6] Clearing /tmp/datalink/markers ..."
docker exec datalink-airflow-scheduler rm -rf /tmp/datalink/markers 2>/dev/null || true
rm -rf /tmp/datalink/markers 2>/dev/null || true

# ---- 5. Restart scheduler + webserver so they re-scan dags/ folder cleanly ----
echo ""
echo "[5/6] Restarting Airflow scheduler + webserver ..."
docker compose restart airflow_scheduler airflow_webserver

# ---- 6. Verify clean state ----
echo ""
echo "[6/6] Verifying clean state ..."
echo ""
echo "    --- Source-of-truth row counts (should be non-zero) ---"
"${REPO_ROOT}/.venv/bin/python3" - <<PY
import os
from datalink.config.loader import load_settings
from datalink.adapters.factory import build_adapters
from datalink.quality.control import CONTROL_SCHEMA
wh = build_adapters(load_settings(env=os.environ.get('DL_ENV','dev'))).warehouse
checks = [
    ('global_bronze_catalog_datasets', 'KEEP'),
    ('global_bronze_catalog_fields',   'KEEP'),
    ('onprem_routing_rules',           'KEEP'),
    ('standard_registry',              'KEEP'),
    ('global_silver_schema_datasets',  'WIPE'),
    ('global_gold_schema_datasets',    'WIPE'),
    ('client_pipeline_instances',      'WIPE'),
]
for t, kind in checks:
    try:
        n = list(wh.query(f'SELECT COUNT(*) c FROM {CONTROL_SCHEMA}.{t}'))[0]['c']
    except Exception as e:
        n = f'ERR: {str(e)[:40]}'
    flag = '✅' if (kind == 'KEEP' and isinstance(n, int) and n > 0) or \
                   (kind == 'WIPE' and n == 0) else '⚠️ '
    print(f'      {flag} [{kind}] {t:40s} = {n}')
PY

echo ""
echo "    --- Generated artifacts (should all be clean) ---"
if ls dags/*_pipeline.py >/dev/null 2>&1; then
    echo "      ⚠️  dags/ still has generated DAG files:"
    ls dags/*_pipeline.py | sed 's/^/         /'
else
    echo "      ✅ dags/ clean"
fi
[ -d dbt/models/silver/aetna ]    && echo "      ⚠️  dbt/models/silver/aetna present"    || echo "      ✅ Silver dbt clean"
[ -d dbt/models/gold/aetna ]      && echo "      ⚠️  dbt/models/gold/aetna present"      || echo "      ✅ Gold dbt clean"
[ -d data/generated/aetna ]       && echo "      ⚠️  data/generated/aetna present"       || echo "      ✅ Phase 15 markers clean"

echo ""
echo "======================================================================"
echo " 🟢 RESET COMPLETE — ready to re-run Membership end-to-end."
echo "======================================================================"
echo ""
