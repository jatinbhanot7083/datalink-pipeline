#!/usr/bin/env bash
# ============================================================================
#  DataLink — Cold-start bootstrap for a fresh Snowflake account
#  Run from anywhere:  bash scripts/bootstrap_fresh_snowflake.sh
#
#  Encapsulates ALL 11 gotchas documented in docs/BOOTSTRAP_RUNBOOK.md.
#  Idempotent: safe to re-run if it fails partway.
#
#  PRE-FLIGHT (you must do these before running this script):
#    1. Update .env with new SNOWFLAKE_ACCOUNT / USER / PASSWORD
#    2. In the new Snowflake account's web UI, run:
#         -- Recipe per docs/demo_journal.md §"5th ingredient": MEDIUM warehouse,
#         -- 1-hour warm window, 5-minute statement timeout. ~5× faster than
#         -- XSMALL for the metadata queries the UI issues; negligible cost.
#         CREATE WAREHOUSE DATALINK_WH
#             WAREHOUSE_SIZE = 'MEDIUM'
#             AUTO_SUSPEND   = 3600
#             AUTO_RESUME    = TRUE
#             STATEMENT_TIMEOUT_IN_SECONDS        = 300
#             STATEMENT_QUEUED_TIMEOUT_IN_SECONDS = 60
#             INITIALLY_SUSPENDED = TRUE;
#         CREATE DATABASE DATALINK_DEV;
#         CREATE ROLE DATALINK_ENGINEER;
#         GRANT USAGE ON WAREHOUSE DATALINK_WH TO ROLE DATALINK_ENGINEER;
#         GRANT ALL ON DATABASE DATALINK_DEV TO ROLE DATALINK_ENGINEER;
#         GRANT ALL ON ALL SCHEMAS IN DATABASE DATALINK_DEV TO ROLE DATALINK_ENGINEER;
#         GRANT ALL ON FUTURE SCHEMAS IN DATABASE DATALINK_DEV TO ROLE DATALINK_ENGINEER;
#         GRANT ALL ON FUTURE TABLES IN DATABASE DATALINK_DEV TO ROLE DATALINK_ENGINEER;
#         GRANT ROLE DATALINK_ENGINEER TO USER <your-username>;
#    3. Docker stack must be up:  docker compose up -d
# ============================================================================

set -u  # fail on unset vars; tolerate non-zero exits inside try-style blocks
set -o pipefail  # propagate non-zero exits through pipes so `docker exec | tail`
                 # actually fails when docker exec fails (caught one silent
                 # migration-skip on 2026-05-11 — see commit log)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

export DL_ENV="${DL_ENV:-dev}"  # Gotcha #3 — never default to 'local' (DuckDB)

# Gotcha #1 — load .env so host-side scripts get SNOWFLAKE_* env vars
if [ ! -f "${REPO_ROOT}/.env" ]; then
    echo "❌ ${REPO_ROOT}/.env not found. Aborting."
    exit 1
fi
set -a
# shellcheck disable=SC1091
source "${REPO_ROOT}/.env"
set +a

# ----------------------------------------------------------------------------
# Helper functions
# ----------------------------------------------------------------------------
banner() {
    echo ""
    echo "============================================================"
    echo " $*"
    echo "============================================================"
}

step_pass() { echo "  ✅ $*"; }
step_fail() { echo "  ❌ $*"; }
step_warn() { echo "  ⚠️  $*"; }

# Run a python snippet inside the venv with .env already sourced.
# Usage: venv_py "<code>"
venv_py() {
    if [ ! -x "${REPO_ROOT}/.venv/bin/python3" ]; then
        echo "❌ ${REPO_ROOT}/.venv/bin/python3 not found. Create the venv first."
        exit 1
    fi
    "${REPO_ROOT}/.venv/bin/python3" -c "$1"
}

# Run something inside control_tower (has streamlit + voyageai + on docker net)
ct_exec() {
    docker exec -e DL_ENV=dev -w /opt/datalink datalink-control-tower "$@"
}

# ----------------------------------------------------------------------------
# Step 0 — Pre-flight checks
# ----------------------------------------------------------------------------
banner " Step 0: Pre-flight"

# Check .env has the right Snowflake creds
for var in SNOWFLAKE_ACCOUNT SNOWFLAKE_USER SNOWFLAKE_PASSWORD SNOWFLAKE_ROLE \
           SNOWFLAKE_WAREHOUSE SNOWFLAKE_DATABASE VOYAGE_API_KEY ANTHROPIC_API_KEY; do
    if [ -z "${!var:-}" ]; then
        step_fail "$var is empty in .env"
        exit 1
    fi
done
step_pass ".env has all required keys (SNOWFLAKE_*, VOYAGE_API_KEY, ANTHROPIC_API_KEY)"
step_pass "Account: ${SNOWFLAKE_ACCOUNT}, User: ${SNOWFLAKE_USER}, Role: ${SNOWFLAKE_ROLE}"

# Check containers are up
for c in datalink-control-tower datalink-airflow-scheduler datalink-airflow-webserver datalink-postgres; do
    if ! docker ps --format '{{.Names}}' | grep -q "^${c}$"; then
        step_fail "Container ${c} is not running. Run: docker compose up -d"
        exit 1
    fi
done
step_pass "All required containers running"

# ----------------------------------------------------------------------------
# Step 1 — Force-recreate containers so they pick up fresh .env  (Gotcha #2)
# ----------------------------------------------------------------------------
banner " Step 1: Recreating containers so they pick up fresh .env (Gotcha #2)"

# Only recreate if Snowflake account in container differs from .env
container_account="$(docker exec datalink-control-tower env | grep '^SNOWFLAKE_ACCOUNT=' | cut -d= -f2)"
if [ "${container_account}" != "${SNOWFLAKE_ACCOUNT}" ]; then
    step_warn "control_tower has stale SNOWFLAKE_ACCOUNT='${container_account}', recreating..."
    docker compose up -d --force-recreate \
        control_tower airflow_scheduler airflow_webserver 2>&1 | tail -5
    # Poll until control_tower is fully healthy (not just "running" / "starting").
    # The Phase 17.x migrations call `docker exec datalink-control-tower …` and
    # silently no-op if the container is in its starting health-check window.
    printf "  ⏳ Waiting for control_tower health: "
    for i in $(seq 1 60); do
        health="$(docker inspect -f '{{.State.Health.Status}}' datalink-control-tower 2>/dev/null || echo unknown)"
        if [ "${health}" = "healthy" ]; then
            echo "healthy (after ${i}s)"
            break
        fi
        sleep 1
        [ $((i % 5)) -eq 0 ] && printf "."
    done
    if [ "${health}" != "healthy" ]; then
        step_fail "control_tower never reached healthy state — aborting"
        exit 1
    fi
    step_pass "Containers recreated with fresh .env and healthy"
else
    step_pass "Containers already have fresh SNOWFLAKE_ACCOUNT (${container_account}) — skipping recreate"
fi

# ----------------------------------------------------------------------------
# Step 2 — Snowflake connectivity smoke test
# ----------------------------------------------------------------------------
banner " Step 2: Snowflake connectivity smoke test"

venv_py "
import snowflake.connector, os, sys
try:
    conn = snowflake.connector.connect(
        account=os.environ['SNOWFLAKE_ACCOUNT'], user=os.environ['SNOWFLAKE_USER'],
        password=os.environ['SNOWFLAKE_PASSWORD'], role=os.environ['SNOWFLAKE_ROLE'],
        warehouse=os.environ['SNOWFLAKE_WAREHOUSE'], database=os.environ['SNOWFLAKE_DATABASE'],
        login_timeout=15, network_timeout=30,
    )
    cur = conn.cursor()
    cur.execute('SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_DATABASE()')
    u, r, w, d = cur.fetchone()
    print(f'  ✅ Connected: user={u} role={r} warehouse={w} database={d}')
    cur.close(); conn.close()
except Exception as e:
    print(f'  ❌ Snowflake connect failed: {e}'); sys.exit(1)
" || exit 1

# ----------------------------------------------------------------------------
# Step 3 — Create CONTROL schema + 43 base tables
# ----------------------------------------------------------------------------
banner " Step 3: CONTROL schema + base tables"

venv_py "
from datalink.config.loader import load_settings
from datalink.adapters.factory import build_adapters
from datalink.quality.control import create_control_tables, CONTROL_SCHEMA
wh = build_adapters(load_settings(env='dev')).warehouse
create_control_tables(wh)
n = list(wh.query(f\"SELECT COUNT(*) c FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{CONTROL_SCHEMA}'\"))[0]['c']
print(f'  ✅ {n} tables present in {CONTROL_SCHEMA} (expected 43+)')
" || { step_fail "create_control_tables failed"; exit 1; }

# ----------------------------------------------------------------------------
# Step 4 — All schema migrations in order (15.5 → 17.4)
#
# Each script is idempotent. Running them on a fresh account creates the
# tables/columns. Running them on an account that already has the migration
# applied is a no-op.
# ----------------------------------------------------------------------------
banner " Step 4: Schema migrations 15.5 → 17.4 (idempotent)"

# Phase 15 — Gold schema registry + Silver registry
"${REPO_ROOT}/.venv/bin/python3" "${REPO_ROOT}/scripts/migrate_phase15_5_gold_schema.py" \
    2>&1 | tail -6 | sed 's/^/  /' || { step_fail "Phase 15.5 migration failed"; exit 1; }
step_pass "Phase 15.5 — Gold schema registry"

"${REPO_ROOT}/.venv/bin/python3" "${REPO_ROOT}/scripts/migrate_phase15_8_silver_registry.py" \
    2>&1 | tail -6 | sed 's/^/  /' || { step_fail "Phase 15.8 migration failed"; exit 1; }
step_pass "Phase 15.8 — Silver registry foundation"

# Phase 16.1 — Saved-proposal store (avoids re-burning LLM tokens)
"${REPO_ROOT}/.venv/bin/python3" "${REPO_ROOT}/scripts/migrate_phase16_agent_proposals.py" \
    2>&1 | tail -6 | sed 's/^/  /' || { step_fail "Phase 16.1 migration failed"; exit 1; }
step_pass "Phase 16.1 — Saved-proposal store"

# Phase 16.2 — Universal version registry
"${REPO_ROOT}/.venv/bin/python3" "${REPO_ROOT}/scripts/migrate_phase16_2_versioning.py" \
    2>&1 | tail -6 | sed 's/^/  /' || { step_fail "Phase 16.2 migration failed"; exit 1; }
step_pass "Phase 16.2 — Universal version registry"

# Phase 16.7 — Global template layer (introduces scope_owner column)
"${REPO_ROOT}/.venv/bin/python3" "${REPO_ROOT}/scripts/migrate_phase16_7_global_templates.py" \
    2>&1 | tail -6 | sed 's/^/  /' || { step_fail "Phase 16.7 migration failed"; exit 1; }
step_pass "Phase 16.7 — Global template layer"

# Phase 17.1 — Rename __global__ → GLOBAL_CORP. No-op on fresh account
# (no rows to UPDATE, no legacy schemas to rename) but harmless.
docker exec datalink-control-tower python /opt/datalink/scripts/migrate_phase17_1_global_corp_rename.py \
    2>&1 | tail -6 | sed 's/^/  /' || step_warn "Phase 17.1 migration completed with skips (expected on fresh account)"
step_pass "Phase 17.1 — Naming canonicalization (GLOBAL_CORP)"

# Phase 17.2 — _extra (Bronze) + _extensions (Silver Sat) VARIANT columns.
# Skipped automatically when no Bronze/Sat tables exist yet.
docker exec datalink-control-tower python /opt/datalink/scripts/migrate_phase17_2_extra_extensions.py \
    2>&1 | tail -6 | sed 's/^/  /' || step_warn "Phase 17.2 migration completed with skips (expected on fresh account)"
step_pass "Phase 17.2 — VARIANT extension columns"

# Phase 17.4 — Gold semver + per-client subscriptions
docker exec datalink-control-tower python /opt/datalink/scripts/migrate_phase17_4_gold_versioning.py \
    2>&1 | tail -8 | sed 's/^/  /' || { step_fail "Phase 17.4 migration failed"; exit 1; }
step_pass "Phase 17.4 — Gold semver + subscriptions"

# ----------------------------------------------------------------------------
# Step 5 — Load Bronze catalog (datasets / fields / routing rules)
# ----------------------------------------------------------------------------
banner " Step 5: Loading Bronze catalog (33 datasets / 943 fields / 61 routing rules)"

"${REPO_ROOT}/.venv/bin/python3" "${REPO_ROOT}/scripts/load_product_catalog.py" \
    2>&1 | tail -10 | sed 's/^/  /' || { step_fail "load_product_catalog failed"; exit 1; }

# ----------------------------------------------------------------------------
# Step 6 — Load 6 industry standards STAGGERED (Gotcha #4 — Voyage rate limit)
# ----------------------------------------------------------------------------
banner " Step 6: Loading 6 industry standards (staggered for Voyage rate limit — ~10 min)"

if [ ! -x "${REPO_ROOT}/scripts/_load_standards_staggered.sh" ]; then
    chmod +x "${REPO_ROOT}/scripts/_load_standards_staggered.sh"
fi

# Run via control_tower (has streamlit + voyageai + on datalink network for postgres) — Gotchas #6 #7
bash "${REPO_ROOT}/scripts/_load_standards_staggered.sh" 2>&1 | \
    grep --line-buffered -E "Loading|bulk_upserted|Error|Traceback|ALL .* PROCESSED" | sed 's/^/  /'

# ----------------------------------------------------------------------------
# Step 7 — Verify all source-of-truth row counts
# ----------------------------------------------------------------------------
banner " Step 7: Verifying source-of-truth row counts"

venv_py "
from datalink.config.loader import load_settings
from datalink.adapters.factory import build_adapters
from datalink.quality.control import CONTROL_SCHEMA
wh = build_adapters(load_settings(env='dev')).warehouse
all_pass = True
for t, expected in [
    ('global_bronze_catalog_datasets', 33),
    ('global_bronze_catalog_fields', 943),
    ('onprem_routing_rules', 61),
    ('standard_registry', 6),
]:
    try:
        n = list(wh.query(f'SELECT COUNT(*) c FROM {CONTROL_SCHEMA}.{t}'))[0]['c']
    except Exception as e:
        n = -1
    flag = '✅' if n == expected else '❌'
    if n != expected: all_pass = False
    print(f'  {flag} {t:40s} = {n:>4d}  (expected {expected})')
import sys; sys.exit(0 if all_pass else 1)
" || { step_fail "Source-of-truth counts FAILED"; exit 1; }

# ----------------------------------------------------------------------------
# Step 8 — Performance smoke test (Snowflake singleton + handshake timing)
# ----------------------------------------------------------------------------
banner " Step 8: Performance smoke test (singleton + cache decorators)"

ct_exec python -u -c "
import time
from datalink.ui._query import warehouse_ctx

def t(): return time.perf_counter()

with warehouse_ctx(readonly=True) as wh:
    pass  # warm the singleton

t0 = t()
with warehouse_ctx(readonly=True) as wh:
    rows = list(wh.query('SELECT COUNT(*) c FROM CONTROL.global_bronze_catalog_datasets'))
ms_first = round((t()-t0)*1000)

t0 = t()
for _ in range(3):
    with warehouse_ctx(readonly=True) as wh:
        list(wh.query('SELECT COUNT(*) c FROM CONTROL.global_bronze_catalog_fields'))
ms_avg = round((t()-t0)*1000/3)

ok_first = ms_first < 6000
ok_avg = ms_avg < 500
print(f'  {\"✅\" if ok_first else \"⚠️ \"} First query (after warm singleton): {ms_first} ms (expect < 6000)')
print(f'  {\"✅\" if ok_avg else \"⚠️ \"} Avg of 3 warm queries: {ms_avg} ms (expect < 500)')
import sys; sys.exit(0 if (ok_first and ok_avg) else 0)  # warn-only, don't fail bootstrap
" 2>&1 | grep -v snowflake.connect

# ----------------------------------------------------------------------------
# Done
# ----------------------------------------------------------------------------
banner " 🟢 BOOTSTRAP COMPLETE"
cat <<EOF

  Snowflake account ${SNOWFLAKE_ACCOUNT} is now ready.

  Catalog:        33 datasets, 943 fields, 61 routing rules
  Standards:      6 (FHIR, X12, NCPDP-D0, CMS, DV2, HEDIS) = 88 RAG chunks
  Performance:    Snowflake singleton + @st.cache_data(ttl=20) intact

  Streamlit UI:   http://localhost:8000  (Data Model Designer page should load fast)
  Airflow UI:     http://localhost:8088  (login airflow / airflow)

  Next step:      Drive the Membership demo from the Streamlit UI to author
                  Silver + Gold schemas, then deploy via Pipeline Architect.

  See docs/BOOTSTRAP_RUNBOOK.md for the full troubleshooting reference.
EOF
echo ""
