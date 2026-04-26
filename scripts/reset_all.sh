#!/usr/bin/env bash
# ============================================================================
# reset_all.sh — Blow away all DataLink pipeline state and start from scratch.
# ============================================================================
#
# Covers every data/artifact store used by the EvokeConnectCare™ pipeline:
#
#   1. DuckDB         — warehouse.duckdb: drop BRONZE_*/SILVER_* schemas,
#                       truncate every CONTROL.* table.
#   2. Snowflake      — DATALINK_DEV: truncate every non-INFORMATION_SCHEMA
#                       table across BRONZE/SILVER/CONTROL (+ any _* variants).
#   3. Postgres UM    — datalink_um: TRUNCATE um.member/claim/provider.
#   4. Postgres Repl. — datalink_um_replica: TRUNCATE (in case replication
#                       doesn't propagate TRUNCATE, do it explicitly).
#   5. SQL Server UM  — DataLinkUM: TRUNCATE UM.member/claim/provider.
#   6. GX artifacts   — gx/uncommitted/validations, gx/uncommitted/data_docs.
#   7. (optional) Airflow DAG run history — skipped unless --wipe-airflow.
#   8. Reseed         — re-runs baseline_seeder so CONTROL.dq_suites
#                       is repopulated with aetna / cano_health / etc.
#
# Safe to re-run. Idempotent. Prints a loud banner for every section.
#
# USAGE
# -----
#   ./scripts/reset_all.sh               # soft reset + reseed registry
#   ./scripts/reset_all.sh --nuke        # also drops dq_suites (full nuke,
#                                          registry rebuilt by seeder)
#   ./scripts/reset_all.sh --dry-run     # show what would run, do nothing
#   ./scripts/reset_all.sh --wipe-airflow
#                                        # also clears Airflow run history
#   ./scripts/reset_all.sh --skip-snowflake
#                                        # skip the Snowflake section
#   ./scripts/reset_all.sh --skip-sqlserver
#                                        # skip the SQL Server section
#   ./scripts/reset_all.sh --yes         # skip the confirmation prompt
#
# REQUIREMENTS
# ------------
#   - Run from repo root (scripts/ subfolder).
#   - Docker containers must be up (postgres, sqlserver running).
#   - .env in repo root with SNOWFLAKE_* + POSTGRES_* + SQLSERVER_*.
#   - Python venv at .venv with duckdb + datalink installed.
#
# ============================================================================

set -euo pipefail

# ---- Flags ----
DRY_RUN=0
NUKE=0
WIPE_AIRFLOW=0
SKIP_SF=0
SKIP_SS=0
SKIP_PG=0
SKIP_DUCK=0
SKIP_GX=0
SKIP_SEED=0
ASSUME_YES=0

for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY_RUN=1 ;;
    --nuke) NUKE=1 ;;
    --wipe-airflow) WIPE_AIRFLOW=1 ;;
    --skip-snowflake) SKIP_SF=1 ;;
    --skip-sqlserver) SKIP_SS=1 ;;
    --skip-postgres) SKIP_PG=1 ;;
    --skip-duckdb) SKIP_DUCK=1 ;;
    --skip-gx) SKIP_GX=1 ;;
    --skip-seed) SKIP_SEED=1 ;;
    --yes|-y) ASSUME_YES=1 ;;
    -h|--help)
      sed -n '2,40p' "$0"; exit 0 ;;
    *) echo "Unknown flag: $arg"; exit 2 ;;
  esac
done

# ---- Resolve repo root ----
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

# ---- Load .env so SNOWFLAKE_*, POSTGRES_*, SQLSERVER_* are available ----
if [[ -f .env ]]; then
  set -a; source .env; set +a
fi

# ---- Pretty printing ----
C_RED=$'\033[1;31m'; C_GRN=$'\033[1;32m'; C_YEL=$'\033[1;33m'
C_BLU=$'\033[1;34m'; C_CYA=$'\033[1;36m'; C_RST=$'\033[0m'

banner() {
  local title="$1"
  echo
  echo "${C_CYA}==============================================================${C_RST}"
  echo "${C_CYA}  $title${C_RST}"
  echo "${C_CYA}==============================================================${C_RST}"
}

info()    { echo "${C_BLU}[INFO]${C_RST}   $*"; }
ok()      { echo "${C_GRN}[ OK ]${C_RST}   $*"; }
warn()    { echo "${C_YEL}[WARN]${C_RST}   $*"; }
err()     { echo "${C_RED}[FAIL]${C_RST}   $*"; }
run()     {
  if [[ $DRY_RUN -eq 1 ]]; then
    echo "${C_YEL}[DRY ]${C_RST}   $*"
  else
    echo "${C_BLU}[RUN ]${C_RST}   $*"
    eval "$@"
  fi
}

# ---- Confirm (unless --yes or --dry-run) ----
if [[ $DRY_RUN -eq 0 && $ASSUME_YES -eq 0 ]]; then
  banner "⚠️  RESET ALL PIPELINE STATE"
  echo "This will wipe data from:"
  [[ $SKIP_DUCK -eq 0 ]] && echo "  • DuckDB (warehouse.duckdb)"
  [[ $SKIP_SF   -eq 0 ]] && echo "  • Snowflake (DATALINK_DEV)"
  [[ $SKIP_PG   -eq 0 ]] && echo "  • Postgres (datalink_um + replica)"
  [[ $SKIP_SS   -eq 0 ]] && echo "  • SQL Server (DataLinkUM)"
  [[ $SKIP_GX   -eq 0 ]] && echo "  • GX artifacts (gx/uncommitted/)"
  [[ $WIPE_AIRFLOW -eq 1 ]] && echo "  • Airflow DAG run history"
  [[ $NUKE -eq 1 ]] && echo "  • CONTROL.dq_suites REGISTRY ${C_RED}(full nuke)${C_RST}"
  [[ $SKIP_SEED -eq 0 ]] && echo "  • Then reseed CONTROL.dq_suites via baseline_seeder"
  echo
  read -r -p "Type 'yes' to proceed: " CONFIRM
  if [[ "$CONFIRM" != "yes" ]]; then
    err "Aborted."
    exit 1
  fi
fi

# ============================================================================
# SECTION 1 — DuckDB
# ============================================================================
if [[ $SKIP_DUCK -eq 0 ]]; then
  banner "1/8  DuckDB — warehouse.duckdb"

  if [[ ! -f warehouse.duckdb ]]; then
    warn "warehouse.duckdb not found at repo root — skipping"
  else
    PYTHON_BIN="${PYTHON_BIN:-.venv/bin/python}"
    if [[ ! -x "$PYTHON_BIN" ]]; then
      # fall back to system python if venv path differs
      PYTHON_BIN="$(command -v python3 || command -v python)"
    fi

    NUKE_FLAG="$NUKE"
    DRY_FLAG="$DRY_RUN"

    run "$PYTHON_BIN - <<PYEOF
import duckdb, sys
NUKE = ${NUKE_FLAG} == 1
DRY  = ${DRY_FLAG} == 1
con = duckdb.connect('warehouse.duckdb')

# Drop all per-tenant BRONZE_*/SILVER_* schemas (everything + re-create empty)
rows = con.execute('''
    SELECT schema_name FROM information_schema.schemata
    WHERE schema_name LIKE 'BRONZE_%' OR schema_name LIKE 'SILVER_%'
    ORDER BY 1
''').fetchall()
for (s,) in rows:
    if DRY:
        print(f'  [DRY] DROP SCHEMA \"{s}\" CASCADE')
    else:
        con.execute(f'DROP SCHEMA IF EXISTS \"{s}\" CASCADE')
        print(f'  dropped schema {s}')

# Truncate every table in CONTROL (except dq_suites unless --nuke)
rows = con.execute('''
    SELECT table_name FROM information_schema.tables
    WHERE table_schema = 'CONTROL' ORDER BY 1
''').fetchall()
for (t,) in rows:
    if t.lower() == 'dq_suites' and not NUKE:
        print(f'  kept CONTROL.{t} (registry — pass --nuke to truncate)')
        continue
    if DRY:
        print(f'  [DRY] TRUNCATE CONTROL.{t}')
    else:
        # DuckDB doesn't have TRUNCATE; use DELETE
        con.execute(f'DELETE FROM CONTROL.\"{t}\"')
        print(f'  truncated CONTROL.{t}')

con.close()
print('  DuckDB reset complete.')
PYEOF"
    ok "DuckDB wiped"
  fi
else
  warn "Skipping DuckDB (--skip-duckdb)"
fi

# ============================================================================
# SECTION 2 — Snowflake
# ============================================================================
if [[ $SKIP_SF -eq 0 ]]; then
  banner "2/8  Snowflake — ${SNOWFLAKE_DATABASE:-DATALINK_DEV}"

  if [[ -z "${SNOWFLAKE_ACCOUNT:-}" ]]; then
    warn "SNOWFLAKE_ACCOUNT not set — skipping Snowflake"
  else
    PYTHON_BIN="${PYTHON_BIN:-.venv/bin/python}"
    [[ -x "$PYTHON_BIN" ]] || PYTHON_BIN="$(command -v python3 || command -v python)"

    run "$PYTHON_BIN - <<PYEOF
import os, sys
try:
    import snowflake.connector as sf
except ImportError:
    print('  snowflake-connector-python not installed — pip install snowflake-connector-python')
    sys.exit(0)

NUKE = ${NUKE} == 1
DRY  = ${DRY_RUN} == 1

con = sf.connect(
    account  = os.environ['SNOWFLAKE_ACCOUNT'],
    user     = os.environ['SNOWFLAKE_USER'],
    password = os.environ['SNOWFLAKE_PASSWORD'],
    role     = os.environ.get('SNOWFLAKE_ROLE', 'DATALINK_ENGINEER'),
    warehouse= os.environ.get('SNOWFLAKE_WAREHOUSE', 'DATALINK_WH'),
    database = os.environ.get('SNOWFLAKE_DATABASE', 'DATALINK_DEV'),
)
cur = con.cursor()
db = os.environ.get('SNOWFLAKE_DATABASE', 'DATALINK_DEV')

# Find all non-system schemas
cur.execute(f\"SHOW SCHEMAS IN DATABASE {db}\")
schemas = [r[1] for r in cur.fetchall()
           if r[1] not in ('INFORMATION_SCHEMA','PUBLIC')]
print(f'  schemas found: {schemas}')

for schema in schemas:
    cur.execute(f\"SHOW TABLES IN SCHEMA {db}.{schema}\")
    tables = [r[1] for r in cur.fetchall()]
    for t in tables:
        fq = f'{db}.{schema}.{t}'
        if schema == 'CONTROL' and t.upper() == 'DQ_SUITES' and not NUKE:
            print(f'  kept {fq} (registry — pass --nuke to truncate)')
            continue
        if DRY:
            print(f'  [DRY] TRUNCATE {fq}')
        else:
            try:
                cur.execute(f'TRUNCATE TABLE {fq}')
                print(f'  truncated {fq}')
            except Exception as e:
                print(f'  [warn] {fq}: {e}')

cur.close(); con.close()
print('  Snowflake reset complete.')
PYEOF"
    ok "Snowflake wiped"
  fi
else
  warn "Skipping Snowflake (--skip-snowflake)"
fi

# ============================================================================
# SECTION 3 — Postgres UM (datalink_um)
# ============================================================================
if [[ $SKIP_PG -eq 0 ]]; then
  banner "3/8  Postgres — datalink_um"

  PG_USER="${POSTGRES_USER:-datalink}"
  PG_PASS="${POSTGRES_PASSWORD:-datalink_local_only}"
  PG_DB="${POSTGRES_DATABASE:-datalink_um}"
  PG_HOST="${POSTGRES_HOST:-localhost}"
  PG_PORT="${POSTGRES_PORT:-5432}"

  run "PGPASSWORD='$PG_PASS' psql -h $PG_HOST -p $PG_PORT -U $PG_USER -d $PG_DB -v ON_ERROR_STOP=1 -c \"
    DO \\\$\\\$
    DECLARE r record;
    BEGIN
      FOR r IN SELECT tablename FROM pg_tables WHERE schemaname='um' LOOP
        EXECUTE 'TRUNCATE TABLE um.' || quote_ident(r.tablename) || ' RESTART IDENTITY CASCADE';
        RAISE NOTICE 'truncated um.%', r.tablename;
      END LOOP;
    END \\\$\\\$;
  \" 2>&1 || echo '  (psql not on PATH — falling back to docker exec)'"

  # Fallback via docker exec if psql isn't local
  if ! command -v psql >/dev/null 2>&1; then
    run "docker exec -e PGPASSWORD='$PG_PASS' datalink-postgres \
      psql -U $PG_USER -d $PG_DB -v ON_ERROR_STOP=1 -c \"
        DO \\\$\\\$
        DECLARE r record;
        BEGIN
          FOR r IN SELECT tablename FROM pg_tables WHERE schemaname='um' LOOP
            EXECUTE 'TRUNCATE TABLE um.' || quote_ident(r.tablename) || ' RESTART IDENTITY CASCADE';
          END LOOP;
        END \\\$\\\$;
      \""
  fi
  ok "Postgres UM wiped"

  # Replica — replication should carry it, but be explicit if configured
  banner "3b/8  Postgres Replica — datalink_um_replica (belt & suspenders)"
  if docker ps --format '{{.Names}}' 2>/dev/null | grep -q datalink-postgres-replica; then
    run "docker exec -e PGPASSWORD='$PG_PASS' datalink-postgres-replica \
      psql -U $PG_USER -d datalink_um_replica -v ON_ERROR_STOP=1 -c \"
        SELECT 'replica is read-only — TRUNCATE skipped, replication carries the wipe';
      \" 2>&1 || warn '  replica not writable (expected if streaming replication)'"
  else
    warn "  replica container not running — skipping"
  fi
else
  warn "Skipping Postgres (--skip-postgres)"
fi

# ============================================================================
# SECTION 4 — SQL Server UM (DataLinkUM)
# ============================================================================
if [[ $SKIP_SS -eq 0 ]]; then
  banner "4/8  SQL Server — DataLinkUM"

  SS_SA_PASS="${MSSQL_SA_PASSWORD:-DataLink_LocalOnly_2026!}"
  SS_CONTAINER="${SS_CONTAINER:-datalink-sqlserver}"

  run "docker exec $SS_CONTAINER /opt/mssql-tools18/bin/sqlcmd \
    -S localhost -U sa -P '$SS_SA_PASS' -No -b -d DataLinkUM -Q \"
      DECLARE @sql NVARCHAR(MAX) = N'';
      SELECT @sql = @sql + 'TRUNCATE TABLE [UM].[' + name + '];' + CHAR(10)
        FROM sys.tables WHERE schema_id = SCHEMA_ID('UM');
      PRINT @sql;
      EXEC sp_executesql @sql;
    \""
  ok "SQL Server wiped"
else
  warn "Skipping SQL Server (--skip-sqlserver)"
fi

# ============================================================================
# SECTION 5 — GX artifacts (validations + data_docs)
# ============================================================================
if [[ $SKIP_GX -eq 0 ]]; then
  banner "5/8  GX — gx/uncommitted/"

  # GX artifacts are written by the Airflow container running as UID 50000.
  # Bind-mounted onto the host they appear owned by 50000, and the host's
  # $USER (usually 1000) can't delete them. Solution: delete from INSIDE the
  # airflow container, where UID 50000 is the owner. Falls back to host
  # delete if airflow isn't running.
  _gx_clean_in_container() {
    local container="$1" subdir="$2"
    # -u 0 = run as root inside the container, bypasses UID 50000 vs root-
    # owned leftovers (e.g. data_docs/local_site created by nginx container).
    # The nested find+rm avoids failing on empty root-owned dirs we can't remove.
    run "docker exec -u 0 $container bash -c '
      cd /opt/datalink/gx/uncommitted/$subdir 2>/dev/null || exit 0
      find . -mindepth 1 -maxdepth 1 -exec rm -rf {} + 2>&1 | grep -v \"Permission denied\" || true
    '"
  }
  _gx_clean_on_host() {
    local subdir="$1"
    run "find gx/uncommitted/$subdir -mindepth 1 -maxdepth 1 -exec rm -rf {} + 2>&1 | tee /tmp/gx_clean.err; \
         if grep -q 'Permission denied' /tmp/gx_clean.err; then \
           warn 'host delete hit Permission denied — falling back to container'; \
           return 1; \
         fi"
  }

  if docker ps --format '{{.Names}}' 2>/dev/null | grep -q datalink-airflow-scheduler; then
    info "Airflow container is up — deleting GX artifacts from inside (UID 50000)"
    _gx_clean_in_container datalink-airflow-scheduler validations
    _gx_clean_in_container datalink-airflow-scheduler data_docs
    ok "Cleared gx/uncommitted/{validations,data_docs}"
  else
    warn "Airflow container down — attempting host delete (may fail if UID mismatch)"
    [[ -d gx/uncommitted/validations ]] && run "find gx/uncommitted/validations -mindepth 1 -maxdepth 1 -exec rm -rf {} + 2>/dev/null || true"
    [[ -d gx/uncommitted/data_docs ]] && run "find gx/uncommitted/data_docs -mindepth 1 -maxdepth 1 -exec rm -rf {} + 2>/dev/null || true"
    ok "Cleared gx/uncommitted/{validations,data_docs} (host-side, best-effort)"
  fi
else
  warn "Skipping GX (--skip-gx)"
fi

# ============================================================================
# SECTION 6 — Airflow DAG run history (optional)
# ============================================================================
if [[ $WIPE_AIRFLOW -eq 1 ]]; then
  banner "6/8  Airflow — DAG run history"

  run "docker exec datalink-airflow-webserver airflow db clean \
    --clean-before-timestamp '$(date -u -Iseconds)' --yes 2>&1 || \
    warn 'airflow db clean failed — try airflow db reset manually'"
  ok "Airflow history cleared"
else
  info "Skipping Airflow history (pass --wipe-airflow to include)"
fi

# ============================================================================
# SECTION 7 — Reseed CONTROL.dq_suites
# ============================================================================
if [[ $SKIP_SEED -eq 0 ]]; then
  banner "7/8  Reseed — CONTROL.dq_suites via baseline_seeder"

  PYTHON_BIN="${PYTHON_BIN:-.venv/bin/python}"
  [[ -x "$PYTHON_BIN" ]] || PYTHON_BIN="$(command -v python3 || command -v python)"

  run "$PYTHON_BIN -c '
from datalink.config.loader import load_settings
from datalink.adapters.factory import build_adapters
from datalink.quality.baseline_seeder import seed_all_real_clients

settings = load_settings()
adapters = build_adapters(settings)
wh = adapters.warehouse
result = seed_all_real_clients(wh)
print(\"  reseeded:\", result)
'"
  ok "Registry reseeded"
else
  warn "Skipping reseed (--skip-seed)"
fi

# ============================================================================
# DONE
# ============================================================================
banner "8/8  ✅ RESET COMPLETE"
echo "Next steps:"
echo "  1. Open Control Tower → confirm sidebar shows clients again"
echo "  2. Every metric should read 0 / empty for every tenant"
echo "  3. Trigger ▶ Run Bronze → ▶ Run Silver → ▶ Run Gold to re-populate"
echo
echo "Verify from the outside:"
echo "  Postgres: PGPASSWORD=... psql -h localhost -U ${POSTGRES_USER:-datalink} \\"
echo "              -d ${POSTGRES_DATABASE:-datalink_um} \\"
echo "              -c \"SELECT schemaname||'.'||tablename AS t, \\"
echo "                   (xpath('/row/c/text()', query_to_xml("
echo "                     'SELECT COUNT(*) FROM '||schemaname||'.'||tablename, true, true, '')))[1]::text AS rows"
echo "                   FROM pg_tables WHERE schemaname='um';\""
echo "  SQL Server: docker exec datalink-sqlserver /opt/mssql-tools18/bin/sqlcmd \\"
echo "              -S localhost -U sa -P '...' -No -d DataLinkUM \\"
echo "              -Q \"SELECT t.name, p.rows FROM sys.tables t \\"
echo "                   JOIN sys.partitions p ON p.object_id=t.object_id \\"
echo "                   WHERE p.index_id IN (0,1) AND SCHEMA_NAME(schema_id)='UM';\""
