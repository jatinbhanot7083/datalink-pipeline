# DataLink Cold-Start Bootstrap Runbook

**Purpose:** Stand up DataLink on a brand-new Snowflake account (e.g. fresh trial) without falling into the gotchas we hit on 2026-05-04.

This is the **single source of truth** when you need to re-bootstrap. Every issue listed here has bitten us at least once.

---

## TL;DR — One-shot bootstrap (after .env is updated)

```bash
cd ~/dev/DataPipelinesWithGX
bash scripts/bootstrap_fresh_snowflake.sh
```

If that script doesn't exist yet, follow the manual sequence below.

---

## 0. Prerequisites checklist

Before touching any script, verify ALL of these:

| Check | Command | Pass criteria |
|---|---|---|
| Docker stack up | `docker ps \| grep datalink` | airflow-scheduler, airflow-webserver, airflow-db, control-tower, postgres all "Up (healthy)" |
| Repo at correct path | `pwd` | `~/dev/DataPipelinesWithGX` (matches docker-compose mount) |
| `.env` file exists | `ls -la .env` | mode 600, owned by jatin, ~30+ lines |
| Snowflake creds in `.env` | `grep ^SNOWFLAKE_ .env` | 6 lines: ACCOUNT, USER, PASSWORD, ROLE, WAREHOUSE, DATABASE |
| Voyage API key in `.env` | `grep ^VOYAGE_API_KEY .env` | one line, starts with `pa-` |
| Anthropic API key in `.env` | `grep ^ANTHROPIC_API_KEY .env` | one line, starts with `sk-ant-` |
| Containers see fresh `.env` | `docker exec datalink-control-tower env \| grep SNOWFLAKE_ACCOUNT` | matches `.env` value |

**If the last check fails** — containers were started BEFORE `.env` was updated. Fix:
```bash
docker compose up -d --force-recreate control_tower airflow_scheduler airflow_webserver
```

---

## 1. Snowflake account preparation (in Snowflake web UI)

Run this ONCE in a Snowflake worksheet on the fresh account, as `ACCOUNTADMIN`:

```sql
USE ROLE ACCOUNTADMIN;

-- Performance-tuned warehouse (was the cause of 8-9s page loads on 2026-05-04)
CREATE WAREHOUSE IF NOT EXISTS DATALINK_WH
    WAREHOUSE_SIZE = 'SMALL'                  -- 2x faster than XSMALL for our workload
    AUTO_SUSPEND = 300                        -- stay warm 5 min idle (vs 60s default)
    AUTO_RESUME = TRUE
    STATEMENT_TIMEOUT_IN_SECONDS = 60         -- fail fast on rogue queries
    INITIALLY_SUSPENDED = FALSE;

CREATE DATABASE IF NOT EXISTS DATALINK_DEV;
CREATE ROLE IF NOT EXISTS DATALINK_ENGINEER;
GRANT USAGE ON WAREHOUSE DATALINK_WH TO ROLE DATALINK_ENGINEER;
GRANT ALL ON DATABASE DATALINK_DEV TO ROLE DATALINK_ENGINEER;
GRANT ALL ON ALL SCHEMAS IN DATABASE DATALINK_DEV TO ROLE DATALINK_ENGINEER;
GRANT ALL ON FUTURE SCHEMAS IN DATABASE DATALINK_DEV TO ROLE DATALINK_ENGINEER;
GRANT ALL ON FUTURE TABLES IN DATABASE DATALINK_DEV TO ROLE DATALINK_ENGINEER;
GRANT ROLE DATALINK_ENGINEER TO USER <YOUR_USERNAME>;

-- If warehouse already exists with defaults, alter it instead:
ALTER WAREHOUSE DATALINK_WH SET
    WAREHOUSE_SIZE = 'SMALL'
    AUTO_SUSPEND = 300
    STATEMENT_TIMEOUT_IN_SECONDS = 60;

-- Wake it up so the next page nav is hot:
ALTER WAREHOUSE DATALINK_WH RESUME IF SUSPENDED;
```

**Why these settings matter:** XSMALL + 60s suspend = warehouse cold-starts on every page nav after 1min idle, adding 2-3s per click. SMALL + 300s suspend keeps the demo snappy.

**Account identifier formats** — Snowflake supports two:
- New (org-account): `ZEIARBG-FXA28735` ← use this for trials post-2023
- Legacy (locator): `eda86013.east-us-2.azure`

Both work. Strip `.snowflakecomputing.com` from the URL.

---

## 2. Update `.env` (3 lines only)

```bash
nano ~/dev/DataPipelinesWithGX/.env
```

Change:
```bash
SNOWFLAKE_ACCOUNT=<new-account-identifier>
SNOWFLAKE_USER=<signup-username>
SNOWFLAKE_PASSWORD='<password>'
```

Leave `SNOWFLAKE_ROLE`, `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_DATABASE` as-is — codebase expects `DATALINK_ENGINEER`, `DATALINK_WH`, `DATALINK_DEV`.

**Then restart containers** so they pick up the new values:
```bash
docker compose up -d --force-recreate control_tower airflow_scheduler airflow_webserver
```

---

## 3. Bootstrap sequence (manual, if script missing)

### Step 1 — Connectivity test (do this BEFORE migrations)

```bash
cd ~/dev/DataPipelinesWithGX
set -a && source .env && set +a
source .venv/bin/activate
DL_ENV=dev python3 -c "
import snowflake.connector, os
conn = snowflake.connector.connect(
    account=os.environ['SNOWFLAKE_ACCOUNT'], user=os.environ['SNOWFLAKE_USER'],
    password=os.environ['SNOWFLAKE_PASSWORD'], role=os.environ['SNOWFLAKE_ROLE'],
    warehouse=os.environ['SNOWFLAKE_WAREHOUSE'], database=os.environ['SNOWFLAKE_DATABASE'])
cur = conn.cursor()
cur.execute('SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_DATABASE()')
print(cur.fetchone())
"
```

Should print all 4 context values. If it errors, do NOT proceed — fix creds first.

### Step 2 — Create CONTROL schema + base tables

```bash
DL_ENV=dev python3 -c "
from datalink.config.loader import load_settings
from datalink.adapters.factory import build_adapters
from datalink.quality.control import create_control_tables, CONTROL_SCHEMA
wh = build_adapters(load_settings(env='dev')).warehouse
create_control_tables(wh)
rows = list(wh.query(f\"SELECT COUNT(*) AS c FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{CONTROL_SCHEMA}'\"))
print(f'CONTROL tables: {rows[0][\"c\"]}')
"
```
Expect: **43 tables**.

### Step 3 — Phase 15.5 + 15.8 migrations

```bash
DL_ENV=dev python3 scripts/migrate_phase15_5_gold_schema.py
DL_ENV=dev python3 scripts/migrate_phase15_8_silver_registry.py
```
Expect: **+5 Gold tables, +6 Silver tables** = 54 total.

### Step 4 — Load Bronze catalog + routing rules

```bash
DL_ENV=dev python3 scripts/load_product_catalog.py
```
Expect:
```
datasets             33
fields               943
routing_rules        61
```

### Step 5 — Load industry standards (RATE-LIMITED — see gotcha #4)

**DO NOT just run** `python3 scripts/load_industry_standards.py` on a fresh load. Voyage free tier will rate-limit. Use the staggered loop:

```bash
for code in fhir-r4 x12 ncpdp-d0 cms dv2 hedis; do
    echo "=== Loading $code ==="
    docker exec -e DL_ENV=dev datalink-control-tower \
        python -u /opt/datalink/scripts/load_industry_standards.py --only $code
    echo "--- Sleeping 75s for Voyage rate-limit window ---"
    sleep 75
done
```

ETA: ~12 minutes. Standards table should end up with **6 rows**, agent_memory.standard_references should have **~88 rows**.

### Step 6 — Verify all source-of-truth counts

```bash
DL_ENV=dev python3 -c "
from datalink.config.loader import load_settings
from datalink.adapters.factory import build_adapters
from datalink.quality.control import CONTROL_SCHEMA
wh = build_adapters(load_settings(env='dev')).warehouse
for t, expected in [
    ('global_bronze_catalog_datasets', 33),
    ('global_bronze_catalog_fields', 943),
    ('onprem_routing_rules', 61),
    ('standard_registry', 6),
]:
    n = list(wh.query(f'SELECT COUNT(*) c FROM {CONTROL_SCHEMA}.{t}'))[0]['c']
    flag = '✅' if n == expected else '⚠️ '
    print(f'  {flag} {t:40s} = {n} (expected {expected})')
"
```

---

## 4. Gotchas (each one bit us on 2026-05-04)

### Gotcha #1 — `.env` not auto-loaded by host scripts
**Symptom:** `SnowflakeWarehouse: missing required credential(s): account, user, password...`
**Cause:** Plain `python3 scripts/foo.py` doesn't auto-source `.env`.
**Fix:** Always run scripts via `set -a && source .env && set +a` first, OR via `docker exec` into a container that has `env_file: .env` in compose.

### Gotcha #2 — Containers pin `.env` at start time
**Symptom:** Updated `.env` but containers still hit old Snowflake account.
**Cause:** `env_file:` in compose is read at container creation. `docker compose restart` does NOT re-read it; only `up --force-recreate` does.
**Fix:** After ANY `.env` change:
```bash
docker compose up -d --force-recreate control_tower airflow_scheduler airflow_webserver
```

### Gotcha #3 — `DL_ENV=local` defaults to DuckDB, not Snowflake
**Symptom:** Reset/migration scripts run against the wrong warehouse silently. Errors mention DuckDB tables.
**Cause:** `os.environ.get("DL_ENV", "local")` defaults to `local` config, which is DuckDB.
**Fix:** ALWAYS export `DL_ENV=dev` before running anything that should hit Snowflake. The `master_reset.sh` script now does this automatically.

### Gotcha #4 — Voyage AI free tier rate-limits cold-start standards load
**Symptom:** `memory.embed_rate_limit_retry attempt=0 sleep_s=25 → 50 → 100`, then connection error mid-retry.
**Cause:** Loading all 6 standards at once = ~20 embedding requests in <30s. Voyage free tier ~3 req/min.
**Fix:** Use the staggered loop in Step 5 above. ~75s between standards lets the per-minute window reset.
**Worse:** Each retry re-embeds the SAME chunks (script isn't idempotent at embedding layer), burning more quota. Don't `Ctrl+C` and re-run on rate-limit error — let the retries finish or wait 2 min before any new attempt.

### Gotcha #5 — `load_product_catalog.py` used wrong column name
**Symptom:** `invalid identifier 'GOLD_COLUMN_NAME'` during field insert.
**Cause:** Script's INSERT used `gold_column_name`; Phase 15.5 migration renamed it to `bronze_column_name`. Script wasn't updated.
**Fix:** Already patched in `scripts/load_product_catalog.py` line 415. If you see this again, the patch was reverted.

### Gotcha #6 — Host venv missing `streamlit` / `voyageai`
**Symptom:** `ModuleNotFoundError: No module named 'streamlit'` when running `load_industry_standards.py` from host venv.
**Cause:** Host `.venv` was minimal; full deps live in the docker images.
**Fix:** `uv pip install streamlit voyageai` OR run the script via `docker exec datalink-control-tower python ...` (container already has them).

### Gotcha #7 — Script needs Postgres (`postgres` hostname)
**Symptom:** `psycopg.OperationalError: failed to resolve host 'postgres'`
**Cause:** AgentMemoryStore connects to `postgres:5432` (docker-network name); host can't resolve it.
**Fix:** Run via `docker exec datalink-control-tower` or `docker exec datalink-airflow-scheduler` (both on `datalink` network).

### Gotcha #8 — `-e VAR="$VAR"` env-passing in docker exec drops values
**Symptom:** `docker exec -e SNOWFLAKE_ACCOUNT="$SNOWFLAKE_ACCOUNT"` results in `SNOWFLAKE_ACCOUNT=` (blank) in container.
**Cause:** Quoting/expansion mismatch in `wsl bash -lc "..."` invocations.
**Fix:** Don't fight env-passing. Instead, restart the container with the new `.env` (Gotcha #2 fix), then exec into it.

### Gotcha #9 — Pipeline Architect writes files as `root` inside container
**Symptom:** `dbt/models/silver/aetna/` exists owned by `root:root`; `shutil.rmtree(ignore_errors=True)` from host venv silently fails to remove.
**Cause:** Airflow tasks run as `root` inside the airflow container; files created in mounted volumes inherit that ownership.
**Fix:** Cleanup must use `docker exec datalink-airflow-scheduler rm -rf /opt/datalink/...` (root-in-container can delete root-owned files). Already wired into `master_reset.sh`.

### Gotcha #11 — Git Bash mangles `$VAR` before passing to `wsl bash -lc '...'`
**Symptom:** A `for code in a b c; do ... $code ...; done` loop runs with `$code` BLANK every iteration; output shows `=== Loading  ===` instead of `=== Loading fhir-r4 ===`.
**Cause:** When you run `wsl bash -lc '...'` from Git Bash on Windows, Git Bash performs its OWN variable-expansion pass on the command string before handing it to WSL. `$code` (undefined in Git Bash's outer context) expands to empty string, so the inner WSL bash receives `--only ` with no value.
**Fix:** Write multi-line / loop / variable-using bash logic into a `.sh` file and invoke as `wsl bash -lc 'bash /path/to/script.sh'`. Don't try to inline bash loops through `wsl bash -lc "..."` or `'...'` — they will silently break.
**Worse:** Single-quoting the outer string (`bash -lc '...'`) doesn't help; Git Bash still expands. The ONLY safe pattern is a script file.

### Gotcha #10 — Snowflake free trial expires silently
**Symptom:** `250001 (08001): Failed to connect to DB: ... Your free trial has ended and all of your virtual warehouses have been suspended.`
**Cause:** 30-day trial elapsed; Snowflake auto-suspends warehouses.
**Fix:** Either add billing in Snowflake UI (Admin → Billing & Terms → add CC), or sign up for a fresh trial (creates new account identifier — must update `.env` and re-bootstrap).

---

## 5. Reset back to clean state

When the demo gets noisy and you want to start over:

```bash
bash scripts/master_reset.sh
```

This script (per Gotchas #1, #2, #3, #9 fixes):
- Sources `.env` automatically
- Defaults `DL_ENV=dev`
- Uses `docker exec` for root-owned file cleanup
- Truncates CONTROL tables
- Drops physical schemas (BRONZE_AETNA, SILVER_AETNA, GOLD_AETNA)
- Wipes Airflow metadata for `aetna_membership_pipeline`
- Restarts scheduler + webserver

After reset, the **catalog data stays** (Bronze 33/943/61 + standards 88) — only test artifacts are wiped. If you also need to re-load catalog/standards, re-run sections 3 (steps 4-6) above.

---

## 6. Quick-glance: container responsibilities

| Container | Role | Has streamlit? | Has dbt? | Snowflake creds? | On `datalink` network? |
|---|---|---|---|---|---|
| `datalink-control-tower` | Streamlit UI | ✅ | ❌ | ✅ (from .env) | ✅ |
| `datalink-airflow-scheduler` | DAG executor | ❌ | ✅ | ✅ (from .env) | ✅ |
| `datalink-airflow-webserver` | Airflow UI | ❌ | ✅ | ✅ (from .env) | ✅ |
| Host `.venv` | Dev / scripts | ⚠️ install on demand | ❌ | ⚠️ source .env first | ❌ (can't resolve `postgres`) |

**Rule of thumb:**
- Touching pgvector? → `docker exec datalink-control-tower` or `datalink-airflow-scheduler`
- Touching only Snowflake? → host venv (after `set -a && source .env`)
- Touching dbt? → `docker exec datalink-airflow-scheduler`

---

## 7. Open items

- [ ] Convert this runbook to an executable `scripts/bootstrap_fresh_snowflake.sh`
- [ ] Make `load_industry_standards.py` idempotent at the embedding layer (skip chunks already in pgvector)
- [ ] Add `--rate-limit-sleep N` flag to `load_industry_standards.py` so sequential loop isn't needed
- [ ] Add a single `scripts/health_check.py` that runs all pre-flight checks from §0

---

*Last updated: 2026-05-04 (after the new-Snowflake-trial bootstrap pain).*
