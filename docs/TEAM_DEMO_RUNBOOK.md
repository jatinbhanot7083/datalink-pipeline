# Team Demo — Membership End-to-End

**Goal:** Walk the team from "nothing in Snowflake" → "data flowing Bronze→Silver→Gold via Airflow."

Total runtime: ~8 min. Five "before/after" reveal moments.

---

## ✋ Pre-flight (do this 1 min before the team joins)

Open three browser tabs and one terminal:

| Tab/Window | URL/Command | Purpose |
|---|---|---|
| Browser tab 1 | https://app.snowflake.com → `DATALINK_DEV` worksheet | Show "before" state |
| Browser tab 2 | http://localhost:8000 | Streamlit Control Tower (your driver) |
| Browser tab 3 | http://localhost:8088 (login: `airflow` / `airflow`) | Airflow UI |
| Terminal | `cd ~/dev/DataPipelinesWithGX && source .env` | For 2 manual commands |

---

## 🎬 ACT 1 — Show the "before" state (1 min)

### Tab 1 — Snowflake worksheet, paste and run:

```sql
USE DATABASE DATALINK_DEV;
SHOW SCHEMAS;
SELECT COUNT(*) AS catalog_datasets FROM CONTROL.global_bronze_catalog_datasets;
SELECT COUNT(*) AS silver_schemas_live FROM CONTROL.global_silver_schema_datasets;
SELECT COUNT(*) AS gold_schemas_live FROM CONTROL.global_gold_schema_datasets;
SELECT COUNT(*) AS pipeline_instances FROM CONTROL.client_pipeline_instances;
```

**Tell the team:**
> "Notice the only schemas are `CONTROL` (our metadata) plus the two Snowflake defaults. There's NO `BRONZE_AETNA`, NO `SILVER_AETNA`, NO `GOLD_AETNA`. Our catalog has 33 Bronze datasets cataloged with 943 fields — but zero LIVE Silver, zero LIVE Gold, zero deployed pipelines. We're starting from a true blank slate."

### Tab 3 — Airflow UI:

Show DAG list. **Tell the team:**
> "No `aetna_membership_pipeline` DAG either. We haven't generated it yet."

---

## 🎬 ACT 2 — Author Silver schema via AI (2 min)

### Tab 2 — Streamlit Control Tower:

1. Sidebar → **Data Model Designer**
2. Confirm tile counts: `33 Bronze / 0 Silver / 0 Gold / 0 Onboarded clients`
3. Click on **`membership`** in the dataset drill-down
4. Switch layer radio to **🥈 Silver**
5. Click **🤖 AI Construct**
6. Form values (defaults are correct, just submit):
   - Anchor: `CATALOG_ANCHOR`
   - Pattern: `HUB_SAT_LINK`
7. Click **Generate** → wait ~60 sec
8. Skim proposal: 13 tables, 44 mappings, ~15K tokens
9. Click **✅ Approve to LIVE**

**Tell the team:**
> "Claude Haiku 4.5 just designed a 13-table Data Vault 2.0 Silver schema for Membership in 60 seconds, anchored to our Bronze catalog. Cost: ~$0.02. This is now LIVE in our metadata registry."

---

## 🎬 ACT 3 — Author Gold schema via AI (1 min)

Same dataset (Membership). Switch layer to **🥇 Gold** → **🤖 AI Construct** → defaults → Generate → Approve to LIVE.

Expected: ~38 cols, 38 mappings, ~14K tokens, ~50 sec.

**Tell the team:**
> "Gold is the canonical flat schema downstream products consume. AI generated it from the Silver design + Bronze catalog + standards RAG context."

---

## 🎬 ACT 4 — Pipeline Architect deploy (2 min)

### Tab 2 — Sidebar → **Pipeline Architect**

1. Confirm dataset shows **Membership (41 fields, Member)** as default
2. Defaults are correct: `Bronze anchor=FLAT_FILE`, `Decision=AUTO`, `Schedule=0 4 * * *`
3. Click **🧬 Propose pipeline** → wait ~5 sec
4. Skim proposal: decision=BUILD, 41 resolved Gold cols, 51 GX expectations
5. Scroll to bottom → click **✅ Approve and Deploy**

**Show the team — tab 1 (Snowflake):**

```sql
SHOW SCHEMAS IN DATABASE DATALINK_DEV;
-- Notice: still no BRONZE_AETNA / SILVER_AETNA / GOLD_AETNA yet.
-- Pipeline metadata is registered, but physical DDLs need execution.
```

> "The Pipeline Architect emitted DDL files, dbt models, an Airflow DAG, and a GX expectation suite — but we still need to execute the DDLs to create physical schemas. One command:"

### Terminal — run the DDL executor:

```bash
.venv/bin/python3 scripts/_run_pipeline_ddls.py
```

Expected output: `BRONZE_AETNA / SILVER_AETNA / GOLD_AETNA` schemas created, with `RAW_MEMBERSHIP` (49 cols) + `MEMBERSHIP` (47 cols) tables.

### Tab 1 — Re-run:

```sql
SHOW SCHEMAS IN DATABASE DATALINK_DEV;
SHOW TABLES IN SCHEMA BRONZE_AETNA;
SHOW TABLES IN SCHEMA GOLD_AETNA;
DESC TABLE BRONZE_AETNA.RAW_MEMBERSHIP;
```

**Tell the team:**
> "Three new schemas. Physical Bronze + Gold tables exist with the exact 41 catalog columns plus our audit/lineage columns. Silver schema exists but its tables get created at runtime by the dbt task. **This is the moment the platform stopped being metadata-only.**"

---

## 🎬 ACT 5 — Generate sample vendor data (30 sec)

### Terminal:

```bash
.venv/bin/python3 scripts/generate_membership_bronze_sample.py
```

Output: `data/generated/membership_bronze_sample.psv` — 100 synthetic rows, 41 cols, 36 KB.

**Tell the team:**
> "In production this file would arrive on SFTP from Aetna. For demo, we synthesize 100 deterministic rows."

---

## 🎬 ACT 6 — Trigger DAG via Airflow (2 min)

### Tab 3 — Airflow UI:

1. Refresh DAG list — **`aetna_membership_pipeline`** now appears
2. Click on it → top-right toggle to **unpause**
3. Click **▶️ Trigger DAG**
4. Watch the graph view: `start → bronze_land → bronze_validate → silver_dbt → gold_dbt → onprem_push → end`
5. Tasks turn green sequentially (~33 sec total)

**Tell the team while waiting:**
> "Each task is a Python callable. `bronze_land` is doing real PUT + COPY INTO. `silver_dbt` materializes Silver from Bronze. `gold_dbt` materializes Gold from Silver. `onprem_push` is the (still-stub) routing fanout to the 5 downstream products — that's the next phase."

### Tab 1 — When all green, run:

```sql
SELECT COUNT(*) AS bronze_rows FROM BRONZE_AETNA.RAW_MEMBERSHIP;
SELECT COUNT(*) AS silver_rows FROM SILVER_AETNA.MEMBERSHIP_CLEAN;
SELECT COUNT(*) AS gold_rows   FROM GOLD_AETNA.MEMBERSHIP;

SELECT member_card_id, member_first_name, member_last_name,
       member_state, member_line_of_business, member_product
FROM GOLD_AETNA.MEMBERSHIP
ORDER BY member_card_id
LIMIT 5;
```

Expected: 100 / 100 / 100. Sample rows show real data (Patricia Rodriguez NJ MEDICAID, etc.).

**Tell the team:**
> "100 rows ingested raw, cleansed in Silver, canonicalized in Gold — all in 33 seconds, fully orchestrated, with audit columns + GX expectations on every layer."

---

## 🎬 CLOSING

**Tell the team:**
> "What you saw: Bronze catalog (the spec) → AI authored Silver (DV2) and Gold (canonical) → Pipeline Architect deployed Bronze DDL, Silver+Gold dbt models, Airflow DAG, and GX expectations → DAG ran end-to-end against Snowflake. From zero-to-pipeline in roughly 8 minutes, including 60 seconds of LLM thinking time. This same flow scales to all 33 datasets across N healthcare clients."

---

## 🆘 If something breaks mid-demo

| Symptom | Quick fix |
|---|---|
| Streamlit page slow on first load | Normal — singleton auth handshake. Subsequent navs are fast. |
| Silver Construct times out | Voyage rate limit. Wait 90s, retry. |
| Pipeline Architect deploy errors | Refresh page, try again. Rare. |
| DAG doesn't appear in Airflow | `docker compose restart airflow_scheduler` then wait 30s |
| Task failure in DAG | Check `docker exec datalink-airflow-scheduler airflow tasks log aetna_membership_pipeline <task_id> <run_id>` |

If the demo goes sideways and you need to reset:

```bash
bash scripts/master_reset.sh
```

Then start over from Act 1.
