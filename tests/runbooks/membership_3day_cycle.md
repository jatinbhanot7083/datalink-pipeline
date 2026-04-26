# Membership 3-Day Cycle — QA Runbook

**Tenant:** `aetna` · **Backend:** Snowflake · **Phase:** 9.6 (Phase 9 close-out)

This runbook demonstrates the full medallion stack — Bronze append-only,
Silver SCD2 with soft-delete-on-FULL, Gold outbox-driven egress, Pipeline
Control state machine, Bronze retention guard — driving a realistic
**Sun Full → Mon-Sat Incremental → Next Sun Full** membership cycle plus
two negative resilience paths.

It can be run two ways:

  1. **Demo on existing data** *(recommended for first read)* — uses the
     3-batch aetna state already in Snowflake to verify the architecture.
     Quickest path to "show me it works."
  2. **Fresh 3-day simulation** — pre-stage 3 synthetic membership files
     and walk the cycle end-to-end. Higher effort, more realistic.

The verifier script (`scripts/runbook_verify.py`) supports both modes.

---

## TL;DR — what this proves

| Phase | What it does | How the runbook proves it |
|---|---|---|
| **9.1** | Bronze is append-only — every batch becomes new physical rows | Bronze row count grows linearly with batches (e.g. 30 K claims rows after 3 batches of 10 K) |
| **9.2** | Silver SCD2 dedups via hash_diff — same content arriving twice = no double row | Sat row count = number of *distinct attribute states*, not number of source rows |
| **9.2** | Silver soft-deletes on FULL Bronze — members absent from latest FULL flip `is_active=FALSE` | Drop a Day 3 FULL file with 10 members removed, see those 10 flip to inactive |
| **9.3** | Gold outbox carries deltas only — no full-table push to On-Prem | Day-N outbox row count = number of changes that day, not full table size |
| **9.4** | Pipeline Control auto-pauses on breach — operator resumes via Control Tower | Drop a malformed file, see PAUSE in UI, click Resume, see RUNNING + audit log |
| **9.4** | Auto-aborts on > 25 % fail rate; only operator Force Resume can re-arm | Drop a 50 %-row-count file, see ABORT, walk the Force Resume flow |
| **9.5** | Bronze retention prune is watermark-guarded — refuses to prune un-silvered Bronze | Run prune with `--days 0` while Silver is stale → SKIPPED; with valid window → DRY_RUN reports correct row counts |

---

## Setup (one-time)

You need:

  * `.env` flipped to Snowflake (`DL_ADAPTERS__WAREHOUSE__TYPE=snowflake`)
  * Anthropic key set if you want real Claude during checkpoints (else stub)
  * Containers up: `docker compose ps` shows `airflow_*`, `control_tower`,
    `postgres*`, `sqlserver` all healthy
  * Phases 9.1–9.5 migrations applied:
    * `python -m scripts.migrate_bronze_audit_cols`
    * `python -m scripts.migrate_pipeline_control_state`
  * **Sidebar client = aetna**

---

## Mode 1 — Demo on existing data

**Pre-condition:** you've already run Bronze 3 times for aetna across
Phase 9.1 testing, so Bronze has ≥ 30 K claims / 15 K membership / 3 K
provider rows. Silver has been built once after the latest Bronze.

```bash
cd ~/dev/DataPipelinesWithGX && set -a && source .env && set +a && \
  POSTGRES_HOST=localhost POSTGRES_PORT=5434 \
  python -m scripts.runbook_verify --client aetna --mode existing
```

The verifier prints **PASS / FAIL per assertion** plus a summary line.
Expected: every assertion PASSES — proves Phases 9.1 + 9.2 + 9.3 + 9.5
landed cleanly on the data we already have.

---

## Mode 2 — Fresh 3-day simulation

Use this when you want to walk the cycle from Day 1.

### Day 1 — Sunday Full Load

**Action:**

1. Drop a fresh full membership file via SFTP. The simplest path: trigger
   ▶ Run Bronze in Control Tower for aetna — the existing data generator
   creates a `membership_AETNA_*.csv` with `_load_type='UNKNOWN'` (no
   `_full_` / `_inc_` keyword in filename). For demo purposes that's fine
   — the architecture works the same regardless of label.

2. Wait for Bronze DAG green, then ▶ Run Silver, then ▶ Run Gold.

3. Run the verifier:

```bash
python -m scripts.runbook_verify --client aetna --mode day1
```

**Expected end state:**

| Layer | Object | Rows | Why |
|---|---|---|---|
| Bronze | `RAW_MEMBERSHIP` | +5,000 | Net-new batch |
| Silver | `sat_member_demographics` | 5,000 active | One Sat row per (member, plan); all is_active=TRUE; effective_end_date NULL |
| Operational | `UM.PatientAuth` | growing — proportional to claims with prior_auth_ref | Outbox push of Phase 9.3 |
| `CONTROL.egress_batch_log` | new rows | 1 per (target, entity) for this run | Phase 9.3 audit trail |
| `CONTROL.pipeline_control_state` | aetna rows | RUNNING for all 3 pipelines | Phase 9.4 per-tenant state |

### Day 2 — Monday Incremental

**Action:**

1. Trigger ▶ Run Bronze again. The synthetic data generator typically
   produces an identical-content file (no real changes day-over-day on
   aetna seed data). For an honest incremental demo, the operator has 2
   options:
   * **(a) Quick** — accept that the Day 2 batch is identical content;
     watch hash_diff dedup keep `sat_member_demographics` row count
     unchanged. This proves the SCD2 dedup logic.
   * **(b) Realistic** — manually mutate a few rows in the SFTP file
     before triggering, e.g. change 5 members' state codes. Verifier
     will then show 5 new Sat rows + 5 closed-out previous versions.

2. Run Silver (no need to re-run Gold unless Bronze produced new
   silvered keys → in option (a), Silver run 2 is a no-op).

```bash
python -m scripts.runbook_verify --client aetna --mode day2
```

**Expected:** Bronze grows by file size; Silver sat row count UNCHANGED
in option (a) — proving hash_diff dedup. With option (b) Silver Sat
gains 5 rows: 5 closed-out (is_active=FALSE, effective_end_date set) +
5 new (is_active=TRUE).

### Day 3 — Sunday Full Load

**Action:**

1. To exercise SCD2 soft-delete, the operator should produce a FULL file
   with 10 fewer members than the prior batch. The seed-data generator
   doesn't do this out of the box — for a proper demo, manually delete
   10 random rows from the file before triggering.

2. Trigger Bronze → Silver → Gold.

3. Verifier:

```bash
python -m scripts.runbook_verify --client aetna --mode day3
```

**Expected:** 10 members in `sat_member_demographics` flip to
`is_active=FALSE` because they're absent from the latest FULL Bronze
batch. Outbox emits 10 DEACTIVATE rows. On-Prem sees 10 UPSERTs that
mark them inactive (per Gold's egress_action handling).

---

## Negative paths

### N1 — Malformed file → PAUSE → operator resume

**Setup:** Drop a `.csv` into SFTP `/drop/` that's missing the `dob`
column (the GX bronze_membership suite asserts `dob` not-null, so the
column being absent fails 100 % of rows).

```bash
# In WSL, with the SFTP container running:
docker exec datalink-sftp bash -c "
  cat > /home/datalink/drop/membership_AETNA_malformed_$(date +%Y%m%d_%H%M%S).csv << 'EOF'
member_id,subscriber_id,gender,plan_id,group_id,effective_date,termination_date,coverage_type,state
M999001,S999001,F,P001,G001,2026-04-01,2027-04-01,PPO,VA
EOF
  chown datalink:datalink /home/datalink/drop/*.csv
"
```

Trigger Bronze in Control Tower for aetna.

**Expected behavior:**

1. Bronze ingest fails — column mismatch
2. `pipeline_control_state` for `bronze_ingest/aetna` flips to PAUSED
3. Control Tower **🚦 Pipeline Control** panel surfaces the PAUSE with
   the failure reason
4. Click **▶ Resume** in the UI → state walks PAUSED → RESUMING →
   RUNNING with two audit log entries
5. Drop a CLEAN membership file → re-trigger Bronze → DAG passes

### N2 — 50 % row-count drop → ABORT → Force Resume

**Setup:** Same as N1 but the file has only 50 % of normal row count
(the bronze_membership suite has a row-count expectation; > 25 % miss
triggers Phase 9.4's auto-abort branch).

**Expected behavior:**

1. CP1 fails > 25 % → `pipeline_control_state` flips to **ABORTED**
2. Control Tower panel shows ABORTED + Force Resume input
3. Operator types a reason ("validated offline, 50% file is correct
   subset for testing"), clicks **⚠ Force Resume**
4. Audit log records `FORCE RESUME: …` prefix on the transition
5. Future runs proceed; no DAG trigger needed because Force Resume
   doesn't auto-replay — operator must explicitly retry

---

## Closing

Once both modes + both negatives have been exercised, tag the milestone:

```bash
git tag -a phase9-complete -m "Phase 9 complete: append-only Bronze, SCD2 Silver, Gold outbox, pipeline control state machine, retention. 3-day membership runbook proves the architecture."
git push origin phase9-complete
```

The runbook can then be linked from the customer-facing demo deck or the
internal QA wiki. Re-run cadence: at minimum on every release branch.
