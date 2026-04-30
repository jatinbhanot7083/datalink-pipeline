# DataLink Demo Journal

> **Purpose** — Single ledger of everything we shape between now and the team
> demo. Every fix, every design choice, every "I want X" from Jatin lands here
> so we can roll it into release notes / runbook / training deck.
>
> **Maintainer:** Claude (auto-updated each working session) · **Owner:** Jatin
> Bhanot (VP Data Operations).

---

## ⚡ FAST RECOVERY — when the URL is unreachable

**TL;DR — two commands:**

```powershell
# In ADMIN PowerShell (right-click → Run as administrator):
& "C:\Users\Jatin\OneDrive - MASTER WORKSPACE\1_AA_WORKSPACE\DataPipelinesWithGX\scripts\recover-windows-network.ps1"
```

```bash
# Then in WSL bash:
cd /home/jatin/dev/DataPipelinesWithGX && make recover
```

Total time: ~3 minutes. Browser ready when `make recover` prints "URL READY".

---

## 🛟 EMERGENCY ROLLBACK — when the project is in total-failure state

If the codebase itself is broken (not just networking) — botched edits, failed
migration, accidental deletions — restore to the last verified-working state:

```bash
cd /home/jatin/dev/DataPipelinesWithGX
make list-checkpoints         # see what known-good states exist
make rollback-to-last-good    # hard-reset to the most recent
make recover                  # bring stack up against the restored code
```

Auto-stashes any in-flight work first (recoverable via `git stash list`).
Tags follow the pattern `known-good-YYYY-MM-DD-NN` and live on GitHub —
durable even if the laptop dies.

**Most recent checkpoint as of last update:** `known-good-2026-04-30-01`.

---

## 🔁 ALWAYS-ON STARTUP — deterministic, NAT mode + portproxy (validated 2026-04-30 evening)

**Final architecture** — replaces the earlier `mirrored` + flaky-localhost-mirror
attempt. NAT mode + explicit `netsh portproxy` rules = mechanically deterministic.

### What's running

| Layer | Component | Persists across Windows reboot? |
|---|---|---|
| WSL networking mode | `%USERPROFILE%\.wslconfig` → `[wsl2] networkingMode=NAT` | ✅ |
| Container port binding | `docker-compose.yml` → `${DL_BIND_HOST:-127.0.0.1}:PORT:PORT` (default 127.0.0.1, dev `.env` overrides to 0.0.0.0) | ✅ |
| systemd in WSL | `/etc/wsl.conf` → `[boot] systemd=true` | ✅ |
| Docker daemon auto-start | `systemctl enable docker` | ✅ |
| Container restart | `restart: unless-stopped` in compose | ✅ |
| WSL boots + stack up at login | Scheduled Task `DataLink-WSL-AutoStart` (regular user) | ✅ |
| Windows ↔ WSL bridge created at login | Scheduled Task `DataLink-Network-AutoRecover` (admin RunLevel, 30s logon delay) → runs `scripts/recover-windows-network-at-login.ps1` | ✅ |

### Sequence at every Windows login

```
T+0     Log in
T+1     DataLink-WSL-AutoStart fires       → wsl --exec docker compose up -d
        (boots WSL → systemd → docker → containers)
T+30    DataLink-Network-AutoRecover fires (30s delayed)
        → discovers WSL_IP via `wsl hostname -I`
        → resets stale portproxy
        → adds 7 rules: 0.0.0.0:PORT → WSL_IP:PORT
        → adds Windows Firewall inbound TCP allow rules
        → logs everything to %USERPROFILE%/datalink-network-recover.log
T+90    control_tower healthy, URL ready
```

### Why NAT instead of mirrored

`mirrored` networking mode was attempted earlier in the day and failed because:
- WSL VM and Windows share the same network adapter / IP in mirrored mode
- A `netsh portproxy` rule pointing at the WSL IP creates a self-loop
  (Windows forwards localhost:8000 → 192.168.1.246:8000 = back to itself)
- The container's listener is invisible to the portproxy listener

NAT mode gives WSL its own `172.17.x.x` IP separate from Windows. Portproxy
then forwards to a real different host (the WSL VM), which exposes the
container's `0.0.0.0` binding directly.

### Production safety

| Artifact | Lives at | Production impact |
|---|---|---|
| `${DL_BIND_HOST:-127.0.0.1}` | `docker-compose.yml` | None — production never sets the env var, defaults to `127.0.0.1` (today's exact behaviour) |
| `DL_BIND_HOST=0.0.0.0` | `.env` (gitignored) | Dev-only; production has its own `.env` |
| `scripts/recover-windows-network-at-login.ps1` | `scripts/` | None — `.ps1` doesn't execute on Linux |
| `.wslconfig` | `C:\Users\Jatin\.wslconfig` (Windows-only) | None — no equivalent file on Linux |
| Scheduled Tasks | Windows-only | None |

**Net production effect: zero.** Every Windows-side hack is inert on Linux.

### Verifying the architecture is intact

```powershell
# In Windows PowerShell:
Get-ScheduledTask -TaskName 'DataLink-WSL-AutoStart','DataLink-Network-AutoRecover' | Format-Table TaskName, State -AutoSize
netsh interface portproxy show v4tov4
```

Expected:
```
DataLink-WSL-AutoStart        Ready
DataLink-Network-AutoRecover  Ready

7 portproxy rules pointing at WSL_IP (172.17.x.x):
  0.0.0.0:8000 -> 172.17.x.x:8000
  ... (and 6 more)
```

### Recovery script logs

`%USERPROFILE%\datalink-network-recover.log` — append-only timestamped log.
Tail it after a Windows login to verify the bridge was set up correctly.

---

## 🔁 ALWAYS-ON STARTUP — earlier mirrored-mode attempt (2026-04-30 afternoon, superseded)

After a Windows restart, the URL `http://localhost:8000` should be reachable
without manual intervention. The architecture has **5 layers**, all in place:

| # | Layer | Status / location | Owner |
|---|---|---|---|
| 1 | systemd enabled in WSL | `/etc/wsl.conf` → `[boot] systemd=true` | Linux |
| 2 | Docker daemon auto-starts inside WSL | `systemctl enable docker` | systemd |
| 3 | Containers auto-restart | `restart: unless-stopped` in `docker-compose.yml` | Docker |
| 4 | WSL boots + Docker stack comes up at Windows login | Scheduled Task `DataLink-WSL-AutoStart` | Windows Task Scheduler |
| 5 | Hyper-V firewall + portproxy state reset for fresh WSL↔Windows bridge | Scheduled Task `DataLink-Network-AutoRecover` (RunLevel Highest = silent admin) | Windows Task Scheduler |

**Sequence at every Windows login:**

```
T+0     User logs in
T+1     Both Scheduled Tasks fire in parallel:
         DataLink-Network-AutoRecover  →  resets portproxy + restarts iphlpsvc
         DataLink-WSL-AutoStart        →  boots WSL → docker compose up -d
T+30    All other containers up; control_tower bootstrapping
T+90    control_tower healthy, URL ready
```

User can log in → walk to coffee machine → come back → URL is hot.

### Verifying the tasks are still registered

```powershell
Get-ScheduledTask -TaskName 'DataLink-WSL-AutoStart','DataLink-Network-AutoRecover' | Format-Table TaskName, State -AutoSize
```

Expected:
```
TaskName                       State
--------                       -----
DataLink-WSL-AutoStart         Ready
DataLink-Network-AutoRecover   Ready
```

### If a task gets disabled / deleted

Re-register from `docs/demo_journal.md` → look for "scheduled-tasks" sections in
the change log table below for the exact `Register-ScheduledTask` blocks. The
admin-RunLevel one (`DataLink-Network-AutoRecover`) must be registered from
an Admin PowerShell — `Access denied` from regular PS.

### Recovery script logs

Each `DataLink-Network-AutoRecover` run appends a timestamp to
`C:\Users\Jatin\datalink-network-recover.log`. Tail that file if you suspect
the task didn't fire at login.

---

## 📌 COMMIT DISCIPLINE — operator agreement (2026-04-30)

Per Jatin's directive after the 2026-04-30 networking storm cost half a day:

1. **After every successful module completion**, run `make checkpoint MSG="..."`.
   Commits + tags + pushes in one command. Even **5 times a day** is fine.
2. **Tags follow `known-good-YYYY-MM-DD-NN`** so rollback to any prior moment
   is one command away.
3. **Branch + tags pushed to GitHub immediately** — work survives any local
   disaster.
4. **It's Claude's duty** to remind/run this after each verified module — not
   the operator's job to remember.

### What counts as a "successful module"?

| Trigger | Action |
|---|---|
| Phase verifier passes (e.g., `runbook_verify_phase12.py` 27/27 PASS) | `make checkpoint MSG="phase 12.x verifier 27/27 PASS"` |
| UI page visually confirmed by operator | `make checkpoint MSG="<page> reviewed by Jatin, working"` |
| Recovery from incident validated | `make checkpoint MSG="recovered from <X>; URL=200, all pages load"` |
| Config change tested end-to-end (e.g., new Snowflake account) | `make checkpoint MSG="<change>; verified end-to-end"` |
| Demo dry-run passes | `make checkpoint MSG="demo dry-run pass — pre-meeting state"` |

### What does NOT need a checkpoint?

- WIP / half-finished edits — finish first, then checkpoint.
- Pre-commit hook auto-fixes that haven't been verified yet.
- "It compiles" without operator-visible verification.

---

## 🛠 RECOVERY RUNBOOK — Manual Step-by-Step (validated 2026-04-30)

Use this when the make target / script aren't enough, or you want to understand
each step. **This exact sequence recovered the project on 2026-04-30 after a
multi-hour networking storm caused by mixed Hyper-V firewall state, stale
`netsh portproxy` rules from prior experiments, and `.wslconfig` mode
oscillation.**

### Symptom you're recovering from
- Browser at `http://localhost:8000` → "Site can't be reached" / connection
  refused / connection reset.
- Container is healthy inside WSL (`curl localhost:8000` from WSL bash works,
  returns 200).
- `docker ps` shows containers Up, no crash loop.
- The break is the **Windows host ↔ WSL VM bridge**.

### Step 1 — clean stop the docker stack (WSL bash)
```bash
cd /home/jatin/dev/DataPipelinesWithGX
docker compose down --remove-orphans
```
Expect: 24 containers + 1 network removed cleanly.

### Step 2 — check Windows-side ports (regular PowerShell)
```powershell
netstat -ano | Select-String ':8000.*LISTENING|:8081.*LISTENING|:8090.*LISTENING|:5050.*LISTENING|:8088.*LISTENING|:3000.*LISTENING|:8082.*LISTENING'
```
**If empty** → ports are clean, jump to Step 10.
**If you see PID lines (typically all PID 6104 = `iphlpsvc`)** → continue to Step 3.

### Step 3 — full WSL shutdown (regular PowerShell)
```powershell
wsl --shutdown
wsl --list --running
```
Expect: `There are no running distributions.`

### Step 4 — re-check ports
```powershell
netstat -ano | Select-String ':8000.*LISTENING|:8081.*LISTENING|:8090.*LISTENING|:5050.*LISTENING|:8088.*LISTENING|:3000.*LISTENING|:8082.*LISTENING'
```
**If still showing PID lines** → continue to Step 5.

### Step 5 — identify what holds them
```powershell
Get-CimInstance Win32_Service | Where-Object { $_.ProcessId -eq 6104 } | Format-Table Name, DisplayName, State -AutoSize
```
Expect: `iphlpsvc — IP Helper — Running`. That's the Windows service that hosts `netsh portproxy` rules.

### Step 6 — list stale portproxy rules (regular PowerShell)
```powershell
netsh interface portproxy show v4tov4
```
Expect: 7 leftover rules from earlier experiments.

### Step 7 — open ADMIN PowerShell
1. Press Windows key
2. Type "PowerShell"
3. Right-click → "Run as administrator" → Yes on UAC

### Step 8 — clear portproxy rules (admin PowerShell)
```powershell
netsh interface portproxy reset
netsh interface portproxy show v4tov4
```
Expect: empty list after reset.

### Step 9 — restart iphlpsvc (admin PowerShell)
```powershell
Restart-Service iphlpsvc -Force
netstat -ano | Select-String ':8000.*LISTENING|:8081.*LISTENING|:8090.*LISTENING|:5050.*LISTENING|:8088.*LISTENING|:3000.*LISTENING|:8082.*LISTENING'
```
Expect: empty — all 7 ports released.

You can close the admin PowerShell now.

### Step 10 — boot WSL (regular PowerShell)
```powershell
wsl bash -c "uname -n"
```
Expect: `BHANOTS-XPS8940`.

### Step 11 — bring up Docker stack (WSL bash)
```bash
cd /home/jatin/dev/DataPipelinesWithGX
docker compose up -d
```
Expect: 24/24 containers started, no port-bind errors.

### Step 12 — wait for control_tower healthy (WSL bash)
```bash
docker ps --filter name=datalink-control-tower --format '{{.Status}}'
```
Repeat every 15 s until `(healthy)` appears (typically 60–90 s after `compose up`).

### Step 13 — pre-warm + open browser
```bash
docker exec datalink-control-tower python3 -c "
from datalink.ui._query import warehouse_ctx
with warehouse_ctx(readonly=True) as wh:
    wh.query('SELECT 1')
    wh.query('SELECT COUNT(*) FROM CONTROL.dq_suites')
print('warmed')
"
for path in '' Pipeline_Control DQ_AI_Architect Smart_Mapper Schema_Drift DQ_Author DQ_Review; do curl -s -o /dev/null "http://localhost:8000/${path}?client=aetna"; done
```

Open **http://localhost:8000** in browser. ✅ Done.

### Why each step matters

| Step | Without it |
|---|---|
| 1 (compose down) | Stale containers fight new ones for ports |
| 2 (port check) | You skip the hard part if ports are already clean |
| 3 (wsl --shutdown) | WSL2 daemon may keep relay listeners alive |
| 5 (identify svchost) | You don't know it's `iphlpsvc` (could be HNS, vmcompute, etc.) |
| 7–9 (admin reset) | Without admin, you can't clear `iphlpsvc` cache |
| 12 (wait healthy) | If you hit URL too early you get connection-reset and think it's broken |
| 13 (pre-warm) | First click pays the 3 s Snowflake warehouse cold-start |

---

## Demo storyline (target narrative)

1. **Day-1 file lands** on Aetna SFTP → mirrors to ADLS → Bronze ingest into
   Snowflake (raw_*) with schema-drift gating.
2. **Pipeline Control policies** decide thresholds per (client, pipeline, source_type).
   The cascade picks the most-specific tenant override, falling back through
   global `*` to a hard-coded DEFAULT.
3. **DQ AI Architect** (custom Anthropic-API agent — *not* CrewAI) profiles the
   landed Bronze table, emits Great Expectations objects across the 6 DQ
   dimensions (Completeness, Uniqueness, Timeliness, Accuracy, Consistency,
   Validity) with stage gating per the metric matrix.
4. **Smart Mapper** proposes Silver hubs/sats, then Gold operational view, then
   Push scripts (Postgres + SQL Server) for downstream UM.
5. **Schema Drift** page handles vendor-driven additive changes
   (promote-to-contract operator action) and halts on destructive change.

---

## Environment baseline (verified 2026-04-27)

- **App entrypoint:** Streamlit Control Tower → http://localhost:8000 (no login)
- **LLM:** Anthropic Claude (real API, no stub fallback)
- **Snowflake account:** `eda86013.east-us-2.azure` · MEDIUM warehouse · DB
  `DATALINK_DEV`
- **Operational targets:** Postgres `datalink_um` (port 5434), SQL Server
  `DataLinkUM` (port 1433)
- **6 baseline tenants** seeded into `CONTROL.dq_suites`

Full URL + credential inventory: see `docs/demo_environment.md` (TBD — pull
from session-end summary).

---

## Phase 14 — Data Contract Architect (DELIVERED 2026-04-30)

**The marquee Phase 14 deliverable.** Lets operators design Bronze table
contracts BEFORE files arrive, with an AI agent grounded against
industry-standard healthcare references (FHIR R4, X12 EDI, NCPDP D.0,
CMS dictionaries, Data Vault 2.0, NCQA HEDIS) plus operator-uploaded
custom standards.

### What it does

Two operator entry modes share the same RAG-grounded core:

- **Mode A (FILE_DRIVEN)** — vendor sends a sample file. AI profiles
  the headers + samples, retrieves top-k standard chunks, proposes a
  Bronze DDL with column-by-column reasoning. Vendor abbreviations like
  `MbrNo`, `SvcDt`, `BilledAmt` get RENAMEd to canonical `member_id`,
  `service_date`, `billed_amount` with citations to specific X12 segment
  / FHIR field definitions.

- **Mode B (CONTRACT_FIRST)** — no file yet. Operator describes the
  source in natural language ("Aetna will send us a daily 834 enrollment
  feed..."). AI proposes a complete contract from scratch — including
  fields the operator didn't mention but the standard requires/permits
  (`maintenance_type_code`, `marital_status`, `pcp_npi`, etc.).

### Architecture (5 layers)

| Layer | Component |
|---|---|
| Reference corpus | 6 industry standards in markdown under `corpora/<code>/`, chunked + embedded into `agent_memory.standard_references` (pgvector, voyage-3.5-lite, 1024-dim, 88 chunks total) |
| Standards registry | `CONTROL.standard_registry` — display name + version + chunk count per registered standard (industry + operator-uploaded custom) |
| Custom uploads | `CONTROL.custom_standards_registry` (table) + a `Custom Standard Upload` flow for operators to add their own org-specific spec docs |
| Agent | `datalink.agents.contract_architect.ContractArchitectAgent` — Claude haiku-4.5, temperature/strictness/grounding_k controls, JSON contract enforced |
| UI | `datalink/ui/pages/12_Data_Contract_Architect.py` — file uploader / NL textarea, anchored standards multi-select, AI controls expander, proposal card with editable column grid, side-by-side deviation log, Approval flow producing 5 artifacts |

### 5 artifacts emitted on Approve

| # | Artifact | Location |
|---|---|---|
| 1 | Bronze DDL committed file | `datalink/pipeline/bronze/ddl/<client>_<source>_<table>.sql` |
| 2 | Schema contract row | `CONTROL.source_schema_contracts` (drift detection now active) |
| 3 | GX expectation suite | `CONTROL.dq_suites` PENDING_REVIEW with auto-derived NPI/ICD-10/CPT regex + NOT NULL on PKs |
| 4 | dbt Silver model stub | `dbt/models/silver/<client>/silver_<table>.sql` (Smart Mapper extends from here) |
| 5 | Vendor Data Contract Spec | `docs/specs/<client>_<source>_v1.md` + `.html` (operator opens in browser, prints to PDF, hands to vendor) |

### Verifier — `scripts/runbook_verify_phase14.py`

24/24 assertions PASS across 6 sections:
1. CONTROL DDL exists + has all required columns
2. 6 industry standards registered with chunks
3. RAG retrieval returns relevant hits with monotonic distances within anchor scope
4. Agent JSON contract validation (good accepted, bad rejected, fences stripped)
5. Audit-column injection (7 cols, idempotent)
6. Full approval flow with real LLM produces all 5 artifacts + Snowflake rows + audit log

### Production safety

| Production-portable code | Touched? |
|---|---|
| `datalink/agents/contract_architect/*` | ✅ Yes — ships to production. Agent + bridge are pure Python, work against any warehouse + any LLM via existing protocol. |
| Phase 14 CONTROL DDL (4 new tables) | ✅ Yes — created idempotently by `create_control_tables()`. Ships. |
| `agent_memory.standard_references` | ✅ Yes — pgvector table created by `AgentMemoryStore.ensure_schema()`. Ships. |
| `corpora/*` markdown reference docs | ✅ Yes — committed to repo, loaded at deploy time by `scripts/load_industry_standards.py`. |
| UI page | ✅ Yes — Streamlit page like every other in `datalink/ui/pages/`. |
| Custom standard uploads | ✅ Yes — `agent_memory.standard_references` is the same table for industry + custom; operator uploads land at runtime. |

**Net production effect**: ships cleanly on any Linux deployment that
already has Postgres+pgvector and Snowflake. Same configuration as
the rest of the stack.

### Demo storyline integration

When demoing on operator day:

1. Open `/Data_Contract_Architect`. Pick `aetna` / `CLAIMS`. Anchor on
   `fhir-r4` + `x12`.
2. Drag-drop a sample claims CSV (use the included `data/generated/aetna_claims.csv`).
3. Show AI proposal: column-by-column with deviation pills, citation hover.
4. Edit one column inline (rename or add a column).
5. Approve as AUTO_APPROVE — show all 5 artifacts emitted.
6. Switch to Mode B → describe a 270/271 eligibility feed → show how the
   agent adds standard-mandated fields the operator didn't list.

This is the WOW moment of the demo. The audience sees:
- Real industry standards driving AI proposals (not a model hallucinating)
- Citation-by-citation traceability
- Vendor spec PDF the client team gets back

### Pre-warm before demo

```bash
docker exec datalink-control-tower python3 scripts/load_industry_standards.py
docker exec datalink-control-tower python3 scripts/runbook_verify_phase14.py
```

If both come back green, you're demo-ready.

---

## Phase 12.5 — Pipeline Control granularity (DELIVERED 2026-04-27)

### What changed
- `ThresholdPolicy` gained a `source_type` column (NULL = pipeline-wide).
- 5-tier cascade in `get_or_default()`:
  1. (client, pipeline, source_type) exact
  2. (client, pipeline, NULL) tenant pipeline-wide
  3. (`*`, pipeline, source_type) global per-source
  4. (`*`, pipeline, NULL) global pipeline-wide
  5. Hard-coded DEFAULT
- `clone_to_client(...)` cross-tenant copy (refuses same-client + pre-existing
  DRAFT collisions).
- Pipeline Control UI v2: scope toggle (client vs global `*`), source-type
  picker, resolved-policy grid (3 pipelines × 4 source levels = 12 cells with
  tier-of-origin label), clone-to-client button.

### Verifier
`scripts/runbook_verify_phase12.py` — 27/27 PASS including 9 new assertions for
cascade precedence + clone refusals.

### Operator stories this unlocks
- "Make CLAIMS uniqueness stricter than MEMBERSHIP for client X."
- "Set an org-wide Bronze completeness floor; let any tenant tighten it."
- "Roll out client X's mature CLAIMS policy to clients Y and Z."

---

## Demo prep / housekeeping log

| Date | Action | Reason |
|---|---|---|
| 2026-04-27 | Migrated Snowflake account `vma92639` → `eda86013` | Free-tier credits expired on prior account |
| 2026-04-27 | Restored MEDIUM warehouse size (was XSMALL in bootstrap) | Demo-day responsiveness; Snowflake auto-suspends after 60s anyway |
| 2026-04-27 | UI rebrand: light-navy sidebar, white hover cards, color logo on white plaque, "Command Center" wordmark in gold | Sponsor brand alignment |
| 2026-04-27 | Removed silent stub-LLM fallback in `llm_router.py` | "I want NOTHING on stub for real-business runs" — now raises `RuntimeError` unless `ALLOW_STUB_FALLBACK=true` |
| 2026-04-27 | Smart Mapper: explicit Step 1 (Silver) / Step 2 (Gold) / Step 3 (Push) buttons | Earlier single-button flow generated MERGE statements that Silver validator rejected |
| 2026-04-27 | Snowflake LIKE-pyformat hotfix in Smart Mapper | Literal `%` in SQL collided with adapter's `%(name)s` substitution |
| 2026-04-27 | `AgentBase.__init__` now sets `_tokens_this_run = 0` | Direct calls (not via `run()`) hit AttributeError otherwise |
| 2026-04-27 | Cleared Airflow DAG history + truncated PG `um.*` + MSSQL `UM.*` transactional tables | Clean slate for end-to-end demo run |
| 2026-04-27 | Memory pin: agent framework is custom Anthropic-API (NOT CrewAI) | External docs reference CrewAI; live code does not |
| 2026-04-27 | Pipeline Control: added "Active policies registered" section as section 1 (always visible, scoped to client + global `*`) | Operator complaint: actual registered policies were only visible after picking a source_type — couldn't see footprint at a glance |
| 2026-04-27 | Pipeline Control: each row in active-policies list now has inline ✏️ Edit + 🗑️ Archive buttons on the right | Operator ask: edit/archive should be one click from the row, not "scroll down → repick scope/pipeline/source → act" |
| 2026-04-27 | Pipeline Control: active-policies block restyled as a proper table — navy header strip with gold underline, uppercase column labels, drift-action chips (LOG/WARN/HALT colour-coded), monospace pipeline names, right-aligned tabular numerals, alternating row separators, contained 1px outer border | "Poor formatting — keep it tabular, every detail matters" |
| 2026-04-27 | Pipeline Control: replaced custom flex-based table with native `st.dataframe(selection_mode='single-row', on_select='rerun')`, identical visual to "Resolved policies" section. Edit / Archive moved to a toolbar below that activates on row selection. | "Keep it in same table format as Resolved Policies — basic formatting request" |
| 2026-04-27 | Pipeline Control: added 🗑️ Archive button (two-click confirm) on active policy | Operator ask: "Can I delete a Policy?" — soft-delete via existing backend `archive()` |
| 2026-04-27 | Snowflake AUTO_SUSPEND raised 60s → 600s (operator-applied via Snowflake console) | Cold-start was 3.3 s on every idle-then-click; demo-day responsiveness |
| 2026-04-28 | Recovered the full 4-commit "lightning fast" recipe from the prior Snowflake-account session (21a922c, c7c3555, 08ce474, e50caeb). Verified all four still present in current code. Documented as a permanent runbook in this file under "Performance recipe". | Operator ask: "track what we did to make it lightning fast and execute exactly same steps" |
| 2026-04-28 | Bumped Snowflake `AUTO_SUSPEND` 600s → 3600s (recipe value). Pre-warmed warehouse + Streamlit caches across all 7 main pages. | Original recipe `21a922c` used 3600s; previous 600s setting was an underestimate |
| 2026-04-29 | Diagnosed empty client dropdown — transient Snowflake `ProgrammingError` poisoned the negative cache (120s TTL). Restarted control_tower + pre-warmed pages. | Operator-visible symptom: dropdown blank despite 84 suites + 7 clients living in `CONTROL.dq_suites` |
| 2026-04-29 | Added 🔄 **Refresh data** button to the sidebar (under "Maintenance" section, just above the footer). Calls `clear_query_cache()` + `st.rerun()`. | Operators no longer need a container restart to escape a poisoned negative cache; one click + a toast |
| 2026-04-29 | Diagnosed Windows browser → DataLink unreachability: `.wslconfig` was set to `networkingMode=mirrored` (configured during `ecc-rules-engine` work). Mirrored mode does NOT see into Docker bridge networks. Stripped `.wslconfig` to minimal `[wsl2] localhostForwarding=true`, kept `docker-compose.yml` `control_tower` bound to `127.0.0.1:8000:8000` (its original config). Saga + checklist captured as item #8 in troubleshooting. | "Why is app not reachable when WSL says 200?" — the rules-engine project's mirror-mode WSL config carried over into our docker-only environment |

---

## Open questions / pending decisions

- [x] **Policy delete** — backend `archive()` already exists (soft delete →
  `status='ARCHIVED'`). UI button to be added — same session.
- [x] **Performance** — diagnosed (see "Performance findings" below). Demo-day
  fix in flight.
- [ ] **mypy clean-up** — 5 errors in `11_Smart_Mapper.py` lines 508–545 (`Item
  "None" of "MappingSession | None"`).
- [ ] **bootstrap.sql** — repo still has XSMALL; needs MEDIUM patch committed.
- [ ] **Phase 11.7 (deferred)** — row-level extra-attributes capture for Silver
  EXTRA_ATTRIBUTES.

---

## Open gaps surfaced during demo prep

## Performance recipe — "lightning fast" reference (recovered 2026-04-28)

Recovered from the prior Snowflake-account session (account `vma92639`) where
Jatin twice confirmed *"Lightening fast, all links THANK YOU!!!"* on
2026-04-24. The recipe is **5 ingredients** — 4 in code (already in the repo,
all 4 verified present 2026-04-28), 1 in Snowflake config (operator action).

### The 4 code commits (Phase 7, days 4–5)

| # | Commit | Fix | File(s) |
|---|---|---|---|
| 1 | `21a922c` | **Snowflake adapter as module-level singleton** — first query pays the 2–3 s auth handshake; every subsequent query in the process reuses it (~100 ms RTT). Without this: ~7 queries per page render × 2–3 s handshake = 15–20 s per click. | `datalink/ui/_query.py` (`_snowflake_singleton`) |
| 2 | `c7c3555` | **`@st.cache_data` 30 s TTL on query results** — repeat page renders within 30 s return from memory instead of round-tripping to Snowflake. Control Tower "Refresh now" button clears the cache for forced fetch. | `datalink/ui/_query.py`, `datalink/ui/control_tower.py` |
| 3 | `08ce474` | **Three linked fixes**: (a) `_snowflake_bootstrap_done` module flag — Streamlit re-executes page bodies on every interaction, was re-running CONTROL bootstrap (90 baseline suites) every refresh; (b) custom in-process TTL dict (positive 30 s, negative 120 s) replacing `@st.cache_data` for full HIT/MISS/NEG/ERR observability; (c) `_existing_schemas_upper()` lists schemas once (cached 30 s), tiles for non-existent schemas skipped — avoided 8 failing round-trips per render (~1.2 s wasted). | `datalink/ui/_bootstrap.py`, `datalink/ui/_query.py`, `datalink/ui/control_tower.py` |
| 4 | `e50caeb` | **`warehouse_ctx` no longer closes the backend** — DQ Author/Review drove SuiteRegistry through `warehouse_ctx`, which closed the singleton on exit. For DuckDB harmless; for Snowflake forced fresh 2–3 s handshake on every interaction (5 handshakes in 18 s observed). Now yields the backend without closing. | `datalink/ui/_query.py` |

**Verification 2026-04-28** — all four still present in current code: `_snowflake_singleton` at `_query.py:145`, `_snowflake_bootstrap_done` at `_bootstrap.py:158`, `_QUERY_CACHE_TTL` at `_query.py:54`, `_NEG_CACHE_TTL = 120` at `_query.py:256`, `_existing_schemas_upper()` at `control_tower.py:148`, `warehouse_ctx` does NOT close at `_query.py:185+`.

### The 5th ingredient — Snowflake-side warehouse tuning

Run as `ACCOUNTADMIN` (one-time, persists per account):

```sql
ALTER WAREHOUSE DATALINK_WH SET
  WAREHOUSE_SIZE = 'MEDIUM',                    -- vs default XSMALL
  AUTO_SUSPEND = 3600,                          -- 1-hour warm window
  STATEMENT_TIMEOUT_IN_SECONDS = 300,
  STATEMENT_QUEUED_TIMEOUT_IN_SECONDS = 60;

SHOW WAREHOUSES LIKE 'DATALINK_WH';
-- expect: size=Medium, auto_suspend=3600, auto_resume=true
```

**Why MEDIUM:** Snowflake's XSMALL pays roughly 5× the per-query latency vs MEDIUM for the metadata-style queries the dashboards issue. Demo cost impact is ~$0.10 per session — negligible. Original recipe value: `AUTO_SUSPEND = 3600` (the commit body says so, ref `21a922c`).

### Things that wipe the in-process cache (perceived "slowness regressions")

1. **Container restart** — `_snowflake_singleton`, `_QUERY_CACHE`, `_snowflake_bootstrap_done` all live in Python module globals. `docker compose restart control_tower` resets them, the next click pays the cold handshake again.
2. **Streamlit hot-reload after a code edit** — saving any file under `datalink/ui/` triggers Streamlit's auto-reload, which re-imports modules and clears the singletons. Same effect as a container restart.
3. **Warehouse auto-suspend** — if `AUTO_SUSPEND` is too low and the warehouse goes idle, the next query pays a 3–10 s spin-up.
4. **Snowflake Standard Edition has no result-cache across users**, so even with the 30 s in-process TTL, two operators on different sessions both hit Snowflake on first query.

### Pre-warm script (optional, for demo dry-run)

```bash
# Inside the container:
docker exec datalink-control-tower python3 -c "
from datalink.ui._query import warehouse_ctx
with warehouse_ctx(readonly=True) as wh:
    wh.query('SELECT 1')                                                 # handshake + warehouse resume
    wh.query('SELECT COUNT(*) FROM CONTROL.dq_suites')                   # populate Snowflake metadata cache
    wh.query(\"SELECT schema_name FROM information_schema.schemata\")    # tile-render dependency
"
# Then hit each Streamlit page once to populate _QUERY_CACHE for that page's reads:
for path in '' Pipeline_Control Smart_Mapper Schema_Drift DQ_AI_Architect DQ_Author DQ_Review; do
  curl -s -o /dev/null "http://localhost:8000/${path}?client=aetna"
done
```

Run before a live demo. Subsequent operator clicks are then sub-second.

### Troubleshooting checklist (in order)

1. `SHOW WAREHOUSES LIKE 'DATALINK_WH'` — confirm `size=Medium`, `auto_suspend=3600`, `state=STARTED`.
2. `docker logs --tail 50 datalink-control-tower | grep _query.` — should show `HIT` events alongside `MISS`. If 100% MISS, the singleton/cache is broken.
3. Time a cold and hot query inside the container (script in §"Pre-warm" above) — cold should be ~3 s, hot ~150 ms. If hot is > 500 ms, the singleton isn't sticking.
4. Did you just edit a `datalink/ui/*.py` file? Streamlit hot-reload has already cleared the cache; first click is expected to be cold.
5. **Empty dropdowns / metrics showing zero?** Look for `[_query.ERR]` followed by `[_query.NEG]` lines. The negative cache (TTL 120 s) memoises the empty result of a transient Snowflake error — by design (don't spam on flapping connections), but the cure is `clear_query_cache()` (sidebar **🔄 Refresh data** button) or `docker restart datalink-control-tower`. Symptom seen 2026-04-29: client dropdown empty after warehouse cold-start `ProgrammingError`.
6. **App refusing to load entirely (`http=000` / "site can't be reached") for 30–60 s?** The whole stack is bootstrapping. Check `wsl uptime` and `ps -eo etime,cmd | grep dockerd` — if WSL or dockerd uptime is < 1 min, WSL just rebooted (Windows kernel update, Docker Desktop update, hibernation, or explicit `wsl --shutdown`). Bootstrap chain takes ~50 s: editable pip install (10 s) → Snowflake bootstrap connect (3 s) → seed baseline suites (30 s) → start Streamlit (5 s) → healthcheck pass. Wait it out, do not retry. Symptom seen 2026-04-29: stack came up "Up 20 seconds (health: starting)" with `http=000`; resolved automatically at ~50 s.
7. **Concurrent project conflict — `ecc-rules-engine` (`C:\PROJECTS\ecc-rules-engine`)** uses the same host ports as DataLink: **4317** (OTel gRPC), **4318** (OTel HTTP), **8889** (Prometheus exporter), **8080** (rules-engine API vs Airflow scheduler internal). Whichever stack starts second silently fails to bind those ports — the OTel collector failing back-pressures telemetry into Streamlit. **Do not run both compose stacks simultaneously during demo prep.** If the rules-engine project is needed, take Command Center down first: `cd /home/jatin/dev/DataPipelinesWithGX && docker compose down`.
8. **Worst-case: Windows browser can't reach `localhost:8000` even though WSL `curl localhost:8000` returns 200.** This means the WSL2 localhost-forwarder is broken or `.wslconfig` is set incorrectly. Most common trigger: `networkingMode=mirrored` in `.wslconfig` (set up for `ecc-rules-engine`). Mirrored mode mirrors **native** WSL TCP binds straight to Windows — great for native `uvicorn`, **invisible to Docker bridge networks**. Cure:
    - **`%USERPROFILE%\.wslconfig` last-known-good for THIS project:**
      ```ini
      [wsl2]
      localhostForwarding=true
      ```
    - From PowerShell: `wsl --shutdown` → wait 5 s → `wsl bash -c "uname"` to boot.
    - In `docker-compose.yml`, control_tower must bind to `127.0.0.1:8000:8000` (the default).
    - Note the relay has occasional micro-blips on Windows 11 — synchronous PowerShell probes (`Invoke-WebRequest`, `Test-NetConnection`) may show 3/5 success while the **browser experience is fine** (retries + keep-alive).
    - Do not run `ecc-rules-engine` and DataLink under different `.wslconfig` modes back-to-back without a `wsl --shutdown` in between. If you must run rules-engine, dockerise it so both projects share the same NAT-mode networking model.

---

## Performance findings (2026-04-27)

Measured with `warehouse_ctx().query()` from inside `datalink-control-tower`:

| Path | Cold | Hot | Notes |
|---|---|---|---|
| Snowflake connection open | 123 ms | 5 ms | Connector pool effective |
| First SELECT after idle | **3,275 ms** | 143 ms | **Warehouse auto-resume** |

**Root cause #1 — auto-suspend too aggressive**
- `DATALINK_WH` had `AUTO_SUSPEND = 60s`. Any pause > 1 min and the next click
  pays 3 s warehouse spin-up.
- **Fix (operator action, ACCOUNTADMIN):**
  ```sql
  ALTER WAREHOUSE DATALINK_WH SET AUTO_SUSPEND = 600;
  ```
- Cost impact: warehouse stays warm 10 min vs 1 min. For a 30-min demo this is
  ~$0.10 of credits — negligible. Reverse to 60 after demo.

**Root cause #2 — N+1 cascade lookups in Pipeline Control grid**
- The resolved-policy grid renders 12 cells (3 pipelines × 4 source levels).
  Each cell calls `PipelineControlPolicyRegistry.get_or_default()`, which
  internally does up to 4 separate `SELECT`s (one per cascade tier).
- Worst case: **48 SELECTs per page render** → ~1.4 s of pure round-trips even
  with a hot warehouse.
- **Optimization (follow-up, not blocking demo):** in
  `control_policy.py`, add `get_all_live_for(client_ids: list[str], pipeline_id:
  str) -> list[ThresholdPolicy]` and resolve the cascade in Python from the
  in-memory list. One query, same answer. UI side: build a `dict[(client,
  pipeline, source), policy]` once at the top of the page render and look up.

**Other clean signals**
- Container CPU < 5 % across the board, RAM < 1.5 GiB anywhere — not a Docker
  resource problem.
- Anthropic latency (Claude calls) — only relevant when Smart Mapper / DQ
  Architect runs; 5–15 s expected per agent invocation, real-API not stub.

---

## Open gaps surfaced during demo prep

### 1. Policies cannot be deleted (logged 2026-04-27)
**User ask:** "Can I delete a Policy???"
**State today:** `control_policy.py` exposes create / update / activate /
fork-for-edit / clone-to-client / list-versions — but no `delete()` or
`archive()`. UI mirrors the API.
**Open design questions:**
- Hard delete vs soft delete (set `is_active=false` + `deleted_at` audit
  column)?
- What happens to history-rows that reference a deleted policy via
  `parent_policy_id` (forks)?
- Permission model — every operator, or admin-only?
**Resolution:** Backend `archive(policy_id, actor, reason)` already exists in
`PolicyRegistry` (line 585) — sets `status='ARCHIVED'` + audit fields. UI
button to be added on Pipeline Control page (per-version row).

---
