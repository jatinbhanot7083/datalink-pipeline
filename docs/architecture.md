# DataLink Local-First Medallion Pipeline — Architecture (Phase 0)

**Status:** Draft for Jatin's review (Phase 0)
**Branch:** `phase-0-architecture`
**Prepared by:** Team DataLink
**Last updated:** 2026-04-19

> This document is the Phase 0 deliverable. It summarizes the three source docs, proposes a target architecture, selects the local-emulator stack, and flags conflicts that require Jatin's decision before Phase 1.

---

## 1. Purpose

Build a **local-first, production-parity** end-to-end Medallion pipeline for EvokeConnectCare™ healthcare data (Membership / Claims / Providers), with a **plug-in / plug-out Great Expectations + CrewAI agentic quality layer**. Promotion to production is by **config change only** — no code edits.

Data flow:

```
SFTP drop  →  ADLS Gen2 landing  →  Snowflake BRONZE (MERGE)
          →  Snowflake SILVER (Data Vault 2.0)
          →  Snowflake GOLD (UM operational schema)
          →  dual-warehouse bulk push  →  on-prem SQL Server [UM]  (current)
                                       →  PostgreSQL              (future)
```

---

## 2. Source Document Summaries

### 2.1 `GX_Pipeline_Control_Agentic_AI_Architecture.docx` (BASE — authority on quality + agent layer)

- Establishes **Pipeline Control State Machine**: `RUNNING → PAUSED → RESUMING → ABORTED`. Snowflake tables `pipeline_control_state`, `pipeline_checkpoints`, `pipeline_control_audit_log`, `batch_quarantine`. Airflow `PipelineControlStateSensor` polls state and halts DAG execution.
- **Auto-Pause thresholds:** critical GX failures >5%, invalid CPT >5%, avg LOS >365 days, null `member_id` >2%. **Auto-Abort:** schema mismatch, file format change, data corruption.
- **CrewAI baseline = 6 agents** across 2 independently-triggerable crews:
  - **Pre-Val Crew:** Profiler → Expectation Author → Reviewer.
  - **Post-Val Crew:** Root Cause → Remediation → Reporting (Email + Teams).
- **Design principles (non-negotiable):** fail-safe (agents suggest, never auto-execute); crew separation; full auditability (all reasoning + tool calls logged to Snowflake).
- **HIPAA guardrail:** PHI never leaves Snowflake; agents receive metadata + aggregate statistics only (column names, null%, distinct counts, min/max).
- 5-phase vendor roadmap (16 weeks) — we will map these against our Phase 1–7.

### 2.2 `DataLink_Medallion_Pipeline.docx` (authority on the ingestion pipeline)

- **Three source files:** Claims (EDI 837 / CSV), Membership (EDI 834 / CSV), Provider (NPPES CSV or REST API).
- **Three ingestion paths:** (A) sFTP → ADF Copy → ADLS, 02:00 UTC daily; (B) REST API → Event Hub → ADLS (NPPES, weekly); (C) Azure Blob direct upload.
- **ADLS layout:** `/raw/{claims,membership,provider}/{YYYY-MM-DD}/`, plus `/archive/` (90-day retention) and `/quarantine/`.
- **Bronze design principles:** INSERT-or-MERGE only (never DELETE); idempotent `MERGE` on natural keys; no type coercion (everything VARCHAR); no clinical validation; **exactly 4 metadata columns added** (`_load_dt`, `_source_file`, `_batch_id`, `_record_source`).
- **Bronze tables:** `RAW_CLAIMS`, `RAW_MEMBERSHIP`, `RAW_PROVIDER`.
- **Silver = Data Vault 2.0:** 3 Hubs (`HUB_CLAIM`, `HUB_MEMBER`, `HUB_PROVIDER`), 3 Satellites (`SAT_CLAIM_DETAILS`, `SAT_MEMBER_DEMOGRAPHICS`, `SAT_PROVIDER_INFO`), 3 Links (`LINK_CLAIM_MEMBER`, `LINK_CLAIM_PROVIDER`, `LINK_MEMBER_PLAN`). All **INSERT-ONLY** with `load_dts` — supports point-in-time queries.
- **Gold (per THIS doc)** = 6 analytics tables: `FACT_CLAIMS_SUMMARY`, `DIM_MEMBER`, `DIM_PROVIDER`, `UM_METRICS`, `STAR_RATINGS_FEED`, `AG_SUMMARY`. **→ Superseded by UM-Gold-v2 — see §3 Conflicts.**
- **GX Checkpoints:** CP1 Bronze (structural), CP2 Silver (clinical — CPT/ICD/member eligibility/NPI), CP3 Gold (business-rule/KPI).
- **HIPAA enforcement controls:** Azure Private Endpoints, RBAC, Snowflake dynamic data masking, row-level security, audit logging.

### 2.3 `EvokeConnectCare_UM_Gold_Layer_v2.docx` (authority on Gold schema and dual-warehouse router)

- **Gold Layer is NOT aggregated metrics. It is UM operational data.** Directly feeds the UM application's `[UM]` SQL Server schema at runtime. Analytics are a secondary concern and may live in a separate Gold subdomain.
- **Core transactional tables:** `[UM].[PatientAuth]` (header), `[UM].[AuthDecision]` (decisions), `[UM].[AuthCode]` (CPT/HCPCS), `[UM].[AuthDiagnoses]` (ICD-10), `[UM].[AuthProvider]` (requesting/servicing providers), `[UM].[AuthService]` + `[UM].[AuthServiceDecision]`.
- **Auth sub-type extensions:** `[UM].[InpatientAuth]`, `[UM].[OutpatientAuth]`, `[UM].[PharmacyAuth]`.
- **Workflow ancillaries:** `AuthTatTracking`, `AuthActivity` + `AuthActivityQueue`, `AuthNote`, `AuthDocument`, `AuthAdditionalDetails`.
- **55+ `Lu*` lookup tables** — FK resolution backbone; synced weekly Snowflake Gold → SQL Server.
- **Provider Data (corrected):** Provider Demographics + Provider Contracts + Member PCP Assignments. NOT EDI 278. NPPES is one source of Demographics only.
- **EDI 278 (correctly scoped):** Prior Auth Request (inbound → creates `PatientAuth`) and Response (outbound ← `AuthDecision`). Zero overlap with Provider Data.
- **Snowflake Gold → SQL Server `[UM]` refresh cadence:** 15-min incremental MERGE for transactional tables; 5-min for `AuthTatTracking`; daily 2 AM for provider/PCP; weekly Sunday 2 AM for all `Lu*`.
- **Agents expanded to 10** (5 pre + 5 post): adds Expectation Registry Manager, Data Quality Risk Scorer, Pre-Val Coordinator, Stakeholder Notification, GX Trend Analytics, Post-Val Coordinator. **→ See §3 Conflicts.**
- **UM-specific auto-pause thresholds:** null `PatientId` >2%, invalid CPT >5%, null `DecisionStatusId` >1%, inpatient LOS >365 (any), invalid NDC >3%, NPI Luhn fail >2%, TAT breach spike >15% vs prior week, FK orphan >0.5%.

---

## 3. Conflicts Between Source Docs — Resolution Strategy

| # | Conflict | Docs | Proposed Resolution (needs Jatin's sign-off) |
|---|---|---|---|
| C1 | **Gold schema shape.** BI tables vs. UM operational tables. | Medallion (6 BI tables) vs. UM-Gold-v2 (UM operational + `Lu*`). | **UM-Gold-v2 wins for Phase 4.** Gold is split into two subdomains: `gold_um_operational` (authoritative, powers `[UM]` SQL Server + PostgreSQL targets) and `gold_analytics` (the 6 BI tables — **deferred** unless you want them in Phase 4). |
| C2 | **CrewAI agent count.** 6 vs. 10. | GX base (3+3) vs. UM-Gold-v2 (5+5). | **Phase 5 ships the 6-agent baseline.** The 4 extras (Registry Manager, Risk Scorer, Notification, Trend Analytics — plus the two Coordinator agents if we want strict CrewAI crew-manager pattern) go to **Phase 5.5** (nice-to-have) unless you want the full 10 in Phase 5. |
| C3 | **Auto-pause thresholds.** Slightly different values across docs. | GX base (generic) vs. UM-Gold-v2 (UM-specific). | UM-Gold-v2 wins — more specific. Thresholds live in config, not code, so they're adjustable per pipeline. |
| C4 | **Provider file definition.** Medallion doc frames Provider = NPPES. UM-Gold-v2 says Provider = Demographics + Contracts + PCP. | Medallion vs. UM-Gold-v2 | Both compatible: NPPES feeds Demographics. Contracts + PCP come from separate sources (credentialing system, enrollment). Demo uses NPPES sample + synthetic Contracts/PCP CSVs. |
| C5 | **Pipeline Control schema naming.** `pipeline_control_state` (GX base) vs. `PIPELINE_CONTROL_STATE` (UM-Gold-v2). | Stylistic only. | Standardize on `SNAKE_CASE` uppercase for Snowflake table identifiers per Snowflake convention. |

**→ I need Jatin's explicit decisions on C1 and C2 before Phase 1.** All others are my call unless you disagree.

---

## 4. Target Architecture

### 4.1 Prod vs. Local — Component Map

| Layer | Prod | Local emulator | Adapter interface |
|---|---|---|---|
| File drop | Client-managed sFTP | `atmoz/sftp` container | `SftpSource` |
| Object store (landing) | Azure ADLS Gen2 | Azurite (Blob emulator) | `ObjectStore` |
| Warehouse | Snowflake (real) | **DuckDB** (see §5) | `Warehouse` |
| Orchestrator | Airflow on AKS | Airflow on **kind** (see §5) | — (Airflow is portable) |
| Transformations | dbt-snowflake | dbt-duckdb | — (dbt abstracts) |
| Operational DB #1 | SQL Server 2022 (on-prem) | `mcr.microsoft.com/mssql/server:2022-latest` container | `OperationalDb` |
| Operational DB #2 (future) | PostgreSQL 16 | `postgres:16` container | `OperationalDb` |
| Secrets | Azure Key Vault | `.env` via direnv | `SecretProvider` |
| Notifications | MS Teams webhook + SMTP | local webhook receiver stub (`smee.io`-like, logs to file) | `Notifier` |
| LLM (agents) | Anthropic Claude (prod key) | Anthropic Claude (dev key, same API) | `LlmProvider` |
| File processing | Pandas (small) / PySpark (large) | Same (in-process) | — |

**Processing threshold (Pandas vs. PySpark):** default **Pandas** for batches ≤ 500k rows *and* ≤ 500 MB input; **PySpark** above either bound. This is a config knob per DAG task, not hardcoded.

### 4.2 Data Flow — Full E2E

```
┌──────────┐  SFTP watcher   ┌────────────┐  Airflow: bronze_ingest_dag
│  atmoz/  │ ───────────────▶│  Azurite   │ ─────────────────────────┐
│  sftp    │                 │  (ADLS)    │                          │
└──────────┘                 └────────────┘                          │
                                                                     ▼
                                                          ┌──────────────────┐
                                                          │ DuckDB BRONZE    │
                                                          │ (RAW_CLAIMS,     │
                                                          │  RAW_MEMBERSHIP, │
                                                          │  RAW_PROVIDER)   │
                                                          └──────────────────┘
                                                                     │
                                                       GX Checkpoint 1 (structural)
                                                       Pre-Val Crew (Profiler→Author→Reviewer)
                                                                     │
                                                                     ▼
                                                          ┌──────────────────┐
                                                          │ DuckDB SILVER    │
                                                          │ Data Vault 2.0   │
                                                          │ (3 Hub / 3 Sat / │
                                                          │  3 Link)         │
                                                          └──────────────────┘
                                                                     │
                                                       GX Checkpoint 2 (clinical)
                                                       Post-Val Crew (on failure)
                                                                     │
                                                                     ▼
                                                          ┌──────────────────┐
                                                          │ DuckDB GOLD      │
                                                          │ gold_um_operational
                                                          │ (PatientAuth,    │
                                                          │  AuthDecision,   │
                                                          │  AuthCode, etc.) │
                                                          └──────────────────┘
                                                                     │
                                                       GX Checkpoint 3 (business-rule)
                                                                     │
                                                                     ▼
                                                     ┌───────────────────────────┐
                                                     │ Dual-Warehouse Router     │
                                                     │ (config-selected targets) │
                                                     └───────────────────────────┘
                                                              │             │
                                                              ▼             ▼
                                                    ┌────────────┐   ┌────────────┐
                                                    │ SQL Server │   │ PostgreSQL │
                                                    │   [UM]     │   │   um       │
                                                    └────────────┘   └────────────┘
```

---

## 5. Local Stack — Key Recommendations

### 5.1 Snowflake Local Emulator — **Recommend: DuckDB + `dbt-duckdb`**

| Option | Parity | Local-first? | Setup time | Cost | Verdict |
|---|---|---|---|---|---|
| **A. DuckDB** (`dbt-duckdb`) | ~80% for our use cases. MERGE, CTE, window fns, JSON all supported. Missing: `EXTERNAL STAGE`, `VARIANT`, `COPY INTO @stage`, `TASK`, `STREAM`. | ✅ Yes — embedded, no server | <1 min | Free | **Recommended.** |
| B. PostgreSQL as Snowflake stand-in | ~60%. Different MERGE syntax (`INSERT ... ON CONFLICT`), no VARIANT, dbt-postgres has different macros. Requires a compatibility shim layer. | ✅ Yes | 2–3 min | Free | Reject — higher dialect drift. |
| C. Real Snowflake trial | 100% | ❌ Needs network | 10–15 min account setup | Free 30 days / $400 credit, then paid | Reserve as **parity fallback** for pre-prod validation only. |

**Why DuckDB wins:**
1. **Zero external dependencies** — runs in-process, one `.db` file. Fits the "airplane demo on a fresh laptop" bar.
2. **dbt-duckdb is mature** — project structure and model SQL are portable to dbt-snowflake with a 1-line profile switch.
3. **Fastest iteration** — sub-second `dbt run` on the demo dataset.
4. **Deterministic demo resets** — `rm warehouse.duckdb && make up` resets everything.

**Gaps I'll paper over with adapters:**
- `EXTERNAL STAGE` / `COPY INTO FROM @stage` → `ObjectStoreToWarehouse` adapter. In Snowflake mode it issues `COPY INTO`; in DuckDB mode it pulls the file from Azurite and does `INSERT INTO ... SELECT * FROM read_csv_auto(...)`.
- `VARIANT` JSON column → Bronze source data is flat CSV, so not needed. If we add a JSON payload column later, we'll use DuckDB's `JSON` type with a dbt macro that maps to `VARIANT` in Snowflake mode.
- `TASK` / `STREAM` → not used — orchestration is Airflow, not Snowflake Tasks. Prod path is identical.

### 5.2 Local Kubernetes — **Recommend: `kind`**

| Option | Pros | Cons | Verdict |
|---|---|---|---|
| **A. kind** (Kubernetes-in-Docker) | Upstream K8s (same as AKS/EKS); starts/stops cleanly; releases memory; <2 min up; official Airflow Helm chart works unmodified. | Multi-node needs extra config. | **Recommended.** |
| B. minikube | Featureful (ingress/dashboard/LoadBalancer addons). | Heavier (VM by default on Windows); slower start; Docker driver is newer and occasionally flaky on Win11. | Reject — weight doesn't buy us anything. |
| C. Docker Desktop Kubernetes | Zero install (toggle in settings). | Single-node; version lag (often 1 minor behind upstream); memory persists even when "off"; reset requires full DD restart; not reproducible across team members (DD version drift). | Reject — worst story for team reproducibility. |
| D. k3d (K3s in Docker) | Smaller than kind, fast. | K3s isn't strictly upstream (Traefik + SQLite etcd) → semantic drift vs. AKS. | Reject — parity matters. |

**Why kind wins:** prod parity (upstream K8s = AKS semantics), clean start/stop, CI-friendly, zero VM layer.

### 5.3 Other Local Services

- **sFTP:** `atmoz/sftp` container. Known-good. Mounts a host dir as the drop zone; our sFTP adapter watches and pulls.
- **SQL Server:** `mcr.microsoft.com/mssql/server:2022-latest`. ~2 GB image, first boot takes 60 s. EULA accept via `ACCEPT_EULA=Y`. Works on Windows/WSL2.
- **PostgreSQL:** `postgres:16`. 150 MB image.
- **Azurite:** Microsoft-official ADLS Gen2 Blob emulator. Runs in Docker. Supports hierarchical namespace.
- **Airflow:** official `apache/airflow` Helm chart on kind. KubernetesExecutor so each task gets its own pod — matches prod.

### 5.4 Pinned Versions (initial — committed in Phase 1)

```
Python           3.11.x        (NOT 3.13 — CrewAI + PySpark wheels best on 3.11)
Docker Desktop   ≥ 4.30        (WSL2 backend)
kind             0.23.x
kubectl          1.30.x
helm             3.15.x
Airflow          2.9.x         (official Helm chart equivalent)
dbt-core         1.8.x
dbt-duckdb       1.8.x
dbt-snowflake    1.8.x
dbt-postgres     1.8.x
dbt-sqlserver    1.8.x         (community adapter — verify compatibility)
DuckDB           1.0.x
Great Expectations 1.x          (GX 1.x; migration from 0.18 legacy APIs)
CrewAI           0.80.x         (pin on lockfile — breaking API changes frequent)
anthropic SDK    0.40.x
Pandas           2.2.x
PySpark          3.5.x
uv               0.4.x          (dep manager)
ruff             0.6.x
mypy             1.11.x
pre-commit       3.7.x
```

> **Caveat:** Python 3.11 instead of 3.13 because CrewAI + PySpark + some dbt adapters lag the latest Python release. I'll pin exactly in Phase 1 and verify the combination builds on WSL2.

---

## 6. Plug-In / Plug-Out Contract

### 6.1 Adapter Interfaces (Python Protocols)

Every external dependency is accessed through a `typing.Protocol`. Concrete implementations are selected by config at startup via a factory. **No adapter may leak env-specific types (e.g., `snowflake.connector.Connection`) above the adapter boundary.**

Draft interfaces (final surface area settles in Phase 1):

```python
class SftpSource(Protocol):
    def list_files(self, pattern: str) -> list[RemoteFile]: ...
    def download(self, remote: RemoteFile, local_dest: Path) -> None: ...

class ObjectStore(Protocol):
    def put(self, local_path: Path, key: str) -> None: ...
    def get(self, key: str, local_dest: Path) -> None: ...
    def list(self, prefix: str) -> list[str]: ...
    def exists(self, key: str) -> bool: ...

class Warehouse(Protocol):
    def execute(self, sql: str, *params: Any) -> None: ...
    def query(self, sql: str, *params: Any) -> pd.DataFrame: ...
    def copy_from_stage(self, stage_uri: str, target_table: str, file_format: str) -> int: ...
    def merge(self, target: str, source: str, keys: list[str]) -> int: ...

class OperationalDb(Protocol):
    """Target for Gold bulk push. Adapters: SQL Server, PostgreSQL."""
    def bulk_upsert(self, table: str, rows: Iterable[dict], keys: list[str]) -> int: ...

class Notifier(Protocol):
    def notify(self, channel: str, severity: str, title: str, body: str) -> None: ...

class SecretProvider(Protocol):
    def get(self, name: str) -> str: ...

class LlmProvider(Protocol):
    def complete(self, messages: list[Message], **kwargs) -> Completion: ...
```

### 6.2 Config Layering

```
config/
├── base.yaml                   # cross-env defaults (schema names, threshold shapes)
├── environments/
│   ├── local.yaml              # atmoz-sftp, Azurite, DuckDB, kind, SQL Server + PG containers
│   ├── dev.yaml                # dev Snowflake, dev ADLS, dev SQL Server
│   ├── stage.yaml
│   └── prod.yaml               # prod Snowflake, prod ADLS Gen2, on-prem SQL Server, PostgreSQL
└── features/
    ├── gx.yaml                 # GX on/off, thresholds (one block per checkpoint)
    ├── agents.yaml             # CrewAI on/off, per-crew enable, LLM model, token budget
    └── warehouse_router.yaml   # which operational DBs receive Gold push (SQL Server, PG, both)
```

Resolution order (later overrides earlier): `base.yaml` → `environments/{env}.yaml` → `features/*.yaml` → env vars (prefixed `DL_`). Selected by `DL_ENV=local` (default).

### 6.3 Plug-Out Proof Test

A test named `test_plugout_gx_agents.py` runs the full pipeline with `features/gx.yaml: {enabled: false}` and `features/agents.yaml: {enabled: false}`. Pipeline must complete, data must flow end-to-end, and the logs must show `GX: disabled (config)` / `Agents: disabled (config)` messages. If this test fails, Phase 5 is not done.

### 6.4 PHI Guardrail (Enforced in Code)

A single `PhiRedactionLayer` class wraps all agent → LLM calls. It accepts only:
- Column names, types
- Aggregate statistics (null%, distinct count, min/max of numeric cols)
- Row counts, batch IDs, timestamps
- GX expectation suite JSON (no data values)

Any call that tries to pass a row-level payload raises `PhiBoundaryViolation`. A pytest fixture verifies this boundary on every build.

---

## 7. Repo Layout (to be scaffolded in Phase 1)

```
DataPipelinesWithGX/
├── CLAUDE.md                      # durable project brain (this repo's AGENTS.md)
├── README.md                      # team-facing onboarding (30-min demo)
├── Makefile                       # setup / up / down / demo / test / verify-phase-N
├── pyproject.toml                 # uv-managed
├── uv.lock
├── .env.example
├── .gitignore
├── .pre-commit-config.yaml
├── docker-compose.yml             # local services: sftp, azurite, sqlserver, postgres, webhook-stub
├── config/
│   ├── base.yaml
│   ├── environments/{local,dev,stage,prod}.yaml
│   └── features/{gx,agents,warehouse_router}.yaml
├── infra/
│   ├── kind/                      # kind cluster config
│   └── helm/                      # Airflow Helm values
├── dags/                          # Airflow DAGs
│   ├── bronze_ingest_dag.py
│   ├── silver_transform_dag.py
│   ├── gold_aggregate_dag.py
│   ├── pipeline_resume_dag.py
│   └── agent_trigger_dag.py
├── datalink/                      # Python package root
│   ├── adapters/                  # one subpackage per interface
│   │   ├── sftp/{atmoz.py,azure_sftp.py}
│   │   ├── object_store/{azurite.py,adls.py}
│   │   ├── warehouse/{duckdb_adapter.py,snowflake_adapter.py}
│   │   ├── operational_db/{sqlserver.py,postgres.py}
│   │   ├── notifier/{file.py,teams.py,smtp.py}
│   │   └── llm/{anthropic.py,stub.py}
│   ├── pipeline/
│   │   ├── control/              # state machine, checkpoints
│   │   ├── bronze/
│   │   ├── silver/
│   │   ├── gold/
│   │   └── router/               # dual-warehouse bulk-push router
│   ├── quality/                  # GX integration (plug-in layer)
│   ├── agents/                   # CrewAI crews (plug-in layer)
│   ├── phi/                      # PhiRedactionLayer
│   └── config/                   # config loader + factory
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles/{local.yml,dev.yml,prod.yml}
│   └── models/
│       ├── bronze/               # light — mostly MERGE macros
│       ├── silver/               # Data Vault (hubs, sats, links)
│       └── gold/
│           ├── um_operational/   # PatientAuth, AuthDecision, AuthCode, ...
│           ├── lookups/          # Lu*
│           └── analytics/        # (deferred — the 6 BI tables)
├── gx/                           # Great Expectations project
│   ├── great_expectations.yml
│   ├── expectations/
│   └── checkpoints/
├── data/
│   ├── sample/                   # synthetic Claims/Membership/Provider for demo
│   └── cms_reference/            # CPT/ICD-10 reference tables (public-domain sample)
├── tests/
│   ├── unit/
│   ├── integration/
│   ├── plugout/                  # GX/agents plug-out tests
│   └── phase/                    # verify-phase-N tests
├── docs/
│   ├── architecture.md           # this file
│   ├── install_guide.md          # (Phase 6)
│   ├── operations_runbook.md     # (Phase 6)
│   └── video_script.md           # (Phase 7)
└── scripts/
    ├── demo_reset.sh
    └── seed_sample_data.py
```

---

## 8. Phase Roadmap

| Phase | Scope | Exit — `make verify-phase-N` must exit 0 |
|---|---|---|
| **0** | Source-doc summary, architecture decision, emulator picks, conflict flags. **This doc + CLAUDE.md.** | Jatin approves. |
| **1** | Scaffold: repo, `uv`, config loader, adapter interfaces, secrets, logging, Makefile, docker-compose, kind cluster, Airflow Helm. | `make up && make verify-phase-1` — all services healthy, Airflow webserver reachable. |
| **2** | Bronze ingestion: SFTP watcher → Azurite → DuckDB Bronze with idempotent MERGE. | Drop file on SFTP; `bronze_ingest_dag` runs green; row count matches; re-running is a no-op (idempotency proof). |
| **3** | Silver DV 2.0: 3 Hubs, 3 Sats, 3 Links via dbt. | `dbt run && dbt test` green; point-in-time query works. |
| **4** | Gold UM operational + dual-warehouse router. | Same Gold row in both SQL Server and PostgreSQL when router = `both`; flipping to `sqlserver_only` in config skips PG — no code change. |
| **5** | GX + CrewAI plug-in layer (6-agent baseline from GX base doc). | **Both** `verify-phase-5-plugged-in` AND `verify-phase-5-plugged-out` pass. PHI boundary test green. |
| **5.5** *(if approved)* | 4 agent extras from UM-Gold-v2 (Registry Manager, Risk Scorer, Notification, Trend Analytics, Coordinators). | New agent tests green; Phase 5 tests still green. |
| **6** | Install guide (PDF+DOCX), Architecture guide (PDF+DOCX), Operations runbook, Team README. | A teammate on a fresh Win11 laptop clones → `make setup` → `make up` → `make demo` in <30 min with zero questions. |
| **7** | Video tutorial script + recording checklist + `make demo-reset`. | Script reviewed; no recording yet. |

---

## 9. Risks & Mitigations

| Risk | Severity | Mitigation |
|---|---|---|
| **Repo is inside OneDrive** — sync can silently prune `.git/objects/` (already observed today). | High | Either (a) move repo to `C:\Users\Jatin\dev\DataPipelinesWithGX`, or (b) add `.git/` and `warehouse.duckdb` to OneDrive sync exclusion list. Decision needed from Jatin. |
| DuckDB dialect drift from Snowflake. | Medium | Every dbt model targets both via a dialect macro layer. Phase 5 adds a nightly job that runs the same models against a real Snowflake trial account to catch drift. |
| CrewAI API churn. | Medium | Pin exact version in `uv.lock`. Adapter wraps CrewAI — if 1.0 lands, only the adapter changes. |
| Windows line-ending issues (CRLF vs LF). | Low | `.gitattributes` enforces LF for `.py`, `.sh`, `.yml`, `.md`. |
| Docker Desktop + WSL2 memory pressure on developer laptops. | Medium | `make down` releases all containers; kind releases cleanly. Document 16 GB RAM minimum in Phase 6. |
| `dbt-sqlserver` community adapter compatibility lag. | Medium | Gold bulk-push to SQL Server uses a **direct Python adapter** (`pyodbc` + `fast_executemany`), not dbt. dbt is only used for in-Snowflake transformations. This also keeps dbt dialect surface minimal. |

---

## 10. Open Questions for Jatin (answer before Phase 1)

1. **Conflict C1 — Gold scope for Phase 4.** `gold_um_operational` only (my recommendation), or include `gold_analytics` (6 BI tables: FACT_CLAIMS_SUMMARY, DIM_MEMBER, DIM_PROVIDER, UM_METRICS, STAR_RATINGS_FEED, AG_SUMMARY) in Phase 4 too?
2. **Conflict C2 — Agent scope for Phase 5.** 6-agent baseline (recommendation), or all 10 from UM-Gold-v2?
3. **Repo location.** Move out of OneDrive, or exclude `.git/` + `warehouse.duckdb` from OneDrive sync?
4. **Sample data.** OK to generate ~10k synthetic claims + 2k members + 500 providers as seed data, or do you have a sanitized sample file to drop in?
5. **LLM key for local dev.** Use your personal Anthropic dev key via `.env`, or should I stub the LLM for fully-offline demos (stub returns pre-canned profiler/author outputs)? Recommendation: **both** — stub by default so fresh clones work offline, real key opt-in via env var.
6. **Airflow on kind overhead.** Kind + Airflow KubernetesExecutor is ~4 GB RAM. If any teammate's laptop is <16 GB, we should add a `DL_ORCHESTRATOR=local_sequential` mode that runs DAGs as a plain Python process. Worth the complexity?

---

*End of Phase 0 architecture. Awaiting Jatin's review.*
