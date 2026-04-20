# CLAUDE.md — Project Brain

> **This file is the persistent brain for the DataLink Local-First Medallion Pipeline project.** Every future Claude Code session reads this first. Update it at every phase boundary. For full design detail, see [docs/architecture.md](docs/architecture.md).

---

## 1. Project in One Sentence

A local-first, production-parity end-to-end Medallion data pipeline (Bronze → Silver DV2.0 → Gold UM operational → dual-warehouse push to SQL Server + PostgreSQL) with a plug-in / plug-out Great Expectations + CrewAI agentic quality layer for EvokeConnectCare™ healthcare data (Membership / Claims / Providers).

## 2. Authoritative Source Documents

In priority order — when this CLAUDE.md conflicts with a source doc, **the source doc wins**:

1. `GX_Pipeline_Control_Agentic_AI_Architecture.docx` — authority on quality + agent layer.
2. `DataLink_Medallion_Pipeline.docx` — authority on ingestion pipeline.
3. `EvokeConnectCare_UM_Gold_Layer_v2.docx` — authority on Gold schema + dual-warehouse router.

All three live at the repo root. Extraction script (stdlib only): `C:\tmp\docx_extract.py`.

## 3. Architecture Summary (see `docs/architecture.md` for detail)

**Data flow:** `SFTP → ADLS Gen2 → Snowflake Bronze → Silver (DV2.0) → Gold (UM operational) → bulk push to SQL Server + PostgreSQL (config-selected).`

**Local emulator stack (decided Phase 0):**
- Snowflake → **DuckDB** via `dbt-duckdb` (with adapter shim for `EXTERNAL STAGE`).
- ADLS Gen2 → **LocalFsObjectStore** by default (Phase 2 pivot). Azurite remains available (scripts + adapter) for the day MCR pulls work again.
- sFTP → **atmoz/sftp** container.
- SQL Server → **`mcr.microsoft.com/mssql/server:2022-latest`** container.
- PostgreSQL → **`postgres:16`** container.
- Orchestration → **local_sequential** (default) or **Airflow on kind** (opt-in).
- Transformations → **dbt-core** (dbt-duckdb local, dbt-snowflake prod).
- Processing → **Pandas** (≤500k rows / 500 MB) / **PySpark** (above).
- LLM → **stub** (default, offline) or **Anthropic Claude** (opt-in via `ANTHROPIC_API_KEY`).

**Prod parity:** every external dep is accessed through a Python `Protocol` (adapter interface). Promotion to prod = flip `DL_ENV` from `local` to `prod`. No code edits.

## 4. Tech Stack — Pinned Versions

```
Python 3.11        Docker Desktop ≥4.30     kind 0.23 (opt-in)
kubectl 1.30 (opt) helm 3.15 (opt-in)       Airflow 2.9 (opt-in)
dbt-core 1.8       DuckDB 1.0               Great Expectations 1.x (Phase 5)
CrewAI 0.80 (P5)   anthropic SDK 0.40 (P5)  Pandas 2.2    PySpark 3.5 (Phase 2)
uv 0.4             ruff 0.6                 mypy 1.11     pre-commit 3.7
pydantic 2.7       structlog 24.1           pyyaml 6.0
```

Phase 1 installs only the lean runtime deps + dev tools. Phase-N-only libs (duckdb, paramiko, crewai, …) are opt-in extras: `uv sync --extra ingest`, `--extra agents`, etc. See `pyproject.toml` `[project.optional-dependencies]`.

## 5. Plug-In / Plug-Out Contract (non-negotiable)

- Every external system behind a Python `Protocol` in `datalink/adapters/protocols.py`.
- Config selects concrete adapter per env via `datalink/adapters/factory.py` (one `build_adapters(settings) → AdapterSet`).
- GX + CrewAI must be **provably removable**: `DL_FEATURES__GX__ENABLED=false` + `DL_FEATURES__AGENTS__ENABLED=false` → pipeline runs E2E unchanged. Proven by `tests/plugout/` (Phase 5).
- **PHI guardrail enforced in code**, not docs. `datalink.phi.PhiRedactionLayer.assert_clean(payload)` raises `PhiBoundaryViolationError` on anything shaped like a row or containing PHI-keyed fields. Enforced on every agent → LLM boundary.
- Agents suggest, never auto-execute.
- No hardcoded paths, credentials, endpoints, or env-specific logic in pipeline code — everything through `datalink.config.load_settings()`.

## 6. Commands (all targets live in `Makefile`)

```bash
make setup               # install uv, Python 3.11, deps, pre-commit hooks
make up                  # docker compose up -d --wait (sftp/azurite/sqlserver/postgres/webhook)
make down                # stop services (keep volumes)
make nuke                # stop + delete volumes (full reset)
make test                # pytest -m unit
make test-all            # all tests (unit + integration + phase + plugout)
make lint                # ruff check + mypy
make format              # ruff format + ruff --fix
make verify-phase-0      # Phase 0 docs present + correct
make verify-phase-1      # Phase 1: code + services
make verify-phase-N      # filled in at each phase
make up-airflow          # opt-in: kind + Helm + Airflow
make down-airflow        # tear down kind cluster
make help                # list all targets
```

## 7. Conventions

- **Branches:** `phase-N-<short-slug>` (e.g., `phase-1-scaffolding`).
- **Commits:** Conventional Commits — `feat:`, `fix:`, `docs:`, `test:`, `chore:`, `refactor:`. Small, one logical change each. Always end with the `Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>` trailer when committed via Claude Code.
- **Python style:** `ruff` (line length 100, target py311) + `mypy` strict on `datalink/`. No disabled rules without a justifying comment.
- **Snowflake identifiers:** `SNAKE_CASE` uppercase (per Snowflake convention).
- **SQL Server identifiers:** `[Schema].[PascalCase]` per UM-Gold-v2 doc.
- **Env vars:** `DL_` prefix, `__` for nesting. `DL_ADAPTERS__WAREHOUSE__PATH` → `settings.adapters.warehouse.path`.
- **Secrets:** `.env` is gitignored. `.env.example` lists every var with a comment.
- **Paths:** POSIX-style (`/`). Works in WSL2. Windows-native paths only in user-facing docs.
- **Line endings:** LF enforced by `.gitattributes`.

## 8. Never Do

- Hardcode paths, credentials, endpoints, or env-specific logic in pipeline code.
- Log PHI. Logs show metadata + aggregates only (`datalink.logging` has a PHI scrub processor).
- `git push` without Jatin's explicit approval.
- `git push --force` on any branch. Ever.
- `git commit --amend` or `git rebase -i` on pushed history.
- Touch prod endpoints without Jatin's explicit go-ahead.
- Skip pre-commit hooks (`--no-verify`) unless Jatin explicitly says so.
- `rm -rf /` or `sudo <anything>` without explicit confirmation.
- Commit `.env`, credentials, real customer data, or a `warehouse.duckdb`.
- Auto-execute a pipeline-altering action from an agent. Agents suggest; humans decide.

## 9. Current Phase

**Phase 4 — Gold UM Operational + Dual-Warehouse Router.** `make verify-phase-4` PASS — code (43 phase + 43 unit tests, ruff + mypy clean), dbt (Silver + Gold green), router (fan-out to two Postgres targets + idempotency + config-flip all green). Awaiting Jatin's review before Phase 5 (GX + CrewAI plug-in layer).

### Phase 4 deliverables (on `phase-4-gold-router`)

- **5 Gold UM transactional dbt models** (`dbt/models/gold/um_operational/`): `gold_patient_auth`, `gold_auth_decision`, `gold_auth_code`, `gold_auth_diagnoses`, `gold_auth_provider`. Derived from Silver Hub/Sat/Link joins. ~4,070 PatientAuths from the 10k sample claims (the 40% with `prior_auth_ref`). Materialized as `table` in a `gold_um` schema.
- **3 Lu\* seeds**: `lu_auth_status`, `lu_decision_status`, `lu_auth_type` (5 rows each, dbt seeds from CSV).
- **Dual-warehouse router** (`datalink/pipeline/router/`):
  - `schema.py` — central `GoldUmTable` map of all 8 UM tables + dialect-aware `postgres_ddl()` and `sqlserver_ddl()` generators (SQL Server uses `[UM].[PascalCase]`, TIMESTAMP→DATETIME2, BOOLEAN→BIT).
  - `bulk_push.py` — `push_gold_um_to_operational(adapters, settings)` fans out to every target in `features.warehouse_router.targets`. Bootstraps target schema on first call.
- **Real operational-DB adapters** (replaces Phase 1 stubs):
  - `PostgresOperationalDb` — psycopg 3, `INSERT … ON CONFLICT … DO UPDATE` for idempotent upsert. Safe rollback on error so one failure doesn't stick a connection in aborted state.
  - `SqlServerOperationalDb` — pyodbc, `MERGE INTO` + `fast_executemany`. pyodbc import is method-deferred so the adapter is instantiable on machines without libODBC.
- **Second Postgres in docker-compose** (`postgres_replica` on host port `:5433`): second target for the router fan-out demo, because Docker Desktop 4.51.0 blocks the SQL Server MCR pull. Primary `postgres` moved to `:5434` (Jatin's Windows has a native Postgres on `:5432`). Prod stays `[sqlserver, postgres]` — the replica service and port shifts disappear there.
- **Config**: `local.yaml` registers 3 `operational_dbs` (`sqlserver`, `postgres`, `postgres_replica`). Default `features.warehouse_router.targets = [postgres, postgres_replica]` (SQL Server excluded locally; flip to `[sqlserver, postgres]` for prod — config only, no code change).
- **Makefile targets**: `make gold-run`, `gold-test`, `push-to-ops`, `verify-phase-4{,-code,-dbt,-router}`.
- **Tests**: `tests/phase/test_phase_4.py` — 20 new tests covering Gold model layout, seed presence, router schema, DDL generators (PG + SQL Server), mocked fan-out, skip-unregistered-target, config-flip.
- **`scripts/smoke_router.py`**: full end-to-end — reset targets, push, verify counts match across both, re-run for idempotency, flip config and verify replica untouched.

### Phase 4 scope calls

1. **5 of 20+ UM tables**, not all. Medallion/UM-Gold-v2 lists `PatientAuth`, `AuthDecision`, `AuthCode`, `AuthDiagnoses`, `AuthProvider`, `AuthService`, `AuthServiceDecision`, `InpatientAuth`, `OutpatientAuth`, `PharmacyAuth`, `AuthTatTracking`, `AuthActivity`, `AuthNote`, `AuthDocument`, plus 55+ `Lu*` lookups. Phase 4 ships a representative 5 + 3 that exercises the full pipeline + router. Remaining UM tables go in Phase 4.5 (format is identical — add rows to `GOLD_UM_TABLES` + a dbt model).
2. **Two Postgres targets instead of SQL Server + Postgres**, locally only, to demo the router without depending on the MCR-blocked SQL Server image. Adapter code for SQL Server is real and tested via unit tests + mocked fan-out; it becomes active when `make up-mcr` succeeds.
3. **Integer surrogate keys via `ROW_NUMBER()`** — deterministic within a `dbt run --full-refresh`. Prod may want `SEQUENCE`/`IDENTITY` — easy migration.

### Phase 3 deliverables (on `phase-3-silver-dv2`)

### Phase 3 deliverables (on `phase-3-silver-dv2`)

- **dbt project** under `dbt/` — DuckDB-local / Snowflake-prod via profile switch:
  - `dbt_project.yml` (silver models: incremental, append, INSERT-ONLY)
  - `profiles.yml` (targets: local-duckdb, dev-snowflake, prod-snowflake)
  - `macros/dv_helpers.sql` — `dv_hash_key` + `dv_hash_diff` (dialect-agnostic MD5 over `|`-joined fields with NULL sentinel)
- **10 Silver DV2.0 models:**
  - Hubs (4): `hub_claim`, `hub_member`, `hub_provider`, `hub_plan` (last one added in Phase 3 so `LINK_MEMBER_PLAN` connects two hubs per DV2.0 spec)
  - Satellites (3): `sat_claim_details`, `sat_member_demographics`, `sat_provider_info` — INSERT-ONLY, hash-diff-based SCD Type 2
  - Links (3): `link_claim_member`, `link_claim_provider`, `link_member_plan` — INSERT-ONLY, composite hashes
- **60 dbt tests:** every hub_hk unique + not_null, every business key unique, every sat and link has a relationships test back to its hub(s), custom composite-uniqueness on all satellites, source not_null on claim_id/member_id/plan_id/npi
- **Makefile:** `make silver-run`, `silver-test`, `silver-clean`, `verify-phase-3{,-code,-dbt}`
- **`tests/phase/test_phase_3.py`:** structural checks (10 new phase tests) — every model file exists and uses the hash macros, schema.yml has required tests, profile has all 3 targets
- Bumped to 39 phase tests + 37 unit tests, ruff + mypy clean

### Phase 3 key call

Added a 4th hub `HUB_PLAN` beyond the Medallion doc's 3 hubs. Reason: the doc's listed `LINK_MEMBER_PLAN` needs two hubs to be a proper DV2.0 link; without `HUB_PLAN` it collapses into a degenerate single-hub bridge. `HUB_PLAN` is populated from both Bronze Claims and Bronze Membership (union) — captures every plan_id ever seen.

### Still deferred (Phase 4+)

- Real `SnowflakeWarehouse` adapter (prod warehouse path — Phase 4).
- Airflow DAGs wrapping the Silver build — local_sequential is default.
- `local_sequential` orchestrator CLI — smoke scripts cover the same path for now.

### Phase 2 deliverables (on `phase-2-bronze-ingestion`, commit `39ab4b5`)

### Phase 2 deliverables (on `phase-2-bronze-ingestion`, commit `39ab4b5`)

Bronze ingestion E2E: SFTP → LocalFs/Azurite → DuckDB MERGE with 4 audit columns. Real adapters (paramiko SFTP, Azure Blob SDK, DuckDB), deterministic sample data (500 providers + 2,000 members + 10,000 claims), idempotency proven. LocalFsObjectStore default for local (Docker Desktop 4.51.0 blocks MCR pulls; prod still uses ADLS Gen2 — plug-in contract preserved).

### Phase 1 deliverables (this branch)

- Repo skeleton per `docs/architecture.md` §7.
- `pyproject.toml` (uv-managed, pin py3.11).
- Config subsystem (`datalink/config/`) with validated Pydantic models + layered YAML loader + env-var overrides.
- Adapter Protocols (`datalink/adapters/protocols.py`) + subpackage per kind (sftp, object_store, warehouse, operational_db, notifier, secrets, llm) + `factory.py`.
- Working impls: `FileNotifier`, `EnvSecrets`, `StubLlm`. Others raise `NotImplementedError("lands in Phase N")` — instantiable but not callable.
- PHI guard (`datalink/phi/guard.py`) + scrubbing log processor (`datalink/logging.py`).
- `docker-compose.yml` (sftp + azurite + sqlserver + postgres + webhook-stub).
- Webhook-stub service (stdlib-only HTTP receiver).
- `infra/kind/cluster.yaml` + `infra/helm/airflow-values.yaml` (opt-in path, not in `verify-phase-1`).
- `Makefile` with setup/up/down/nuke/test/lint/format/demo/verify-phase-0/verify-phase-1(-code|-services)/up-airflow/down-airflow.
- `.pre-commit-config.yaml` (ruff + mypy + trailing-ws + large-file guard).
- Tests: `tests/unit/test_config_loader.py`, `test_phi_guard.py`, `test_adapter_factory.py`; `tests/phase/test_phase_1.py`.
- Team docs: `README.md`, `docs/install.md` (Win11 + WSL2 zero-guesswork).

### What Phase 1 does NOT include (by design)

- Real SFTP/ObjectStore/Warehouse/OperationalDb implementations — those land in Phase 2+.
- Airflow DAGs — Phase 2+.
- Any SQL DDL for Bronze/Silver/Gold — Phase 2+.
- CrewAI agent code — Phase 5.

## 10. Notes for Future Sessions

- **OneDrive-repo hazard:** `.git/objects/` was silently pruned by OneDrive once (2026-04-19). If `git status` reports "not a repository" despite `.git/` existing, re-run `git init` (idempotent) to restore missing subdirectories. Long-term fix is in `docs/install.md` §4.
- **Docx extraction:** source docs are `.docx`. Use `python C:\tmp\docx_extract.py <file>` (stdlib-only) to dump to text.
- **Windows + bash specifics:** use Unix syntax (`/dev/null`, forward slashes) inside this harness. Windows paths only in user-facing docs.
- **uv not installed globally:** the toolchain scan on 2026-04-19 found docker yes, uv/kind/helm/kubectl no. `make setup` installs uv; kind/helm/kubectl are opt-in per `infra/kind/README.md`.
- **Python 3.13 vs 3.11:** the dev box has 3.13 installed globally. `.python-version` pins to 3.11, and `uv python install 3.11` in `make setup` provisions it inside the venv.
