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
- ADLS Gen2 → **Azurite**.
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
- **PHI guardrail enforced in code**, not docs. `datalink.phi.PhiRedactionLayer.assert_clean(payload)` raises `PhiBoundaryViolation` on anything shaped like a row or containing PHI-keyed fields. Enforced on every agent → LLM boundary.
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

**Phase 1 — Scaffolding.** Awaiting Jatin's review.

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
