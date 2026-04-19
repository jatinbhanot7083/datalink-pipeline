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
- Orchestration → **Airflow** on **kind** (K8s-in-Docker), Helm-deployed.
- Transformations → **dbt-core** (dbt-duckdb local, dbt-snowflake prod).
- Processing → **Pandas** (≤500k rows / 500 MB) / **PySpark** (above).
- LLM → **Anthropic Claude** (prod + dev); stub for offline demos.

**Prod parity:** every external dep is accessed through a Python `Protocol` (adapter interface). Promotion to prod = flip `DL_ENV` from `local` to `prod`. No code edits.

## 4. Tech Stack — Pinned Versions (finalized Phase 1)

```
Python 3.11      Docker Desktop ≥4.30    kind 0.23     kubectl 1.30    helm 3.15
Airflow 2.9      dbt-core 1.8            DuckDB 1.0    Great Expectations 1.x
CrewAI 0.80      anthropic SDK 0.40      Pandas 2.2    PySpark 3.5
uv 0.4           ruff 0.6                mypy 1.11     pre-commit 3.7
```

## 5. Plug-In / Plug-Out Contract (non-negotiable)

- Every external system behind a Python `Protocol` in `datalink/adapters/`.
- One YAML per environment (`local` / `dev` / `stage` / `prod`) selects adapters.
- GX + CrewAI must be **provably removable**: `features/gx.yaml: enabled: false` and `features/agents.yaml: enabled: false` → pipeline runs E2E unchanged. Proven by `tests/plugout/test_plugout_gx_agents.py`.
- **PHI guardrail enforced in code**, not docs. `datalink/phi/PhiRedactionLayer` raises `PhiBoundaryViolation` if an agent → LLM call passes row-level data.
- Agents suggest, never auto-execute.
- No hardcoded paths, credentials, endpoints, or env-specific logic in pipeline code — everything through config.

## 6. Commands (stubs exist; real implementations land in Phase 1)

```bash
make setup          # install deps via uv, pre-commit hooks, kind cluster
make up             # start: docker-compose services + kind + Airflow helm release
make down           # stop and clean everything
make demo           # full E2E run on sample data (Bronze → Silver → Gold → dual push)
make demo-reset     # nuke warehouse.duckdb, re-seed sample data
make test           # unit + integration + plugout tests
make verify-phase-0 # Phase 0: docs exist + lint pass
make verify-phase-1 # Phase 1: all services healthy
make verify-phase-N # per-phase exit-0 verification
```

## 7. Conventions

- **Branches:** `phase-N-<short-slug>` (e.g., `phase-1-scaffolding`).
- **Commits:** Conventional Commits — `feat:`, `fix:`, `docs:`, `test:`, `chore:`, `refactor:`. Small, one logical change each.
- **Python style:** enforced by `ruff` + `mypy`. No disabled rules without a comment justifying.
- **Snowflake identifiers:** `SNAKE_CASE` uppercase (per Snowflake convention).
- **SQL Server identifiers:** `[Schema].[PascalCase]` per UM-Gold-v2 doc.
- **Secrets:** `.env` is gitignored. `.env.example` lists every var with a comment. Loaded via direnv.
- **Paths:** POSIX-style (`/`). Works in WSL2 bash. Windows-native paths only in user-facing docs.
- **Line endings:** LF enforced by `.gitattributes` for `*.py *.sh *.yml *.md`.

## 8. Never Do

- Hardcode paths, credentials, endpoints, or env-specific logic in pipeline code.
- Log PHI. Logs show metadata + aggregates only.
- `git push` without Jatin's explicit approval.
- `git push --force` on any branch. Ever.
- `git commit --amend` or `git rebase -i` on pushed history.
- Touch prod endpoints without Jatin's explicit go-ahead.
- Skip pre-commit hooks (`--no-verify`) unless Jatin explicitly says so.
- `rm -rf /` or `sudo <anything>` without explicit confirmation.
- Commit `.env`, credentials, or real customer data.
- Auto-execute a pipeline-altering action from an agent. Agents suggest; humans decide.

## 9. Current Phase

**Phase 0 — Architecture.** Deliverables: `docs/architecture.md` + this file. Awaiting Jatin's review before starting Phase 1.

### Open Questions (must be answered before Phase 1)

1. **Gold scope for Phase 4** — UM operational only, or also the 6 BI analytics tables?
2. **Agent scope for Phase 5** — 6-agent baseline, or all 10 from UM-Gold-v2?
3. **Repo location** — keep inside OneDrive (add exclusion) or move out?
4. **Sample data** — synthesize, or use a sanitized file Jatin provides?
5. **LLM key strategy** — stub-by-default with real-key opt-in, or require real key?
6. **Low-RAM orchestrator fallback** — add `DL_ORCHESTRATOR=local_sequential` mode for laptops with <16 GB RAM?

## 10. Notes for Future Sessions

- **OneDrive-repo hazard:** `.git/objects/` was silently pruned by OneDrive between the harness init and the first bash call on 2026-04-19. If `git status` reports "not a repository" despite `.git/` existing, re-run `git init` (idempotent) to restore missing subdirectories. Long-term: move the repo out of OneDrive or exclude `.git/`.
- **Docx extraction:** source docs are `.docx`. Use `python C:\tmp\docx_extract.py <file>` (stdlib-only) to dump to text. Read output via the `Read` tool.
- **Windows + bash specifics:** use Unix syntax (`/dev/null`, forward slashes). Windows paths only in user-facing docs.
