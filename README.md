# DataLink Local-First Medallion Pipeline

End-to-end Medallion data pipeline (Bronze → Silver DV 2.0 → Gold UM operational) for **EvokeConnectCare™**, with a plug-in / plug-out Great Expectations + CrewAI agentic quality layer. Runs 100% locally on Windows 11 + WSL2 + Docker Desktop; promotes to Azure + Snowflake + on-prem SQL Server + PostgreSQL via config change only.

**Prepared by Team DataLink.**

---

## Quick start (WSL2, ~30 min on a fresh machine)

Prerequisites: Windows 11 + WSL2 (Ubuntu 22.04+) + Docker Desktop (with WSL2 integration). See [docs/install.md](docs/install.md) for the zero-guesswork install path.

```bash
git clone <this-repo>
cd DataPipelinesWithGX
make setup          # installs uv, Python 3.11, project deps, pre-commit hooks
cp .env.example .env
make up             # starts SFTP, Azurite, SQL Server, Postgres, webhook stub
make verify-phase-1 # runs structural + unit tests + service health checks
make demo           # (Phase 2+) full E2E on sample Claims/Membership/Provider data
```

---

## Architecture in one picture

```
SFTP → ADLS Gen2 → Snowflake BRONZE (MERGE)
                 → SILVER (Data Vault 2.0)
                 → GOLD (UM operational schema)
                 → bulk push to SQL Server [UM]  (current)
                 →                  PostgreSQL  (future)
```

Local: atmoz/sftp, Azurite, DuckDB, SQL Server 2022 container, PostgreSQL 16 container.
Prod: Azure SFTP, ADLS Gen2, Snowflake, on-prem SQL Server, PostgreSQL.

See [docs/architecture.md](docs/architecture.md) for the full design, conflict analysis, and phase roadmap.

---

## Plug-in / plug-out contract

Great Expectations and the CrewAI agent layer are **provably removable**:

```bash
DL_FEATURES__GX__ENABLED=false DL_FEATURES__AGENTS__ENABLED=false make demo
# pipeline runs end-to-end, logs "GX: disabled" / "Agents: disabled"
```

PHI never leaves Snowflake. The `datalink.phi.PhiRedactionLayer` raises
`PhiBoundaryViolationError` on any agent → LLM call that contains row-level data.
This is enforced in code, not docs — see [tests/unit/test_phi_guard.py](tests/unit/test_phi_guard.py).

---

## Project layout

```
datalink/           # Python package — config, adapters, PHI guard, pipeline code
  adapters/         # one Protocol per external dep, concrete impls per env
  config/           # YAML loader + Pydantic models
  phi/              # PhiRedactionLayer — enforced PHI boundary
config/             # base + per-environment + per-feature YAML
docker/             # webhook-stub service
docker-compose.yml  # local service stack
infra/              # kind cluster + Airflow Helm values (opt-in)
tests/              # unit / integration / plugout / phase
docs/               # architecture, install, runbook
```

---

## Commands

```bash
make help               # list all targets
make up / down / nuke   # start / stop / full-reset docker-compose services
make test               # fast unit tests
make test-all           # unit + integration + phase + plugout
make lint               # ruff + mypy
make verify-phase-N     # per-phase exit-criteria gate
make up-airflow         # opt-in: spin up Airflow on kind (see infra/kind/README.md)
```

---

## Current state

Phase 1 complete — scaffolding done. Phase 2 (Bronze ingestion) is next.
For team onboarding feedback, ping @jatin.
