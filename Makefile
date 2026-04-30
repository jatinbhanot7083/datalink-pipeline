# =============================================================================
# DataLink Local-First Medallion Pipeline — Makefile
# Run from WSL2 bash. `make help` lists targets.
# =============================================================================

SHELL := /bin/bash
.DEFAULT_GOAL := help
.ONESHELL:
# With ONESHELL, a whole recipe runs in one shell — without -e, an earlier
# failing command silently continues. -e = abort on first non-zero; -c =
# read from command string; -u would blow up on make's $$$VAR idiom so skip.
.SHELLFLAGS := -ec

# Colors (disabled when stdout isn't a TTY)
BOLD := $(shell tput bold 2>/dev/null)
DIM  := $(shell tput dim 2>/dev/null)
RST  := $(shell tput sgr0 2>/dev/null)

# uv: dep manager. Installed by `make setup` if missing.
UV := $(shell command -v uv 2>/dev/null)

# Python the project is pinned to.
PY_VERSION := 3.11

# docker-compose project name (matches name: in docker-compose.yml)
COMPOSE_PROJECT := datalink-local

# Airflow Helm release name + namespace
KIND_CLUSTER := datalink-local
AIRFLOW_NS   := airflow
AIRFLOW_RELEASE := airflow

# -----------------------------------------------------------------------------
.PHONY: help
help: ## List available targets with descriptions
	@echo "$(BOLD)DataLink Pipeline — Makefile targets$(RST)"
	@echo
	@grep -E '^[a-zA-Z0-9_-]+:.*?##' $(MAKEFILE_LIST) | \
	  awk 'BEGIN {FS = ":.*?## "}; {printf "  $(BOLD)%-28s$(RST) %s\n", $$1, $$2}'
	@echo
	@echo "$(DIM)Typical onboarding: make setup && make up && make verify-phase-1$(RST)"

# =============================================================================
# Setup
# =============================================================================

.PHONY: setup
setup: _install-uv _install-python _sync-deps _install-hooks ## Install toolchain + Python deps + pre-commit hooks
	@echo "$(BOLD)setup complete$(RST) — try: make up && make verify-phase-1"

.PHONY: _install-uv
_install-uv:
	@if [ -z "$(UV)" ]; then \
	  echo "installing uv..."; \
	  curl -LsSf https://astral.sh/uv/install.sh | sh; \
	  echo "uv installed. Add ~/.local/bin to PATH if not already there."; \
	else echo "uv already installed: $$(uv --version)"; fi

.PHONY: _install-python
_install-python:
	@uv python install $(PY_VERSION)

.PHONY: _sync-deps
_sync-deps:
	@uv sync --group dev

.PHONY: _install-hooks
_install-hooks:
	@uv run pre-commit install

# =============================================================================
# Local stack — docker-compose services
# =============================================================================

.PHONY: up
up: ## Start the full Phase-5.7 stack (SFTP + 3 ops DBs + Azurite + SQLServer + pgAdmin + Adminer + nginx + OTEL/Prom/Loki/Tempo/Grafana)
	docker compose up -d --wait
	@echo ""
	@echo "$(BOLD)services up$(RST)"
	@docker compose ps --format 'table {{.Service}}\t{{.Status}}'
	@echo ""
	@echo "$(BOLD)Browse — exec URL first, ops drill-downs after:$(RST)"
	@echo "  $(BOLD)Control Tower     → http://localhost:8000$(RST)   (single exec URL)"
	@echo "  Airflow            → http://localhost:8088   (admin / admin_local_only)"
	@echo "  Filebrowser        → http://localhost:8082   (drag-drop SFTP drop zone)"
	@echo "  GX Data Docs       → http://localhost:8090"
	@echo "  Webhook Inbox      → http://localhost:9000   (agent notifications)"
	@echo "  pgAdmin            → http://localhost:5050   (datalink@example.com / datalink_local_only)"
	@echo "  Adminer            → http://localhost:8081   (system=MS SQL / server=sqlserver / user=datalink)"
	@echo "  Grafana            → http://localhost:3000   (admin / admin_local_only)"
	@echo "  Prometheus         → http://localhost:9090"
	@echo "  Portainer          → https://localhost:9443  (container ops)"

.PHONY: up-mcr
up-mcr: up ## Alias for `make up` (kept for backwards compat; mcr profile is folded into default now)

.PHONY: down
down: ## Stop and remove docker-compose services (keeps volumes)
	docker compose down

.PHONY: nuke
nuke: ## Stop + remove services AND named volumes — full reset
	docker compose down -v

.PHONY: logs
logs: ## Tail all service logs
	docker compose logs -f --tail=100

.PHONY: ps
ps: ## Show running services
	docker compose ps

# =============================================================================
# Tests + lint
# =============================================================================

.PHONY: test
test: ## Run unit tests (fast, no Docker)
	uv run pytest -m unit

.PHONY: test-all
test-all: ## Run unit + integration + phase + plugout tests
	uv run pytest

.PHONY: lint
lint: ## ruff check + mypy
	uv run ruff check .
	uv run mypy datalink

.PHONY: format
format: ## ruff format in place
	uv run ruff format .
	uv run ruff check --fix .

# =============================================================================
# Per-phase verification
# =============================================================================

.PHONY: verify-phase-0
verify-phase-0: ## Phase 0: architecture docs present + correct
	@echo "Verifying Phase 0 deliverables..."
	@test -s docs/architecture.md                 || { echo "FAIL: docs/architecture.md missing"; exit 1; }
	@test -s CLAUDE.md                            || { echo "FAIL: CLAUDE.md missing"; exit 1; }
	@test -s .gitignore                           || { echo "FAIL: .gitignore missing"; exit 1; }
	@test -s .gitattributes                       || { echo "FAIL: .gitattributes missing"; exit 1; }
	@grep -q "^## 3. Conflicts"       docs/architecture.md || { echo "FAIL: missing Conflicts section"; exit 1; }
	@grep -q "^## 5. Local Stack"     docs/architecture.md || { echo "FAIL: missing Local Stack section"; exit 1; }
	@grep -q "^## 10. Open Questions" docs/architecture.md || { echo "FAIL: missing Open Questions section"; exit 1; }
	@grep -q "^## 5. Plug-In" CLAUDE.md           || { echo "FAIL: CLAUDE.md missing Plug-In contract"; exit 1; }
	@grep -q "^## 8. Never Do" CLAUDE.md          || { echo "FAIL: CLAUDE.md missing Never Do list"; exit 1; }
	@echo "OK: Phase 0 deliverables present."

.PHONY: verify-phase-1
verify-phase-1: verify-phase-1-code verify-phase-1-services ## Phase 1: code + services
	@echo "$(BOLD)verify-phase-1 PASS$(RST)"

.PHONY: verify-phase-1-code
verify-phase-1-code: ## Phase 1 code checks — no Docker required
	@echo "--- phase-1 structural tests ---"
	uv run pytest -m phase -q
	@echo "--- phase-1 unit tests ---"
	uv run pytest -m unit -q
	@echo "--- phase-1 lint ---"
	uv run ruff check .
	@echo "--- phase-1 type check ---"
	uv run mypy datalink
	@echo "OK: phase-1 code checks"

.PHONY: verify-phase-1-services
verify-phase-1-services: ## Phase 1 services — requires `make up` to have run
	@echo "--- phase-1 service health (default profile: Hub-only) ---"
	@docker compose ps --format 'table {{.Service}}\t{{.Status}}'
	@for svc in sftp postgres webhook-stub; do \
	  status=$$(docker compose ps --format '{{.Service}}={{.Status}}' | grep "^$$svc=" | cut -d= -f2); \
	  echo "  $$svc: $$status"; \
	  case "$$status" in \
	    *unhealthy*)  echo "FAIL: $$svc is unhealthy"; exit 1 ;; \
	    *healthy*)    : ;; \
	    *)            echo "FAIL: $$svc not healthy — status: $$status"; exit 1 ;; \
	  esac; \
	done
	@echo "OK: default-profile services healthy"
	@echo "  (azurite + sqlserver are in the 'mcr' compose profile — off by default;"
	@echo "   bring up with 'make up-mcr' once MCR pulls work on your machine.)"

# =============================================================================
# Demo (Phase 2+ will flesh this out)
# =============================================================================

.PHONY: demo
demo: ## Bronze E2E demo — upload 3 sample CSVs via SFTP → LocalFs → DuckDB MERGE
	uv run python scripts/smoke_bronze.py

.PHONY: demo-reset
demo-reset: ## Wipe warehouse.duckdb + localfs object store + re-seed sample CSVs (10K small)
	rm -f warehouse.duckdb warehouse.duckdb.wal
	rm -rf /tmp/datalink-localfs
	rm -rf data/sample
	uv run python scripts/seed_sample_data.py

.PHONY: demo-reset-exec
demo-reset-exec: ## Wipe + seed Phase 5.7 executive-demo dataset (100K claims, 3 date-stamped daily batches)
	rm -f warehouse.duckdb warehouse.duckdb.wal
	rm -rf /tmp/datalink-localfs
	rm -rf data/sample
	uv run python scripts/seed_sample_data.py \
	  --claims 100000 --members 20000 --providers 5000 \
	  --batches 3 --end-date 2026-04-19

.PHONY: verify-phase-2
verify-phase-2: verify-phase-2-code verify-phase-2-demo ## Phase 2: code + full E2E demo
	@echo "$(BOLD)verify-phase-2 PASS$(RST)"

.PHONY: verify-phase-2-code
verify-phase-2-code: ## Phase 2 code checks — no Docker required
	@echo "--- phase-2 structural tests ---"
	uv run pytest -m phase -q
	@echo "--- phase-2 unit tests ---"
	uv run pytest -m unit -q
	@echo "--- phase-2 lint ---"
	uv run ruff check .
	@echo "--- phase-2 type check ---"
	uv run mypy datalink
	@echo "OK: phase-2 code checks"

.PHONY: silver-run
silver-run: ## dbt run — build Silver DV2.0 models from Bronze
	uv run dbt run --project-dir dbt --profiles-dir dbt

.PHONY: silver-test
silver-test: ## dbt test — uniqueness + not_null + relationships on Silver
	uv run dbt test --project-dir dbt --profiles-dir dbt

.PHONY: silver-clean
silver-clean: ## Drop all Silver tables (next silver-run rebuilds from scratch)
	uv run python -c "import duckdb; duckdb.connect('warehouse.duckdb').execute('DROP SCHEMA IF EXISTS SILVER_silver CASCADE')"

.PHONY: verify-phase-3
verify-phase-3: verify-phase-3-code verify-phase-3-dbt ## Phase 3: code + dbt run/test green
	@echo "$(BOLD)verify-phase-3 PASS$(RST)"

.PHONY: verify-phase-3-code
verify-phase-3-code: ## Phase 3 code checks — no Docker required
	@echo "--- phase-3 structural tests ---"
	uv run pytest -m phase -q
	@echo "--- phase-3 unit tests ---"
	uv run pytest -m unit -q
	@echo "--- phase-3 lint ---"
	uv run ruff check .
	@echo "--- phase-3 type check ---"
	uv run mypy datalink
	@echo "OK: phase-3 code checks"

.PHONY: verify-phase-3-dbt
verify-phase-3-dbt: ## Phase 3 dbt verify — dbt run + dbt test (requires Bronze populated)
	@echo "--- phase-3 Silver DV 2.0 (dbt run) ---"
	uv run dbt run --project-dir dbt --profiles-dir dbt
	@echo "--- phase-3 Silver tests (dbt test) ---"
	uv run dbt test --project-dir dbt --profiles-dir dbt
	@echo "OK: Silver DV 2.0 built + tested"

# ---------------------------------------------------------------------
# Phase 4 — Gold UM + dual-warehouse router
# ---------------------------------------------------------------------

.PHONY: gold-run
gold-run: ## dbt seed + dbt run — build Lu* + 5 Gold UM models from Silver
	uv run dbt seed --project-dir dbt --profiles-dir dbt
	uv run dbt run --project-dir dbt --profiles-dir dbt --select gold

.PHONY: gold-test
gold-test: ## dbt test on Gold only
	uv run dbt test --project-dir dbt --profiles-dir dbt --select gold

.PHONY: push-to-ops
push-to-ops: ## Push Gold UM tables to every target in features.warehouse_router.targets
	uv run python scripts/smoke_router.py

.PHONY: verify-phase-4
verify-phase-4: verify-phase-4-code verify-phase-4-dbt verify-phase-4-router ## Phase 4: code + gold dbt + router end-to-end
	@echo "$(BOLD)verify-phase-4 PASS$(RST)"

.PHONY: verify-phase-4-code
verify-phase-4-code: ## Phase 4 code checks — no services required
	@echo "--- phase-4 structural tests ---"
	uv run pytest -m phase -q
	@echo "--- phase-4 unit tests ---"
	uv run pytest -m unit -q
	@echo "--- phase-4 lint ---"
	uv run ruff check .
	@echo "--- phase-4 type check ---"
	uv run mypy datalink
	@echo "OK: phase-4 code checks"

.PHONY: verify-phase-4-dbt
verify-phase-4-dbt: ## Phase 4 dbt verify — Gold models + tests (Silver+Bronze must exist)
	@echo "--- phase-4 Gold (dbt seed + run) ---"
	uv run dbt seed --project-dir dbt --profiles-dir dbt
	uv run dbt run --project-dir dbt --profiles-dir dbt
	@echo "--- phase-4 Gold tests ---"
	uv run dbt test --project-dir dbt --profiles-dir dbt
	@echo "OK: Gold UM built + tested"

.PHONY: verify-phase-4-router
verify-phase-4-router: ## Phase 4 router verify — push Gold to both Postgres targets
	@echo "--- phase-4 dual-warehouse router ---"
	uv run python scripts/smoke_router.py
	@echo "OK: router fan-out + idempotency + config-flip green"

# ---------------------------------------------------------------------
# Phase 5 — GX + CrewAI plug-in layer
# ---------------------------------------------------------------------

.PHONY: gx-demo
gx-demo: ## Run the full Phase 5 demo — 6 agents + 3 GX checkpoints + PHI guard
	uv run python scripts/demo_phase_5.py

.PHONY: verify-phase-5
verify-phase-5: verify-phase-5-code verify-phase-5-plugged-in verify-phase-5-plugged-out ## Phase 5: code + plugged-in demo + plug-out proof
	@echo "$(BOLD)verify-phase-5 PASS$(RST)"

.PHONY: verify-phase-5-code
verify-phase-5-code: ## Phase 5 code checks — no warehouse state required
	@echo "--- phase-5 structural tests ---"
	uv run pytest -m phase -q
	@echo "--- phase-5 unit tests ---"
	uv run pytest -m unit -q
	@echo "--- phase-5 lint ---"
	uv run ruff check .
	@echo "--- phase-5 type check ---"
	uv run mypy datalink
	@echo "OK: phase-5 code checks"

.PHONY: verify-phase-5-plugged-in
verify-phase-5-plugged-in: ## Phase 5 demo — all 6 agents + 3 GX suites + PHI boundary
	@echo "--- phase-5 demo (features.gx=true, features.agents=true) ---"
	uv run python scripts/demo_phase_5.py
	@echo "OK: Phase 5 plugged-in demo green"

.PHONY: verify-phase-5-plugged-out
verify-phase-5-plugged-out: ## Phase 5 plug-out proof — GX + Agents disabled, pipeline unchanged
	@echo "--- phase-5 plug-out (features.gx=false, features.agents=false) ---"
	uv run python scripts/plugout_phase_5.py
	@echo "OK: Phase 5 plug-out proof green"

.PHONY: verify-phase-2-demo
verify-phase-2-demo: ## Phase 2 E2E demo — requires `make up` first
	@echo "--- phase-2 E2E (SFTP → LocalFs → DuckDB, idempotent MERGE) ---"
	uv run python scripts/smoke_bronze.py
	@echo "OK: phase-2 E2E green"

# ---------------------------------------------------------------------
# Phase 5.5 — orchestration (local_sequential + Airflow DAGs)
# ---------------------------------------------------------------------

.PHONY: local-seq-list
local-seq-list: ## List available pipelines + their task graphs
	uv run python -m datalink.orchestration.local_sequential list

.PHONY: local-seq-run
local-seq-run: ## Run one pipeline via the local_sequential runner (PIPELINE=bronze_ingest|silver_transform|gold_um_push)
	uv run python -m datalink.orchestration.local_sequential run --pipeline $(or $(PIPELINE),bronze_ingest)

.PHONY: setup-airflow-venv
setup-airflow-venv: ## Build a dedicated .venv-airflow venv (Airflow 2.9 conflicts with main venv)
	uv venv .venv-airflow --python 3.11
	uv pip install --python .venv-airflow/bin/python \
	  "apache-airflow>=2.9,<2.10" \
	  "pydantic>=2.7,<3.0" "pydantic-settings>=2.3,<3.0" "pyyaml>=6.0,<7.0" \
	  "structlog>=24.1,<25.0" "click>=8.1,<9.0" "python-dotenv>=1.0,<2.0"
	uv pip install --python .venv-airflow/bin/python -e . --no-deps
	@echo "$(BOLD).venv-airflow ready$(RST) — now try: make dag-parse"

.PHONY: dag-parse
dag-parse: ## Load every DAG under dags/ via airflow.DagBag, fail on any import error (needs: .venv-airflow)
	@test -x .venv-airflow/bin/python || { \
	  echo "FAIL: .venv-airflow not found. Create with:"; \
	  echo "  uv venv .venv-airflow --python 3.11"; \
	  echo "  uv pip install --python .venv-airflow/bin/python 'apache-airflow>=2.9,<2.10' pydantic pydantic-settings pyyaml structlog click python-dotenv"; \
	  echo "  uv pip install --python .venv-airflow/bin/python -e . --no-deps"; \
	  exit 1; \
	}
	.venv-airflow/bin/python -c "import sys; from airflow.models import DagBag; \
	  b=DagBag(dag_folder='dags', include_examples=False, safe_mode=False); \
	  print('loaded:', sorted(b.dag_ids)); \
	  [print('IMPORT ERROR', p, ':', e) for p,e in b.import_errors.items()]; \
	  sys.exit(1 if b.import_errors or len(b.dag_ids) != 3 else 0)"

.PHONY: verify-phase-5.5
verify-phase-5.5: verify-phase-5.5-code verify-phase-5.5-local ## Phase 5.5: code + local_sequential smoke (airflow opt-in)
	@echo "$(BOLD)verify-phase-5.5 PASS$(RST)"

.PHONY: verify-phase-5.5-code
verify-phase-5.5-code: ## Phase 5.5 code checks — no warehouse state required
	@echo "--- phase-5.5 structural tests ---"
	uv run pytest -m phase -q
	@echo "--- phase-5.5 unit tests ---"
	uv run pytest -m unit -q
	@echo "--- phase-5.5 lint ---"
	uv run ruff check .
	@echo "--- phase-5.5 type check ---"
	uv run mypy datalink
	@echo "OK: phase-5.5 code checks"

.PHONY: verify-phase-5.5-local
verify-phase-5.5-local: ## Phase 5.5 local_sequential smoke — all 3 pipelines + sensor short-circuit
	@echo "--- phase-5.5 local_sequential smoke ---"
	uv run python scripts/smoke_orchestration.py
	@echo "OK: phase-5.5 local_sequential smoke green"

.PHONY: verify-phase-5.5-airflow
verify-phase-5.5-airflow: ## Phase 5.5 Airflow DAG-parse verify (requires --extra orchestration)
	@echo "--- phase-5.5 Airflow DAG parse ---"
	$(MAKE) dag-parse
	@echo "OK: phase-5.5 Airflow DAG parse green"

# =============================================================================
# Airflow-on-kind — opt-in path (requires kind + helm + kubectl installed)
# =============================================================================

.PHONY: up-airflow
up-airflow: ## Create kind cluster + install Airflow via Helm (opt-in)
	@command -v kind    >/dev/null || { echo "kind not installed — see infra/kind/README.md"; exit 1; }
	@command -v helm    >/dev/null || { echo "helm not installed"; exit 1; }
	@command -v kubectl >/dev/null || { echo "kubectl not installed"; exit 1; }
	kind create cluster --config infra/kind/cluster.yaml
	helm repo add apache-airflow https://airflow.apache.org 2>/dev/null || true
	helm repo update
	kubectl create namespace $(AIRFLOW_NS) --dry-run=client -o yaml | kubectl apply -f -
	helm upgrade --install $(AIRFLOW_RELEASE) apache-airflow/airflow \
	  --namespace $(AIRFLOW_NS) \
	  --values infra/helm/airflow-values.yaml \
	  --timeout 10m
	@echo "Airflow UI → http://localhost:8080  (admin / admin_local_only)"

.PHONY: down-airflow
down-airflow: ## Delete the Airflow kind cluster
	@command -v kind >/dev/null && kind delete cluster --name $(KIND_CLUSTER) || echo "kind not installed; nothing to do"

# =============================================================================
# Phase 6 smoke — THE regression safety net.
# Catches the class of bug that passes unit tests but fails in containers.
# =============================================================================

.PHONY: verify-phase-6-containers
verify-phase-6-containers: ## Phase 6 container-contract tests (requires stack running)
	@echo '--- Phase 6 container contracts ---'
	@.venv/bin/python -m pytest tests/integration/test_container_contracts.py -v -m integration

.PHONY: smoke-phase-6
smoke-phase-6: ## Phase 6 E2E smoke — assumes stack is already up
	@.venv/bin/python scripts/smoke_phase_6.py --skip-pipeline

.PHONY: smoke-phase-6-full
smoke-phase-6-full: ## Phase 6 E2E smoke with bronze_ingest for aetna (5-10 min)
	@.venv/bin/python scripts/smoke_phase_6.py --client aetna

.PHONY: smoke-phase-6-reset
smoke-phase-6-reset: ## Full nuke + rebuild + smoke — THE before-demo command
	@.venv/bin/python scripts/smoke_phase_6.py --reset --client aetna

# =============================================================================
# RECOVERY TOOLKIT — when http://localhost:8000 is unreachable from Windows.
# Background: WSL2 + Hyper-V firewall + iphlpsvc cache stale port bindings
# after experiments / reboots. The validated recipe (2026-04-30) is:
#
#   1. ADMIN PowerShell:  scripts\recover-windows-network.ps1
#      └─ resets netsh portproxy + restarts iphlpsvc + wsl --shutdown
#   2. WSL bash:          make recover
#      └─ starts stack, waits for healthy, pre-warms caches
#
# Total time ~3 minutes. See docs/demo_journal.md for the manual fallback.
# =============================================================================

.PHONY: recover
recover: ## End-to-end WSL-side recovery (run AFTER recover-windows-network.ps1)
	@echo
	@echo "$(BOLD)============================================================$(RST)"
	@echo "$(BOLD)  DataLink Command Center — WSL-side recovery$(RST)"
	@echo "$(BOLD)============================================================$(RST)"
	@echo
	@echo "$(BOLD)>>> Step 1/5 — Sanity check: WSL is up$(RST)"
	@uname -n
	@echo
	@echo "$(BOLD)>>> Step 2/5 — Stop any leftover Docker stack$(RST)"
	@docker compose down --remove-orphans 2>&1 | tail -3 || echo "  (nothing to stop)"
	@echo
	@echo "$(BOLD)>>> Step 3/5 — Bring the stack up clean$(RST)"
	@docker compose up -d 2>&1 | tail -10
	@echo
	@echo "$(BOLD)>>> Step 4/5 — Wait for control_tower healthy (up to 150s)$(RST)"
	@for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15; do \
	  status=$$(docker inspect datalink-control-tower --format '{{.State.Health.Status}}' 2>/dev/null || echo missing); \
	  if [ "$$status" = "healthy" ]; then echo "  attempt $$i: healthy"; break; fi; \
	  echo "  attempt $$i: $$status — waiting 10s..."; \
	  sleep 10; \
	done
	@echo
	@echo "$(BOLD)>>> Step 5/5 — Pre-warm Snowflake + Streamlit caches$(RST)"
	@docker exec datalink-control-tower python3 -c "from datalink.ui._query import warehouse_ctx; \
	  ctx = warehouse_ctx(readonly=True); wh = ctx.__enter__(); \
	  wh.query('SELECT 1'); wh.query('SELECT COUNT(*) FROM CONTROL.dq_suites'); \
	  print('  Snowflake warmed')" 2>&1 | tail -2
	@for path in '' Pipeline_Control DQ_AI_Architect Smart_Mapper Schema_Drift DQ_Author DQ_Review; do \
	  curl -s -o /dev/null --max-time 8 "http://localhost:8000/$$path?client=aetna"; \
	done
	@echo "  Streamlit pre-warmed (7 pages)"
	@echo
	@code=$$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 http://localhost:8000); \
	if [ "$$code" = "200" ]; then \
	  echo "$(BOLD)============================================================$(RST)"; \
	  echo "$(BOLD)  ✓ URL READY — open http://localhost:8000 in browser$(RST)"; \
	  echo "$(BOLD)============================================================$(RST)"; \
	else \
	  echo "$(BOLD)WARNING: WSL-side curl returned http=$$code$(RST)"; \
	  echo "  If browser also fails, run the admin PowerShell script first:"; \
	  echo "  scripts\\recover-windows-network.ps1"; \
	fi
	@echo

.PHONY: recover-help
recover-help: ## Print the full 2-command recovery procedure
	@echo
	@echo "$(BOLD)Recovery procedure — when http://localhost:8000 is unreachable:$(RST)"
	@echo
	@echo "  $(BOLD)1.$(RST) In ADMIN PowerShell (right-click PowerShell → Run as Administrator):"
	@echo "       cd 'C:\\Users\\Jatin\\OneDrive - MASTER WORKSPACE\\1_AA_WORKSPACE\\DataPipelinesWithGX'"
	@echo "       .\\scripts\\recover-windows-network.ps1"
	@echo
	@echo "  $(BOLD)2.$(RST) In WSL bash (this terminal):"
	@echo "       make recover"
	@echo
	@echo "  Total time: ~3 minutes."
	@echo
	@echo "  See docs/demo_journal.md → 'RECOVERY RUNBOOK' for the 13-step"
	@echo "  manual procedure if scripts can't be used."
	@echo

.PHONY: show-url
show-url: ## Print the URLs that should work in your browser
	@WSL_IP=$$(hostname -I | awk '{print $$1}'); \
	echo; \
	echo "$(BOLD)============================================================$(RST)"; \
	echo "$(BOLD)  DataLink — URLs to use in your Windows browser$(RST)"; \
	echo "$(BOLD)============================================================$(RST)"; \
	echo; \
	echo "  Control Tower : $(BOLD)http://localhost:8000$(RST)"; \
	echo "  Airflow       : http://localhost:8088"; \
	echo "  Grafana       : http://localhost:3000"; \
	echo "  pgAdmin       : http://localhost:5050"; \
	echo "  Filebrowser   : http://localhost:8082"; \
	echo "  Adminer       : http://localhost:8081"; \
	echo "  GX Docs       : http://localhost:8090"; \
	echo; \
	echo "$(DIM)Direct WSL IP fallback (if localhost fails):$(RST)"; \
	echo "$(DIM)  http://$$WSL_IP:8000$(RST)"; \
	echo

.PHONY: nuke-rules-engine
nuke-rules-engine: ## If ecc-rules-engine left state behind, kill all of it
	@echo "$(BOLD)>>> Stopping any rules-engine containers$(RST)"
	@if [ -f /mnt/c/PROJECTS/ecc-rules-engine/deploy/docker-compose.yml ]; then \
	  cd /mnt/c/PROJECTS/ecc-rules-engine/deploy && docker compose down 2>&1 | tail -5; \
	fi
	@echo "$(BOLD)>>> Killing rules-engine python processes$(RST)"
	@pkill -f 'ecc_rules_engine' 2>/dev/null || true
	@pkill -f 'uvicorn.*8080' 2>/dev/null || true
	@echo "  done"

# =============================================================================
# CHECKPOINT / ROLLBACK — durable safety net for the demo.
# Discipline (operator agreement, 2026-04-30):
#   * After every successful module completion, run `make checkpoint`.
#   * Even multiple times a day. Cheap. Saves hours of recovery.
#   * Each checkpoint creates an annotated tag `known-good-YYYY-MM-DD-NN`
#     and pushes it to GitHub so the work is durable even if the laptop dies.
#   * If the project is in total-failure state, `make rollback-to-last-good`
#     hard-resets working tree to the latest checkpoint tag.
# =============================================================================

.PHONY: checkpoint
checkpoint: ## Commit + tag known-good + push (after a verified-working state)
	@if [ -z "$(MSG)" ]; then \
	  echo "$(BOLD)Usage: make checkpoint MSG='short description of what works'$(RST)"; \
	  echo "  e.g. make checkpoint MSG='archived Aetna policy v1; pipeline cascade verified'"; \
	  exit 1; \
	fi
	@TODAY=$$(date -u +%Y-%m-%d); \
	NEXT_NUM=$$(git tag -l "known-good-$$TODAY-*" | sed "s|known-good-$$TODAY-||" | sort -n | tail -1); \
	if [ -z "$$NEXT_NUM" ]; then NEXT_NUM=01; else NEXT_NUM=$$(printf '%02d' $$((10#$$NEXT_NUM + 1))); fi; \
	TAG="known-good-$$TODAY-$$NEXT_NUM"; \
	echo "$(BOLD)>>> Step 1/4 — Stage all changes$(RST)"; \
	git add -A; \
	echo; \
	echo "$(BOLD)>>> Step 2/4 — Commit$(RST)"; \
	if git diff --cached --quiet; then \
	  echo "  (no changes to commit; tagging current HEAD as $$TAG)"; \
	else \
	  git commit -m "checkpoint: $(MSG)" -m "Tagged as $$TAG. Verified working state — safe rollback point." -m "" -m "Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>" || exit 1; \
	fi; \
	echo; \
	echo "$(BOLD)>>> Step 3/4 — Tag as $$TAG$(RST)"; \
	git tag -a "$$TAG" -m "$(MSG)"; \
	echo "  tagged HEAD as $$TAG"; \
	echo; \
	echo "$(BOLD)>>> Step 4/4 — Push branch + tag to GitHub$(RST)"; \
	git push origin HEAD; \
	git push origin "$$TAG"; \
	echo; \
	echo "$(BOLD)============================================================$(RST)"; \
	echo "$(BOLD)  ✓ CHECKPOINT SAVED: $$TAG$(RST)"; \
	echo "$(BOLD)============================================================$(RST)"; \
	echo "  To rollback to this point later:  make rollback-to-last-good"; \
	echo

.PHONY: rollback-to-last-good
rollback-to-last-good: ## EMERGENCY: hard-reset working tree to latest known-good tag
	@LAST_TAG=$$(git tag -l "known-good-*" | sort -V | tail -1); \
	if [ -z "$$LAST_TAG" ]; then \
	  echo "$(BOLD)ERROR: no known-good-* tags exist yet. Run `make checkpoint MSG=...` first.$(RST)"; \
	  exit 1; \
	fi; \
	CURRENT=$$(git rev-parse --short HEAD); \
	TARGET=$$(git rev-parse --short "$$LAST_TAG"); \
	echo "$(BOLD)============================================================$(RST)"; \
	echo "$(BOLD)  EMERGENCY ROLLBACK$(RST)"; \
	echo "$(BOLD)============================================================$(RST)"; \
	echo "  Current HEAD : $$CURRENT"; \
	echo "  Will reset to: $$TARGET ($$LAST_TAG)"; \
	echo "  Tag message:"; \
	git tag -l --format='    %(contents:subject)' "$$LAST_TAG"; \
	echo; \
	echo "$(BOLD)This will DISCARD any uncommitted changes.$(RST)"; \
	echo "  Press Ctrl-C in the next 5 seconds to abort..."; \
	sleep 5; \
	echo; \
	echo "$(BOLD)>>> Stashing any in-flight work (recoverable via 'git stash list')$(RST)"; \
	git stash push -u -m "auto-stash-before-rollback-$$(date -u +%Y%m%dT%H%M%SZ)" 2>&1 | tail -3 || true; \
	echo; \
	echo "$(BOLD)>>> Hard reset to $$LAST_TAG$(RST)"; \
	git reset --hard "$$LAST_TAG"; \
	echo; \
	echo "$(BOLD)============================================================$(RST)"; \
	echo "$(BOLD)  ✓ ROLLED BACK TO: $$LAST_TAG$(RST)"; \
	echo "$(BOLD)============================================================$(RST)"; \
	echo "  Working tree now matches the last known-good checkpoint."; \
	echo "  Stashed changes (if any): git stash list"; \
	echo "  Next step: bring stack up:  make recover"; \
	echo

.PHONY: list-checkpoints
list-checkpoints: ## Show all known-good checkpoint tags newest first
	@echo
	@echo "$(BOLD)============================================================$(RST)"
	@echo "$(BOLD)  Known-good checkpoints (newest first)$(RST)"
	@echo "$(BOLD)============================================================$(RST)"
	@git tag -l "known-good-*" --sort=-creatordate --format='%(refname:short)  %(creatordate:short)  %(contents:subject)' | head -20
	@echo
	@echo "$(DIM)Rollback to most recent: make rollback-to-last-good$(RST)"
	@echo "$(DIM)Rollback to specific:    git reset --hard <tag-name>$(RST)"
	@echo
