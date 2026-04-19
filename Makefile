# =============================================================================
# DataLink Local-First Medallion Pipeline — Makefile
# Run from WSL2 bash. `make help` lists targets.
# =============================================================================

SHELL := /bin/bash
.DEFAULT_GOAL := help
.ONESHELL:

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
up: ## Start docker-compose services (sftp, azurite, sqlserver, postgres, webhook-stub)
	docker compose up -d --wait
	@echo "$(BOLD)services up$(RST)"
	@docker compose ps

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
verify-phase-1-services: ## Phase 1 services — require docker compose up first
	@echo "--- phase-1 service health ---"
	@docker compose ps --format 'table {{.Service}}\t{{.Status}}'
	@for svc in sftp azurite sqlserver postgres webhook-stub; do \
	  status=$$(docker compose ps --format '{{.Service}}={{.Status}}' | grep "^$$svc=" | cut -d= -f2); \
	  echo "  $$svc: $$status"; \
	  case "$$status" in *healthy*|*Up*) : ;; *) echo "FAIL: $$svc not healthy"; exit 1 ;; esac; \
	done
	@echo "OK: all services healthy"

# =============================================================================
# Demo (Phase 2+ will flesh this out)
# =============================================================================

.PHONY: demo
demo: ## Run the full E2E demo (populated in Phase 2+)
	@echo "demo target lands in Phase 2 — Bronze ingestion"

.PHONY: demo-reset
demo-reset: ## Wipe warehouse + re-seed sample data (Phase 2+)
	@echo "demo-reset target lands in Phase 2"

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
