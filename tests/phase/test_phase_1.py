"""Phase 1 exit criteria — executed by `make verify-phase-1`.

All assertions in this file must pass before Phase 2 can start. These tests
do NOT require docker-compose services to be running — they are pure Python
structural + configuration checks. The service-liveness checks run in
`make verify-phase-1-services` (see Makefile).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from datalink.adapters.factory import build_adapters
from datalink.config.loader import load_settings

REPO_ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.phase
def test_repo_skeleton_exists() -> None:
    required = [
        "datalink/__init__.py",
        "datalink/config/loader.py",
        "datalink/adapters/factory.py",
        "datalink/phi/guard.py",
        "config/base.yaml",
        "config/environments/local.yaml",
        "config/environments/dev.yaml",
        "config/environments/stage.yaml",
        "config/environments/prod.yaml",
        "config/features/gx.yaml",
        "config/features/agents.yaml",
        "config/features/warehouse_router.yaml",
        "docker-compose.yml",
        "Makefile",
        "pyproject.toml",
        ".env.example",
        ".gitignore",
        ".gitattributes",
        "CLAUDE.md",
        "docs/architecture.md",
        "README.md",
    ]
    missing = [p for p in required if not (REPO_ROOT / p).exists()]
    assert not missing, f"Phase 1 repo skeleton incomplete: missing {missing}"


@pytest.mark.phase
def test_all_environment_configs_load() -> None:
    """Every environment must parse cleanly — catches typos in prod config
    before deploy, not after."""
    for env in ["local", "dev", "stage", "prod"]:
        settings = load_settings(env=env)
        assert settings.env.value == env


@pytest.mark.phase
def test_local_adapters_construct_without_network() -> None:
    settings = load_settings(env="local")
    adapters = build_adapters(settings)
    assert set(adapters.operational_dbs.keys()) == {"sqlserver", "postgres"}


@pytest.mark.phase
def test_prod_adapters_construct_without_credentials() -> None:
    """Prod adapters must be pure construction — no credential lookups at
    build time. Caller opens connections lazily."""
    settings = load_settings(env="prod")
    adapters = build_adapters(settings)
    assert adapters is not None
