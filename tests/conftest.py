"""Shared pytest fixtures."""

from __future__ import annotations

import os
from collections.abc import Iterator
from pathlib import Path

import pytest


@pytest.fixture()
def config_root() -> Path:
    """Path to the repo's config/ directory."""
    return Path(__file__).resolve().parents[1] / "config"


@pytest.fixture()
def clean_dl_env(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Strip every DL_-prefixed env var before the test so overrides don't leak in."""
    for key in list(os.environ.keys()):
        if key.startswith("DL_"):
            monkeypatch.delenv(key, raising=False)
    yield


@pytest.fixture()
def tmp_notifications_dir(tmp_path: Path) -> Path:
    """Writable directory for FileNotifier output in tests."""
    d = tmp_path / "logs"
    d.mkdir(parents=True, exist_ok=True)
    return d
