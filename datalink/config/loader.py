"""Config loader: layers YAML files and env vars into a validated Settings object.

Layering order (later overrides earlier):
  1. config/base.yaml
  2. config/environments/{env}.yaml
  3. config/features/*.yaml
  4. DL_-prefixed environment variables (use __ for nesting)

The env name is selected by DL_ENV (defaults to 'local').
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from copy import deepcopy
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field

from datalink.config.models import (
    AdapterConfig,
    EnvName,
    FeatureConfig,
    LoggingConfig,
    OrchestratorName,
    ProcessingConfig,
    SourcesConfig,
    TenancyConfig,
)


class Settings(BaseModel):
    """Top-level validated config — the one object the rest of the app reads from."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    env: EnvName = EnvName.LOCAL
    orchestrator: OrchestratorName = OrchestratorName.LOCAL_SEQUENTIAL
    project_name: str = "datalink-pipeline"
    processing: ProcessingConfig = Field(default_factory=ProcessingConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    adapters: AdapterConfig = Field(default_factory=AdapterConfig)
    features: FeatureConfig = Field(default_factory=FeatureConfig)
    tenancy: TenancyConfig = Field(default_factory=TenancyConfig)
    sources: SourcesConfig = Field(default_factory=SourcesConfig)


# ----------------------------------------------------------------------------
# Internal helpers
# ----------------------------------------------------------------------------


def _deep_merge(base: dict[str, Any], override: Mapping[str, Any]) -> dict[str, Any]:
    """Recursively merge `override` into `base`. `override` wins on conflicts."""
    result = deepcopy(base)
    for key, val in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(val, Mapping):
            result[key] = _deep_merge(result[key], val)
        else:
            result[key] = deepcopy(val) if not isinstance(val, Mapping) else dict(val)
    return result


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Config file {path} must contain a mapping at the top level")
    return data


def _coerce(value: str) -> Any:
    """Best-effort string→scalar coercion for env-var overrides."""
    lower = value.lower()
    if lower in ("true", "yes", "1"):
        return True
    if lower in ("false", "no", "0"):
        return False
    if "," in value:
        return [_coerce(v.strip()) for v in value.split(",") if v.strip()]
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        pass
    return value


def _env_overrides(prefix: str = "DL_") -> dict[str, Any]:
    """Translate DL_ADAPTERS__WAREHOUSE__PATH=... into {'adapters': {'warehouse': {'path': ...}}}."""
    result: dict[str, Any] = {}
    for key, raw in os.environ.items():
        if not key.startswith(prefix):
            continue
        path = key[len(prefix) :].lower().split("__")
        if not path or not path[0]:
            continue
        cursor = result
        for part in path[:-1]:
            # If a shallower DL_FOO=scalar was set first, setdefault returns
            # that scalar and the next iteration will raise — letting config
            # fail loudly at load time is the right behavior (better than
            # silently dropping the conflicting var).
            cursor = cursor.setdefault(part, {})
        cursor[path[-1]] = _coerce(raw)
    return result


# ----------------------------------------------------------------------------
# Public API
# ----------------------------------------------------------------------------


def load_settings(
    config_root: Path | str | None = None,
    env: str | None = None,
) -> Settings:
    """Load and validate Settings from config files + env vars.

    Args:
        config_root: Path to the `config/` directory. Defaults to repo-root `config/`.
        env: Environment name (local/dev/stage/prod). Defaults to DL_ENV or 'local'.

    Raises:
        ValidationError: if config files or env vars produce invalid shapes.
        FileNotFoundError: if the resolved environment file is missing.
    """
    root = Path(config_root) if config_root else _default_config_root()
    env_name = env or os.environ.get("DL_ENV", "local")

    merged: dict[str, Any] = {}
    # 1. base.yaml
    merged = _deep_merge(merged, _read_yaml(root / "base.yaml"))
    # 2. environments/{env}.yaml
    env_file = root / "environments" / f"{env_name}.yaml"
    if not env_file.exists():
        raise FileNotFoundError(f"Environment config not found: {env_file}")
    merged = _deep_merge(merged, _read_yaml(env_file))
    # 3. features/*.yaml (sorted for determinism)
    features_dir = root / "features"
    if features_dir.exists():
        for f in sorted(features_dir.glob("*.yaml")):
            merged = _deep_merge(merged, _read_yaml(f))
    # 4. env-var overrides
    merged = _deep_merge(merged, _env_overrides())

    return Settings(**merged)


def _default_config_root() -> Path:
    """Locate the `config/` dir relative to the repo root."""
    # datalink/config/loader.py → parents[2] == repo root
    return Path(__file__).resolve().parents[2] / "config"
