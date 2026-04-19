"""EnvSecrets — read secrets from process env vars.

.env loading is handled at process start by datalink.cli (or by direnv).
This adapter just wraps os.environ.
"""

from __future__ import annotations

import os

from datalink.config.models import SecretProviderConfig


class EnvSecrets:
    def __init__(self, cfg: SecretProviderConfig) -> None:
        self._cfg = cfg

    def get(self, name: str) -> str:
        value = os.environ.get(name)
        if value is None:
            raise KeyError(f"Secret '{name}' not set in environment")
        return value

    def get_optional(self, name: str) -> str | None:
        return os.environ.get(name)
