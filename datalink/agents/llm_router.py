"""LLM router — pick stub or real Anthropic per settings.

If settings.adapters.llm.type == "anthropic" AND ANTHROPIC_API_KEY is set,
return a real Anthropic LlmProvider. Otherwise return the StubLlm — works
offline, returns deterministic canned responses for the demo.

The stub is NOT a mock-only construct; it is a real, production-quality
adapter used whenever agents should be available but network / API key
isn't (CI, airplane demos, teammate onboarding).
"""

from __future__ import annotations

import os

from datalink.adapters.llm.stub import StubLlm
from datalink.adapters.protocols import LlmProvider
from datalink.config.loader import Settings
from datalink.logging import get_logger

_log = get_logger(__name__)


def get_llm(settings: Settings) -> LlmProvider:
    """Return the appropriate LlmProvider for the current settings."""
    cfg = settings.adapters.llm
    if cfg.type == "anthropic" and os.environ.get("ANTHROPIC_API_KEY"):
        # Defer import so stub-only setups don't need the anthropic lib at import time.
        from datalink.adapters.llm.anthropic import AnthropicLlm

        _log.info("llm_router.selected", provider="anthropic", model=cfg.model)
        return AnthropicLlm(cfg)
    # Fallback / default.
    if cfg.type == "anthropic":
        _log.warning(
            "llm_router.no_api_key",
            note="ANTHROPIC_API_KEY not set, using StubLlm. Agents will return canned responses.",
        )
    _log.info("llm_router.selected", provider="stub", model=cfg.model)
    return StubLlm(cfg)
