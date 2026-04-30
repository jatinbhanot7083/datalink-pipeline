"""LLM router — pick stub or real Anthropic per settings.

Default behaviour (production):

    DL_ADAPTERS__LLM__TYPE=anthropic + ANTHROPIC_API_KEY set     →  real Claude
    DL_ADAPTERS__LLM__TYPE=anthropic + no API key                →  RuntimeError (NO silent stub)
    DL_ADAPTERS__LLM__TYPE=stub                                  →  StubLlm (explicit opt-in)
    unset                                                        →  StubLlm (Phase 0 default)

The legacy "anthropic configured + no key → stub fallback" behaviour was
removed in Phase 13.5 cleanup at Jatin's explicit request: real-business
runs MUST hit real Claude, no silent downgrade. Set
``DL_ADAPTERS__LLM__ALLOW_STUB_FALLBACK=true`` to opt back into the old
behaviour for offline demos, but never in production.

The stub is still a legitimate provider when type=stub is requested
explicitly — used in CI, the runbook verifiers, and offline laptops.
"""

from __future__ import annotations

import os

from datalink.adapters.llm.stub import StubLlm
from datalink.adapters.protocols import LlmProvider
from datalink.config.loader import Settings
from datalink.logging import get_logger

_log = get_logger(__name__)


def get_llm(settings: Settings) -> LlmProvider:
    """Return the appropriate LlmProvider for the current settings.

    Raises ``RuntimeError`` when ``cfg.type == "anthropic"`` but
    ``ANTHROPIC_API_KEY`` is missing, unless
    ``DL_ADAPTERS__LLM__ALLOW_STUB_FALLBACK=true`` is set explicitly.
    """
    cfg = settings.adapters.llm
    if cfg.type == "anthropic":
        if os.environ.get("ANTHROPIC_API_KEY"):
            # Defer import so stub-only setups don't need the anthropic lib.
            from datalink.adapters.llm.anthropic import AnthropicLlm

            _log.info("llm_router.selected", provider="anthropic", model=cfg.model)
            return AnthropicLlm(cfg)

        # Production path: explicit error rather than silent stub.
        if os.environ.get("DL_ADAPTERS__LLM__ALLOW_STUB_FALLBACK", "false").lower() != "true":
            raise RuntimeError(
                "DL_ADAPTERS__LLM__TYPE=anthropic but ANTHROPIC_API_KEY is not set. "
                "Real-business calls require a real API key. "
                "To explicitly allow the stub fallback (for offline demos / CI), "
                "set DL_ADAPTERS__LLM__ALLOW_STUB_FALLBACK=true. "
                "To use the stub directly, set DL_ADAPTERS__LLM__TYPE=stub."
            )

        # Explicit opt-in to legacy behaviour — log loudly.
        _log.warning(
            "llm_router.stub_fallback_allowed",
            note=(
                "ANTHROPIC_API_KEY missing but DL_ADAPTERS__LLM__ALLOW_STUB_FALLBACK=true. "
                "Falling back to StubLlm. Agents will return canned responses."
            ),
        )

    _log.info("llm_router.selected", provider="stub", model=cfg.model)
    return StubLlm(cfg)
