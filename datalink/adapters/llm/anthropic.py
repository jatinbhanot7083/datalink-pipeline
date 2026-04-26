"""AnthropicLlm — real Anthropic Claude API adapter.

Phase 5/6. Reads ANTHROPIC_API_KEY from env. Honours model + max_tokens from
LlmConfig. Returns real `input_tokens` + `output_tokens` in the
`LlmCompletion` so the agent_reasoning_log and AI Agents dashboard show
actual token spend + cost estimates.

Contract (per datalink.adapters.protocols.LlmProvider):
  * `complete(messages, max_tokens, temperature) -> LlmCompletion`
  * The FIRST `role="system"` message becomes Anthropic's system prompt;
    remaining messages are user / assistant turns. This matches the
    Anthropic 1.x SDK separation of system vs conversation.

Error handling:
  * Auth / rate-limit / invalid-request errors raise LlmProviderError
    with the wrapped anthropic exception. The agent framework records the
    failure in agent_reasoning_log so the AI Agents dashboard surfaces it.
"""

from __future__ import annotations

import os
from typing import Any

from datalink.adapters.protocols import LlmCompletion, LlmMessage
from datalink.config.models import LlmConfig
from datalink.logging import get_logger

_log = get_logger(__name__)


class LlmProviderError(RuntimeError):
    """Wraps errors from the underlying LLM SDK so agent code catches one type."""


class AnthropicLlm:
    def __init__(self, cfg: LlmConfig) -> None:
        self.model = cfg.model
        self._cfg = cfg
        # Deferred client construction so import-time is cheap + a missing
        # key doesn't crash module loading (router decides before constructing).
        self._client: Any = None

    def _get_client(self) -> Any:
        if self._client is not None:
            return self._client
        try:
            from anthropic import Anthropic
        except ImportError as exc:
            raise LlmProviderError(
                "anthropic package not installed. Install via "
                "`uv sync --extra agents` or pip install anthropic>=0.40"
            ) from exc
        api_key = self._cfg.api_key or os.environ.get("ANTHROPIC_API_KEY")
        if not api_key:
            raise LlmProviderError(
                "ANTHROPIC_API_KEY env var not set. "
                "Export it OR set adapters.llm.api_key in config."
            )
        self._client = Anthropic(api_key=api_key)
        return self._client

    def complete(
        self,
        messages: list[LlmMessage],
        max_tokens: int,
        temperature: float = 0.0,
        thinking_mode: str = "off",
    ) -> LlmCompletion:
        # Split the first system message out per Anthropic 1.x convention.
        # The current API requires `system` as a LIST of content blocks
        # (plain string form rejected on newer Opus/Sonnet endpoints).
        # Also enables prompt caching later via cache_control on the block.
        system_parts: list[str] = []
        convo: list[dict[str, str]] = []
        for m in messages:
            if m.role == "system":
                system_parts.append(m.content)
            else:
                convo.append({"role": m.role, "content": m.content})
        system_prompt: list[dict[str, Any]] | None = None
        if system_parts:
            system_prompt = [{"type": "text", "text": "\n\n".join(system_parts)}]

        # Extended-thinking config. "adaptive" lets Claude decide the budget;
        # "enabled" forces thinking with a fixed budget; "off" skips it.
        # Anthropic requires max_tokens > thinking_budget, so we scale if needed.
        create_kwargs: dict[str, Any] = {
            "model": self.model,
            "max_tokens": max_tokens,
            "messages": convo,
        }
        # Only include `system` when we actually have one — the API rejects
        # null / empty-string for that field.
        if system_prompt is not None:
            create_kwargs["system"] = system_prompt
        # When thinking is on, Claude REQUIRES temperature=1. Honor that contract.
        if thinking_mode == "adaptive":
            create_kwargs["thinking"] = {"type": "adaptive"}
            create_kwargs["temperature"] = 1.0
            # Boost max_tokens to accommodate thinking + final answer.
            create_kwargs["max_tokens"] = max(max_tokens, 16_000)
        elif thinking_mode == "enabled":
            # Fixed-budget thinking — reserve 60% of max_tokens for thinking.
            budget = max(1024, int(max_tokens * 0.6))
            create_kwargs["thinking"] = {"type": "enabled", "budget_tokens": budget}
            create_kwargs["temperature"] = 1.0
            create_kwargs["max_tokens"] = max(max_tokens, budget + 2048)
        else:
            create_kwargs["temperature"] = temperature

        try:
            client = self._get_client()
            resp = client.messages.create(**create_kwargs)
        except LlmProviderError:
            raise
        except Exception as exc:
            # If thinking mode isn't supported on this model version,
            # retry WITHOUT thinking so the agent still gets an answer.
            if thinking_mode != "off" and "thinking" in str(exc).lower():
                _log.warning(
                    "llm.anthropic.thinking_unsupported",
                    model=self.model,
                    error=str(exc)[:200],
                )
                create_kwargs.pop("thinking", None)
                create_kwargs["temperature"] = temperature
                create_kwargs["max_tokens"] = max_tokens
                try:
                    resp = client.messages.create(**create_kwargs)
                except Exception as exc2:
                    raise LlmProviderError(
                        f"Anthropic API call failed (with + without thinking): {exc2}"
                    ) from exc2
            else:
                raise LlmProviderError(f"Anthropic API call failed: {exc}") from exc

        # Anthropic returns a list of content blocks — separate thinking from text.
        content_parts: list[str] = []
        thinking_parts: list[str] = []
        for block in getattr(resp, "content", []):
            btype = getattr(block, "type", "")
            if btype == "thinking":
                thinking_parts.append(getattr(block, "thinking", "") or "")
            elif btype == "text" or hasattr(block, "text"):
                text = getattr(block, "text", None)
                if text:
                    content_parts.append(text)
        content = "\n".join(content_parts)
        thinking_content = "\n".join(thinking_parts)

        usage = getattr(resp, "usage", None)
        in_tok = int(getattr(usage, "input_tokens", 0) or 0)
        out_tok = int(getattr(usage, "output_tokens", 0) or 0)
        # Approximate thinking-token count from the thinking block length
        # (Anthropic's usage object splits this in newer SDKs; fall back
        # to char/4 heuristic otherwise).
        th_tok = int(
            getattr(usage, "thinking_tokens", 0)
            or getattr(usage, "cache_creation_input_tokens", 0)
            or (len(thinking_content) // 4 if thinking_content else 0)
        )

        _log.info(
            "llm.anthropic.complete",
            model=self.model,
            input_tokens=in_tok,
            output_tokens=out_tok,
            thinking_tokens=th_tok,
            thinking_mode=thinking_mode,
            stop_reason=str(getattr(resp, "stop_reason", "")),
        )
        return LlmCompletion(
            content=content,
            input_tokens=in_tok,
            output_tokens=out_tok,
            thinking_tokens=th_tok,
            thinking_content=thinking_content[:2000],  # truncate for audit storage
            model=self.model,
            stop_reason=str(getattr(resp, "stop_reason", "end_turn")),
        )
