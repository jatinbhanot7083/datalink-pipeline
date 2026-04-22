"""AnthropicLlm — real Anthropic Claude API adapter.

Phase 5/6. Reads ANTHROPIC_API_KEY from env. Honours model + max_tokens from
LlmConfig. Returns real `input_tokens` + `output_tokens` in the
`LlmCompletion` so the agent_reasoning_log and CrewAI Dashboard show
actual token spend + cost estimates.

Contract (per datalink.adapters.protocols.LlmProvider):
  * `complete(messages, max_tokens, temperature) -> LlmCompletion`
  * The FIRST `role="system"` message becomes Anthropic's system prompt;
    remaining messages are user / assistant turns. This matches the
    Anthropic 1.x SDK separation of system vs conversation.

Error handling:
  * Auth / rate-limit / invalid-request errors raise LlmProviderError
    with the wrapped anthropic exception. The agent framework records the
    failure in agent_reasoning_log so the CrewAI Dashboard surfaces it.
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
    ) -> LlmCompletion:
        # Split the first system message out per Anthropic 1.x convention.
        system_parts: list[str] = []
        convo: list[dict[str, str]] = []
        for m in messages:
            if m.role == "system":
                system_parts.append(m.content)
            else:
                convo.append({"role": m.role, "content": m.content})
        system_prompt = "\n\n".join(system_parts) if system_parts else None

        try:
            client = self._get_client()
            resp = client.messages.create(
                model=self.model,
                max_tokens=max_tokens,
                temperature=temperature,
                system=system_prompt,
                messages=convo,
            )
        except LlmProviderError:
            raise
        except Exception as exc:
            raise LlmProviderError(f"Anthropic API call failed: {exc}") from exc

        # Anthropic returns a list of content blocks; we want the TextBlock.
        content_parts: list[str] = []
        for block in getattr(resp, "content", []):
            text = getattr(block, "text", None)
            if text:
                content_parts.append(text)
        content = "\n".join(content_parts)

        usage = getattr(resp, "usage", None)
        in_tok = int(getattr(usage, "input_tokens", 0) or 0)
        out_tok = int(getattr(usage, "output_tokens", 0) or 0)

        _log.info(
            "llm.anthropic.complete",
            model=self.model,
            input_tokens=in_tok,
            output_tokens=out_tok,
            stop_reason=str(getattr(resp, "stop_reason", "")),
        )
        return LlmCompletion(
            content=content,
            input_tokens=in_tok,
            output_tokens=out_tok,
            model=self.model,
            stop_reason=str(getattr(resp, "stop_reason", "end_turn")),
        )
