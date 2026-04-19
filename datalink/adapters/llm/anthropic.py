"""AnthropicLlm — real Anthropic Claude API.

Lands in Phase 5. API key read from ANTHROPIC_API_KEY env var via SecretProvider.
"""

from __future__ import annotations

from datalink.adapters.protocols import LlmCompletion, LlmMessage
from datalink.config.models import LlmConfig


class AnthropicLlm:
    def __init__(self, cfg: LlmConfig) -> None:
        self.model = cfg.model
        self._cfg = cfg

    def complete(
        self,
        messages: list[LlmMessage],
        max_tokens: int,
        temperature: float = 0.0,
    ) -> LlmCompletion:
        raise NotImplementedError(
            "AnthropicLlm lands in Phase 5 (CrewAI integration). "
            "For offline demos use DL_ADAPTERS__LLM__TYPE=stub."
        )
