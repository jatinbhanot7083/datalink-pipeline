"""StubLlm — canned responses for offline demos and tests.

Purpose: let a teammate clone the repo on a laptop with no API key and still
have agent-driven paths produce deterministic outputs. The canned outputs
match the shape the real Anthropic adapter would produce.

Every call still goes through PhiRedactionLayer.assert_clean — stubbing the
LLM does NOT exempt callers from the PHI boundary contract.
"""

from __future__ import annotations

from datalink.adapters.protocols import LlmCompletion, LlmMessage
from datalink.config.models import LlmConfig


class StubLlm:
    def __init__(self, cfg: LlmConfig) -> None:
        self.model = cfg.model
        self._cfg = cfg

    def complete(
        self,
        messages: list[LlmMessage],
        max_tokens: int,
        temperature: float = 0.0,
        thinking_mode: str = "off",  # Phase 6: ignored by stub
    ) -> LlmCompletion:
        # Deterministic canned response shaped like a profile/expectation output.
        # Agent code that wants richer stub output can parse the last user message
        # to route to a specific canned response in Phase 5.
        last_user = next(
            (m.content for m in reversed(messages) if m.role == "user"),
            "",
        )
        canned = (
            '{"status":"ok","stubbed":true,"echo_len":'
            + str(len(last_user))
            + ',"note":"stub LLM — set DL_ADAPTERS__LLM__TYPE=anthropic for real completions"}'
        )
        return LlmCompletion(
            content=canned,
            input_tokens=sum(len(m.content) for m in messages) // 4,
            output_tokens=len(canned) // 4,
            model=self.model,
            stop_reason="stop_sequence",
        )
