"""Base classes for the agent layer — mandatory PHI boundary + audit log.

Every agent:
  1. Declares a role + goal (human-readable, used in LLM prompts).
  2. Accepts a PHI-safe input dict and produces a JSON output dict.
  3. Records invocation to CONTROL.agent_reasoning_log BEFORE and AFTER the call.
  4. Routes every LLM call through PhiRedactionLayer.assert_clean.

A Crew composes multiple agents into a sequential pipeline. Output of agent N
is fed (possibly filtered) as input to agent N+1. Crews are triggered by the
pipeline runtime at defined points (before/after GX checkpoints).
"""

from __future__ import annotations

import json
import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from datalink.adapters.protocols import LlmProvider, Warehouse
from datalink.logging import get_logger
from datalink.phi import PhiRedactionLayer
from datalink.quality.control import CONTROL_SCHEMA

_log = get_logger(__name__)


@dataclass
class AgentResult:
    """Structured agent output. `payload` is JSON-serializable."""

    agent_name: str
    success: bool
    payload: dict[str, Any]
    tokens_used: int = 0
    duration_ms: int = 0
    error: str | None = None


class AgentBase(ABC):
    """A single agent. Subclass must implement `execute(context)`."""

    role: str = ""
    goal: str = ""
    crew_name: str = ""

    def __init__(self, llm: LlmProvider, warehouse: Warehouse) -> None:
        self._llm = llm
        self._wh = warehouse
        self._phi = PhiRedactionLayer()

    # --- subclass contract -------------------------------------------------

    @abstractmethod
    def execute(self, context: dict[str, Any]) -> dict[str, Any]:
        """Run the agent. Return a JSON-safe dict. Subclass may call self._ask_llm
        with a PHI-safe payload."""
        ...

    # --- framework -------------------------------------------------------

    def run(self, context: dict[str, Any]) -> AgentResult:
        invocation_id = str(uuid.uuid4())
        started = time.time()
        payload: dict[str, Any] = {}
        success = False
        error: str | None = None
        # Reset per-invocation token counter. _ask_llm accumulates into this
        # so a single agent call that makes multiple LLM requests aggregates
        # its full spend.
        self._tokens_this_run = 0
        try:
            payload = self.execute(context)
            success = True
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
            _log.error(
                "agent.error",
                agent=self.__class__.__name__,
                crew=self.crew_name,
                error=error,
            )
        duration_ms = int((time.time() - started) * 1000)
        self._audit(
            invocation_id=invocation_id,
            context=context,
            payload=payload,
            duration_ms=duration_ms,
            tokens_used=self._tokens_this_run,
            success=success,
            error=error,
        )
        return AgentResult(
            agent_name=self.__class__.__name__,
            success=success,
            payload=payload,
            tokens_used=self._tokens_this_run,
            duration_ms=duration_ms,
            error=error,
        )

    # --- PHI-guarded LLM call --------------------------------------------

    def _ask_llm(
        self, instruction: str, safe_payload: dict[str, Any], max_tokens: int = 1024
    ) -> str:
        """Send `instruction` + `safe_payload` to the LLM. Raises
        PhiBoundaryViolationError if the payload tries to smuggle PHI.

        Phase 6: accumulates input_tokens + output_tokens on self._tokens_this_run
        so AgentBase.run() can persist the total to agent_reasoning_log.tokens_used.
        """
        self._phi.assert_clean(safe_payload)
        from datalink.adapters.protocols import LlmMessage

        payload_json = json.dumps(safe_payload, default=str, ensure_ascii=False)
        messages = [
            LlmMessage(role="system", content=f"Role: {self.role}\nGoal: {self.goal}"),
            LlmMessage(
                role="user",
                content=f"{instruction}\n\nContext (safe metadata only):\n{payload_json}",
            ),
        ]
        completion = self._llm.complete(messages, max_tokens=max_tokens)
        # Accumulate tokens — multiple _ask_llm calls in one execute() sum up.
        self._tokens_this_run += int(completion.input_tokens) + int(completion.output_tokens)
        return completion.content

    # --- audit log --------------------------------------------------------

    def _audit(
        self,
        *,
        invocation_id: str,
        context: dict[str, Any],
        payload: dict[str, Any],
        duration_ms: int,
        tokens_used: int,
        success: bool,
        error: str | None,
    ) -> None:
        # PHI-safe preview — key names + values. Truncation widened to 3KB
        # in Phase 6 so the CrewAI Dashboard can render human-readable
        # narrative (full action-list, full classification, full suite_name)
        # instead of cut-off strings. Still metadata-only — no row data.
        preview = json.dumps(
            {k: str(v)[:600] for k, v in (payload or {}).items() if k != "_phi"},
            default=str,
        )[:3000]
        input_preview = json.dumps(list((context or {}).keys()))[:400]
        try:
            self._wh.execute(
                f"INSERT INTO {CONTROL_SCHEMA}.agent_reasoning_log "
                "(invocation_id, agent_name, crew_name, ts, input_preview, output_preview, "
                " tokens_used, duration_ms, phi_check) "
                "VALUES ($i, $a, $c, $t, $ip, $op, $tk, $d, $p)",
                {
                    "i": invocation_id,
                    "a": self.__class__.__name__,
                    "c": self.crew_name,
                    "t": datetime.now(UTC),
                    "ip": input_preview,
                    "op": preview,
                    # Phase 6: real token count propagated from _ask_llm calls.
                    # StubLlm: char/4 estimate. AnthropicLlm: real usage.
                    "tk": int(tokens_used or 0),
                    "d": duration_ms,
                    "p": "clean" if success else f"error: {error}",
                },
            )
        except Exception:
            _log.warning("agent.audit_log_failed", agent=self.__class__.__name__)


@dataclass
class CrewBase:
    """Sequential crew orchestrator. Passes each agent's output as context to next."""

    name: str
    agents: list[AgentBase]
    initial_context_key: str = "input"

    def kickoff(self, inputs: dict[str, Any]) -> list[AgentResult]:
        context = dict(inputs)
        results: list[AgentResult] = []
        for agent in self.agents:
            r = agent.run(context)
            results.append(r)
            # Feed successful payload into next agent's context.
            if r.success:
                context[f"{agent.__class__.__name__}_output"] = r.payload
            else:
                _log.warning(
                    "crew.stop",
                    crew=self.name,
                    failed_agent=agent.__class__.__name__,
                    error=r.error,
                )
                break
        return results
