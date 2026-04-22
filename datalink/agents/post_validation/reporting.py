"""Reporting Agent — routes the Root Cause + Remediation output.

Distributes via:
  - the Notifier adapter (FileNotifier writes JSONL locally; prod swaps in
    TeamsNotifier / SmtpNotifier via config)
  - optional POST to the webhook stub (http://127.0.0.1:9000/notify) — the
    local Teams webhook replacement.
"""

from __future__ import annotations

from typing import Any

from datalink.agents.base import AgentBase


class ReportingAgent(AgentBase):
    role = (
        "Ops Reporting Agent for EvokeConnectCare™ data incidents. "
        "Formats findings for Teams + email."
    )
    goal = (
        "Combine Root Cause + Remediation outputs into a concise incident "
        "report and dispatch it via the configured notifier."
    )
    crew_name = "post_validation"

    def __init__(self, llm, warehouse, notifier, thinking_mode: str = "off") -> None:  # type: ignore[no-untyped-def]
        super().__init__(llm, warehouse, thinking_mode=thinking_mode)
        self._notifier = notifier

    def execute(self, context: dict[str, Any]) -> dict[str, Any]:
        rca = context.get("RootCauseAgent_output") or {}
        remediation = context.get("RemediationAgent_output") or {}

        safe_payload: dict[str, Any] = {
            "checkpoint_name": rca.get("checkpoint_name"),
            "run_id": rca.get("run_id"),
            "failure_summary": f"{rca.get('failure_count', 0)} failed expectations",
            "expectations": rca.get("classified_failures", [])[:10],
            "instruction": "format summary",
        }
        llm_summary = self._ask_llm(
            instruction=(
                "Write a 2-paragraph incident summary: first paragraph is the "
                "failure + classification; second paragraph is the recommended "
                "remediation and approval path. Use plain text, no markdown."
            ),
            safe_payload=safe_payload,
            max_tokens=512,
        )

        severity = remediation.get("priority", "MEDIUM")
        title = (
            f"[{severity}] GX checkpoint {rca.get('checkpoint_name', 'unknown')} — "
            f"{rca.get('failure_count', 0)} failed expectations"
        )
        body = llm_summary + "\n\nPlaybook: " + str(remediation.get("playbook_id"))

        self._notifier.notify(
            channel="ops",
            severity=severity.lower(),
            title=title,
            body=body,
            metadata={
                "checkpoint_name": rca.get("checkpoint_name"),
                "run_id": rca.get("run_id"),
                "playbook_id": remediation.get("playbook_id"),
                "recommendation": remediation.get("recommendation"),
            },
        )

        return {
            "sent": True,
            "title": title,
            "severity": severity,
            "body_preview": body[:300],
        }
