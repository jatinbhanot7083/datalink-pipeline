"""Remediation Agent — suggests a structured fix. NEVER auto-executes.

Produces a playbook-style recommendation per GX base doc §4.4.2:
  - What failed
  - Why it failed (hypothesis)
  - Recommended corrective action (DBA review required)
  - Estimated MTTR
  - Risk-adjusted recommendation (PROCEED / FIX_AND_RESUME / PARTIAL_LOAD /
    ABORT_AND_INVESTIGATE)
"""

from __future__ import annotations

from typing import Any, ClassVar

from datalink.agents.base import AgentBase


class RemediationAgent(AgentBase):
    role = (
        "Remediation Planner for EvokeConnectCare™ data pipelines. "
        "Suggests fixes. NEVER executes anything."
    )
    goal = (
        "Take the Root Cause analysis and produce a structured remediation "
        "playbook. Output MUST include the string 'DBA_APPROVAL_REQUIRED' — "
        "proof to the human operator that nothing was auto-applied."
    )
    crew_name = "post_validation"

    _PLAYBOOKS: ClassVar[dict[str, dict[str, Any]]] = {
        "SCHEMA_CHANGE": {
            "playbook_id": "PB-SCHEMA-CHANGE",
            "priority": "CRITICAL",
            "actions": [
                "ABORT the current batch — do not let the schema drift propagate.",
                "Quarantine the source file in CONTROL.batch_quarantine.",
                "Notify the data engineering team to update the Bronze DDL.",
                "Once DDL is updated, resume from the last successful checkpoint.",
            ],
            "estimated_hours": 2,
            "recommendation": "ABORT_AND_INVESTIGATE",
        },
        "DATA_QUALITY": {
            "playbook_id": "PB-DATA-QUALITY",
            "priority": "HIGH",
            "actions": [
                "Split the batch: load the records that pass, quarantine those that fail.",
                "Escalate the failing records to the source system owner for correction.",
                "If fail rate drops below 1% after partial load, mark the pipeline RESUMING.",
            ],
            "estimated_hours": 1,
            "recommendation": "PARTIAL_LOAD",
        },
        "CONFIG_ERROR": {
            "playbook_id": "PB-CONFIG-ERROR",
            "priority": "MEDIUM",
            "actions": [
                "Review the GX expectation threshold — may be too tight vs observed distribution.",
                "If threshold adjustment is warranted, submit via the Registry Manager (not yet in code).",
                "Resume from the last successful checkpoint once threshold is updated.",
            ],
            "estimated_hours": 0.5,
            "recommendation": "FIX_AND_RESUME",
        },
        "VOLUME_ANOMALY": {
            "playbook_id": "PB-VOLUME-ANOMALY",
            "priority": "MEDIUM",
            "actions": [
                "Verify source file completeness with upstream system.",
                "If partial file, await the full arrival and retry.",
                "If file is complete but volume is legitimately lower, override the threshold.",
            ],
            "estimated_hours": 0.5,
            "recommendation": "FIX_AND_RESUME",
        },
    }

    def execute(self, context: dict[str, Any]) -> dict[str, Any]:
        rca = context.get("RootCauseAgent_output") or {}
        classified = rca.get("classified_failures", [])
        if not classified:
            return {
                "playbook_id": None,
                "message": "no failures to remediate",
                "DBA_APPROVAL_REQUIRED": False,
            }

        # Pick the top classification (by frequency) to drive the playbook.
        counts: dict[str, int] = {}
        for c in classified:
            k = c["classification"]
            counts[k] = counts.get(k, 0) + 1
        primary = max(counts.items(), key=lambda kv: kv[1])[0]
        playbook = self._PLAYBOOKS.get(primary, self._PLAYBOOKS["DATA_QUALITY"])

        # PHI-safe payload for the LLM.
        safe_payload: dict[str, Any] = {
            "checkpoint_name": rca.get("checkpoint_name"),
            "failure_summary": f"{len(classified)} failed expectations (primary={primary})",
            "expectations": [
                {k: v for k, v in c.items() if k not in {"value", "raw_sample"}} for c in classified
            ],
            "instruction": "summarise suggested action",
        }
        narrative = self._ask_llm(
            instruction=(
                "Draft a concise remediation note (≤ 4 sentences) for the Ops team. "
                "End with the literal string 'DBA_APPROVAL_REQUIRED'."
            ),
            safe_payload=safe_payload,
            max_tokens=384,
        )

        return {
            "primary_classification": primary,
            "playbook_id": playbook["playbook_id"],
            "priority": playbook["priority"],
            "recommended_actions": playbook["actions"],
            "estimated_hours": playbook["estimated_hours"],
            "recommendation": playbook["recommendation"],
            "DBA_APPROVAL_REQUIRED": True,
            "narrative": narrative,
        }
