"""Reviewer Agent — flags expectations that need human business-rule sign-off.

Examines the draft suite from the Expectation Author and produces a prioritised
review queue. Nothing is auto-activated — a human must approve before the
suite becomes ACTIVE.
"""

from __future__ import annotations

from typing import Any

from datalink.agents.base import AgentBase


class ReviewerAgent(AgentBase):
    role = (
        "Data Quality Reviewer for EvokeConnectCare™ — responsible for gating "
        "new expectation suites before they reach production."
    )
    goal = (
        "Flag every expectation that requires human business-rule input "
        "(CPT ranges, ICD-10 groupings, plan-LOB rules). Output a JSON "
        "prioritised review queue with severity and recommended reviewer."
    )
    crew_name = "pre_validation"

    def execute(self, context: dict[str, Any]) -> dict[str, Any]:
        author_output = context.get("ExpectationAuthorAgent_output") or {}
        expectations = author_output.get("expectations", [])

        flagged: list[dict[str, Any]] = []
        for exp in expectations:
            if exp.get("review_required"):
                severity = (
                    "HIGH"
                    if "icd" in exp.get("column", "").lower()
                    or "cpt" in exp.get("column", "").lower()
                    else "MEDIUM"
                )
                flagged.append(
                    {
                        "expectation_type": exp["expectation_type"],
                        "column": exp.get("column"),
                        "severity": severity,
                        "rationale": exp.get("rationale", ""),
                        "recommended_reviewer": (
                            "Clinical Informatics" if severity == "HIGH" else "Data Ops"
                        ),
                    }
                )

        # LLM-generated summary (human-in-the-loop gate per GX base doc §4.3.3).
        safe_payload: dict[str, Any] = {
            "suite_name": author_output.get("suite_name", "unknown"),
            "expectation_count": author_output.get("expectation_count", 0),
            "flagged_count": len(flagged),
            "failure_summary": f"{len(flagged)} expectations need human review",
        }
        summary = self._ask_llm(
            instruction=(
                "Draft a ≤ 3-sentence summary for an ops Teams channel — what "
                "needs human review and estimated effort. No PHI."
            ),
            safe_payload=safe_payload,
            max_tokens=256,
        )

        return {
            "suite_name": author_output.get("suite_name"),
            "total_expectations": author_output.get("expectation_count"),
            "flagged_for_human_review": len(flagged),
            "review_queue": flagged,
            "summary": summary,
            "approval_required": len(flagged) > 0,
        }
