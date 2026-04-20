"""Root Cause Agent — classifies a GX checkpoint failure.

Queries CONTROL.gx_validation_results + CONTROL.pipeline_checkpoints to:
  - extract failed expectations
  - classify: DATA_QUALITY / CONFIG_ERROR / SCHEMA_CHANGE / VOLUME_ANOMALY
  - compare against prior N runs (one-off vs recurring)
  - produce a structured analysis
"""

from __future__ import annotations

from typing import Any

from datalink.agents.base import AgentBase
from datalink.quality.control import CONTROL_SCHEMA


class RootCauseAgent(AgentBase):
    role = "Data Incident Root-Cause Analyst for EvokeConnectCare™ pipelines."
    goal = (
        "Classify each GX failure into one of DATA_QUALITY / CONFIG_ERROR / "
        "SCHEMA_CHANGE / VOLUME_ANOMALY and output a structured JSON analysis. "
        "Use only metadata — no raw failing rows."
    )
    crew_name = "post_validation"

    def execute(self, context: dict[str, Any]) -> dict[str, Any]:
        run_id = context["run_id"]
        checkpoint_name = context["checkpoint_name"]

        # Pull failed expectations for this run.
        failures = self._wh.query(
            f"SELECT expectation, column_name, unexpected_count, unexpected_pct, details "
            f"FROM {CONTROL_SCHEMA}.gx_validation_results "
            f"WHERE run_id = $r AND checkpoint_name = $c AND success = false",
            {"r": run_id, "c": checkpoint_name},
        )
        # Historical frequency of each failing (expectation, column) pair.
        classified: list[dict[str, Any]] = []
        for f in failures:
            history = self._wh.query(
                f"SELECT COUNT(*) AS n FROM {CONTROL_SCHEMA}.gx_validation_results "
                f"WHERE checkpoint_name = $c AND expectation = $e "
                f"  AND column_name = $col AND success = false",
                {"c": checkpoint_name, "e": f["expectation"], "col": f["column_name"]},
            )
            seen_count = int(history[0]["n"]) if history else 0
            classification = self._classify(f)
            classified.append(
                {
                    "expectation": f["expectation"],
                    "column": f["column_name"],
                    "unexpected_count": int(f["unexpected_count"] or 0),
                    "unexpected_pct": float(f["unexpected_pct"] or 0),
                    "classification": classification,
                    "recurrence_count_including_this": seen_count,
                    "known_issue": seen_count > 3,
                }
            )

        # PHI-safe payload for the LLM narrative
        safe_payload: dict[str, Any] = {
            "checkpoint_name": checkpoint_name,
            "run_id": run_id,
            "failure_summary": f"{len(classified)} failed expectations",
            "expectations": [{k: v for k, v in c.items() if k != "value"} for c in classified],
        }
        narrative = self._ask_llm(
            instruction=(
                "Write a ≤ 5-sentence root-cause analysis suitable for an "
                "Ops Teams post. Focus on the most severe failure. Suggest a "
                "likely upstream cause. No PHI."
            ),
            safe_payload=safe_payload,
            max_tokens=512,
        )

        return {
            "run_id": run_id,
            "checkpoint_name": checkpoint_name,
            "failure_count": len(classified),
            "classified_failures": classified,
            "narrative": narrative,
        }

    @staticmethod
    def _classify(failure: dict[str, Any]) -> str:
        exp_type = failure.get("expectation", "")
        if "columns_to_match_ordered_list" in exp_type:
            return "SCHEMA_CHANGE"
        if "row_count_to_be_between" in exp_type:
            return "VOLUME_ANOMALY"
        if "values_to_be_in_set" in exp_type:
            return "DATA_QUALITY"  # bad CPT / ICD / claim_status, etc.
        return "DATA_QUALITY"
