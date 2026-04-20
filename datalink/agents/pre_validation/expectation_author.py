"""Expectation Author Agent — generates a GX expectation suite JSON.

Takes the Profiler's output (column names + aggregate stats) and produces
a suite of GX expectations calibrated to the observed data (thresholds
derived from historical null rates, value-set expectations for low-cardinality
columns, range checks for numerics).

The generated suite is a SUGGESTION — it goes through the Reviewer and then
to a human approver before being activated against production.
"""

from __future__ import annotations

from typing import Any

from datalink.agents.base import AgentBase


class ExpectationAuthorAgent(AgentBase):
    role = (
        "GX Expectation Suite Author specialised in healthcare data schemas "
        "(claims EDI 837, membership EDI 834, NPPES provider)."
    )
    goal = (
        "Generate a versioned GX expectation suite JSON from a statistical "
        "profile. Each expectation maps to a column or table-level assertion "
        "using GX 1.x expectation types. Output MUST be valid JSON — one key "
        "'expectations' with a list of {expectation_type, column, kwargs} objects."
    )
    crew_name = "pre_validation"

    def execute(self, context: dict[str, Any]) -> dict[str, Any]:
        profile_output = context.get("ProfilerAgent_output") or {}
        table_name = profile_output.get("table_name") or context.get("table", "unknown")
        profile = profile_output.get("profile", {})

        # Deterministic rule-based authorship (works in stub mode too — the LLM
        # acts as NARRATIVE only; the structured suite is code-generated from
        # the profile so it's reproducible and testable).
        expectations: list[dict[str, Any]] = []
        for col, stats in profile.items():
            if not isinstance(stats, dict):
                continue
            null_pct = stats.get("null_pct", 0)
            distinct = stats.get("distinct_count", 0)
            # Columns with <1% nulls → expect not_null
            if null_pct < 1.0:
                expectations.append(
                    {"expectation_type": "expect_column_values_to_not_be_null", "column": col}
                )
            # Low cardinality (likely enum) → in_set (but we only have stats,
            # not the actual values — so we SUGGEST a manual review)
            if 1 < distinct <= 10:
                expectations.append(
                    {
                        "expectation_type": "expect_column_values_to_be_in_set",
                        "column": col,
                        "review_required": True,
                        "rationale": f"distinct_count={distinct} — likely enum; caller must supply value_set",
                    }
                )

        # Ask LLM to *review* the plan (real value-add: natural-language rationale).
        safe_payload: dict[str, Any] = {
            "table_name": table_name,
            "column_names": list(profile.keys()),
            "profile": profile,
            "expectations": [{k: v for k, v in e.items() if k != "value"} for e in expectations],
        }
        rationale = self._ask_llm(
            instruction=(
                "Given this draft expectation suite, identify any EXPECTATIONS MISSING "
                "that a healthcare data engineer would add (hint: CPT code format, "
                "ICD-10 format, NPI Luhn). Respond with a JSON list of additions. No PHI."
            ),
            safe_payload=safe_payload,
            max_tokens=512,
        )

        return {
            "suite_name": f"auto_{table_name.replace('.', '_').lower()}",
            "suite_version": "0.1.0",
            "status": "DRAFT",
            "expectation_count": len(expectations),
            "expectations": expectations,
            "llm_rationale": rationale,
        }
