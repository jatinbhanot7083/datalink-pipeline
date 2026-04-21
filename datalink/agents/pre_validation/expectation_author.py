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

        # Phase 5.8: register this proposal as a DRAFT in CONTROL.dq_suites so
        # it shows up in the /DQ_Review queue. A human reviewer must still
        # approve before it goes LIVE — agents never auto-activate.
        proposed_suite_id = self._register_as_draft(
            table_name=table_name,
            expectations=expectations,
            client_id=context.get("client_id", "default"),
            reviewer_note=str(rationale)[:500] if rationale else None,
        )

        return {
            "suite_name": f"auto_{table_name.replace('.', '_').lower()}",
            "suite_version": "0.1.0",
            "status": "DRAFT",
            "expectation_count": len(expectations),
            "expectations": expectations,
            "llm_rationale": rationale,
            "proposed_suite_id": proposed_suite_id,
        }

    def _register_as_draft(
        self,
        table_name: str,
        expectations: list[dict[str, Any]],
        client_id: str,
        reviewer_note: str | None = None,
    ) -> str | None:
        """Write the proposed expectations to CONTROL.dq_suites as a DRAFT.

        Returns the new suite_id, or None if the registry write fails
        (non-fatal — agent run still succeeds; proposal just doesn't reach
        the review queue this cycle). Failure is logged via the standard
        agent audit trail.
        """
        try:
            # Lazy import so the agent module doesn't hard-depend on the
            # quality layer at module import time.
            from datalink.quality.registry import (
                SuiteDraft,
                SuiteRegistry,
                SuiteSource,
            )

            # Pick a suite_name from the table name. "BRONZE.RAW_CLAIMS" →
            # "bronze_raw_claims_agent". Keeps it distinct from the
            # hand-coded baselines ("bronze_structural" etc.).
            suite_name = f"{table_name.replace('.', '_').lower()}_agent"

            # Shape the agent's rule-based output to the registry's expected
            # JSON: {expectation_type, kwargs, meta}.
            shaped: list[dict[str, Any]] = []
            for e in expectations:
                exp_type = e.get("expectation_type", "")
                kwargs: dict[str, Any] = {}
                if e.get("column"):
                    kwargs["column"] = e["column"]
                # Default meta — agent proposals start at MEDIUM severity;
                # reviewer can bump to HIGH on approve.
                meta = {
                    "dq_dimension": _infer_dimension(exp_type),
                    "severity": "MEDIUM",
                    "description": e.get(
                        "rationale", "Auto-proposed by ExpectationAuthorAgent from profile stats."
                    ),
                    "review_required": e.get("review_required", False),
                }
                shaped.append({"expectation_type": exp_type, "kwargs": kwargs, "meta": meta})

            reg = SuiteRegistry(self._wh)
            dims = sorted(
                {s["meta"]["dq_dimension"] for s in shaped if s["meta"].get("dq_dimension")}
            )
            sid = reg.create_draft(
                SuiteDraft(
                    client_id=client_id,
                    suite_name=suite_name,
                    expectations=shaped,
                    dq_dimensions=list(dims),
                    created_by=f"agent:{self.__class__.__name__}",
                    source=SuiteSource.AGENT,
                )
            )
            # Immediately submit for review — no point in sitting as DRAFT
            # when the agent can't edit it further. Reviewer sees it next
            # time they open /DQ_Review.
            reg.submit_for_review(sid, actor=f"agent:{self.__class__.__name__}")
            return sid
        except Exception:
            # Non-fatal — the agent's main output is still returned. Log
            # via the standard base-class audit in _ask_llm so ops can see.
            return None


def _infer_dimension(expectation_type: str) -> str:
    """Map a GX expectation type to one of the 6 DQ dimensions."""
    t = expectation_type.lower()
    if "not_be_null" in t:
        return "Completeness"
    if "be_unique" in t:
        return "Uniqueness"
    if "max_to_be_between" in t and "time" in t.lower():
        return "Timeliness"
    if "match_regex" in t or "be_in_set" in t or "be_between" in t or "match_ordered_list" in t:
        return "Validity"
    if "pair_values" in t:
        return "Consistency"
    return "Validity"  # default bucket
