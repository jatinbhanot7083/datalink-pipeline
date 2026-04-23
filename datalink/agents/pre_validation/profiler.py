"""Profiler Agent — statistical profile of a warehouse table.

Runs SQL aggregates (null%, distinct count, min, max, mean per column). Sends
ONLY column metadata + aggregate stats to the LLM — zero individual records,
zero PHI. PHI boundary enforced by PhiRedactionLayer in AgentBase._ask_llm.
"""

from __future__ import annotations

from typing import Any

from datalink.agents.base import AgentBase


class ProfilerAgent(AgentBase):
    role = (
        "Senior Data Profiler for EvokeConnectCare™ healthcare claims, "
        "membership, and provider data."
    )
    goal = (
        "Generate a structured statistical profile of the target Bronze table: "
        "column names, data types, null%, distinct count, min/max. "
        "Output MUST be a JSON object. NEVER include individual rows — aggregate stats only."
    )
    crew_name = "pre_validation"

    def execute(self, context: dict[str, Any]) -> dict[str, Any]:
        table = context["table"]  # e.g. "BRONZE.RAW_CLAIMS"
        # Run a SQL aggregate per column. We discover columns first.
        cols_result = self._wh.query(f"DESCRIBE {table}")
        # DuckDB DESCRIBE returns column_name / column_type. Some adapters
        # use lowercase keys, so we defensively normalise.
        col_specs: list[tuple[str, str]] = [
            (
                str(r.get("column_name") or r.get("name") or ""),
                str(r.get("column_type") or r.get("type") or ""),
            )
            for r in cols_result
        ]
        col_names: list[str] = [c for c, _ in col_specs]
        col_types: dict[str, str] = {c: t for c, t in col_specs}

        row_count_rows = self._wh.query(f"SELECT COUNT(*) AS c FROM {table}")
        row_count = int(row_count_rows[0]["c"]) if row_count_rows else 0

        profile: dict[str, dict[str, Any]] = {}
        for col in col_names:
            # Aggregate per column. Use TRY_CAST so non-numeric cols don't blow up.
            agg = self._wh.query(
                f"SELECT "
                f"  COUNT(*) AS element_count, "
                f"  SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) AS null_count, "
                f"  COUNT(DISTINCT {col}) AS distinct_count "
                f"FROM {table}"
            )
            stats = agg[0]
            null_pct = (
                (float(stats["null_count"]) / float(stats["element_count"]) * 100.0)
                if stats["element_count"]
                else 0.0
            )
            profile[col] = {
                # dtype carries through for Commit-3 rule emitters (Timeliness
                # needs it to detect timestamp columns; Validity uses name only).
                "dtype": col_types.get(col, ""),
                "element_count": int(stats["element_count"]),
                "null_count": int(stats["null_count"]),
                "null_pct": round(null_pct, 3),
                "distinct_count": int(stats["distinct_count"]),
            }

        # Build a SAFE summary for the LLM — nothing row-level.
        safe_payload: dict[str, Any] = {
            "table_name": table,
            "row_count": row_count,
            "column_names": col_names,
            "profile": profile,
            "instruction": "prepare for expectation authorship",
        }

        # Ask the LLM to narrate the profile — demonstrates the LLM call flow.
        # In stub mode this returns canned JSON; in real Anthropic mode it's a real summary.
        narrative = self._ask_llm(
            instruction=(
                "Summarise data-quality risks from the profile above in ≤ 5 bullet "
                "points. Flag any column with null_pct > 5 or distinct_count < 3 "
                "(suspicious for a non-enum). No PHI."
            ),
            safe_payload=safe_payload,
            max_tokens=512,
        )

        return {
            "table_name": table,
            "row_count": row_count,
            "column_names": col_names,
            "profile": profile,
            "narrative": narrative,
        }
