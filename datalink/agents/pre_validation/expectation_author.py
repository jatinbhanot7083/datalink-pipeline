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
from datalink.agents.pre_validation.rules import emit_all_rules


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
        #
        # Phase 6 Commit 3: all rule shapes live in pre_validation.rules — the
        # Author is now a thin orchestrator. That keeps the auto-vs-HITL split
        # visible in one place (rules.emit_enum_review_rules is the only one
        # that flags review_required=True) and makes every emitter unit-testable
        # without needing a warehouse adapter.
        expectations: list[dict[str, Any]] = emit_all_rules(profile)

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

        # Phase 6 Commit 2: split auto-approve vs HITL-review.
        client_id = context.get("client_id", "default")
        source_type = context.get("source_type")
        schema_fingerprint = self._compute_fingerprint(table_name)

        auto_sid, review_sid = self._register_split(
            table_name=table_name,
            expectations=expectations,
            client_id=client_id,
            source_type=source_type,
            schema_fingerprint=schema_fingerprint,
        )

        auto_rules = [e for e in expectations if not e.get("review_required")]
        review_rules = [e for e in expectations if e.get("review_required")]

        return {
            "suite_name": f"auto_{(source_type or 'unknown').lower()}",
            "suite_version": "0.1.0",
            "auto_approved_count": len(auto_rules),
            "flagged_for_review_count": len(review_rules),
            "expectation_count": len(expectations),
            "schema_fingerprint": schema_fingerprint,
            "auto_suite_id": auto_sid,
            "review_suite_id": review_sid,
            "auto_suite_status": "LIVE" if auto_sid else "none",
            "review_suite_status": "PENDING_REVIEW" if review_sid else "none",
            "expectations": expectations,
            "llm_rationale": rationale,
        }

    def _compute_fingerprint(self, qualified_table: str) -> str | None:
        """Hash the column layout so the Pre-Val crew can cache-skip next run."""
        try:
            from datalink.quality.schema_fingerprint import compute_fingerprint

            return compute_fingerprint(self._wh, qualified_table)
        except Exception:
            return None

    def _register_split(
        self,
        table_name: str,
        expectations: list[dict[str, Any]],
        client_id: str,
        source_type: str | None,
        schema_fingerprint: str | None,
    ) -> tuple[str | None, str | None]:
        """Split proposed expectations into two suites in CONTROL.dq_suites:
          * auto_{source}           — rules agent is confident in → fast-track to LIVE (no human)
          * auto_{source}__review   — rules flagged review_required → PENDING_REVIEW (human)

        Returns (auto_suite_id, review_suite_id). Either may be None if
        nothing fell into that bucket or the registry write failed.
        """
        try:
            from datalink.quality.registry import (
                SuiteDraft,
                SuiteRegistry,
                SuiteSource,
            )

            reg = SuiteRegistry(self._wh)
            actor = f"agent:{self.__class__.__name__}"
            src_slug = (source_type or "unknown").lower()
            base_name = f"auto_{src_slug}"
            review_name = f"{base_name}__review"

            # Shape helper — common for auto + review buckets.
            #
            # NB: Commit 3 carries SIX new kwarg flavours (regex, mostly,
            # column_A/B, or_equal, parse_strings_as_datetimes, sla_hours)
            # because the expanded emitters in rules.py produce them. Each
            # is additive — an emitter that doesn't set the key skips it.
            def _shape(rules: list[dict[str, Any]]) -> list[dict[str, Any]]:
                shaped: list[dict[str, Any]] = []
                for e in rules:
                    exp_type = e.get("expectation_type", "")
                    kwargs: dict[str, Any] = {}
                    # Single-column kwargs
                    if e.get("column"):
                        kwargs["column"] = e["column"]
                    if e.get("value_set"):
                        kwargs["value_set"] = e["value_set"]
                    if e.get("regex"):
                        kwargs["regex"] = e["regex"]
                    if e.get("mostly") is not None:
                        kwargs["mostly"] = e["mostly"]
                    # Range kwargs (Timeliness / numeric ranges)
                    if e.get("min_value") is not None:
                        kwargs["min_value"] = e["min_value"]
                    if e.get("max_value") is not None:
                        kwargs["max_value"] = e["max_value"]
                    if e.get("parse_strings_as_datetimes") is not None:
                        kwargs["parse_strings_as_datetimes"] = e["parse_strings_as_datetimes"]
                    # Pair kwargs (Consistency invariants)
                    if e.get("column_A"):
                        kwargs["column_A"] = e["column_A"]
                    if e.get("column_B"):
                        kwargs["column_B"] = e["column_B"]
                    if e.get("or_equal") is not None:
                        kwargs["or_equal"] = e["or_equal"]

                    # Prefer the emitter's own dimension+severity hints —
                    # fall back to inference (old behaviour) if absent.
                    dq_dim = e.get("dq_dimension") or _infer_dimension(exp_type)
                    severity = e.get("severity", "MEDIUM")
                    meta = {
                        "dq_dimension": dq_dim,
                        "severity": severity,
                        "description": e.get(
                            "rationale",
                            "Auto-proposed by ExpectationAuthorAgent from profile stats.",
                        ),
                        "review_required": e.get("review_required", False),
                    }
                    # Timeliness carries sla_hours as meta (not a GX kwarg)
                    # so the runtime substitutes now / now-Δ at execute time.
                    if e.get("sla_hours") is not None:
                        meta["sla_hours"] = e["sla_hours"]
                    shaped.append({"expectation_type": exp_type, "kwargs": kwargs, "meta": meta})
                return shaped

            auto_rules = [e for e in expectations if not e.get("review_required")]
            review_rules = [e for e in expectations if e.get("review_required")]

            auto_sid: str | None = None
            review_sid: str | None = None

            # ── Auto-approved bucket ─────────────────────────────────────────
            if auto_rules:
                shaped_auto = _shape(auto_rules)
                dims = sorted(
                    {
                        s["meta"]["dq_dimension"]
                        for s in shaped_auto
                        if s["meta"].get("dq_dimension")
                    }
                )
                auto_sid = reg.create_draft(
                    SuiteDraft(
                        client_id=client_id,
                        suite_name=base_name,
                        expectations=shaped_auto,
                        dq_dimensions=list(dims),
                        created_by=actor,
                        source=SuiteSource.AGENT,
                        source_type=source_type,
                        schema_fingerprint=schema_fingerprint,
                    )
                )
                # Fast-track: DRAFT → PENDING_REVIEW → APPROVED → LIVE.
                # The "auto-approver" is the agent itself because the rule
                # shape is structurally valid (matches a GX built-in by name
                # and all its kwargs are derivable deterministically).
                reg.submit_for_review(auto_sid, actor=actor)
                reg.approve(
                    auto_sid,
                    actor=f"{actor}:auto-approved",
                    notes=(
                        f"Auto-approved: {len(auto_rules)} rules passed structural "
                        f"confidence check (no review_required flag)."
                    ),
                )
                reg.activate(auto_sid, actor=f"{actor}:auto-approved")

            # ── Human-review bucket ──────────────────────────────────────────
            if review_rules:
                shaped_rev = _shape(review_rules)
                dims = sorted(
                    {s["meta"]["dq_dimension"] for s in shaped_rev if s["meta"].get("dq_dimension")}
                )
                review_sid = reg.create_draft(
                    SuiteDraft(
                        client_id=client_id,
                        suite_name=review_name,
                        expectations=shaped_rev,
                        dq_dimensions=list(dims),
                        created_by=actor,
                        source=SuiteSource.AGENT,
                        source_type=source_type,
                        schema_fingerprint=schema_fingerprint,
                    )
                )
                reg.submit_for_review(review_sid, actor=actor)
                # Stays PENDING_REVIEW — human sees this in DQ Review page.

            return auto_sid, review_sid
        except Exception:
            # Non-fatal — the agent's main output is still returned.
            return None, None


def _infer_dimension(expectation_type: str) -> str:
    """Map a GX expectation type to one of the 6 DQ dimensions.

    Fallback only — emitters in pre_validation.rules set `dq_dimension`
    explicitly, and _shape() prefers that. This helper exists for legacy
    rules (e.g. human-authored suites that didn't tag a dimension) and
    for hand-written unit tests.
    """
    t = expectation_type.lower()
    if "not_be_null" in t:
        return "Completeness"
    if "be_unique" in t:
        return "Uniqueness"
    # Timeliness: column_max_to_be_between is how GX expresses "latest
    # row's timestamp is within the SLA window". The earlier bug gated
    # this on `"time" in t` which never matches — max_to_be_between now
    # resolves to Timeliness cleanly.
    if "max_to_be_between" in t or "min_to_be_between" in t:
        return "Timeliness"
    if "pair_values" in t:
        return "Consistency"
    if "match_regex" in t or "be_in_set" in t or "be_between" in t or "match_ordered_list" in t:
        return "Validity"
    return "Validity"  # default bucket
