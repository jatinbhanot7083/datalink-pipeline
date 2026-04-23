"""Rule emitters — deterministic, testable, zero-LLM.

Phase 6 Commit 3: closes the gaps identified by comparing the
`DataQuality_Metrics.docx` specification to the Author's Commit-2 behaviour.

Design principle
----------------
The LLM writes **prose** (rationale, ops-summary).  The *structured suite*
is code-generated from the Profiler's aggregate stats + column names so
the output is reproducible, diff-able, and cheap.  Every emitter in this
module is a pure function that takes a profile dict and returns a list
of rule dicts — no warehouse access, no LLM, no side effects.

Each rule dict is the internal "pre-shape" — the Author's `_shape()`
normaliser turns it into the GX-native `{expectation_type, kwargs, meta}`
form at registry-write time.

Auto-approve vs human-review
----------------------------
A rule is auto-approvable when its **shape is fully derivable** without
a human judgment call:

  * Uniqueness on a PK-candidate column that is observed-unique → auto
  * Timeliness (24h rolling SLA on a timestamp col) → auto
  * Validity regex for *industry standards* (NPI, ICD-10, CPT, TIN, ZIP)
    → auto (these formats are fixed by HIPAA / CMS, not by a client)
  * Consistency pair (stop_date >= start_date) → auto (logical invariant)
  * Completeness (not_null) → auto

A rule is `review_required=True` when a human must supply a value the
code cannot infer:

  * Enum value_set for low-cardinality columns
  * Cross-system Accuracy joins (needs reference_table context)
"""

from __future__ import annotations

import re
from typing import Any

# ---------------------------------------------------------------------------
# Healthcare regex library
# ---------------------------------------------------------------------------
# Keyed by a column-name regex; matches any column whose normalised (lower,
# underscore-stripped) name satisfies the pattern.  We prefer name-based
# dispatch over Profiler-sample-based inference because Profiler MUST NOT
# ship sample values to the agent (PHI constraint, Jatin Q2).
#
# Each entry ships:
#   * regex        — the GX match_regex value
#   * description  — one-liner for the expectation's meta.description
#   * dq_dimension — Validity (per §1.6 of the DataQuality_Metrics doc)
#   * severity     — HIGH for identifiers that drive joins; MEDIUM otherwise
#
# NOTE: the regexes are *format* checks, not semantic validity — e.g. NPI
# passes a Luhn check at the ProviderNPI level that a pure regex can't
# catch.  The Reviewer Agent can narrate this caveat in the LLM rationale.
HEALTHCARE_REGEXES: dict[str, dict[str, Any]] = {
    r"(^|_)npi($|_)|provider_?npi": {
        "regex": r"^[0-9]{10}$",
        "description": "NPI must be 10 numeric digits (CMS / NPPES).",
        "severity": "HIGH",
    },
    r"(^|_)icd(_?10)?(_?code)?($|_)|diagnosis_?code": {
        # ICD-10-CM: letter + 2 digits, optional .subcategory up to 4 alphanumerics.
        "regex": r"^[A-Z][0-9]{2}(\.[0-9A-Z]{1,4})?$",
        "description": "ICD-10-CM format: letter + 2 digits, optional .subcategory (CMS).",
        "severity": "HIGH",
    },
    r"(^|_)cpt(_?code)?($|_)|procedure_?code": {
        # CPT Category-I codes: 5 digits. Category-II/III end in F/T respectively.
        "regex": r"^[0-9]{4}[0-9A-Z]$",
        "description": "CPT Category-I/II/III format: 5 chars, digits with optional trailing F/T.",
        "severity": "HIGH",
    },
    r"(^|_)tin($|_)|tax_?id|ein": {
        "regex": r"^[0-9]{9}$",
        "description": "TIN / EIN must be 9 numeric digits (IRS).",
        "severity": "MEDIUM",
    },
    r"(^|_)zip(_?code)?($|_)|postal_?code": {
        # USPS ZIP or ZIP+4.
        "regex": r"^[0-9]{5}(-[0-9]{4})?$",
        "description": "USPS ZIP (5-digit) or ZIP+4 format.",
        "severity": "MEDIUM",
    },
    r"(^|_)state(_?code)?($|_)": {
        "regex": r"^[A-Z]{2}$",
        "description": "US state code (2 uppercase letters, ISO 3166-2).",
        "severity": "MEDIUM",
    },
    r"(^|_)phone($|_)|home_?phone|work_?phone|mobile_?phone|contact_?number": {
        # Digits only, 10 (US) or 11 (with leading 1).  Client data is usually
        # pre-normalised by the Bronze loader — if not, Reviewer should flag.
        "regex": r"^[0-9]{10,11}$",
        "description": "Phone number as 10 or 11 digits (normalised, no punctuation).",
        "severity": "LOW",
    },
    r"(^|_)ssn($|_)|social_?security": {
        "regex": r"^[0-9]{9}$",
        "description": "SSN as 9 digits (dashes stripped).",
        "severity": "HIGH",
    },
    r"(^|_)email($|_)|email_?address": {
        # RFC-5322 simple subset — good enough for DQ validity; full RFC is
        # impractical in SQL regex.
        "regex": r"^[^@\s]+@[^@\s]+\.[^@\s]+$",
        "description": "Basic email shape (local@domain.tld).",
        "severity": "LOW",
    },
}

# ---------------------------------------------------------------------------
# PK-candidate detector
# ---------------------------------------------------------------------------
# Column-name patterns that *suggest* a primary-key candidate.  We only
# emit `expect_column_values_to_be_unique` when BOTH the name matches AND
# the Profiler observed distinct_count == element_count (or ≥ 99.9% to
# absorb a single late-arriving duplicate during a live ingest).
PK_CANDIDATE_PATTERNS: tuple[str, ...] = (
    r"^member_?no$",
    r"^member_?id$",
    r"^claim_?no$",
    r"^claim_?id$",
    r"^provider_?id$",
    r"^provider_?npi_?id$",
    r"^patient_?id$",
    r"^policy_?no$",
    r"^policy_?id$",
    r"^auth_?no$",
    r"^auth_?id$",
    r"(^|_)(sk|pk|business_?key)$",  # vault keys
)
_PK_REGEX = re.compile("|".join(PK_CANDIDATE_PATTERNS), re.IGNORECASE)

# ---------------------------------------------------------------------------
# Timestamp / date name hints
# ---------------------------------------------------------------------------
# We emit `expect_column_max_to_be_between` with a rolling 24-hour SLA
# for columns whose name *suggests* a load/update timestamp AND whose
# declared type looks like a timestamp.
_TS_NAME_HINTS = re.compile(
    r"(?:^|_)(load(ed)?_?(at|datetime|ts)?|updated_?(at|ts)?|created_?(at|ts)?|"
    r"inserted_?(at|ts)?|report_?period|date_?stamp|run_?date)(?:$|_)",
    re.IGNORECASE,
)
_TS_TYPE_HINTS = re.compile(r"timestamp|datetime|date", re.IGNORECASE)

# ---------------------------------------------------------------------------
# Date-pair detector (consistency: stop >= start)
# ---------------------------------------------------------------------------
# We pair columns whose suffixes match one of these stop/start patterns
# and both exist in the same profile.
DATE_PAIR_SUFFIXES: tuple[tuple[str, str], ...] = (
    ("stop_date", "start_date"),
    ("end_date", "start_date"),
    ("service_stop_date", "service_start_date"),
    ("service_end_date", "service_start_date"),
    ("term_date", "effective_date"),
    ("discharge_date", "admit_date"),
    ("admit_date", "effective_date"),  # defensive — admit shouldn't precede effective
    ("thru_date", "from_date"),
    ("to_date", "from_date"),
)


# ---------------------------------------------------------------------------
# Emitters
# ---------------------------------------------------------------------------


def _norm(col: str) -> str:
    """Lowercase + strip non-word chars for robust name matching."""
    return re.sub(r"[^a-z0-9_]", "", col.lower())


def emit_completeness_rules(profile: dict[str, Any]) -> list[dict[str, Any]]:
    """Null checks.

    Strict `not_null` when observed null_pct == 0.  Soft `not_null` with
    `mostly: 0.99` when 0 < null_pct < 1 — mirrors the doc's
    `null_pct < 1%` threshold and absorbs a single late-null without
    flapping the suite.
    """
    out: list[dict[str, Any]] = []
    for col, stats in profile.items():
        if not isinstance(stats, dict):
            continue
        null_pct = float(stats.get("null_pct", 0) or 0)
        if null_pct == 0.0:
            out.append(
                {
                    "expectation_type": "expect_column_values_to_not_be_null",
                    "column": col,
                    "dq_dimension": "Completeness",
                    "rationale": "Profiler observed zero nulls.",
                }
            )
        elif null_pct < 1.0:
            out.append(
                {
                    "expectation_type": "expect_column_values_to_not_be_null",
                    "column": col,
                    "mostly": 0.99,
                    "dq_dimension": "Completeness",
                    "rationale": (
                        f"Profiler observed null_pct={null_pct:.2f}% — soft 99% threshold."
                    ),
                }
            )
    return out


def emit_uniqueness_rules(profile: dict[str, Any]) -> list[dict[str, Any]]:
    """Primary-key uniqueness.

    Fires when (a) the column name matches a PK-candidate pattern AND
    (b) the Profiler observed distinct_count == element_count (or ≥99.9%).
    Both conditions must hold — name alone isn't enough (a typo'd column
    named `member_id` could still have duplicates), and distinct==total
    alone isn't either (any small column can hit that by luck).
    """
    out: list[dict[str, Any]] = []
    for col, stats in profile.items():
        if not isinstance(stats, dict):
            continue
        norm = _norm(col)
        if not _PK_REGEX.search(norm):
            continue
        element_count = int(stats.get("element_count", 0) or 0)
        distinct_count = int(stats.get("distinct_count", 0) or 0)
        if element_count == 0:
            continue
        distinct_pct = distinct_count / element_count
        if distinct_pct >= 0.999:
            out.append(
                {
                    "expectation_type": "expect_column_values_to_be_unique",
                    "column": col,
                    "dq_dimension": "Uniqueness",
                    "rationale": (
                        f"Name matches PK pattern; observed distinct_count="
                        f"{distinct_count}/{element_count} ({distinct_pct * 100:.2f}%)."
                    ),
                }
            )
    return out


def emit_timeliness_rules(profile: dict[str, Any]) -> list[dict[str, Any]]:
    """24-hour rolling SLA on timestamp columns.

    `expect_column_max_to_be_between` with `parse_strings_as_datetimes`.
    We leave min/max_value as null — the checkpoint runtime substitutes
    `CURRENT_TIMESTAMP - 24h` and `CURRENT_TIMESTAMP` at evaluation time.
    The rule still registers structurally so the catalog can display it.
    """
    out: list[dict[str, Any]] = []
    for col, stats in profile.items():
        if not isinstance(stats, dict):
            continue
        dtype = str(stats.get("dtype", "") or "")
        name_hit = bool(_TS_NAME_HINTS.search(col))
        type_hit = bool(_TS_TYPE_HINTS.search(dtype))
        if not (name_hit and type_hit):
            continue
        out.append(
            {
                "expectation_type": "expect_column_max_to_be_between",
                "column": col,
                "parse_strings_as_datetimes": True,
                "sla_hours": 24,  # shape hint for the checkpoint runtime
                "dq_dimension": "Timeliness",
                "rationale": (
                    "Column name + dtype suggest a load timestamp; applying 24h rolling SLA."
                ),
            }
        )
    return out


def emit_validity_regex_rules(profile: dict[str, Any]) -> list[dict[str, Any]]:
    """Industry-standard format checks.

    Iterates the HEALTHCARE_REGEXES table.  Auto-approvable because the
    regexes encode CMS / NPPES / IRS / USPS standards, not business
    rules — they're identical for every client on Earth.
    """
    out: list[dict[str, Any]] = []
    for col in profile:
        norm = _norm(col)
        for name_pat, spec in HEALTHCARE_REGEXES.items():
            if re.search(name_pat, norm):
                out.append(
                    {
                        "expectation_type": "expect_column_values_to_match_regex",
                        "column": col,
                        "regex": spec["regex"],
                        "mostly": 0.99,  # absorb a handful of legacy rows
                        "dq_dimension": "Validity",
                        "severity": spec.get("severity", "MEDIUM"),
                        "rationale": spec["description"],
                    }
                )
                break  # first match wins — don't double-emit for overlapping patterns
    return out


def emit_consistency_pair_rules(
    profile: dict[str, Any],
) -> list[dict[str, Any]]:
    """Date-range invariants: stop/end date must be >= start date.

    Scans the column list for pairs whose suffixes match DATE_PAIR_SUFFIXES
    (e.g. `SERVICE_STOP_DATE` + `SERVICE_START_DATE`).  Emits
    `expect_column_pair_values_a_to_be_greater_than_b` with `or_equal=True`
    so same-day services pass.
    """
    out: list[dict[str, Any]] = []
    normalised = {_norm(c): c for c in profile}
    # Dedup across overlapping suffix patterns — e.g. SERVICE_STOP_DATE
    # matches both the generic `stop_date` pair and the specific
    # `service_stop_date` pair; we want ONE rule per physical (A, B) edge.
    seen: set[tuple[str, str]] = set()

    for stop_suffix, start_suffix in DATE_PAIR_SUFFIXES:
        stop_suffix_n = _norm(stop_suffix)
        start_suffix_n = _norm(start_suffix)
        # For every stop-suffix hit, look for a start-suffix sibling with
        # a matching prefix (so SERVICE_STOP_DATE pairs with SERVICE_START_DATE,
        # not with ANY random *_START_DATE column).
        for norm, original in normalised.items():
            if not norm.endswith(stop_suffix_n):
                continue
            prefix = norm[: -len(stop_suffix_n)]
            sibling_norm = f"{prefix}{start_suffix_n}"
            if sibling_norm in normalised and sibling_norm != norm:
                sibling = normalised[sibling_norm]
                key = (original, sibling)
                if key in seen:
                    continue
                seen.add(key)
                out.append(
                    {
                        "expectation_type": "expect_column_pair_values_a_to_be_greater_than_b",
                        "column_A": original,
                        "column_B": sibling,
                        "or_equal": True,
                        "dq_dimension": "Consistency",
                        "rationale": (
                            f"Logical invariant: {original} must be >= "
                            f"{sibling} (same-day allowed)."
                        ),
                    }
                )
    return out


def emit_enum_review_rules(profile: dict[str, Any]) -> list[dict[str, Any]]:
    """Low-cardinality columns flagged for human value_set.

    This is the ONLY emitter that flags `review_required=True` — a
    human must supply the actual allowed values.  Kept here (not in
    the auto emitters) so the auto/HITL split is visible at a glance.
    """
    out: list[dict[str, Any]] = []
    for col, stats in profile.items():
        if not isinstance(stats, dict):
            continue
        distinct = int(stats.get("distinct_count", 0) or 0)
        if 1 < distinct <= 10:
            out.append(
                {
                    "expectation_type": "expect_column_values_to_be_in_set",
                    "column": col,
                    "review_required": True,
                    "dq_dimension": "Validity",
                    "rationale": (
                        f"distinct_count={distinct} — likely enum; caller must supply value_set."
                    ),
                }
            )
    return out


def emit_all_rules(profile: dict[str, Any]) -> list[dict[str, Any]]:
    """Convenience — union of every emitter, in a stable order.

    Order matters for deterministic suite fingerprints and for
    human-readable diff output in the Suite Browser.
    """
    return [
        *emit_completeness_rules(profile),
        *emit_uniqueness_rules(profile),
        *emit_timeliness_rules(profile),
        *emit_validity_regex_rules(profile),
        *emit_consistency_pair_rules(profile),
        *emit_enum_review_rules(profile),
    ]
