"""PhiRedactionLayer — the enforced boundary between DataLink data and any LLM.

Rules:
  1. Only pre-declared SAFE_FIELDS may appear as top-level keys in an LLM payload.
  2. If a value is a list longer than MAX_LIST_LEN, raise — looks row-like.
  3. If a value is a dict whose keys match known-PHI names, raise.
  4. If a value is a string longer than MAX_FREETEXT_LEN, raise — clinical note suspicion.

Conservative by design: it is easier to loosen rules than to recall a PHI leak.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

# Top-level payload keys an agent is allowed to send to an LLM.
# Anything else raises. Add entries here with explicit review.
SAFE_FIELDS: frozenset[str] = frozenset(
    {
        # Identification of the computation, not the data
        "batch_id",
        "run_id",
        "pipeline_id",
        "layer",
        "table_name",
        "schema_name",
        "database_name",
        # Schema / metadata
        "column_name",
        "column_names",
        "data_type",
        "data_types",
        # Aggregate statistics (safe — no row-level info)
        "row_count",
        "null_count",
        "null_pct",
        "distinct_count",
        "min_value",
        "max_value",
        "mean",
        "stddev",
        "percentiles",
        "profile",  # nested dict of per-column aggregates
        "top_values",  # aggregated most-frequent values — see _is_pii_value guard below
        # Validation artifacts (no data values)
        "expectation_type",
        "expectations",
        "suite_name",
        "suite_version",
        "checkpoint_name",
        "failure_summary",  # counts and rates only — no rows
        "fail_pct",
        # Pipeline metadata
        "dag_id",
        "task_id",
        "timestamp",
        "attempt",
        "duration_seconds",
        # Prose instructions / task context (free-form, but validated-length)
        "instruction",
        "context",
    }
)

# Full key names that are always PHI (exact match, case-insensitive).
_PHI_EXACT_KEYS: frozenset[str] = frozenset(
    {
        "social_security_number",
        "member_name",
        "patient_name",
        "subscriber_name",
        "first_name",
        "last_name",
        "full_name",
        "date_of_birth",
        "birthdate",
        "address",
        "address_line1",
        "address_line2",
        "street",
        "phone",
        "phone_number",
        "telephone",
        "cell_phone",
        "email",
        "email_address",
        "tax_id",
        "taxpayer_id",
        "medical_record_number",
        "raw_row",
        "row",
        "rows",
        "record",
        "records",
        "raw_records",
    }
)

# Tokens that are almost always PHI-tag-words. After splitting a key on
# underscores / hyphens / camelCase, any of these tokens triggers a violation.
# Kept narrow on purpose — tokens like "name" or "phone" are too broad to live here.
_PHI_STRICT_TOKENS: frozenset[str] = frozenset({"ssn", "dob", "mrn", "tin"})

_KEY_TOKEN_SPLIT_RE = re.compile(r"[_\-]|(?<=[a-z])(?=[A-Z])")

MAX_LIST_LEN = 50
MAX_FREETEXT_LEN = 4_000


class PhiBoundaryViolationError(Exception):
    """Raised when a payload bound for an LLM looks like it contains PHI."""


@dataclass
class InspectionReport:
    ok: bool
    violations: list[str]

    def raise_if_violated(self) -> None:
        if not self.ok:
            raise PhiBoundaryViolationError("; ".join(self.violations))


class PhiRedactionLayer:
    """Stateless inspector. Instantiate once per process (or as a singleton)."""

    def inspect_payload(self, payload: Mapping[str, Any]) -> InspectionReport:
        violations: list[str] = []

        for key, value in payload.items():
            if key not in SAFE_FIELDS:
                violations.append(f"top-level key not in SAFE_FIELDS: '{key}'")
                continue
            violations.extend(self._inspect_value(f"{key}", value))

        return InspectionReport(ok=not violations, violations=violations)

    def assert_clean(self, payload: Mapping[str, Any]) -> None:
        """Convenience: raise on violation, return None otherwise."""
        self.inspect_payload(payload).raise_if_violated()

    # -- internals ----------------------------------------------------------

    def _inspect_value(self, path: str, value: Any) -> list[str]:
        violations: list[str] = []

        if isinstance(value, str):
            if len(value) > MAX_FREETEXT_LEN:
                violations.append(
                    f"{path}: freetext value exceeds {MAX_FREETEXT_LEN} chars "
                    f"(got {len(value)}) — possible clinical note leak"
                )
        elif isinstance(value, Mapping):
            for k, v in value.items():
                subpath = f"{path}.{k}"
                if self._looks_like_phi_key(str(k)):
                    violations.append(f"{subpath}: key matches PHI substring")
                    continue
                violations.extend(self._inspect_value(subpath, v))
        elif isinstance(value, list | tuple):
            # Iterable[non-str] must be short and contain primitives / flat dicts
            seq = list(value)
            if len(seq) > MAX_LIST_LEN:
                violations.append(
                    f"{path}: list length {len(seq)} exceeds MAX_LIST_LEN={MAX_LIST_LEN} "
                    f"— looks row-like"
                )
            for i, item in enumerate(seq[:MAX_LIST_LEN]):
                violations.extend(self._inspect_value(f"{path}[{i}]", item))
        # numbers, bools, None — always fine
        return violations

    @staticmethod
    def _looks_like_phi_key(key: str) -> bool:
        k = key.lower()
        if k in _PHI_EXACT_KEYS:
            return True
        tokens = {t.lower() for t in _KEY_TOKEN_SPLIT_RE.split(key) if t}
        return bool(tokens & _PHI_STRICT_TOKENS)

    # -- convenience helpers for agent authors ------------------------------

    @staticmethod
    def safe_subset(payload: Mapping[str, Any]) -> dict[str, Any]:
        """Return a shallow copy keeping only SAFE_FIELDS top-level keys."""
        return {k: v for k, v in payload.items() if k in SAFE_FIELDS}

    @staticmethod
    def scrub_iterable_keys(items: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
        """Drop PHI-shaped keys from each dict. Useful for safe sample outputs."""
        out: list[dict[str, Any]] = []
        for item in items:
            out.append(
                {k: v for k, v in item.items() if not PhiRedactionLayer._looks_like_phi_key(str(k))}
            )
        return out
