"""Phase 13.1 — source profiler.

Returns a rich, structured :class:`SourceProfile` for a Bronze (or any)
warehouse table — column-level metadata that downstream stages of the
mapper agent feed to the LLM as ground-truth context. No LLM calls
here; everything is deterministic SQL + heuristics.

Per-column metadata captured:

  * ``raw_type``      — warehouse-native type string (DuckDB or Snowflake)
  * ``logical_type``  — normalised to Phase-11's vocab (TEXT / INTEGER /
                        DECIMAL / DATE / TIMESTAMP / BOOLEAN)
  * ``total_rows``    — table-level
  * ``null_count``    — per-column
  * ``null_pct``      — per-column, rounded to 2 decimals
  * ``distinct_count``— per-column
  * ``sample_values`` — up to ``sample_limit`` distinct non-null values
  * ``min_value`` /
    ``max_value``     — range (NULL on non-orderable types)
  * ``semantic_hint`` — heuristic guess: ``ZIP_CODE`` / ``NPI`` /
                        ``SSN`` / ``ICD_CODE`` / ``EMAIL`` / ``PHONE`` /
                        ``DATE_STRING`` / ``BOOL_FLAG`` / ``UUID`` /
                        ``CURRENCY`` / ``PERCENT`` / ``"" `` (no hint).
                        Pure regex / range checks — no LLM. The mapper
                        agent uses these as starting hypotheses but
                        defers to the operator's NL contract for the
                        final call.

Audit columns (``_load_dt`` etc.) are filtered out by default. Pass
``include_audit_cols=True`` to keep them.

Performance: one batched aggregation query per table (all columns
folded into one SELECT) + one DISTINCT-LIMIT query per column for
samples. Samples are the unavoidable N+1, but we cap them via
``sample_limit`` (default 5) so the cost stays bounded.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from typing import Any

from datalink.adapters._describe import list_columns_typed
from datalink.adapters.protocols import Warehouse
from datalink.logging import get_logger
from datalink.quality.schema_drift import normalize_type

_log = get_logger(__name__)


# ============================================================================
# CORE TYPES
# ============================================================================


@dataclass(frozen=True)
class ColumnProfile:
    """Profile for one column. ``sample_values`` is JSON-coerced for
    portability — datetimes become ISO strings, decimals become floats.
    """

    name: str
    raw_type: str
    logical_type: str
    total_rows: int
    null_count: int
    null_pct: float
    distinct_count: int
    sample_values: tuple[Any, ...]
    min_value: Any | None = None
    max_value: Any | None = None
    semantic_hint: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Stable JSON-friendly shape for storage + LLM prompt injection."""
        return {
            "name": self.name,
            "raw_type": self.raw_type,
            "logical_type": self.logical_type,
            "total_rows": self.total_rows,
            "null_count": self.null_count,
            "null_pct": self.null_pct,
            "distinct_count": self.distinct_count,
            "sample_values": list(self.sample_values),
            "min_value": _coerce(self.min_value),
            "max_value": _coerce(self.max_value),
            "semantic_hint": self.semantic_hint,
        }


@dataclass(frozen=True)
class SourceProfile:
    """Whole-table profile. Carries everything an LLM needs to design
    Silver/Gold transformations without seeing actual rows.
    """

    qualified_table: str
    total_rows: int
    columns: tuple[ColumnProfile, ...]
    profiled_at: datetime
    notes: tuple[str, ...] = field(default_factory=tuple)

    @property
    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]

    @property
    def by_name(self) -> dict[str, ColumnProfile]:
        return {c.name: c for c in self.columns}

    def to_dict(self) -> dict[str, Any]:
        return {
            "qualified_table": self.qualified_table,
            "total_rows": self.total_rows,
            "columns": [c.to_dict() for c in self.columns],
            "profiled_at": self.profiled_at.isoformat(),
            "notes": list(self.notes),
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), default=str, indent=2)


# ============================================================================
# SEMANTIC INFERENCE (pure)
# ============================================================================

# High-confidence patterns — regex is strict enough that ALL samples
# matching is by itself proof. No column-name confirmation needed.
# UUIDs, emails, SSN format, ISO dates, and ICD-10 codes don't really
# look like anything else.
_HIGH_CONFIDENCE_PATTERNS: list[tuple[str, str]] = [
    (r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", "UUID"),
    (r"^[^@\s]+@[^@\s]+\.[^@\s]+$", "EMAIL"),
    (r"^\d{3}-\d{2}-\d{4}$", "SSN"),
    (r"^\d{4}-\d{2}-\d{2}$", "DATE_STRING"),
    (r"^\d{1,2}/\d{1,2}/\d{4}$", "DATE_STRING"),
    (r"^[A-TV-Z][0-9][0-9AB](\.[0-9A-Z]{1,4})?$", "ICD_CODE"),
]

# Name-required patterns — the regex is too generic to fire on its own.
# A 5-digit number could be a ZIP, an account ID, or a building number;
# a 10-digit number could be an NPI, a phone number, or just an ID. We
# require the column name to softly match the hint vocabulary (e.g.
# ``zip``, ``postal`` for ZIP_CODE).
_NAME_REQUIRED_PATTERNS: list[tuple[tuple[str, ...], str, str]] = [
    (("zip", "postal"), r"^\d{5}(-\d{4})?$", "ZIP_CODE"),
    (("npi",), r"^\d{10}$", "NPI"),
    (("phone", "tel", "mobile", "cell"), r"^[\+]?[\d\s\-\(\)]{7,}$", "PHONE"),
]

_BOOL_VALUES = {"Y", "N", "YES", "NO", "TRUE", "FALSE", "T", "F", "0", "1"}


def infer_semantic_hint(col_name: str, samples: list[Any]) -> str:
    """Heuristic semantic guess for a column. No LLM, no DB.

    Rule precedence (first match wins):

      1. **Boolean flag** — every sample is in the boolean vocab
         (``Y/N``, ``YES/NO``, ``T/F``, ``TRUE/FALSE``, ``0/1``).
      2. **Currency / percent name match** — column name contains
         ``amount`` / ``cost`` / ``charge`` / ``billed`` → ``CURRENCY``;
         ``pct`` / ``percent`` / ``ratio`` / ``rate`` → ``PERCENT``.
      3. **High-confidence pattern** — every sample matches a regex
         from :data:`_HIGH_CONFIDENCE_PATTERNS` (UUID, EMAIL, SSN,
         ISO date, US date, ICD-10). These regexes are strict enough
         to identify the type without column-name confirmation.
      4. **Name-required pattern** — every sample matches a regex
         from :data:`_NAME_REQUIRED_PATTERNS` AND the column name
         softly matches the hint vocab (e.g. ``zip`` for ZIP_CODE,
         ``npi`` for NPI). Without a name match, return empty —
         a 5-digit number column named ``account_id`` is NOT a ZIP.
      5. Else → empty string.

    Sample-based inference assumes ``samples`` is non-empty and
    pre-filtered to non-null. Caller (``profile_source``) handles that.
    """
    name_l = col_name.lower()
    str_samples = [str(s).strip() for s in samples if s is not None and str(s).strip()]
    if not str_samples:
        return ""

    # 1. Boolean flag.
    if all(s.upper() in _BOOL_VALUES for s in str_samples):
        return "BOOL_FLAG"

    # 2. Currency / percent by name.
    if any(tok in name_l for tok in ("amount", "amt", "charge", "cost", "fee", "billed", "paid")):
        return "CURRENCY"
    if any(tok in name_l for tok in ("pct", "percent", "ratio", "rate")):
        return "PERCENT"

    # 3. High-confidence patterns — fire on regex match alone.
    for pattern, label in _HIGH_CONFIDENCE_PATTERNS:
        if all(re.match(pattern, s, re.IGNORECASE) for s in str_samples):
            return label

    # 4. Name-required patterns.
    for hint_keys, pattern, label in _NAME_REQUIRED_PATTERNS:
        name_matches = any(k in name_l for k in hint_keys)
        if name_matches and all(re.match(pattern, s, re.IGNORECASE) for s in str_samples):
            return label

    return ""


# ============================================================================
# PROFILER
# ============================================================================


_AUDIT_PREFIX = "_"

# Identifiers safe to interpolate unquoted into SQL on both Snowflake +
# DuckDB. Most healthcare columns are simple snake_case (member_id,
# dob, plan_code) so this covers > 99% of real schemas. Anything that
# fails this check raises early with a clear error rather than producing
# a confusing SQL parse error downstream.
_SAFE_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def profile_source(
    warehouse: Warehouse,
    qualified_table: str,
    *,
    sample_limit: int = 5,
    include_audit_cols: bool = False,
    notes: list[str] | None = None,
) -> SourceProfile:
    """Profile every business column of ``qualified_table``.

    One aggregation query for table-level + per-column counts, then
    one DISTINCT-LIMIT query per column for samples. For a 50-column
    table that's 51 queries — fine for offline analysis (sub-second
    on Snowflake warehouse-warm runs).

    Returns a fully-populated :class:`SourceProfile`. Raises whatever
    the warehouse adapter raises if the table doesn't exist —
    callers (the mapper UI) should catch + display.
    """
    typed = list_columns_typed(warehouse, qualified_table)
    if not typed:
        raise RuntimeError(
            f"profile_source: no columns returned for {qualified_table!r} — does the table exist?"
        )

    # Filter audit columns unless explicitly included.
    business_cols = [
        (n, t) for n, t in typed if include_audit_cols or not n.startswith(_AUDIT_PREFIX)
    ]
    if not business_cols:
        raise RuntimeError(
            f"profile_source: no business columns in {qualified_table!r} (only audit columns found)"
        )

    # ---- Stage 1: batched aggregation ------------------------------------
    # Build SELECT of: COUNT(*), per-col (non-null count, distinct count, min, max).
    # Identifiers are kept UNQUOTED — Snowflake case-folds unquoted names
    # to upper to match its stored identifiers, and DuckDB matches
    # unquoted names case-insensitively. Quoting them with double-quotes
    # forces literal-case matching, which breaks against Snowflake's
    # stored UPPER schema (the case Jatin hit on aetna RAW_CLAIMS).
    # We accept the trade-off that column names with reserved words or
    # special characters won't profile — guard against that explicitly.
    for n, _ in business_cols:
        if not _SAFE_IDENT.match(n):
            raise RuntimeError(
                f"profile_source: column name {n!r} contains characters that "
                f"require quoting; profiler currently only supports "
                f"alphanumeric + underscore identifiers."
            )
    agg_parts: list[str] = ["COUNT(*) AS __total"]
    for n, _ in business_cols:
        agg_parts.append(f"COUNT({n}) AS {n}__nn")
        agg_parts.append(f"COUNT(DISTINCT {n}) AS {n}__dc")
        agg_parts.append(f"MIN({n}) AS {n}__min")
        agg_parts.append(f"MAX({n}) AS {n}__max")
    agg_sql = "SELECT " + ", ".join(agg_parts) + f" FROM {qualified_table}"
    agg_rows = warehouse.query(agg_sql)
    if not agg_rows:
        raise RuntimeError("profile_source: aggregation query returned no rows")
    agg = agg_rows[0]

    # Snowflake folds unquoted identifiers to upper, but our quoting
    # should preserve case. The adapter normalises keys to lowercase
    # for portability, but we look up via lower(name) defensively.
    def _agg_get(col: str, suffix: str) -> Any:
        for k in (f"{col}__{suffix}", f"{col.lower()}__{suffix}", f"{col.upper()}__{suffix}"):
            if k in agg:
                return agg[k]
        return None

    total_rows = int(agg.get("__total") or agg.get("__TOTAL") or agg.get("total") or 0)

    # ---- Stage 2: per-column sample queries ------------------------------
    columns: list[ColumnProfile] = []
    for col_name, raw_type in business_cols:
        non_null = int(_agg_get(col_name, "nn") or 0)
        distinct = int(_agg_get(col_name, "dc") or 0)
        null_count = total_rows - non_null
        null_pct = round(100.0 * null_count / total_rows, 2) if total_rows else 0.0
        min_v = _agg_get(col_name, "min")
        max_v = _agg_get(col_name, "max")

        sample_values: tuple[Any, ...] = ()
        if non_null > 0:
            try:
                sample_rows = warehouse.query(
                    f"SELECT DISTINCT {col_name} AS v "
                    f"FROM {qualified_table} "
                    f"WHERE {col_name} IS NOT NULL "
                    f"LIMIT {int(sample_limit)}"
                )
                sample_values = tuple(_coerce(r["v"]) for r in sample_rows if "v" in r)
            except Exception as e:
                _log.warning(
                    "mapper.profile.sample_failed",
                    table=qualified_table,
                    column=col_name,
                    error=str(e),
                )

        hint = infer_semantic_hint(col_name, list(sample_values))
        columns.append(
            ColumnProfile(
                name=col_name,
                raw_type=raw_type,
                logical_type=normalize_type(raw_type),
                total_rows=total_rows,
                null_count=null_count,
                null_pct=null_pct,
                distinct_count=distinct,
                sample_values=sample_values,
                min_value=_coerce(min_v),
                max_value=_coerce(max_v),
                semantic_hint=hint,
            )
        )

    profile = SourceProfile(
        qualified_table=qualified_table,
        total_rows=total_rows,
        columns=tuple(columns),
        profiled_at=datetime.now(UTC),
        notes=tuple(notes or ()),
    )
    _log.info(
        "mapper.profile.done",
        table=qualified_table,
        total_rows=total_rows,
        column_count=len(columns),
        with_hints=sum(1 for c in columns if c.semantic_hint),
    )
    return profile


# ============================================================================
# INTERNAL
# ============================================================================


def _coerce(v: Any) -> Any:
    """Coerce sample / min / max values to JSON-friendly scalars."""
    if v is None:
        return None
    if isinstance(v, str | int | float | bool):
        return v
    if isinstance(v, datetime | date):
        return v.isoformat()
    # Decimal / numpy / pandas-Timestamp etc. — fall through to str.
    try:
        return float(v)
    except (TypeError, ValueError):
        return str(v)
