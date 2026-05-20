"""Canonical Product Catalogue — the published format contract (v2, single-sheet).

Single source of truth for what "canonical" means across all supported
file types.  All loaders + validators read from this module so we never
drift between the spec, the validator, the samples, and the loader code.

**v2 contract (matches the operator-shipped artifact, May 2026):**

The canonical workbook ships a **single sheet** — `08_Field_Catalogue` — with
one row per FIELD.  The dataset-level table is **derived** at load time by
grouping field rows by their `Dataset` column (which uses Excel's "filled
on first row only" grouped layout, with forward-fill handled by the loader).

  Per-field columns:  Dataset · Field name · Req / Opt · Description ·
                      Additional notes · Example · Frequency

  Per-dataset metadata (derived from each group):
    - Frequency        — first non-empty value in the group
    - Fields           — group row count
    - Required count   — rows where Req / Opt = Required (case-insensitive)
    - Optional count   — group_size − Required count

Product-line linkage (the old "Used by" column) is **no longer part of the
canonical catalogue**.  It is set in DMD per dataset after load.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

# ---------------------------------------------------------------------------
# Required + optional columns
# ---------------------------------------------------------------------------

# Per-FIELD columns (the only thing the operator ships).
FIELD_COLUMNS_REQUIRED: Final[list[str]] = [
    "Dataset",  # display name e.g. "Membership" (filled on first row of each group)
    "Field name",  # display name of the field e.g. "Payer Name"
]
FIELD_COLUMNS_OPTIONAL: Final[list[str]] = [
    "Req / Opt",  # Required / Recommended / Optional / Conditional
    "Description",
    "Additional notes",
    "Example",
    "Frequency",  # Monthly / Weekly / Daily / Real-time
]

# Per-DATASET columns — only honoured if the operator ships them inline
# (CSV with extra columns, or a JSON object with extra keys).  None of these
# are required in v2; the loader derives Frequency / Fields / Required /
# Optional from the per-field rows.
DATASET_COLUMNS_OPTIONAL: Final[list[str]] = [
    "Frequency",  # Monthly / Weekly / Daily / Real-time
    "Fields",  # int — total fields in the dataset
    "Required",  # int — count of required fields
    "Optional",  # int — count of optional fields
    "Category",  # e.g. "Member" / "Claims" / "Provider"
    "Used by",  # legacy: product-line codes — IGNORED in v2 (set in DMD instead)
    "Notes",  # free-text description
]

# v2: kept for back-compat callers that import this name.  Now contains ONLY
# `Dataset` — Frequency / Fields / Used by are no longer required.
DATASET_COLUMNS_REQUIRED: Final[list[str]] = ["Dataset"]


# Preferred sheet name and accepted aliases for the canonical xlsx.
XLSX_PREFERRED_SHEET: Final[str] = "08_Field_Catalogue"
XLSX_SHEET_NAME_TOKENS: Final[list[str]] = [
    # any sheet name whose lowercased form CONTAINS one of these tokens
    # is treated as the canonical field-catalogue sheet
    "field_catalogue",
    "field catalogue",
    "field catalog",
    "fields catalogue",
    "fieldcatalogue",
    "08_field_catalogue",
    "catalogue",  # last-resort — used only if nothing more specific matched
]


# ---------------------------------------------------------------------------
# Per-format spec — readable + machine-checkable
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FormatSpec:
    """Operator-readable contract for one supported upload format."""

    extension: str  # file extension including dot
    display_name: str  # how we name it in the UI
    summary: str  # one-sentence pitch
    shape_description: str  # multi-paragraph spec
    sample_filename: str  # filename in data/sample/canonical_templates/


XLSX_SPEC: Final = FormatSpec(
    extension=".xlsx",
    display_name="Excel (.xlsx) — single-sheet Field Catalogue",
    summary="One sheet named `08_Field_Catalogue` (or any sheet whose name contains `Field_Catalogue`).  Matches what the operational DB architect ships.",
    shape_description="""\
**Single sheet — name must contain `Field_Catalogue`** (e.g. `08_Field_Catalogue`)

  Required columns: Dataset · Field name
  Optional columns: Req / Opt · Description · Additional notes · Example · Frequency

  The `Dataset` column uses Excel's grouped layout — fill it ONLY on the first
  row of each dataset's block; subsequent rows for the same dataset leave it
  blank.  The loader forward-fills automatically.

  The `Frequency` column is read PER ROW but the per-dataset value is taken
  from the FIRST non-empty value in each group.

A title row above the header row is fine — the loader auto-detects the header
via fuzzy column-name match.

**No `Used by` / product-line column is required.**  Product-line mapping
happens in the Data Model Designer after the catalogue is loaded.
""",
    sample_filename="canonical_catalogue.xlsx",
)

CSV_SPEC: Final = FormatSpec(
    extension=".csv",
    display_name="CSV (.csv) — denormalized flat",
    summary="One row per (dataset, field) pair.  Dataset column repeats for every field in the same dataset.",
    shape_description="""\
A single CSV with **one row per FIELD**.

  Required columns: Dataset · Field name
  Optional columns: Req / Opt · Description · Additional notes · Example ·
                    Frequency · Category · Notes · Used by

  Unlike the xlsx layout, the Dataset column should be filled on EVERY row
  (no fill-down) — CSV has no concept of grouped cells.

  Encoding: UTF-8 preferred.  UTF-8-BOM / CP1252 / Latin-1 are auto-detected.

Example (3 rows for Membership):

  Dataset,Field name,Req / Opt,Description,Frequency
  Membership,Payer Name,Required,Name of the member's primary insurer,Monthly
  Membership,Member Card ID,Required,Insurer's unique identification number for the member,Monthly
  Membership,Member First Name,Required,Member's legal first name,Monthly
""",
    sample_filename="canonical_catalogue.csv",
)

TSV_SPEC: Final = FormatSpec(
    extension=".tsv",
    display_name="TSV (.tsv) — denormalized flat",
    summary="Same shape as CSV — tab-separated.",
    shape_description=("Tab-separated equivalent of the canonical CSV format.  See CSV spec."),
    sample_filename="canonical_catalogue.tsv",
)

PSV_SPEC: Final = FormatSpec(
    extension=".psv",
    display_name="PSV (.psv) — denormalized flat",
    summary="Same shape as CSV — pipe-separated.",
    shape_description=("Pipe-separated equivalent of the canonical CSV format.  See CSV spec."),
    sample_filename="canonical_catalogue.psv",
)

JSON_SPEC: Final = FormatSpec(
    extension=".json",
    display_name="JSON (.json) — single field-catalogue array",
    summary="Top-level object with one array: `field_catalogue`.  Dataset rows are derived from field rows.",
    shape_description="""\
A single JSON object with one top-level array (`field_catalogue`):

  {
    "field_catalogue": [
      {
        "dataset": "Membership",
        "field_name": "Payer Name",
        "req_opt": "Required",
        "description": "Name of the member's primary insurer",
        "additional_notes": "",
        "example": "Aetna Health Inc.",
        "frequency": "Monthly"
      },
      {
        "dataset": "Membership",
        "field_name": "Member Card ID",
        "req_opt": "Required",
        "description": "Insurer's unique identification number",
        "frequency": "Monthly"
      }
    ]
  }

  Required keys per field row: dataset · field_name
  Optional keys per field row: req_opt · description · additional_notes · example · frequency

Back-compat: a top-level array (no wrapping object) is also accepted — the
loader treats every element as a field row.

snake_case keys preferred but Title Case (`Dataset` / `Field name`) is also accepted.
""",
    sample_filename="canonical_catalogue.json",
)

JSONL_SPEC: Final = FormatSpec(
    extension=".jsonl",
    display_name="JSONL (.jsonl) — line-delimited field records",
    summary="One JSON object per line, each describing a field.  Dataset rows are derived.",
    shape_description="""\
One JSON object per line, each describing a field:

  {"dataset": "Membership", "field_name": "Payer Name",     "req_opt": "Required", "description": "...", "frequency": "Monthly"}
  {"dataset": "Membership", "field_name": "Member Card ID", "req_opt": "Required", "description": "...", "frequency": "Monthly"}
  {"dataset": "Claims",     "field_name": "Claim ID",       "req_opt": "Required", "description": "...", "frequency": "Daily"}

  Required keys per record: dataset · field_name
  Optional keys per record: req_opt · description · additional_notes · example · frequency

Back-compat: a `_kind` discriminator key with value `"field"` is accepted but
not required.  Records with `_kind="dataset"` are honoured (dataset-level
metadata override) but are now optional — the loader derives the master
table from field rows by default.
""",
    sample_filename="canonical_catalogue.jsonl",
)


ALL_SPECS: Final[list[FormatSpec]] = [
    XLSX_SPEC,
    CSV_SPEC,
    TSV_SPEC,
    PSV_SPEC,
    JSON_SPEC,
    JSONL_SPEC,
]


def spec_for_extension(filename_or_ext: str) -> FormatSpec | None:
    """Look up the spec for a given file extension or filename."""
    lower = filename_or_ext.lower()
    for s in ALL_SPECS:
        if lower.endswith(s.extension):
            return s
    return None


# ---------------------------------------------------------------------------
# Helpers — header normalization (used by validators and loaders)
# ---------------------------------------------------------------------------


def normalize_header(s: str) -> str:
    """Lowercase + strip parens/non-alphanum → space.  Used for case-insensitive,
    punctuation-insensitive column-name matching against the canonical names."""
    import re

    return re.sub(r"[^a-z0-9]+", " ", str(s or "").lower()).strip()


# Canonical → set of acceptable header normalizations.
DATASET_COLUMN_ALIASES: Final[dict[str, set[str]]] = {
    "Dataset": {"dataset", "dataset name"},
    "Frequency": {"frequency", "freq", "default frequency"},
    "Fields": {"fields", "field count", "total fields"},
    "Used by": {"used by", "used", "usedby"},
    "Required": {"required", "required fields"},
    "Optional": {"optional", "optional fields"},
    "Category": {"category"},
    "Notes": {"notes", "description", "remarks"},
}

FIELD_COLUMN_ALIASES: Final[dict[str, set[str]]] = {
    "Dataset": {"dataset", "dataset name"},
    "Field name": {"field name", "fields", "field s", "field", "name"},
    "Req / Opt": {"req opt", "req", "required", "use", "mandatory", "requirement"},
    "Description": {"description", "definition"},
    "Additional notes": {"additional notes", "notes", "additional"},
    "Example": {"example", "sample", "examples"},
    "Frequency": {"frequency", "freq"},
}


def find_column(df_columns: list[str], aliases: set[str]) -> str | None:
    """Locate the first DataFrame column whose normalized header is in
    ``aliases``.  Returns the original column name or None."""
    for raw in df_columns:
        if normalize_header(raw) in aliases:
            return raw
    return None


def is_canonical_xlsx_sheet_name(name: str) -> bool:
    """True if ``name`` looks like the canonical field-catalogue sheet."""
    n = (name or "").lower().replace("-", "_").replace(" ", "_")
    return any(tok.replace(" ", "_") in n for tok in XLSX_SHEET_NAME_TOKENS)


# JSON / JSONL — snake_case key aliases (canonical preferred form)
JSON_DATASET_KEY_ALIASES: Final[dict[str, set[str]]] = {
    "Dataset": {"dataset", "dataset_name", "name"},
    "Frequency": {"frequency", "freq", "default_frequency"},
    "Fields": {"fields", "total_fields", "field_count"},
    "Used by": {"used_by", "used by", "usedby"},
    "Required": {"required", "required_fields"},
    "Optional": {"optional", "optional_fields"},
    "Category": {"category"},
    "Notes": {"notes", "description"},
}

JSON_FIELD_KEY_ALIASES: Final[dict[str, set[str]]] = {
    "Dataset": {"dataset", "dataset_name"},
    "Field name": {"field_name", "field name", "field", "name"},
    "Req / Opt": {"req_opt", "req opt", "requirement", "required", "use"},
    "Description": {"description"},
    "Additional notes": {"additional_notes", "additional notes", "notes"},
    "Example": {"example", "sample"},
    "Frequency": {"frequency", "freq"},
}


def find_key(obj_keys: list[str], aliases: set[str]) -> str | None:
    """Locate the first JSON object key whose lowercase form is in aliases."""
    for raw in obj_keys:
        if str(raw).lower() in aliases:
            return raw
    return None


# ---------------------------------------------------------------------------
# Schema hashing — used to detect exact-duplicate vs schema-changed uploads
# ---------------------------------------------------------------------------


def compute_dataset_schema_hash(
    dataset_code: str,
    fields: list[dict],
) -> str:
    """Deterministic SHA-256 hash of a dataset's STRUCTURAL schema.

    Includes (structural — changes mean the table shape changed):
        * dataset_code (lowercased)
        * for each field: (field_display_name, requirement) tuple
                          sorted lexicographically so order doesn't matter

    Excludes (cosmetic — changes mean docs/copy edits, not schema changes):
        * description / additional_notes / example
        * field_order
        * frequency / category / used_by

    Two uploads of the same dataset that differ only in field-order or
    description text produce the SAME hash.  Two uploads that add a field,
    remove a field, or change a field's required/optional status produce
    DIFFERENT hashes.

    Field-name extraction is robust to both UI shape (Field name / Req / Opt)
    and JSON shape (field_name / req_opt).
    """
    import hashlib
    import json as _json

    def _field_name(f: dict) -> str:
        for key in ("field_name", "Field name", "name", "bronze_column_name", "field_display_name"):
            v = f.get(key)
            if v is not None and str(v).strip():
                return str(v).strip().lower()
        return ""

    def _req(f: dict) -> str:
        for key in ("req_opt", "Req / Opt", "requirement", "use"):
            v = f.get(key)
            if v is not None and str(v).strip():
                return str(v).strip().lower()
        return "optional"

    tuples = sorted((_field_name(f), _req(f)) for f in fields if _field_name(f))
    payload = _json.dumps(
        {"dataset_code": str(dataset_code).lower().strip(), "fields": tuples},
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def diff_dataset_fields(
    existing_fields: list[dict],
    proposed_fields: list[dict],
) -> dict:
    """Compute a field-level diff between existing and proposed schemas.

    Returns a dict with three sorted lists:
        * added    — fields in proposed but not in existing
        * removed  — fields in existing but not in proposed
        * changed  — fields with same name but different requirement

    Used by the loader to surface "schema changed" alerts to the user
    instead of silently overwriting.
    """

    def _norm_name(f: dict) -> str:
        for key in ("field_name", "Field name", "name", "bronze_column_name", "field_display_name"):
            v = f.get(key)
            if v is not None and str(v).strip():
                return str(v).strip().lower()
        return ""

    def _req(f: dict) -> str:
        for key in ("req_opt", "Req / Opt", "requirement", "use"):
            v = f.get(key)
            if v is not None and str(v).strip():
                return str(v).strip().lower()
        return "optional"

    existing_by_name = {_norm_name(f): f for f in existing_fields if _norm_name(f)}
    proposed_by_name = {_norm_name(f): f for f in proposed_fields if _norm_name(f)}

    added = sorted(n for n in proposed_by_name if n not in existing_by_name)
    removed = sorted(n for n in existing_by_name if n not in proposed_by_name)
    changed: list[str] = []
    for name in proposed_by_name:
        if name in existing_by_name:
            if _req(existing_by_name[name]) != _req(proposed_by_name[name]):
                changed.append(name)

    return {
        "added": added,
        "removed": removed,
        "changed": sorted(changed),
    }
