"""Canonical Product Catalogue validators — Phase 22 hardening.

Strict validators that check an inbound file against the canonical
contract BEFORE the loader touches Snowflake.  Each validator returns a
:class:`ValidationReport` with:

  * ``ok`` — all checks passed
  * ``errors[]`` — fatal · upload blocked
  * ``warnings[]`` — non-fatal · upload proceeds

Both errors and warnings carry ``reason`` + ``remediation`` so the UI
can render "what's wrong + how to fix it" without exposing a Python
traceback to the operator.

A unified ``smart_validate(src, filename_hint)`` dispatches based on the
file extension.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import IO, Any

import pandas as pd

from datalink.admin.canonical_spec import (
    DATASET_COLUMN_ALIASES,
    DATASET_COLUMNS_REQUIRED,
    FIELD_COLUMN_ALIASES,
    FIELD_COLUMNS_REQUIRED,
    JSON_FIELD_KEY_ALIASES,
    find_column,
    find_key,
)

# ---------------------------------------------------------------------------
# Report shape
# ---------------------------------------------------------------------------


@dataclass
class ValidationIssue:
    severity: str  # "error" | "warning"
    where: str  # e.g. "sheet:Datasets_Master" / "row 12"
    reason: str  # human-readable
    remediation: str  # what to do about it


@dataclass
class ValidationReport:
    format_detected: str = ""
    ok: bool = True
    errors: list[ValidationIssue] = field(default_factory=list)
    warnings: list[ValidationIssue] = field(default_factory=list)

    def add_error(self, where: str, reason: str, remediation: str) -> None:
        self.errors.append(ValidationIssue("error", where, reason, remediation))
        self.ok = False

    def add_warning(self, where: str, reason: str, remediation: str) -> None:
        self.warnings.append(ValidationIssue("warning", where, reason, remediation))

    def has_errors(self) -> bool:
        return bool(self.errors)


# ---------------------------------------------------------------------------
# Format-specific validators
# ---------------------------------------------------------------------------


def _validate_dataset_columns(df: pd.DataFrame, sheet_label: str, rpt: ValidationReport) -> bool:
    """Check the Datasets-Master columns and emit errors for missing required ones.
    Returns True if all required columns are present."""
    cols = list(df.columns)
    ok = True
    for req in DATASET_COLUMNS_REQUIRED:
        aliases = DATASET_COLUMN_ALIASES.get(req, {req.lower()})
        if find_column(cols, aliases) is None:
            rpt.add_error(
                sheet_label,
                f"missing required column `{req}` in datasets sheet",
                f"Add a column whose header matches one of: " f"{', '.join(sorted(aliases))}",
            )
            ok = False
    return ok


def _validate_field_columns(df: pd.DataFrame, sheet_label: str, rpt: ValidationReport) -> bool:
    cols = list(df.columns)
    ok = True
    for req in FIELD_COLUMNS_REQUIRED:
        aliases = FIELD_COLUMN_ALIASES.get(req, {req.lower()})
        if find_column(cols, aliases) is None:
            rpt.add_error(
                sheet_label,
                f"missing required column `{req}` in fields sheet",
                f"Add a column whose header matches one of: " f"{', '.join(sorted(aliases))}",
            )
            ok = False
    return ok


def _validate_dataset_rows(df: pd.DataFrame, sheet_label: str, rpt: ValidationReport) -> int:
    """Walk rows, count valid ones, surface row-level issues."""
    name_col = find_column(list(df.columns), DATASET_COLUMN_ALIASES["Dataset"])
    if not name_col:
        return 0
    n_real = 0
    for i, row in enumerate(df.itertuples(index=False), 1):
        d = row._asdict() if hasattr(row, "_asdict") else dict(zip(df.columns, row, strict=False))
        name = str(d.get(name_col, "") or "").strip()
        if not name or name.lower() == "dataset":
            continue
        if name.upper() in ("TOTAL", "TOTALS", "SUM", "SUMMARY"):
            continue
        n_real += 1
    if n_real == 0:
        rpt.add_error(
            sheet_label,
            "no dataset rows found (after filtering header / blank / TOTAL rows)",
            "Add at least one dataset row with a non-empty Dataset name and "
            "either Frequency or Fields populated.",
        )
    return n_real


# -----------------------------------------------------------------------
# XLSX (single-sheet canonical — v2 contract)
# -----------------------------------------------------------------------


def validate_xlsx(src: str | Path | IO[bytes]) -> ValidationReport:
    """Validate a single-sheet canonical Field Catalogue xlsx (v2 contract).

    Required: one sheet whose name contains `Field_Catalogue` (or the only
    sheet if the workbook has just one).  Header row must contain `Dataset`
    and `Field name`.  Everything else is optional and derived.
    """
    from datalink.admin.canonical_spec import is_canonical_xlsx_sheet_name

    rpt = ValidationReport(format_detected=".xlsx")
    try:
        if hasattr(src, "seek"):
            try:
                src.seek(0)
            except Exception:
                pass
        xl = pd.ExcelFile(src)
    except Exception as exc:
        rpt.add_error(
            "file",
            f"could not open as Excel: {type(exc).__name__}: {str(exc)[:200]}",
            "Re-export the workbook from Excel as .xlsx (not .xls/.csv with "
            "a fake .xlsx extension).  Confirm the file isn't corrupt or "
            "password-protected.",
        )
        return rpt

    sheets = list(xl.sheet_names)
    # Preferred match: sheet name contains `Field_Catalogue` / `Field Catalogue`.
    # Fallback: if only one sheet exists, treat it as the canonical sheet.
    field_sheet = next((s for s in sheets if is_canonical_xlsx_sheet_name(s)), None)
    if field_sheet is None and len(sheets) == 1:
        field_sheet = sheets[0]

    if not field_sheet:
        rpt.add_error(
            "workbook",
            f"no canonical field-catalogue sheet found (sheets present: {sheets})",
            "Rename your sheet to include the substring `Field_Catalogue` "
            "(e.g. `08_Field_Catalogue`), OR upload a workbook with a single sheet.",
        )
        return rpt

    # Validate the sheet's column shape
    try:
        if hasattr(src, "seek"):
            src.seek(0)
        from datalink.admin.catalogue_loader import _read_sheet_with_header_detection

        fd_df = _read_sheet_with_header_detection(
            src,
            field_sheet,
            ["Dataset", "Field name", "Description"],
        )
    except Exception as exc:
        rpt.add_error(
            "workbook",
            f"failed parsing sheet '{field_sheet}': {type(exc).__name__}: {str(exc)[:200]}",
            "Inspect the workbook in Excel — confirm the header row contains "
            "the expected column names, with no merged cells crossing the header.",
        )
        return rpt

    if fd_df is None:
        rpt.add_error(
            f"sheet:{field_sheet}",
            "could not locate the header row",
            "Ensure the header row contains at least: Dataset, Field name "
            "(Description is recommended).",
        )
        return rpt

    if _validate_field_columns(fd_df, f"sheet:{field_sheet}", rpt):
        # Count distinct datasets (after forward-fill) and field rows
        ds_col = find_column(list(fd_df.columns), FIELD_COLUMN_ALIASES["Dataset"])
        fn_col = find_column(list(fd_df.columns), FIELD_COLUMN_ALIASES["Field name"])
        if ds_col and fn_col:
            ffilled = fd_df[ds_col].ffill()
            distinct_ds = {
                str(v).strip()
                for v in ffilled.tolist()
                if str(v or "").strip() and str(v).strip().lower() not in ("dataset", "nan")
            }
            # Filter out title artefacts
            distinct_ds = {
                d
                for d in distinct_ds
                if not d.lower().startswith("field catalogue")
                and not d.lower().startswith("every field")
                and d.upper() not in ("TOTAL", "TOTALS", "SUM", "SUMMARY")
            }
            n_fields = sum(
                1
                for _, r in fd_df.iterrows()
                if str(r.get(fn_col, "") or "").strip()
                and str(r.get(fn_col, "")).strip().lower() not in ("field name", "field")
            )
            rpt.add_warning(
                f"sheet:{field_sheet}",
                f"{len(distinct_ds)} dataset(s) · {n_fields} field row(s) detected",
                "",
            )
            if not distinct_ds:
                rpt.add_error(
                    f"sheet:{field_sheet}",
                    "no datasets found after parsing the sheet",
                    "Add at least one row with a non-empty Dataset name (on the first row of each group).",
                )

    return rpt


# -----------------------------------------------------------------------
# CSV / TSV / PSV (denormalized flat)
# -----------------------------------------------------------------------


CSV_REQUIRED_COLUMNS_DATASET = ["Dataset"]
CSV_REQUIRED_COLUMNS_FIELD = ["Field name"]


def validate_csv_flat(src: str | Path | IO[bytes], sep: str = ",") -> ValidationReport:
    rpt = ValidationReport(format_detected=".csv" if sep == "," else ".tsv/psv")
    try:
        from datalink.admin.catalogue_loader import _read_csv_safe

        if hasattr(src, "seek"):
            try:
                src.seek(0)
            except Exception:
                pass
        df, _enc = _read_csv_safe(src, sep=sep, nrows=5)  # peek first 5 rows
    except Exception as exc:
        rpt.add_error(
            "file",
            f"could not parse CSV/TSV/PSV: {type(exc).__name__}: {str(exc)[:200]}",
            "Verify the file is plain text with a header row.  Confirm the "
            "delimiter matches the extension (.csv = comma, .tsv = tab, "
            ".psv = pipe).",
        )
        return rpt

    cols = list(df.columns)
    required_all = CSV_REQUIRED_COLUMNS_DATASET + CSV_REQUIRED_COLUMNS_FIELD
    for req in required_all:
        canon = req
        aliases = DATASET_COLUMN_ALIASES.get(req) or FIELD_COLUMN_ALIASES.get(req) or {req.lower()}
        if find_column(cols, aliases) is None:
            rpt.add_error(
                "header",
                f"missing required column `{canon}`",
                f"Add a column whose header matches one of: "
                f"{', '.join(sorted(aliases))}.  See the canonical sample "
                "for the full column list.",
            )

    if not rpt.has_errors():
        rpt.add_warning("header", f"{len(cols)} columns detected", "")

    return rpt


# -----------------------------------------------------------------------
# JSON (2-array canonical)
# -----------------------------------------------------------------------


def validate_json(src: str | Path | IO[bytes]) -> ValidationReport:
    rpt = ValidationReport(format_detected=".json")
    try:
        if isinstance(src, (str, Path)):
            text = Path(str(src)).read_text(encoding="utf-8")
        else:
            if hasattr(src, "seek"):
                src.seek(0)
            raw = src.read()
            text = raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else str(raw)
        data = json.loads(text)
    except json.JSONDecodeError as exc:
        rpt.add_error(
            "file",
            f"invalid JSON: {str(exc)[:200]}",
            "Validate the file with a JSON linter (e.g. jq, or paste at "
            "jsonlint.com) before re-uploading.",
        )
        return rpt
    except Exception as exc:
        rpt.add_error(
            "file",
            f"could not read JSON: {type(exc).__name__}: {str(exc)[:200]}",
            "Check file encoding (UTF-8 expected) and re-upload.",
        )
        return rpt

    # v2: canonical shape is { "field_catalogue": [...] } — dataset rows are
    # derived from field rows.  Back-compat: a bare top-level array of field
    # objects is also accepted; legacy `datasets_master` array is honoured
    # but not required.
    fd_arr: list[Any] = []
    ds_arr: list[Any] = []
    if isinstance(data, list):
        fd_arr = data
    elif isinstance(data, dict):
        if "field_catalogue" in data:
            fd_arr = data.get("field_catalogue") or []
        elif "fields" in data:  # tolerate alternative key
            fd_arr = data.get("fields") or []
        ds_arr = data.get("datasets_master") or data.get("datasets") or []
    else:
        rpt.add_error(
            "root",
            f"top-level JSON must be either an object with `field_catalogue` "
            f"array or a bare array of field objects (got {type(data).__name__})",
            'Wrap your data: `{"field_catalogue": [...]}` or just `[...]`.',
        )
        return rpt

    if not isinstance(fd_arr, list):
        rpt.add_error("root.field_catalogue", "must be a JSON array", "Wrap rows in `[...]`.")
    if ds_arr and not isinstance(ds_arr, list):
        rpt.add_error(
            "root.datasets_master", "must be a JSON array if present", "Wrap rows in `[...]`."
        )
    if rpt.has_errors():
        return rpt

    if not fd_arr:
        rpt.add_error(
            "root.field_catalogue",
            "no field records found",
            'Add at least one field object: `{"field_catalogue": [{"dataset": "...", "field_name": "..."}]}`.',
        )

    # Spot-check first field object — only Dataset + Field name required.
    if fd_arr:
        keys = list(fd_arr[0].keys()) if isinstance(fd_arr[0], dict) else []
        for req in ("Dataset", "Field name"):
            aliases = JSON_FIELD_KEY_ALIASES.get(req) or {req.lower()}
            if find_key(keys, aliases) is None:
                rpt.add_error(
                    "field_catalogue[0]",
                    f"missing required key for `{req}`",
                    f"Add one of: {', '.join(sorted(aliases))} to every field row.",
                )
    if not rpt.has_errors():
        rpt.add_warning(
            "summary",
            f"{len(fd_arr)} field row(s)"
            + (f" · {len(ds_arr)} legacy dataset row(s)" if ds_arr else "")
            + " detected",
            "",
        )
    return rpt


# -----------------------------------------------------------------------
# JSONL (line-delimited, mixed-kind records)
# -----------------------------------------------------------------------


def validate_jsonl(src: str | Path | IO[bytes]) -> ValidationReport:
    rpt = ValidationReport(format_detected=".jsonl")
    try:
        if isinstance(src, (str, Path)):
            text = Path(str(src)).read_text(encoding="utf-8")
        else:
            if hasattr(src, "seek"):
                src.seek(0)
            raw = src.read()
            text = raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else str(raw)
    except Exception as exc:
        rpt.add_error(
            "file",
            f"could not read JSONL: {str(exc)[:200]}",
            "Check file encoding (UTF-8 expected).",
        )
        return rpt

    # v2: every record is treated as a field record by default.
    # Back-compat: if a `_kind` discriminator is present, "dataset" records
    # contribute metadata; "field" records are the canonical entries.
    n_ds, n_fd, n_unkind = 0, 0, 0
    first_field_keys: list[str] = []
    parse_errors: list[tuple[int, str]] = []
    for i, line in enumerate(text.splitlines(), 1):
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError as exc:
            parse_errors.append((i, str(exc)[:100]))
            continue
        if not isinstance(obj, dict):
            continue
        kind = str(obj.get("_kind") or "").strip().lower()
        if kind == "dataset":
            n_ds += 1
        elif kind == "field":
            n_fd += 1
            if not first_field_keys:
                first_field_keys = list(obj.keys())
        else:
            # No discriminator → treat as field record (v2 default).
            n_unkind += 1
            if not first_field_keys:
                first_field_keys = list(obj.keys())

    if parse_errors:
        rpt.add_error(
            "file",
            f"{len(parse_errors)} line(s) failed JSON parse (first: line "
            f"{parse_errors[0][0]} — {parse_errors[0][1]})",
            "Each line must be a valid standalone JSON object.  Run "
            "`jq -c . < file.jsonl > clean.jsonl` to validate.",
        )

    effective_field_count = n_fd + n_unkind
    if effective_field_count == 0:
        rpt.add_error(
            "records",
            "no field records found",
            'Add at least one field record: `{"dataset": "...", "field_name": "..."}`.',
        )

    # Validate that field records have the minimum required keys.
    if first_field_keys:
        for req in ("Dataset", "Field name"):
            aliases = JSON_FIELD_KEY_ALIASES.get(req) or {req.lower()}
            if find_key(first_field_keys, aliases) is None:
                rpt.add_error(
                    "records",
                    f"first field record is missing key for `{req}`",
                    f"Add one of: {', '.join(sorted(aliases))} to every record.",
                )

    if not rpt.has_errors():
        summary = f"{effective_field_count} field record(s)"
        if n_ds:
            summary += f" · {n_ds} legacy dataset record(s)"
        rpt.add_warning("summary", summary + " detected", "")
    return rpt


# -----------------------------------------------------------------------
# Smart dispatcher — picks the right validator by extension
# -----------------------------------------------------------------------


def smart_validate(
    src: str | Path | IO[bytes],
    *,
    filename_hint: str = "",
) -> ValidationReport:
    """Route to the right validator by extension.  Returns a populated
    :class:`ValidationReport` regardless of outcome."""
    fname = filename_hint or (str(src) if isinstance(src, (str, Path)) else "")
    lower = fname.lower()

    if lower.endswith((".xlsx", ".xls", ".xlsm")):
        return validate_xlsx(src)
    if lower.endswith(".csv"):
        return validate_csv_flat(src, sep=",")
    if lower.endswith(".tsv"):
        return validate_csv_flat(src, sep="\t")
    if lower.endswith(".psv"):
        return validate_csv_flat(src, sep="|")
    if lower.endswith(".json"):
        return validate_json(src)
    if lower.endswith((".jsonl", ".ndjson")):
        return validate_jsonl(src)

    rpt = ValidationReport(format_detected="unknown")
    rpt.add_error(
        "file",
        f"unsupported file extension: `{fname.split('.')[-1] if '.' in fname else '(none)'}`",
        "Supported: .xlsx · .csv · .tsv · .psv · .json · .jsonl.  Download a "
        "canonical sample from the upload dialog to see the expected shape.",
    )
    return rpt
