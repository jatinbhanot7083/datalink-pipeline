"""Catalogue loader — Phase 22, v2 single-sheet contract.

Three call patterns the UI supports:

  1. :func:`load_catalogue_xlsx` — canonical single-sheet workbook
     (``08_Field_Catalogue`` — one row per field, dataset metadata derived
     from group context).  Batch-inserts via ``cursor.executemany``.

  2. :func:`load_mapping_spec` — fallback for older / hand-rolled single-
     sheet workbooks that don't conform to the v2 contract.  Infers the
     dataset from the sheet title or filename and inserts one dataset + N
     fields.

  3. :func:`infer_from_data_file` — last resort.  Sniffs a CSV/PSV/JSON data
     file and constructs a Bronze schema from column names + dtypes.  Marks
     all fields Optional with ``logical_type`` from the inferred pandas
     dtype.

Plus :func:`load_canonical_csv`, :func:`load_canonical_json`,
:func:`load_canonical_jsonl` for the flat-text canonical formats.

:func:`smart_load` inspects the upload and routes to the right loader.

Every loader returns :class:`LoadReport` so the UI can render a single
"✅ 34 datasets · 943 fields" toast.
"""

from __future__ import annotations

import io
import json
import os
import re
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import IO, Any

import pandas as pd

from datalink.logging import get_logger
from datalink.quality.control import CONTROL_SCHEMA

_log = get_logger(__name__)


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------


def _validation_to_load_report(rpt: LoadReport, vrpt) -> LoadReport:
    """Copy ValidationReport errors/warnings into a LoadReport with the same
    severity.  ``vrpt`` is :class:`datalink.admin.canonical_validators.ValidationReport`."""
    for e in vrpt.errors:
        rpt.errors.append(f"[{e.where}] {e.reason} — FIX: {e.remediation}")
    for w in vrpt.warnings:
        if w.reason and w.remediation:
            rpt.warnings.append(f"[{w.where}] {w.reason} — {w.remediation}")
        elif w.reason:
            rpt.warnings.append(f"[{w.where}] {w.reason}")
    return rpt


@dataclass
class LoadReport:
    """Outcome of an upload.  Classifies each dataset into one of four
    buckets so the UI can render an actionable per-dataset summary:

      * datasets_added            — net-new dataset codes inserted
      * datasets_unchanged        — already existed AND schema hash matches  (no-op)
      * datasets_schema_changed   — already existed, schema differs from current  (re-applied; diff captured below)
      * datasets_skipped_duplicate — legacy field, kept for back-compat (alias of unchanged)

    Per-dataset diff details land in ``schema_diffs`` keyed by dataset_code:
      { "membership": {"added": [...], "removed": [...], "changed": [...]} }

    NEVER wipes existing datasets that aren't in the upload — additive by
    design.  Uploading just "lab_orders" to a catalog with 33 datasets
    ends with 34 datasets, not 1.
    """

    source: str
    started_at_iso: str
    datasets_added: int = 0
    datasets_unchanged: int = 0  # NEW — schema-hash matched, no write
    datasets_schema_changed: int = 0  # NEW — re-applied (delta) with diff
    datasets_skipped_duplicate: int = 0  # legacy alias of datasets_unchanged
    fields_added: int = 0
    fields_replaced: int = 0  # NEW — total fields rewritten on schema-change
    fields_skipped_duplicate: int = 0
    product_line_links: int = 0
    routing_rules_added: int = 0
    schema_diffs: dict[str, dict] = field(default_factory=dict)  # NEW
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    inferred: bool = False

    @property
    def succeeded(self) -> bool:
        return not self.errors

    def summary(self) -> str:
        if self.errors:
            return f"❌ load failed · {len(self.errors)} error(s)"
        parts = []
        if self.datasets_added:
            parts.append(f"{self.datasets_added} added")
        if self.datasets_schema_changed:
            parts.append(f"{self.datasets_schema_changed} schema-changed (review diff)")
        if self.datasets_unchanged or self.datasets_skipped_duplicate:
            n_unch = self.datasets_unchanged or self.datasets_skipped_duplicate
            parts.append(f"{n_unch} unchanged")
        if self.fields_added:
            parts.append(f"{self.fields_added} new fields")
        if self.fields_replaced:
            parts.append(f"{self.fields_replaced} fields rewritten")
        if self.product_line_links:
            parts.append(f"{self.product_line_links} product-line links")
        if self.routing_rules_added:
            parts.append(f"{self.routing_rules_added} routing rules")
        return "✅ " + " · ".join(parts) if parts else "✅ no-op"


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# "Used by" abbreviation → product_line_code in CONTROL.product_lines
USED_BY_NORMALIZE = {
    "CC": "CC",
    "CONNECTCARE": "CC",
    "EVOKECONNECTCARE": "CC",
    "E360": "E360",
    "RBN": "RBN",
    "EC": "EC",
    "ESV": "ESV",
    # ConnectCare sub-modules — only set if explicitly mentioned
    "CM": "CC_CM",
    "UM": "CC_UM",
    "AG": "CC_AG",
    "PROVIDER": "CC_PROV",
    "MEMBER": "CC_MBR",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _read_csv_safe(src, **kwargs) -> tuple[pd.DataFrame, str]:
    """Read CSV/TSV/PSV with graceful encoding fallback.

    Returns ``(df, encoding_used)``.  Tries in order:

      utf-8-sig → utf-8 → cp1252 → latin-1

    ``utf-8-sig`` handles UTF-8 with a BOM byte sequence Excel often
    prepends.  ``latin-1`` accepts any byte sequence (never raises
    UnicodeDecodeError) — guaranteed last-chance success, possibly with
    minor character mangling on truly weird files.  We surface the
    encoding actually used so the UI can warn the operator.

    Non-encoding errors (delimiter parse failures, missing columns, etc.)
    bubble up as the first attempt's exception.
    """
    encodings = ("utf-8-sig", "utf-8", "cp1252", "latin-1")
    first_non_encoding_err: Exception | None = None
    last_unicode_err: UnicodeDecodeError | None = None
    for enc in encodings:
        try:
            if hasattr(src, "seek"):
                try:
                    src.seek(0)
                except Exception:
                    pass
            df = pd.read_csv(src, encoding=enc, **kwargs)
            return df, enc
        except UnicodeDecodeError as exc:
            last_unicode_err = exc
            continue
        except Exception as exc:
            if first_non_encoding_err is None:
                first_non_encoding_err = exc
            continue
    if first_non_encoding_err is not None:
        raise first_non_encoding_err
    if last_unicode_err is not None:
        raise last_unicode_err
    raise RuntimeError("CSV read failed with no specific cause captured")


def _read_json_safe(src, **kwargs) -> tuple[pd.DataFrame, str]:
    """Read JSON / JSONL with the same encoding-fallback chain.

    pandas's ``read_json`` accepts ``encoding=`` directly.  Same return
    contract as ``_read_csv_safe``.
    """
    encodings = ("utf-8-sig", "utf-8", "cp1252", "latin-1")
    first_non_encoding_err: Exception | None = None
    last_unicode_err: UnicodeDecodeError | None = None
    for enc in encodings:
        try:
            if hasattr(src, "seek"):
                try:
                    src.seek(0)
                except Exception:
                    pass
            df = pd.read_json(src, encoding=enc, **kwargs)
            return df, enc
        except UnicodeDecodeError as exc:
            last_unicode_err = exc
            continue
        except Exception as exc:
            # JSON errors aren't always UnicodeDecodeError — pandas wraps
            # them.  Keep the first one as the canonical cause.
            if first_non_encoding_err is None:
                first_non_encoding_err = exc
            continue
    if first_non_encoding_err is not None:
        raise first_non_encoding_err
    if last_unicode_err is not None:
        raise last_unicode_err
    raise RuntimeError("JSON read failed with no specific cause captured")


def _wh():
    """Get a writable Warehouse instance.

    Phase 22 fix — try the UI's ``_build_backend`` path first (constructs
    SnowflakeWarehouse directly from env vars, skipping the factory chain
    that pulls in azure-core / azurite which isn't installed in the
    Streamlit container).  Falls back to the full factory for CLI contexts
    where the container restrictions don't apply (host venv has azure).
    """
    try:
        from datalink.ui._query import _build_backend

        return _build_backend(readonly=False)
    except Exception:
        from datalink.adapters.factory import build_adapters
        from datalink.config.loader import load_settings

        return build_adapters(load_settings(env=os.environ.get("DL_ENV", "dev"))).warehouse


def _snowflake_safe_id(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_") or "unknown"


def _normalize_requirement(v: Any) -> str:
    s = str(v or "").strip().lower()
    if s.startswith("req"):
        return "Required"
    if s.startswith("rec"):
        return "Recommended"
    if s.startswith("con"):
        return "Conditional"
    return "Optional"


def _normalize_use_or_requirement(v: Any) -> str:
    """Accept the broader vocabulary that mapping specs ship with:

    - ``M-Mandatory``, ``M``, ``Mandatory``, ``Required`` → ``Required``
    - ``O-Optional``, ``O``, ``Optional`` → ``Optional``
    - ``R-Recommended``, ``Recommended`` → ``Recommended``
    - ``C-Conditional``, ``Conditional`` → ``Conditional``
    """
    s = str(v or "").strip().lower()
    if not s:
        return "Optional"
    head = s.split("-", 1)[0].strip()
    if head in ("m", "mandatory") or s.startswith("man") or s.startswith("req"):
        return "Required"
    if head in ("r", "recommended") or s.startswith("rec"):
        return "Recommended"
    if head in ("c", "conditional") or s.startswith("con"):
        return "Conditional"
    return "Optional"


def _normalize_data_type(raw: str) -> str:
    """Map spec data-type strings → canonical Snowflake-friendly types.

    Spec inputs (case-insensitive):
      ``String``, ``Varchar``, ``Char``, ``Text``                → ``TEXT``
      ``Integer``, ``Int``, ``Bigint``, ``Smallint``             → ``NUMBER``
      ``Decimal``, ``Numeric``, ``Float``, ``Double``, ``Money`` → ``NUMBER``
      ``Boolean``, ``Bit``, ``Bool``                             → ``BOOLEAN``
      ``Date``                                                  → ``DATE``
      ``Datetime``, ``Timestamp``, ``DateTime``                  → ``TIMESTAMP_NTZ``
      ``JSON``, ``Variant``                                      → ``VARIANT``
      anything else                                              → ``TEXT``
    """
    s = (raw or "").upper().strip()
    if not s:
        return "TEXT"
    if any(t in s for t in ("STRING", "VARCHAR", "CHAR", "TEXT", "NVARCHAR", "NCHAR", "CLOB")):
        return "TEXT"
    if any(t in s for t in ("BOOL", "BIT", "FLAG")):
        return "BOOLEAN"
    if any(t in s for t in ("DATETIME", "TIMESTAMP", "DT")):
        return "TIMESTAMP_NTZ"
    if "DATE" in s:
        return "DATE"
    if any(
        t in s for t in ("INT", "NUMBER", "DECIMAL", "NUMERIC", "FLOAT", "DOUBLE", "MONEY", "REAL")
    ):
        return "NUMBER"
    if any(t in s for t in ("JSON", "VARIANT", "OBJECT", "ARRAY")):
        return "VARIANT"
    return "TEXT"


def _parse_used_by_to_codes(cell: Any) -> list[tuple[str, str | None]]:
    """Parse 'CC · E360 · RBN' or 'CC (CM, UM)' → [(code, scope_level), …]."""
    if cell is None or (isinstance(cell, float) and pd.isna(cell)):
        return []
    text = str(cell)
    out: list[tuple[str, str | None]] = []
    seen: set[str] = set()
    for tok in re.split(r"[·•\|;,]", text):
        tok = tok.strip()
        if not tok:
            continue
        m = re.match(r"^(\w+)\s*\(([^)]+)\)$", tok)
        if m:
            base_code = USED_BY_NORMALIZE.get(m.group(1).upper())
            if base_code and base_code not in seen:
                out.append((base_code, None))
                seen.add(base_code)
            for sub in re.split(r"[,/]", m.group(2)):
                sub_key = sub.strip().upper()
                sub_code = USED_BY_NORMALIZE.get(sub_key)
                if sub_code and sub_code not in seen:
                    out.append((sub_code, sub_key))
                    seen.add(sub_code)
            continue
        code = USED_BY_NORMALIZE.get(tok.upper())
        if code and code not in seen:
            out.append((code, None))
            seen.add(code)
    return out


def _seed_product_line_junction(wh, *, xlsx_path: str | Path) -> int:
    """After dataset rows are inserted, parse the Excel 'Used by' column and
    populate CONTROL.dataset_product_lines.  Idempotent: deletes prior rows
    per dataset, then re-inserts."""
    # Re-read the Datasets Master sheet to get the original "Used by" string
    ds_df = _read_sheet_with_header_detection(
        xlsx_path,
        "05_Datasets_Master",
        ["Dataset", "Frequency", "Fields", "Used by", "Category"],
    )
    if ds_df is None:
        return 0
    total_links = 0
    for _, row in ds_df.iterrows():
        name = str(row.get("Dataset", "") or "").strip()
        if not name or name.lower() == "dataset":
            continue
        code = _snowflake_safe_id(name)
        links = _parse_used_by_to_codes(row.get("Used by"))
        wh.execute(
            f"DELETE FROM {CONTROL_SCHEMA}.dataset_product_lines WHERE dataset_code = $code",
            {"code": code},
        )
        for pl_code, scope_level in links:
            wh.execute(
                f"INSERT INTO {CONTROL_SCHEMA}.dataset_product_lines "
                f"(dataset_code, product_line_code, scope_level) "
                f"VALUES ($ds, $pl, $sl)",
                {"ds": code, "pl": pl_code, "sl": scope_level},
            )
            total_links += 1
    return total_links


def _find_header_row(df: pd.DataFrame, expected_tokens: list[str]) -> int | None:
    expected_l = [t.lower() for t in expected_tokens]
    best_idx, best_score = None, 0
    for i in range(min(10, len(df))):
        row_text = " ".join(str(v).lower() for v in df.iloc[i].tolist() if pd.notna(v))
        score = sum(1 for t in expected_l if t in row_text)
        if score > best_score:
            best_idx, best_score = i, score
    return best_idx if best_score >= max(2, len(expected_tokens) // 2) else None


def _read_sheet_with_header_detection(
    src: str | Path | IO, sheet_name: str, expected_tokens: list[str]
) -> pd.DataFrame | None:
    try:
        df_raw = pd.read_excel(src, sheet_name=sheet_name, header=None)
    except Exception as exc:
        _log.warning("catalogue.read_sheet_failed", sheet=sheet_name, err=str(exc)[:200])
        return None
    if df_raw.empty:
        return None
    hdr_idx = _find_header_row(df_raw, expected_tokens)
    if hdr_idx is None:
        return None
    df = pd.read_excel(src, sheet_name=sheet_name, header=hdr_idx)
    df = df.dropna(axis=1, how="all").dropna(axis=0, how="all")
    df = df.loc[:, [c for c in df.columns if not str(c).startswith("Unnamed:")]]
    return df


# ---------------------------------------------------------------------------
# Path 1: canonical xlsx — FAST batched loader
#
# Phase 22 fix: previously this delegated to ``scripts.load_product_catalog``
# which does row-by-row INSERTs (~6 minutes for 33 datasets + 943 fields on
# Snowflake's 100 ms statement overhead).  We now parse the xlsx with pandas
# and use ``cursor.executemany`` for batch inserts → 5–10 seconds end-to-end.
# ---------------------------------------------------------------------------


# Product codes the loader uses for routing rules.  Mirrors the canonical
# loader's list so the routing-rules table shape stays consistent.
_KNOWN_PRODUCTS = {
    "E360": ("postgres", ""),
    "CC": ("sqlserver", ""),
    "CC_CM": ("sqlserver", ""),
    "CC_UM": ("sqlserver", ""),
    "CC_AG": ("sqlserver", ""),
    "CC_PROV": ("sqlserver", ""),
    "CC_MBR": ("sqlserver", ""),
    "RBN": ("postgres", ""),
    "EC": ("postgres", ""),
    "ESV": ("postgres", ""),
}


def _bronze_safe_id(s: str) -> str:
    """Bronze column name slug — same rule as _snowflake_safe_id but kept
    separate to make intent obvious."""
    return _snowflake_safe_id(s)


def load_catalogue_xlsx(
    src: str | Path | IO[bytes],
    *,
    actor: str = "ui:upload",
    catalog_version: int | None = None,
) -> LoadReport:
    """Load the canonical **single-sheet** xlsx via batch executemany (v2 contract).

    v2 — May 2026: the operator-shipped artifact is ONE sheet named
    ``08_Field_Catalogue`` (or any sheet whose name contains
    ``Field_Catalogue``).  Dataset-level rows are **derived** by grouping
    field rows on the ``Dataset`` column (which uses Excel's grouped layout
    — filled on the first row of each block, blank on subsequent rows).

    Per-dataset metadata derived from each group:
      * Frequency  — first non-empty value in the group
      * Fields     — group row count
      * Required   — count where Req / Opt = Required (case-insensitive)
      * Optional   — group_size − Required count

    The legacy ``Used by`` column is no longer required.  If a sheet ships
    it inline (e.g. exported from an older spec) it is honoured; otherwise
    product-line links default to empty and are set later in DMD.

    Performance: ~5–10 s for 34 datasets + 943 fields via cursor.executemany.
    """
    from datalink.admin.canonical_spec import is_canonical_xlsx_sheet_name

    rpt = LoadReport(source="catalogue_xlsx", started_at_iso=datetime.now(UTC).isoformat())

    # ---- Buffer → temp path ------------------------------------------
    tmp_path: Path | None = None
    if not isinstance(src, (str, Path)):
        from tempfile import NamedTemporaryFile

        with NamedTemporaryFile("wb", suffix=".xlsx", delete=False) as tf:
            if hasattr(src, "seek"):
                src.seek(0)
            tf.write(src.read())
            tmp_path = Path(tf.name)
        src_path = tmp_path
    else:
        src_path = Path(src)

    try:
        # ---- Find the canonical field-catalogue sheet ----------------
        try:
            xl = pd.ExcelFile(src_path)
        except Exception as exc:
            rpt.errors.append(
                f"could not open .xlsx: {type(exc).__name__}: {str(exc)[:160]} — "
                "FIX: re-export from Excel as .xlsx and confirm it isn't password-protected."
            )
            return rpt
        sheets = list(xl.sheet_names)
        # Preferred match: name contains "Field_Catalogue" / "Field Catalogue".
        # Last resort: if only one sheet exists, treat it as the canonical one.
        field_sheet = next((s for s in sheets if is_canonical_xlsx_sheet_name(s)), None)
        if field_sheet is None and len(sheets) == 1:
            field_sheet = sheets[0]
        if field_sheet is None:
            rpt.errors.append(
                f"no canonical field-catalogue sheet found (sheets present: {sheets}) — "
                "FIX: rename your sheet to include `Field_Catalogue` (e.g. `08_Field_Catalogue`) "
                "or upload a single-sheet workbook."
            )
            return rpt

        fd_df = _read_sheet_with_header_detection(
            src_path,
            field_sheet,
            ["Dataset", "Field name", "Description"],
        )
        if fd_df is None or fd_df.empty:
            rpt.errors.append(
                f"could not locate the header row in sheet '{field_sheet}' — "
                "FIX: ensure the header row contains at least `Dataset` and `Field name`."
            )
            return rpt

        # ---- Forward-fill the Dataset column (Excel grouped layout) --
        ds_col = next(
            (c for c in fd_df.columns if str(c).strip().lower() in ("dataset", "dataset name")),
            None,
        )
        fn_col = next(
            (
                c
                for c in fd_df.columns
                if str(c).strip().lower() in ("field name", "field", "fields")
            ),
            None,
        )
        if ds_col is None:
            rpt.errors.append(
                f"required column `Dataset` not found in sheet '{field_sheet}' (columns: "
                f"{[str(c) for c in fd_df.columns]}) — "
                "FIX: add a `Dataset` column with the dataset name filled on the first row of each group."
            )
            return rpt
        if fn_col is None:
            rpt.errors.append(
                f"required column `Field name` not found in sheet '{field_sheet}' (columns: "
                f"{[str(c) for c in fd_df.columns]}) — "
                "FIX: add a `Field name` column."
            )
            return rpt
        fd_df = fd_df.copy()
        fd_df[ds_col] = fd_df[ds_col].ffill()

        # ---- Compute catalog_version ---------------------------------
        wh = _wh()
        if catalog_version is None:
            try:
                cur_max = list(
                    wh.query(
                        f"SELECT COALESCE(MAX(catalog_version), 0) c "
                        f"FROM {CONTROL_SCHEMA}.global_bronze_catalog_datasets"
                    )
                )[0]["c"]
                catalog_version = int(cur_max) + 1
            except Exception:
                catalog_version = 1

        # ---- Optional column resolution ------------------------------
        def _col(*tokens: str) -> str | None:
            tset = {t.lower() for t in tokens}
            for c in fd_df.columns:
                if str(c).strip().lower() in tset:
                    return c
            return None

        req_col = _col("req / opt", "req opt", "req", "required", "use", "requirement", "mandatory")
        desc_col = _col("description", "definition")
        notes_col = _col("additional notes", "notes", "additional")
        ex_col = _col("example", "sample", "examples")
        freq_col = _col("frequency", "freq")
        usedby_col = _col("used by", "used")
        cat_col = _col("category")
        masternotes_col = (
            _col("notes", "remarks") if _col("notes", "remarks") != notes_col else None
        )

        # ---- Pass 1: walk field rows in order, group by Dataset ------
        ts_now = datetime.now(UTC).replace(tzinfo=None)
        source_doc = f"xlsx:{src_path.name}"

        # Preserve insertion order; collect per-group context as we go.
        per_dataset: dict[
            str, dict[str, Any]
        ] = {}  # code → {"name", "freq", "req", "opt", "fields", "used_links", "category", "notes"}
        ordered_codes: list[str] = []
        field_rows_pre: list[dict[str, Any]] = []  # filled after we have ds_id assignments

        for _, row in fd_df.iterrows():
            ds_name = str(row.get(ds_col, "") or "").strip()
            field_name = str(row.get(fn_col, "") or "").strip()
            if not ds_name or not field_name:
                continue
            # Drop header row & title artifacts that survived header detection
            if ds_name.lower() in ("dataset", "dataset name") or field_name.lower() in (
                "field name",
                "field",
            ):
                continue
            if ds_name.lower().startswith("field catalogue") or ds_name.lower().startswith(
                "every field"
            ):
                continue
            if ds_name.upper() in ("TOTAL", "TOTALS", "SUM", "SUMMARY"):
                continue
            code = _snowflake_safe_id(ds_name)
            if code not in per_dataset:
                ordered_codes.append(code)
                per_dataset[code] = {
                    "name": ds_name,
                    "freq": "",
                    "req": 0,
                    "opt": 0,
                    "fields": 0,
                    "used_links": [],
                    "category": "",
                    "notes": "",
                }
            agg = per_dataset[code]
            agg["fields"] += 1
            # First non-empty Frequency wins for the group
            if freq_col and not agg["freq"]:
                fv = row.get(freq_col)
                fv_s = str(fv or "").strip()
                if fv_s and fv_s.lower() != "nan" and fv_s != "–" and fv_s != "—":
                    agg["freq"] = fv_s
            # First non-empty Category wins
            if cat_col and not agg["category"]:
                cv = str(row.get(cat_col, "") or "").strip()
                if cv and cv.lower() != "nan":
                    agg["category"] = cv
            # First non-empty Used by wins (back-compat — usually absent in v2)
            if usedby_col and not agg["used_links"]:
                links = _parse_used_by_to_codes(row.get(usedby_col))
                if links:
                    agg["used_links"] = links

            req_norm = _normalize_requirement(row.get(req_col)) if req_col else "Optional"
            if req_norm == "Required":
                agg["req"] += 1
            else:
                agg["opt"] += 1

            field_rows_pre.append(
                {
                    "ds_code": code,
                    "name": field_name,
                    "col": _bronze_safe_id(field_name),
                    "req": req_norm,
                    "type": "TEXT",
                    "desc": str(row.get(desc_col, "") or "")[:1000] if desc_col else "",
                    "notes": str(row.get(notes_col, "") or "")[:1000] if notes_col else "",
                    "ex": str(row.get(ex_col, "") or "")[:200] if ex_col else "",
                    "pii": _looks_like_phi(field_name),
                    "phi": _looks_like_phi(field_name),
                    "bkey": False,
                }
            )

        if not ordered_codes:
            rpt.errors.append(
                "no valid dataset/field rows detected after parsing the sheet — "
                "FIX: confirm rows have non-empty Dataset (on first row of each group) "
                "and Field name values."
            )
            return rpt

        # ---- Build dataset rows (one per group) ----------------------
        dataset_rows: list[dict[str, Any]] = []
        dataset_id_by_code: dict[str, str] = {}
        used_by_by_code: dict[str, list[tuple[str, str | None]]] = {}

        for code in ordered_codes:
            agg = per_dataset[code]
            ds_id = str(uuid.uuid4())
            dataset_id_by_code[code] = ds_id
            used_by_by_code[code] = agg["used_links"]
            used_by_codes = [c for c, _ in agg["used_links"]]
            dataset_rows.append(
                {
                    "id": ds_id,
                    "code": code,
                    "name": agg["name"],
                    "cat": agg["category"],
                    "freq": agg["freq"],
                    "used": json.dumps(used_by_codes),
                    "tot": agg["fields"],
                    "req": agg["req"],
                    "opt": agg["opt"],
                    "notes": agg["notes"],
                    "ver": int(catalog_version),
                    "src": source_doc,
                    "ts": ts_now,
                    "by": actor,
                }
            )

        # ---- Build final field rows (now we have ds_id assignments) --
        field_rows: list[dict[str, Any]] = []
        field_order_by_ds: dict[str, int] = {}
        for pre in field_rows_pre:
            code = pre["ds_code"]
            ds_id = dataset_id_by_code.get(code)
            if not ds_id:
                continue
            field_order_by_ds[code] = field_order_by_ds.get(code, 0) + 1
            field_rows.append(
                {
                    "id": str(uuid.uuid4()),
                    "ds_id": ds_id,
                    "ds_code": code,
                    "ord": field_order_by_ds[code],
                    "name": pre["name"],
                    "col": pre["col"],
                    "req": pre["req"],
                    "type": pre["type"],
                    "desc": pre["desc"],
                    "notes": pre["notes"],
                    "ex": pre["ex"],
                    "pii": pre["pii"],
                    "phi": pre["phi"],
                    "bkey": pre["bkey"],
                    "ver": int(catalog_version),
                    "ts": ts_now,
                }
            )

        # ---- Routing rules + product-line junction (legacy "Used by") ----
        # In v2 catalogues this is empty by default; product-line linkage is
        # set in DMD per-dataset after load.  Honour it only if Used by data
        # was present in the uploaded sheet.
        routing_rows: list[dict[str, Any]] = []
        pl_link_rows: list[dict[str, Any]] = []
        for code, links in used_by_by_code.items():
            for pl_code, scope_level in links:
                pl_link_rows.append({"ds": code, "pl": pl_code, "sl": scope_level})
                if pl_code in _KNOWN_PRODUCTS:
                    tgt_sys, tgt_uri = _KNOWN_PRODUCTS[pl_code]
                    routing_rows.append(
                        {
                            "id": str(uuid.uuid4()),
                            "ds": code,
                            "prod": pl_code,
                            "tsys": tgt_sys,
                            "turi": tgt_uri,
                            "notes": f"Default routing seeded from 'Used by' column ({pl_code})",
                            "ts": ts_now,
                            "by": actor,
                        }
                    )

        # ---- UPSERT per dataset (NEVER wipe untouched datasets) ------
        # For each proposed dataset:
        #   * NOT EXISTS  → INSERT dataset + all fields                (added)
        #   * EXISTS + schema hash matches  → SKIP, no write           (unchanged)
        #   * EXISTS + schema hash differs  → DELETE old fields for THIS
        #     dataset only + INSERT new fields; update master row;
        #     record per-field diff in rpt.schema_diffs                (changed)
        # Datasets present in CONTROL but NOT in the upload are LEFT ALONE.
        from datalink.admin.canonical_spec import (
            compute_dataset_schema_hash,
            diff_dataset_fields,
        )

        # Index proposed field_rows by dataset_code (we built field_rows
        # earlier in this function — they carry "ds_code" + "name" + "req")
        proposed_fields_by_code: dict[str, list[dict[str, Any]]] = {}
        for fr in field_rows:
            proposed_fields_by_code.setdefault(fr["ds_code"], []).append(
                {
                    "field_name": fr["name"],
                    "req_opt": fr["req"],
                }
            )

        conn = wh._connect()
        cur = conn.cursor()
        try:
            for dataset_row in dataset_rows:
                code = dataset_row["code"]
                proposed_fields_this_ds = proposed_fields_by_code.get(code, [])
                proposed_hash = compute_dataset_schema_hash(code, proposed_fields_this_ds)

                # Look up existing master row + existing fields for this code
                existing_master = list(
                    wh.query(
                        f"SELECT dataset_id FROM {CONTROL_SCHEMA}.global_bronze_catalog_datasets "
                        f"WHERE dataset_code = $c",
                        {"c": code},
                    )
                )
                existing_fields_raw = (
                    list(
                        wh.query(
                            f"SELECT field_display_name AS field_name, requirement AS req_opt "
                            f"FROM {CONTROL_SCHEMA}.global_bronze_catalog_fields "
                            f"WHERE dataset_code = $c",
                            {"c": code},
                        )
                    )
                    if existing_master
                    else []
                )
                existing_fields = [
                    {
                        "field_name": str(r.get("field_name") or r.get("FIELD_NAME") or ""),
                        "req_opt": str(r.get("req_opt") or r.get("REQ_OPT") or ""),
                    }
                    for r in existing_fields_raw
                ]
                existing_hash = (
                    compute_dataset_schema_hash(code, existing_fields) if existing_master else None
                )

                if not existing_master:
                    # NET-NEW dataset → INSERT master + fields
                    cur.execute(
                        f"INSERT INTO {CONTROL_SCHEMA}.global_bronze_catalog_datasets "
                        f"(dataset_id, dataset_code, display_name, category, "
                        f" default_frequency, used_by, total_fields, required_fields, "
                        f" optional_fields, notes, is_active, catalog_version, "
                        f" source_doc_uri, registered_at, registered_by) "
                        f"VALUES (%(id)s, %(code)s, %(name)s, %(cat)s, %(freq)s, "
                        f"        %(used)s, %(tot)s, %(req)s, %(opt)s, %(notes)s, "
                        f"        TRUE, %(ver)s, %(src)s, %(ts)s, %(by)s)",
                        dataset_row,
                    )
                    these_field_rows = [fr for fr in field_rows if fr["ds_code"] == code]
                    if these_field_rows:
                        cur.executemany(
                            f"INSERT INTO {CONTROL_SCHEMA}.global_bronze_catalog_fields "
                            f"(field_id, dataset_id, dataset_code, field_order, "
                            f" field_display_name, bronze_column_name, requirement, "
                            f" logical_type, description, additional_notes, example, "
                            f" is_pii, is_phi, is_business_key, catalog_version, "
                            f" registered_at) "
                            f"VALUES (%(id)s, %(ds_id)s, %(ds_code)s, %(ord)s, %(name)s, "
                            f"        %(col)s, %(req)s, %(type)s, %(desc)s, %(notes)s, "
                            f"        %(ex)s, %(pii)s, %(phi)s, %(bkey)s, %(ver)s, %(ts)s)",
                            these_field_rows,
                        )
                    rpt.datasets_added += 1
                    rpt.fields_added += len(these_field_rows)
                    rpt.schema_diffs[code] = {
                        "status": "added",
                        "added": sorted(f["field_name"] for f in proposed_fields_this_ds),
                        "removed": [],
                        "changed": [],
                    }

                elif existing_hash == proposed_hash:
                    # EXACT MATCH → no-op (this is what the user asked for)
                    rpt.datasets_unchanged += 1
                    rpt.datasets_skipped_duplicate += 1  # legacy alias for back-compat
                    rpt.schema_diffs[code] = {
                        "status": "unchanged",
                        "added": [],
                        "removed": [],
                        "changed": [],
                    }

                else:
                    # SCHEMA CHANGED → compute field diff, then re-apply
                    diff = diff_dataset_fields(existing_fields, proposed_fields_this_ds)
                    # Get the existing dataset_id so the rewritten fields stay
                    # linked to the same master row (preserve provenance).
                    existing_ds_id = str(
                        existing_master[0].get("dataset_id") or existing_master[0].get("DATASET_ID")
                    )
                    # Wipe old fields for THIS DATASET ONLY (never touch others)
                    cur.execute(
                        f"DELETE FROM {CONTROL_SCHEMA}.global_bronze_catalog_fields "
                        f"WHERE dataset_code = %(c)s",
                        {"c": code},
                    )
                    # Re-insert all fields under the existing ds_id (preserves
                    # any downstream references to that dataset_id)
                    these_field_rows = [
                        {**fr, "ds_id": existing_ds_id}
                        for fr in field_rows
                        if fr["ds_code"] == code
                    ]
                    if these_field_rows:
                        cur.executemany(
                            f"INSERT INTO {CONTROL_SCHEMA}.global_bronze_catalog_fields "
                            f"(field_id, dataset_id, dataset_code, field_order, "
                            f" field_display_name, bronze_column_name, requirement, "
                            f" logical_type, description, additional_notes, example, "
                            f" is_pii, is_phi, is_business_key, catalog_version, "
                            f" registered_at) "
                            f"VALUES (%(id)s, %(ds_id)s, %(ds_code)s, %(ord)s, %(name)s, "
                            f"        %(col)s, %(req)s, %(type)s, %(desc)s, %(notes)s, "
                            f"        %(ex)s, %(pii)s, %(phi)s, %(bkey)s, %(ver)s, %(ts)s)",
                            these_field_rows,
                        )
                    # Refresh dataset master metadata (counts, freq, etc.)
                    cur.execute(
                        f"UPDATE {CONTROL_SCHEMA}.global_bronze_catalog_datasets SET "
                        f"  display_name = %(name)s, category = %(cat)s, "
                        f"  default_frequency = %(freq)s, used_by = %(used)s, "
                        f"  total_fields = %(tot)s, required_fields = %(req)s, "
                        f"  optional_fields = %(opt)s, notes = %(notes)s, "
                        f"  catalog_version = %(ver)s, source_doc_uri = %(src)s, "
                        f"  registered_at = %(ts)s, registered_by = %(by)s "
                        f"WHERE dataset_code = %(code)s",
                        dataset_row,
                    )
                    rpt.datasets_schema_changed += 1
                    rpt.fields_replaced += len(these_field_rows)
                    rpt.schema_diffs[code] = {"status": "changed", **diff}

            # Routing rules + product-line junctions — these are derived
            # from "Used by" which v2 canonical doesn't carry; in additive
            # mode we only insert NEW links and leave existing ones intact.
            for rr in routing_rows:
                try:
                    cur.execute(
                        f"DELETE FROM {CONTROL_SCHEMA}.onprem_routing_rules "
                        f"WHERE dataset_code = %(ds)s AND downstream_product = %(prod)s "
                        f"AND is_default = TRUE",
                        {"ds": rr["ds"], "prod": rr["prod"]},
                    )
                    cur.execute(
                        f"INSERT INTO {CONTROL_SCHEMA}.onprem_routing_rules "
                        f"(rule_id, dataset_code, downstream_product, target_system, "
                        f" target_uri, is_default, is_active, notes, created_at, created_by) "
                        f"VALUES (%(id)s, %(ds)s, %(prod)s, %(tsys)s, %(turi)s, "
                        f"        TRUE, TRUE, %(notes)s, %(ts)s, %(by)s)",
                        rr,
                    )
                    rpt.routing_rules_added += 1
                except Exception as exc:
                    rpt.warnings.append(f"routing rule {rr['ds']}/{rr['prod']}: {str(exc)[:120]}")

            for pl in pl_link_rows:
                try:
                    cur.execute(
                        f"DELETE FROM {CONTROL_SCHEMA}.dataset_product_lines "
                        f"WHERE dataset_code = %(ds)s AND product_line_code = %(pl)s",
                        {"ds": pl["ds"], "pl": pl["pl"]},
                    )
                    cur.execute(
                        f"INSERT INTO {CONTROL_SCHEMA}.dataset_product_lines "
                        f"(dataset_code, product_line_code, scope_level) "
                        f"VALUES (%(ds)s, %(pl)s, %(sl)s)",
                        pl,
                    )
                    rpt.product_line_links += 1
                except Exception as exc:
                    rpt.warnings.append(f"product line {pl['ds']}/{pl['pl']}: {str(exc)[:120]}")
        finally:
            cur.close()
    finally:
        if tmp_path is not None:
            try:
                tmp_path.unlink()
            except Exception:
                pass

    return rpt


# ---------------------------------------------------------------------------
# Path 2: mapping spec fallback (single-sheet workbook or CSV)
# ---------------------------------------------------------------------------


def _ensure_dataset_row(
    wh,
    *,
    dataset_code: str,
    display_name: str,
    category: str,
    frequency: str,
    total_fields: int,
    notes: str,
    source: str,
    actor: str,
) -> tuple[str, bool]:
    """Insert dataset if absent.  Returns (dataset_id, was_new)."""
    existing = list(
        wh.query(
            f"SELECT dataset_id FROM {CONTROL_SCHEMA}.global_bronze_catalog_datasets "
            f"WHERE dataset_code = $c",
            {"c": dataset_code},
        )
    )
    if existing:
        return str(existing[0]["dataset_id"]), False
    dataset_id = str(uuid.uuid4())
    wh.execute(
        f"INSERT INTO {CONTROL_SCHEMA}.global_bronze_catalog_datasets "
        f"(dataset_id, dataset_code, display_name, category, default_frequency, "
        f" used_by, total_fields, required_fields, optional_fields, notes, "
        f" is_active, catalog_version, source_doc_uri, registered_at, registered_by) "
        f"VALUES ($id, $code, $name, $cat, $freq, "
        f"        '[]', $tot, 0, $opt, $notes, "
        f"        TRUE, 1, $src, $ts, $by)",
        {
            "id": dataset_id,
            "code": dataset_code,
            "name": display_name,
            "cat": category or "Inferred",
            "freq": frequency or "Unknown",
            "tot": total_fields,
            "opt": total_fields,
            "notes": notes,
            "src": source,
            "ts": datetime.now(UTC),
            "by": actor,
        },
    )
    return dataset_id, True


def _insert_field_row(
    wh,
    *,
    dataset_id: str,
    dataset_code: str,
    field_order: int,
    display_name: str,
    requirement: str,
    description: str,
    notes: str,
    example: str,
    is_pii: bool = False,
    is_phi: bool = False,
    bronze_column_name: str | None = None,
    logical_type: str = "TEXT",
) -> bool:
    """Insert field if absent (keyed by (dataset_code, bronze_column_name)).
    Returns True if new.

    Phase 22 — accepts ``bronze_column_name`` (overrides slug-of-display_name)
    and ``logical_type`` (overrides default ``TEXT``).  Used by the mapping
    spec loader to honor explicit ``FILE HEADER`` and ``DATA TYPE`` columns.
    """
    bronze_col = bronze_column_name or _snowflake_safe_id(display_name)
    existing = list(
        wh.query(
            f"SELECT 1 FROM {CONTROL_SCHEMA}.global_bronze_catalog_fields "
            f"WHERE dataset_code = $ds AND bronze_column_name = $col",
            {"ds": dataset_code, "col": bronze_col},
        )
    )
    if existing:
        return False
    wh.execute(
        f"INSERT INTO {CONTROL_SCHEMA}.global_bronze_catalog_fields "
        f"(field_id, dataset_id, dataset_code, field_order, "
        f" field_display_name, bronze_column_name, requirement, logical_type, "
        f" description, additional_notes, example, "
        f" is_pii, is_phi, is_business_key, catalog_version, registered_at) "
        f"VALUES ($id, $ds_id, $ds_code, $ord, "
        f"        $name, $col, $req, $type, "
        f"        $desc, $notes, $ex, "
        f"        $pii, $phi, FALSE, 1, $ts)",
        {
            "id": str(uuid.uuid4()),
            "ds_id": dataset_id,
            "ds_code": dataset_code,
            "ord": field_order,
            "name": display_name,
            "col": bronze_col,
            "req": requirement,
            "type": (logical_type or "TEXT"),
            "desc": description,
            "notes": notes,
            "ex": example,
            "pii": bool(is_pii),
            "phi": bool(is_phi),
            "ts": datetime.now(UTC),
        },
    )
    return True


_PHI_NAME_HINTS = (
    "ssn",
    "tax_id",
    "tin",
    "dob",
    "date_of_birth",
    "phone",
    "email",
    "address",
    "zip",
    "postal",
)


def _looks_like_phi(name: str) -> bool:
    n = name.lower().replace(" ", "_")
    return any(t in n for t in _PHI_NAME_HINTS)


def load_mapping_spec(
    src: str | Path | IO[bytes],
    *,
    dataset_name: str | None = None,
    actor: str = "ui:upload",
) -> LoadReport:
    """Single-sheet mapping spec.  Inferred dataset from sheet/file name if not given."""
    rpt = LoadReport(
        source="mapping_spec", started_at_iso=datetime.now(UTC).isoformat(), inferred=True
    )

    encoding_used: str | None = None
    try:
        if isinstance(src, (str, Path)):
            p = str(src).lower()
            if p.endswith(".csv"):
                df, encoding_used = _read_csv_safe(src)
            elif p.endswith(".tsv"):
                df, encoding_used = _read_csv_safe(src, sep="\t")
            elif p.endswith(".psv"):
                df, encoding_used = _read_csv_safe(src, sep="|")
            else:
                xl = pd.ExcelFile(src)
                sheet = xl.sheet_names[0]
                df = _read_sheet_with_header_detection(
                    src, sheet, ["Field", "Description", "Required"]
                )
                if df is None:
                    df = pd.read_excel(src, sheet_name=sheet)
                if dataset_name is None:
                    dataset_name = sheet
        else:
            # Phase 22 — when called via smart_load with an xlsx BUFFER (no
            # filename), we have to discover its real shape.  xlsx files are
            # ZIP archives; magic bytes are 'PK\x03\x04' (50 4B 03 04).
            # Without this branch we'd pipe an xlsx binary into pd.read_csv
            # and hit "utf-8 can't decode byte 0x87 in position 14".
            if hasattr(src, "seek"):
                try:
                    src.seek(0)
                except Exception:
                    pass
            head_bytes = src.read(8) if hasattr(src, "read") else b""
            if hasattr(src, "seek"):
                try:
                    src.seek(0)
                except Exception:
                    pass

            if head_bytes.startswith(b"PK\x03\x04"):
                # xlsx (or any zip-based Office file)
                xl = pd.ExcelFile(src)
                sheet = xl.sheet_names[0]
                if hasattr(src, "seek"):
                    src.seek(0)
                df = _read_sheet_with_header_detection(
                    src, sheet, ["Field", "Description", "Required"]
                )
                if df is None:
                    if hasattr(src, "seek"):
                        src.seek(0)
                    df = pd.read_excel(src, sheet_name=sheet)
                if dataset_name is None:
                    dataset_name = sheet
            else:
                # Text-like (CSV/TSV/PSV) — encoding fallback chain
                df, encoding_used = _read_csv_safe(src, sep=None, engine="python")
    except Exception as exc:
        rpt.errors.append(f"read failed: {str(exc)[:200]}")
        return rpt

    # Surface the encoding that worked if it's NOT the UTF-8 happy path
    if encoding_used and encoding_used not in ("utf-8", "utf-8-sig"):
        rpt.warnings.append(
            f"Read using `{encoding_used}` fallback (UTF-8 failed).  "
            f"Inspect characters with diacritics / special symbols carefully."
        )

    if df is None or df.empty:
        rpt.errors.append("no data rows in source")
        return rpt

    df.columns = [str(c).strip() for c in df.columns]
    col_map: dict[str, str] = {}

    def _slug_for_match(s: str) -> str:
        """Normalize header for matching — strip parens and non-alphanum."""
        return re.sub(r"[^a-z0-9]+", " ", s.lower()).strip()

    # First pass — score each column against the canonical roles.
    # Order matters: more specific patterns first.
    for raw in df.columns:
        norm = _slug_for_match(raw)
        # The field-name column is the most important.  Match any header
        # that looks like a "field" / "column" / "attribute" / "element"
        # identifier — handles FIELD(S), FIELD NAME, COLUMN_NAME, ATTRIBUTE,
        # ELEMENT, ITEM, METRIC, DATA ELEMENT, etc.
        if "field_name" not in col_map and (
            norm
            in (
                "field",
                "fields",
                "field name",
                "field s",
                "column",
                "column name",
                "attribute",
                "element",
                "data element",
                "item",
                "metric",
                "name",
            )
            or norm.startswith("field ")
            or norm.endswith(" field")
            or norm == "field s"
        ):
            col_map["field_name"] = raw
            continue
        if "requirement" not in col_map and (
            norm.startswith("req")
            or norm == "required"
            or norm == "use"
            or norm == "mandatory"
            or norm == "optional"
            or norm in ("req opt",)
        ):
            col_map["requirement"] = raw
            continue
        if "description" not in col_map and (norm.startswith("desc") or norm == "definition"):
            col_map["description"] = raw
            continue
        if "logical_type" not in col_map and (
            norm == "data type" or norm == "type" or norm == "datatype"
        ):
            col_map["logical_type"] = raw
            continue
        if "bronze_column_name" not in col_map and norm in (
            "file header",
            "header",
            "column name",
            "sql column",
            "bronze column",
        ):
            # Lower priority than field_name (since "column name" can be either).
            # But this catches the SQL-side header explicitly.
            col_map["bronze_column_name"] = raw
            continue
        if "notes" not in col_map and "note" in norm:
            col_map["notes"] = raw
            continue
        if "example" not in col_map and (
            norm == "example" or norm == "sample" or "example" in norm or "sample" in norm
        ):
            col_map["example"] = raw
            continue

    if "field_name" not in col_map:
        rpt.errors.append(
            "no field-name column detected · expected one of FIELD / FIELD NAME / "
            "FIELD(S) / COLUMN / COLUMN_NAME / ATTRIBUTE / ELEMENT / DATA ELEMENT / "
            "ITEM / METRIC · "
            f"got: {list(df.columns)[:12]}"
        )
        return rpt

    if dataset_name is None and isinstance(src, (str, Path)):
        dataset_name = Path(str(src)).stem
    if not dataset_name:
        dataset_name = "uploaded_dataset"
    ds_code = _snowflake_safe_id(dataset_name)

    wh = _wh()
    ds_id, was_new = _ensure_dataset_row(
        wh,
        dataset_code=ds_code,
        display_name=dataset_name,
        category="Inferred",
        frequency="Unknown",
        total_fields=int(len(df)),
        notes=f"Uploaded mapping spec — review category/frequency · source: {actor}",
        source="ui:mapping_spec",
        actor=actor,
    )
    if was_new:
        rpt.datasets_added = 1
    else:
        rpt.datasets_skipped_duplicate = 1

    n_fields, n_skip = 0, 0
    # NOTE: use iterrows (not itertuples) because column headers like
    # ``FIELD(S)`` are invalid Python identifiers — itertuples rewrites
    # them to ``_0`` / ``_1`` etc, which breaks the col_map lookup.  Series
    # access preserves the original column name verbatim.
    for i_offset, (_, srow) in enumerate(df.iterrows(), 1):
        i = i_offset
        d = srow.to_dict()
        fn = str(d.get(col_map["field_name"], "") or "").strip()
        if not fn or fn.lower() == col_map["field_name"].lower():
            continue
        # Pull the optional richer fields when present.
        raw_type = str(d.get(col_map.get("logical_type"), "") or "").strip().upper()
        # Map common spec data-type strings to canonical Snowflake types.
        canonical_type = _normalize_data_type(raw_type) if raw_type else "TEXT"
        bronze_col_override = str(d.get(col_map.get("bronze_column_name"), "") or "").strip()
        bronze_col = (
            _bronze_safe_id(bronze_col_override) if bronze_col_override else _bronze_safe_id(fn)
        )
        # USE column patterns: "M-Mandatory", "Mandatory", "Required", "M",
        # "O-Optional", "Optional", "O" → Required / Optional respectively.
        use_raw = str(d.get(col_map.get("requirement"), "") or "").strip()
        requirement = _normalize_use_or_requirement(use_raw)
        was_new_field = _insert_field_row(
            wh,
            dataset_id=ds_id,
            dataset_code=ds_code,
            field_order=i,
            display_name=fn,
            requirement=requirement,
            description=str(d.get(col_map.get("description"), "") or "").strip()[:1000],
            notes=str(d.get(col_map.get("notes"), "") or "").strip()[:1000],
            example=str(d.get(col_map.get("example"), "") or "").strip()[:200],
            is_phi=_looks_like_phi(fn),
            bronze_column_name=bronze_col,
            logical_type=canonical_type,
        )
        if was_new_field:
            n_fields += 1
        else:
            n_skip += 1

    rpt.fields_added = n_fields
    rpt.fields_skipped_duplicate = n_skip
    return rpt


# ---------------------------------------------------------------------------
# Path 3: raw data file → infer schema
# ---------------------------------------------------------------------------


def infer_from_data_file(
    src: str | Path | IO[bytes],
    *,
    dataset_name: str | None = None,
    actor: str = "ui:upload",
) -> LoadReport:
    """Sniff a CSV/PSV/TSV/JSON data file, infer Bronze schema from headers + dtypes."""
    rpt = LoadReport(
        source="data_inference", started_at_iso=datetime.now(UTC).isoformat(), inferred=True
    )
    encoding_used: str | None = None
    try:
        if isinstance(src, (str, Path)):
            lower = str(src).lower()
            if lower.endswith(".csv"):
                df, encoding_used = _read_csv_safe(src, nrows=1000)
            elif lower.endswith(".psv"):
                df, encoding_used = _read_csv_safe(src, sep="|", nrows=1000)
            elif lower.endswith(".tsv"):
                df, encoding_used = _read_csv_safe(src, sep="\t", nrows=1000)
            elif lower.endswith((".json", ".jsonl")):
                df, encoding_used = _read_json_safe(src, lines=lower.endswith(".jsonl"))
                if len(df) > 1000:
                    df = df.head(1000)
            else:
                rpt.errors.append(f"unsupported extension: {Path(str(src)).suffix}")
                return rpt
            if dataset_name is None:
                dataset_name = Path(str(src)).stem
        else:
            df, encoding_used = _read_csv_safe(src, sep=None, engine="python", nrows=1000)
    except Exception as exc:
        rpt.errors.append(f"read failed: {str(exc)[:200]}")
        return rpt

    if encoding_used and encoding_used not in ("utf-8", "utf-8-sig"):
        rpt.warnings.append(f"Read using `{encoding_used}` fallback (UTF-8 failed).")

    if df.empty:
        rpt.errors.append("data file has no rows to sniff")
        return rpt

    if not dataset_name:
        dataset_name = "uploaded_data"
    ds_code = _snowflake_safe_id(dataset_name)

    wh = _wh()
    ds_id, was_new = _ensure_dataset_row(
        wh,
        dataset_code=ds_code,
        display_name=dataset_name,
        category="Inferred",
        frequency="Unknown",
        total_fields=int(len(df.columns)),
        notes=f"Inferred from {len(df)} sampled rows — review types/required",
        source="ui:data_inference",
        actor=actor,
    )
    if was_new:
        rpt.datasets_added = 1
    else:
        rpt.datasets_skipped_duplicate = 1

    n_fields = 0
    for i, col in enumerate(df.columns, 1):
        sample = df[col].dropna().astype(str).head(1).tolist()
        example = sample[0][:200] if sample else ""
        notes_bits = [f"inferred dtype: {df[col].dtype}"]
        if _looks_like_phi(str(col)):
            notes_bits.append("⚠ candidate PHI/PII — review")
        was_new_f = _insert_field_row(
            wh,
            dataset_id=ds_id,
            dataset_code=ds_code,
            field_order=i,
            display_name=str(col),
            requirement="Optional",
            description="",
            notes=" · ".join(notes_bits),
            example=example,
            is_phi=_looks_like_phi(str(col)),
        )
        if was_new_f:
            n_fields += 1
    rpt.fields_added = n_fields
    return rpt


# ---------------------------------------------------------------------------
# Canonical CSV / TSV / PSV loader (denormalized flat)
# ---------------------------------------------------------------------------


def load_canonical_csv(
    src: str | Path | IO[bytes],
    *,
    sep: str = ",",
    actor: str = "ui:upload",
    filename: str = "",
) -> LoadReport:
    """Load the canonical denormalized flat CSV/TSV/PSV (v2 contract).

    Required columns (case-insensitive, punctuation-insensitive matching):
        Dataset · Field name
    Optional columns:
        Req / Opt · Description · Additional notes · Example · Frequency ·
        Fields · Required · Optional · Category · Notes · Used by

    One row per (dataset, field).  Dataset-level columns (Frequency,
    Category, etc.) repeat across all that dataset's rows; we de-duplicate
    at the dataset level.  If `Fields` / `Required` / `Optional` columns
    are absent the loader derives the counts from the field rows.
    """
    _tab = "\t"
    _fmt_label = "csv" if sep == "," else ("tsv" if sep == _tab else "psv")
    rpt = LoadReport(
        source=f"canonical_{_fmt_label}",
        started_at_iso=datetime.now(UTC).isoformat(),
    )

    # Run strict validation first
    try:
        from datalink.admin.canonical_validators import validate_csv_flat

        vrpt = validate_csv_flat(src, sep=sep)
    except Exception as exc:
        rpt.errors.append(f"validator crashed: {type(exc).__name__}: {exc}")
        return rpt
    if not vrpt.ok:
        return _validation_to_load_report(rpt, vrpt)

    # Read full file
    try:
        if hasattr(src, "seek"):
            try:
                src.seek(0)
            except Exception:
                pass
        df, encoding_used = _read_csv_safe(src, sep=sep)
    except Exception as exc:
        rpt.errors.append(
            f"could not parse the file even after validation passed: "
            f"{type(exc).__name__}: {str(exc)[:200]}"
        )
        return rpt

    if encoding_used and encoding_used not in ("utf-8", "utf-8-sig"):
        rpt.warnings.append(f"Read using `{encoding_used}` fallback (UTF-8 failed).")

    # Locate the columns by alias
    from datalink.admin.canonical_spec import (
        DATASET_COLUMN_ALIASES,
        FIELD_COLUMN_ALIASES,
        find_column,
    )

    cols = list(df.columns)
    ds_name_col = find_column(cols, DATASET_COLUMN_ALIASES["Dataset"])
    ds_freq_col = find_column(cols, DATASET_COLUMN_ALIASES["Frequency"])
    ds_used_col = find_column(cols, DATASET_COLUMN_ALIASES["Used by"])
    ds_fields_col = find_column(cols, DATASET_COLUMN_ALIASES["Fields"])
    ds_required_col = find_column(cols, DATASET_COLUMN_ALIASES["Required"])
    ds_optional_col = find_column(cols, DATASET_COLUMN_ALIASES["Optional"])
    ds_category_col = find_column(cols, DATASET_COLUMN_ALIASES["Category"])
    ds_notes_col = find_column(cols, DATASET_COLUMN_ALIASES["Notes"])

    f_name_col = find_column(cols, FIELD_COLUMN_ALIASES["Field name"])
    f_req_col = find_column(cols, FIELD_COLUMN_ALIASES["Req / Opt"])
    f_desc_col = find_column(cols, FIELD_COLUMN_ALIASES["Description"])
    f_notes_col = find_column(cols, FIELD_COLUMN_ALIASES["Additional notes"])
    f_example_col = find_column(cols, FIELD_COLUMN_ALIASES["Example"])

    # Pivot rows into (dataset, fields[]) shape — dedupe dataset-level info
    datasets_master: dict[str, dict[str, Any]] = {}
    field_catalogue: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        ds_name = str(row.get(ds_name_col, "") or "").strip()
        f_name = str(row.get(f_name_col, "") or "").strip()
        if not ds_name or not f_name:
            continue
        if ds_name.upper() in ("TOTAL", "TOTALS", "SUM", "SUMMARY"):
            continue
        if ds_name not in datasets_master:
            datasets_master[ds_name] = {
                "Dataset": ds_name,
                "Frequency": str(row.get(ds_freq_col, "") or "") if ds_freq_col else "",
                "Fields": row.get(ds_fields_col) if ds_fields_col else None,
                "Required": row.get(ds_required_col) if ds_required_col else None,
                "Optional": row.get(ds_optional_col) if ds_optional_col else None,
                "Used by": str(row.get(ds_used_col, "") or "") if ds_used_col else "",
                "Category": str(row.get(ds_category_col, "") or "") if ds_category_col else "",
                "Notes": str(row.get(ds_notes_col, "") or "") if ds_notes_col else "",
            }
        field_catalogue.append(
            {
                "Dataset": ds_name,
                "Field name": f_name,
                "Req / Opt": str(row.get(f_req_col, "") or "") if f_req_col else "",
                "Description": str(row.get(f_desc_col, "") or "") if f_desc_col else "",
                "Additional notes": str(row.get(f_notes_col, "") or "") if f_notes_col else "",
                "Example": str(row.get(f_example_col, "") or "") if f_example_col else "",
            }
        )

    if not datasets_master:
        rpt.errors.append("after parsing, no valid (dataset, field) rows found")
        return rpt

    # v2: derive Fields / Required / Optional counts from the field rows
    # whenever the operator didn't ship explicit count columns.  This lets a
    # bare CSV (Dataset, Field name, Req / Opt, Description, Frequency) be a
    # complete canonical artifact on its own.
    counts: dict[str, dict[str, int]] = {}
    for f in field_catalogue:
        ds = f["Dataset"]
        c = counts.setdefault(ds, {"total": 0, "req": 0, "opt": 0})
        c["total"] += 1
        if _normalize_requirement(f.get("Req / Opt")) == "Required":
            c["req"] += 1
        else:
            c["opt"] += 1
    for ds_name, ds_row in datasets_master.items():
        c = counts.get(ds_name, {"total": 0, "req": 0, "opt": 0})
        # Only override when the source didn't supply explicit counts
        if not ds_row.get("Fields"):
            ds_row["Fields"] = c["total"]
        if not ds_row.get("Required"):
            ds_row["Required"] = c["req"]
        if not ds_row.get("Optional"):
            ds_row["Optional"] = c["opt"]

    # Hand off to the same batch-insert pipeline as the xlsx loader
    return _persist_canonical_two_table(
        rpt=rpt,
        ds_records=list(datasets_master.values()),
        fd_records=field_catalogue,
        source_doc=filename or "uploaded canonical csv/tsv/psv",
        actor=actor,
    )


# ---------------------------------------------------------------------------
# Canonical JSON loader (2-array)
# ---------------------------------------------------------------------------


def load_canonical_json(
    src: str | Path | IO[bytes],
    *,
    actor: str = "ui:upload",
    filename: str = "",
) -> LoadReport:
    """Load the canonical JSON (v2 contract).

    Accepts either:
      * `{"field_catalogue": [...]}` (preferred — dataset metadata is derived)
      * `[...]` (bare top-level array of field objects)
      * legacy `{"datasets_master": [...], "field_catalogue": [...]}` (honoured if present)
    """
    rpt = LoadReport(source="canonical_json", started_at_iso=datetime.now(UTC).isoformat())

    try:
        from datalink.admin.canonical_validators import validate_json

        vrpt = validate_json(src)
    except Exception as exc:
        rpt.errors.append(f"validator crashed: {type(exc).__name__}: {exc}")
        return rpt
    if not vrpt.ok:
        return _validation_to_load_report(rpt, vrpt)

    try:
        if isinstance(src, (str, Path)):
            text = Path(str(src)).read_text(encoding="utf-8")
        else:
            if hasattr(src, "seek"):
                src.seek(0)
            raw = src.read()
            text = raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else str(raw)
        data = json.loads(text)
    except Exception as exc:
        rpt.errors.append(
            f"could not parse JSON after validation passed: "
            f"{type(exc).__name__}: {str(exc)[:200]}"
        )
        return rpt

    from datalink.admin.canonical_spec import (
        JSON_DATASET_KEY_ALIASES,
        JSON_FIELD_KEY_ALIASES,
        find_key,
    )

    def _ds_get(obj: dict, canon: str) -> Any:
        k = find_key(list(obj.keys()), JSON_DATASET_KEY_ALIASES.get(canon, {canon.lower()}))
        return obj.get(k) if k else None

    def _fd_get(obj: dict, canon: str) -> Any:
        k = find_key(list(obj.keys()), JSON_FIELD_KEY_ALIASES.get(canon, {canon.lower()}))
        return obj.get(k) if k else None

    # v2: accept bare top-level array OR {"field_catalogue": [...]}
    # Legacy {"datasets_master": [...]} array is honoured if present, but the
    # field array is the authoritative source — dataset metadata is overlaid
    # by the loader when both are supplied.
    if isinstance(data, list):
        fd_iter = data
        ds_iter: list[Any] = []
    else:
        fd_iter = data.get("field_catalogue") or data.get("fields") or []
        ds_iter = data.get("datasets_master") or data.get("datasets") or []

    ds_records: list[dict[str, Any]] = []
    for obj in ds_iter:
        if not isinstance(obj, dict):
            continue
        ds_records.append(
            {
                "Dataset": str(_ds_get(obj, "Dataset") or "").strip(),
                "Frequency": str(_ds_get(obj, "Frequency") or ""),
                "Fields": _ds_get(obj, "Fields"),
                "Required": _ds_get(obj, "Required"),
                "Optional": _ds_get(obj, "Optional"),
                "Used by": str(_ds_get(obj, "Used by") or ""),
                "Category": str(_ds_get(obj, "Category") or ""),
                "Notes": str(_ds_get(obj, "Notes") or ""),
            }
        )

    fd_records: list[dict[str, Any]] = []
    for obj in fd_iter:
        if not isinstance(obj, dict):
            continue
        fd_records.append(
            {
                "Dataset": str(_fd_get(obj, "Dataset") or "").strip(),
                "Field name": str(_fd_get(obj, "Field name") or "").strip(),
                "Req / Opt": str(_fd_get(obj, "Req / Opt") or ""),
                "Description": str(_fd_get(obj, "Description") or ""),
                "Additional notes": str(_fd_get(obj, "Additional notes") or ""),
                "Example": str(_fd_get(obj, "Example") or ""),
            }
        )

    # If no datasets_master was supplied, derive one row per distinct Dataset
    # from the field records (v2 default).  First-non-empty-frequency wins.
    if not ds_records and fd_records:
        derived: dict[str, dict[str, Any]] = {}
        for fd in fd_records:
            ds_name = fd["Dataset"]
            if not ds_name:
                continue
            row = derived.setdefault(
                ds_name,
                {
                    "Dataset": ds_name,
                    "Frequency": "",
                    "Fields": 0,
                    "Required": 0,
                    "Optional": 0,
                    "Used by": "",
                    "Category": "",
                    "Notes": "",
                },
            )
            row["Fields"] = (row["Fields"] or 0) + 1
            req_norm = _normalize_requirement(fd.get("Req / Opt"))
            if req_norm == "Required":
                row["Required"] = (row["Required"] or 0) + 1
            else:
                row["Optional"] = (row["Optional"] or 0) + 1
        ds_records = list(derived.values())

    return _persist_canonical_two_table(
        rpt=rpt,
        ds_records=ds_records,
        fd_records=fd_records,
        source_doc=filename or "uploaded canonical json",
        actor=actor,
    )


# ---------------------------------------------------------------------------
# Canonical JSONL loader (mixed-kind)
# ---------------------------------------------------------------------------


def load_canonical_jsonl(
    src: str | Path | IO[bytes],
    *,
    actor: str = "ui:upload",
    filename: str = "",
) -> LoadReport:
    """Load the canonical JSONL (v2 contract).

    Each line is a JSON object describing a field.  The legacy `_kind`
    discriminator is honoured — records with `_kind="dataset"` contribute
    dataset-level metadata; everything else is a field record by default.
    """
    rpt = LoadReport(source="canonical_jsonl", started_at_iso=datetime.now(UTC).isoformat())

    try:
        from datalink.admin.canonical_validators import validate_jsonl

        vrpt = validate_jsonl(src)
    except Exception as exc:
        rpt.errors.append(f"validator crashed: {type(exc).__name__}: {exc}")
        return rpt
    if not vrpt.ok:
        return _validation_to_load_report(rpt, vrpt)

    # Parse line-by-line
    try:
        if isinstance(src, (str, Path)):
            text = Path(str(src)).read_text(encoding="utf-8")
        else:
            if hasattr(src, "seek"):
                src.seek(0)
            raw = src.read()
            text = raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else str(raw)
    except Exception as exc:
        rpt.errors.append(f"could not read JSONL: {type(exc).__name__}: {str(exc)[:200]}")
        return rpt

    from datalink.admin.canonical_spec import (
        JSON_DATASET_KEY_ALIASES,
        JSON_FIELD_KEY_ALIASES,
        find_key,
    )

    ds_records: list[dict[str, Any]] = []
    fd_records: list[dict[str, Any]] = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(obj, dict):
            continue
        # v2: every record is a field record by default; legacy "_kind"
        # discriminator is still honoured if present.
        kind = str(obj.get("_kind") or "field").strip().lower()
        if kind == "dataset":
            ds_records.append(
                {
                    "Dataset": str(
                        obj.get(find_key(list(obj.keys()), JSON_DATASET_KEY_ALIASES["Dataset"]))
                        or ""
                    ),
                    "Frequency": str(
                        obj.get(find_key(list(obj.keys()), JSON_DATASET_KEY_ALIASES["Frequency"]))
                        or ""
                    ),
                    "Fields": obj.get(
                        find_key(list(obj.keys()), JSON_DATASET_KEY_ALIASES["Fields"])
                    ),
                    "Required": obj.get(
                        find_key(list(obj.keys()), JSON_DATASET_KEY_ALIASES["Required"])
                    ),
                    "Optional": obj.get(
                        find_key(list(obj.keys()), JSON_DATASET_KEY_ALIASES["Optional"])
                    ),
                    "Used by": str(
                        obj.get(find_key(list(obj.keys()), JSON_DATASET_KEY_ALIASES["Used by"]))
                        or ""
                    ),
                    "Category": str(
                        obj.get(find_key(list(obj.keys()), JSON_DATASET_KEY_ALIASES["Category"]))
                        or ""
                    ),
                    "Notes": str(
                        obj.get(find_key(list(obj.keys()), JSON_DATASET_KEY_ALIASES["Notes"])) or ""
                    ),
                }
            )
        else:  # treat as field
            fd_records.append(
                {
                    "Dataset": str(
                        obj.get(find_key(list(obj.keys()), JSON_FIELD_KEY_ALIASES["Dataset"])) or ""
                    ),
                    "Field name": str(
                        obj.get(find_key(list(obj.keys()), JSON_FIELD_KEY_ALIASES["Field name"]))
                        or ""
                    ),
                    "Req / Opt": str(
                        obj.get(find_key(list(obj.keys()), JSON_FIELD_KEY_ALIASES["Req / Opt"]))
                        or ""
                    ),
                    "Description": str(
                        obj.get(find_key(list(obj.keys()), JSON_FIELD_KEY_ALIASES["Description"]))
                        or ""
                    ),
                    "Additional notes": str(
                        obj.get(
                            find_key(list(obj.keys()), JSON_FIELD_KEY_ALIASES["Additional notes"])
                        )
                        or ""
                    ),
                    "Example": str(
                        obj.get(find_key(list(obj.keys()), JSON_FIELD_KEY_ALIASES["Example"])) or ""
                    ),
                }
            )

    # If no dataset records, derive from field records (v2 default)
    if not ds_records and fd_records:
        derived: dict[str, dict[str, Any]] = {}
        for fd in fd_records:
            ds_name = (fd.get("Dataset") or "").strip()
            if not ds_name:
                continue
            row = derived.setdefault(
                ds_name,
                {
                    "Dataset": ds_name,
                    "Frequency": "",
                    "Fields": 0,
                    "Required": 0,
                    "Optional": 0,
                    "Used by": "",
                    "Category": "",
                    "Notes": "",
                },
            )
            row["Fields"] = (row["Fields"] or 0) + 1
            if _normalize_requirement(fd.get("Req / Opt")) == "Required":
                row["Required"] = (row["Required"] or 0) + 1
            else:
                row["Optional"] = (row["Optional"] or 0) + 1
        ds_records = list(derived.values())

    return _persist_canonical_two_table(
        rpt=rpt,
        ds_records=ds_records,
        fd_records=fd_records,
        source_doc=filename or "uploaded canonical jsonl",
        actor=actor,
    )


# ---------------------------------------------------------------------------
# Shared persistence — used by all three canonical loaders
# ---------------------------------------------------------------------------


def _persist_canonical_two_table(
    *,
    rpt: LoadReport,
    ds_records: list[dict[str, Any]],
    fd_records: list[dict[str, Any]],
    source_doc: str,
    actor: str,
) -> LoadReport:
    """Take the canonical 2-table shape (already parsed + normalized) and
    persist with the same idempotency/duplicate-detection/batch-insert
    pipeline the xlsx loader uses.  Reuses helpers from load_catalogue_xlsx
    by building two DataFrames + handing off."""

    if not ds_records:
        rpt.errors.append("no dataset records to persist")
        return rpt
    if not fd_records:
        rpt.errors.append("no field records to persist")
        return rpt

    # Build dataset rows in canonical shape
    proposed_codes: set[str] = set()
    for d in ds_records:
        name = (d.get("Dataset") or "").strip()
        if not name:
            continue
        proposed_codes.add(_snowflake_safe_id(name))

    wh = _wh()

    # Compute catalog_version
    try:
        cur_max = list(
            wh.query(
                f"SELECT COALESCE(MAX(catalog_version), 0) c "
                f"FROM {CONTROL_SCHEMA}.global_bronze_catalog_datasets"
            )
        )[0]["c"]
        catalog_version = int(cur_max) + 1
    except Exception:
        catalog_version = 1

    ts_now = datetime.now(UTC).replace(tzinfo=None)
    dataset_rows: list[dict[str, Any]] = []
    dataset_id_by_code: dict[str, str] = {}
    used_by_by_code: dict[str, list[tuple[str, str | None]]] = {}

    for d in ds_records:
        name = str(d.get("Dataset") or "").strip()
        if not name:
            continue
        code = _snowflake_safe_id(name)
        ds_id = str(uuid.uuid4())
        dataset_id_by_code[code] = ds_id
        used_by_raw = d.get("Used by") or ""
        links = _parse_used_by_to_codes(used_by_raw)
        used_by_by_code[code] = links
        used_by_codes = [c for c, _ in links]

        def _int(v):
            try:
                if v is None:
                    return 0
                if isinstance(v, float) and pd.isna(v):
                    return 0
                return int(v)
            except Exception:
                return 0

        total = _int(d.get("Fields"))
        req = _int(d.get("Required"))
        opt = _int(d.get("Optional"))

        dataset_rows.append(
            {
                "id": ds_id,
                "code": code,
                "name": name,
                "cat": str(d.get("Category") or ""),
                "freq": str(d.get("Frequency") or ""),
                "used": json.dumps(used_by_codes),
                "tot": total,
                "req": req,
                "opt": opt,
                "notes": str(d.get("Notes") or ""),
                "ver": int(catalog_version),
                "src": source_doc,
                "ts": ts_now,
                "by": actor,
            }
        )

    # Field rows — using synonym map for "Member X" ↔ "X" reconciliation
    synonym_id_by_code: dict[str, str] = dict(dataset_id_by_code)
    for master_code, ds_id in list(dataset_id_by_code.items()):
        if master_code.startswith("member_"):
            synonym_id_by_code.setdefault(master_code[len("member_") :], ds_id)
        if master_code.startswith("provider_") and master_code != "provider":
            synonym_id_by_code.setdefault(master_code[len("provider_") :], ds_id)

    field_rows: list[dict[str, Any]] = []
    field_order_by_ds: dict[str, int] = {}
    for f in fd_records:
        ds_name = str(f.get("Dataset") or "").strip()
        field_name = str(f.get("Field name") or "").strip()
        if not ds_name or not field_name:
            continue
        ds_code_raw = _snowflake_safe_id(ds_name)
        ds_id = synonym_id_by_code.get(ds_code_raw)
        if not ds_id:
            rpt.warnings.append(f"field row references unknown dataset '{ds_name}' (skipped)")
            continue
        ds_code = next(
            (k for k, v in dataset_id_by_code.items() if v == ds_id),
            ds_code_raw,
        )
        field_order_by_ds[ds_code] = field_order_by_ds.get(ds_code, 0) + 1
        col_name = _bronze_safe_id(field_name)
        field_rows.append(
            {
                "id": str(uuid.uuid4()),
                "ds_id": ds_id,
                "ds_code": ds_code,
                "ord": field_order_by_ds[ds_code],
                "name": field_name,
                "col": col_name,
                "req": _normalize_use_or_requirement(f.get("Req / Opt")),
                "type": "TEXT",
                "desc": str(f.get("Description") or "")[:1000],
                "notes": str(f.get("Additional notes") or "")[:1000],
                "ex": str(f.get("Example") or "")[:200],
                "pii": _looks_like_phi(field_name),
                "phi": _looks_like_phi(field_name),
                "bkey": False,
                "ver": int(catalog_version),
                "ts": ts_now,
            }
        )

    # Routing rules from used_by
    routing_rows: list[dict[str, Any]] = []
    for code, links in used_by_by_code.items():
        for pl_code, _scope in links:
            if pl_code not in _KNOWN_PRODUCTS:
                continue
            tgt_sys, tgt_uri = _KNOWN_PRODUCTS[pl_code]
            routing_rows.append(
                {
                    "id": str(uuid.uuid4()),
                    "ds": code,
                    "prod": pl_code,
                    "tsys": tgt_sys,
                    "turi": tgt_uri,
                    "notes": f"Default routing seeded from 'Used by' matrix ({pl_code})",
                    "ts": ts_now,
                    "by": actor,
                }
            )

    pl_link_rows: list[dict[str, Any]] = []
    for code, links in used_by_by_code.items():
        for pl_code, scope_level in links:
            pl_link_rows.append({"ds": code, "pl": pl_code, "sl": scope_level})

    # ---- UPSERT per dataset — same logic as load_catalogue_xlsx -----
    # NEVER wipes datasets that aren't in this upload.  Per-dataset:
    #   * NOT EXISTS              → INSERT master + fields  (added)
    #   * EXISTS + hash matches   → no-op                   (unchanged)
    #   * EXISTS + hash differs   → DELETE fields for THIS code only
    #                               + INSERT new fields + UPDATE master  (changed)
    from datalink.admin.canonical_spec import (
        compute_dataset_schema_hash,
        diff_dataset_fields,
    )

    proposed_fields_by_code: dict[str, list[dict[str, Any]]] = {}
    for fr in field_rows:
        proposed_fields_by_code.setdefault(fr["ds_code"], []).append(
            {
                "field_name": fr["name"],
                "req_opt": fr["req"],
            }
        )

    try:
        conn = wh._connect()
        cur = conn.cursor()
        try:
            for dataset_row in dataset_rows:
                code = dataset_row["code"]
                proposed_fields_this_ds = proposed_fields_by_code.get(code, [])
                proposed_hash = compute_dataset_schema_hash(code, proposed_fields_this_ds)

                existing_master = list(
                    wh.query(
                        f"SELECT dataset_id FROM {CONTROL_SCHEMA}.global_bronze_catalog_datasets "
                        f"WHERE dataset_code = $c",
                        {"c": code},
                    )
                )
                existing_fields_raw = (
                    list(
                        wh.query(
                            f"SELECT field_display_name AS field_name, requirement AS req_opt "
                            f"FROM {CONTROL_SCHEMA}.global_bronze_catalog_fields "
                            f"WHERE dataset_code = $c",
                            {"c": code},
                        )
                    )
                    if existing_master
                    else []
                )
                existing_fields = [
                    {
                        "field_name": str(r.get("field_name") or r.get("FIELD_NAME") or ""),
                        "req_opt": str(r.get("req_opt") or r.get("REQ_OPT") or ""),
                    }
                    for r in existing_fields_raw
                ]
                existing_hash = (
                    compute_dataset_schema_hash(code, existing_fields) if existing_master else None
                )

                if not existing_master:
                    cur.execute(
                        f"INSERT INTO {CONTROL_SCHEMA}.global_bronze_catalog_datasets "
                        f"(dataset_id, dataset_code, display_name, category, "
                        f" default_frequency, used_by, total_fields, required_fields, "
                        f" optional_fields, notes, is_active, catalog_version, "
                        f" source_doc_uri, registered_at, registered_by) "
                        f"VALUES (%(id)s, %(code)s, %(name)s, %(cat)s, %(freq)s, "
                        f"        %(used)s, %(tot)s, %(req)s, %(opt)s, %(notes)s, "
                        f"        TRUE, %(ver)s, %(src)s, %(ts)s, %(by)s)",
                        dataset_row,
                    )
                    these_field_rows = [fr for fr in field_rows if fr["ds_code"] == code]
                    if these_field_rows:
                        cur.executemany(
                            f"INSERT INTO {CONTROL_SCHEMA}.global_bronze_catalog_fields "
                            f"(field_id, dataset_id, dataset_code, field_order, "
                            f" field_display_name, bronze_column_name, requirement, "
                            f" logical_type, description, additional_notes, example, "
                            f" is_pii, is_phi, is_business_key, catalog_version, "
                            f" registered_at) "
                            f"VALUES (%(id)s, %(ds_id)s, %(ds_code)s, %(ord)s, %(name)s, "
                            f"        %(col)s, %(req)s, %(type)s, %(desc)s, %(notes)s, "
                            f"        %(ex)s, %(pii)s, %(phi)s, %(bkey)s, %(ver)s, %(ts)s)",
                            these_field_rows,
                        )
                    rpt.datasets_added += 1
                    rpt.fields_added += len(these_field_rows)
                    rpt.schema_diffs[code] = {
                        "status": "added",
                        "added": sorted(f["field_name"] for f in proposed_fields_this_ds),
                        "removed": [],
                        "changed": [],
                    }
                elif existing_hash == proposed_hash:
                    rpt.datasets_unchanged += 1
                    rpt.datasets_skipped_duplicate += 1
                    rpt.schema_diffs[code] = {
                        "status": "unchanged",
                        "added": [],
                        "removed": [],
                        "changed": [],
                    }
                else:
                    diff = diff_dataset_fields(existing_fields, proposed_fields_this_ds)
                    existing_ds_id = str(
                        existing_master[0].get("dataset_id") or existing_master[0].get("DATASET_ID")
                    )
                    cur.execute(
                        f"DELETE FROM {CONTROL_SCHEMA}.global_bronze_catalog_fields "
                        f"WHERE dataset_code = %(c)s",
                        {"c": code},
                    )
                    these_field_rows = [
                        {**fr, "ds_id": existing_ds_id}
                        for fr in field_rows
                        if fr["ds_code"] == code
                    ]
                    if these_field_rows:
                        cur.executemany(
                            f"INSERT INTO {CONTROL_SCHEMA}.global_bronze_catalog_fields "
                            f"(field_id, dataset_id, dataset_code, field_order, "
                            f" field_display_name, bronze_column_name, requirement, "
                            f" logical_type, description, additional_notes, example, "
                            f" is_pii, is_phi, is_business_key, catalog_version, "
                            f" registered_at) "
                            f"VALUES (%(id)s, %(ds_id)s, %(ds_code)s, %(ord)s, %(name)s, "
                            f"        %(col)s, %(req)s, %(type)s, %(desc)s, %(notes)s, "
                            f"        %(ex)s, %(pii)s, %(phi)s, %(bkey)s, %(ver)s, %(ts)s)",
                            these_field_rows,
                        )
                    cur.execute(
                        f"UPDATE {CONTROL_SCHEMA}.global_bronze_catalog_datasets SET "
                        f"  display_name = %(name)s, category = %(cat)s, "
                        f"  default_frequency = %(freq)s, used_by = %(used)s, "
                        f"  total_fields = %(tot)s, required_fields = %(req)s, "
                        f"  optional_fields = %(opt)s, notes = %(notes)s, "
                        f"  catalog_version = %(ver)s, source_doc_uri = %(src)s, "
                        f"  registered_at = %(ts)s, registered_by = %(by)s "
                        f"WHERE dataset_code = %(code)s",
                        dataset_row,
                    )
                    rpt.datasets_schema_changed += 1
                    rpt.fields_replaced += len(these_field_rows)
                    rpt.schema_diffs[code] = {"status": "changed", **diff}

            # Routing rules + product-line junction — additive (replace per-key)
            for rr in routing_rows:
                try:
                    cur.execute(
                        f"DELETE FROM {CONTROL_SCHEMA}.onprem_routing_rules "
                        f"WHERE dataset_code = %(ds)s AND downstream_product = %(prod)s "
                        f"AND is_default = TRUE",
                        {"ds": rr["ds"], "prod": rr["prod"]},
                    )
                    cur.execute(
                        f"INSERT INTO {CONTROL_SCHEMA}.onprem_routing_rules "
                        f"(rule_id, dataset_code, downstream_product, target_system, "
                        f" target_uri, is_default, is_active, notes, created_at, created_by) "
                        f"VALUES (%(id)s, %(ds)s, %(prod)s, %(tsys)s, %(turi)s, "
                        f"        TRUE, TRUE, %(notes)s, %(ts)s, %(by)s)",
                        rr,
                    )
                    rpt.routing_rules_added += 1
                except Exception as exc:
                    rpt.warnings.append(f"routing rule {rr['ds']}/{rr['prod']}: {str(exc)[:120]}")
            for pl in pl_link_rows:
                try:
                    cur.execute(
                        f"DELETE FROM {CONTROL_SCHEMA}.dataset_product_lines "
                        f"WHERE dataset_code = %(ds)s AND product_line_code = %(pl)s",
                        {"ds": pl["ds"], "pl": pl["pl"]},
                    )
                    cur.execute(
                        f"INSERT INTO {CONTROL_SCHEMA}.dataset_product_lines "
                        f"(dataset_code, product_line_code, scope_level) "
                        f"VALUES (%(ds)s, %(pl)s, %(sl)s)",
                        pl,
                    )
                    rpt.product_line_links += 1
                except Exception as exc:
                    rpt.warnings.append(f"product line {pl['ds']}/{pl['pl']}: {str(exc)[:120]}")
        finally:
            cur.close()
    except Exception as exc:
        rpt.errors.append(
            f"upsert failed — Snowflake state may be inconsistent.  "
            f"{type(exc).__name__}: {str(exc)[:200]}"
        )
    return rpt


# ---------------------------------------------------------------------------
# Smart auto-route  (Phase 22 — STRICT version)
# ---------------------------------------------------------------------------


def smart_load(
    src: str | Path | IO[bytes],
    *,
    filename_hint: str | None = None,
    actor: str = "ui:upload",
) -> LoadReport:
    """Strict canonical-format dispatcher.

    Routes uploads to the right loader using a STRICT, format-aware
    contract.  Validation runs BEFORE any Snowflake write; if the file
    doesn't satisfy the canonical contract, the returned LoadReport
    surfaces structured errors with remediation hints (no Python
    traceback leaks to the operator).

    Routing (v2 — single-sheet canonical):

      ``.xlsx`` with a Field_Catalogue sheet → :func:`load_catalogue_xlsx`
      ``.xlsx`` with a different shape       → :func:`load_mapping_spec`
      ``.csv / .tsv / .psv``                 → :func:`load_canonical_csv`
      ``.json``                              → :func:`load_canonical_json`
      ``.jsonl / .ndjson``                   → :func:`load_canonical_jsonl`
      anything else                          → structured error
    """
    from datalink.admin.canonical_spec import is_canonical_xlsx_sheet_name

    fname = filename_hint or (str(src) if isinstance(src, (str, Path)) else "")
    lower = fname.lower()

    rpt_start = datetime.now(UTC).isoformat()

    # CSV / TSV / PSV
    if lower.endswith(".csv"):
        return load_canonical_csv(src, sep=",", actor=actor, filename=fname)
    if lower.endswith(".tsv"):
        return load_canonical_csv(src, sep="\t", actor=actor, filename=fname)
    if lower.endswith(".psv"):
        return load_canonical_csv(src, sep="|", actor=actor, filename=fname)

    # JSON family
    if lower.endswith(".json"):
        return load_canonical_json(src, actor=actor, filename=fname)
    if lower.endswith((".jsonl", ".ndjson")):
        return load_canonical_jsonl(src, actor=actor, filename=fname)

    # Excel family
    if lower.endswith((".xlsx", ".xls", ".xlsm")):
        try:
            if isinstance(src, (str, Path)):
                xl = pd.ExcelFile(src)
            else:
                if hasattr(src, "seek"):
                    src.seek(0)
                buf = io.BytesIO(src.read())
                if hasattr(src, "seek"):
                    src.seek(0)
                xl = pd.ExcelFile(buf)
            sheets = xl.sheet_names
            # Canonical if any sheet is a Field Catalogue sheet, OR if there's
            # only one sheet (we treat single-sheet xlsx as canonical by default
            # — that's the v2 operator-shipped shape).
            has_canonical = any(is_canonical_xlsx_sheet_name(s) for s in sheets) or len(sheets) == 1
            if has_canonical:
                return load_catalogue_xlsx(src, actor=actor)
            return load_mapping_spec(src, actor=actor)
        except Exception as exc:
            rpt = LoadReport(source="smart_load", started_at_iso=rpt_start)
            rpt.errors.append(
                f"could not open as Excel: {type(exc).__name__}: {str(exc)[:200]} — "
                "FIX: confirm the file is a real .xlsx (not a .csv renamed) and "
                "isn't password-protected.  Download a canonical sample from "
                "the upload dialog if unsure of the expected shape."
            )
            return rpt

    rpt = LoadReport(source="smart_load", started_at_iso=rpt_start)
    rpt.errors.append(
        f"unsupported file extension: `{fname.split('.')[-1] if '.' in fname else '(none)'}` — "
        "FIX: supported = .xlsx · .csv · .tsv · .psv · .json · .jsonl.  "
        "Download a canonical sample from the upload dialog to see the expected shape."
    )
    return rpt
