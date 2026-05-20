"""Format readers for the Bronze Inbound Mapping Agent — Phase 23.

Each reader takes a raw file (path or buffer) and produces a SourceView —
a normalized description of the file's shape that the LLM can reason
about: per-field name + path/position + inferred type + redacted sample
values + free-form notes from the reader.

Readers in this module:

  CSV / TSV / PSV     :func:`read_csv`               ✅ Pass 1
  xlsx (any sheets)   :func:`read_xlsx`              ✅ Pass 1
  JSON (flat+nested)  :func:`read_json`              ✅ Pass 1
  Fixed-width         :func:`read_fixed_width`       🟡 Pass 3 — stub raises NotImplementedError
  .docx               :func:`read_docx`              🟡 Pass 3 — stub
  PDF                 :func:`read_pdf`               🟡 Pass 3 — stub

The dispatcher :func:`detect_and_read` sniffs the file extension and
routes to the right reader.  Mismatched/ambiguous extensions are best-
effort handled (e.g. .txt → try CSV-like sniff).

PHI safety: readers DO NOT redact sample values themselves — that's the
agent's responsibility (PhiRedactionLayer in agent.py).  Readers preserve
raw values so the redactor sees the originals.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import IO, Any

import pandas as pd

# ---------------------------------------------------------------------------
# Data classes — what every reader produces
# ---------------------------------------------------------------------------


@dataclass
class FieldDescriptor:
    """One source-side field — what we'll feed the LLM as candidate input."""

    name: str  # raw header / json key / position label
    path: str | None = None  # JSON dot-path (e.g. "subscribers[].id")
    position: int | None = None  # fixed-width column ordinal or width-pair
    width: int | None = None  # fixed-width char width
    inferred_type: str = "TEXT"  # TEXT / NUMBER / DATE / TIMESTAMP / BOOLEAN / VARIANT
    nullable: bool = True
    sample_values: list[str] = field(default_factory=list)
    notes: str = ""  # reader's note (e.g. "all values look like YYYYMMDD")


@dataclass
class SourceView:
    """The reader's full output.  Stored on the proposal row + sent to LLM."""

    format: str  # csv / xlsx / json / fixed_width / docx / pdf
    filename: str
    sheet_or_section: str | None = None  # for multi-tab xlsx, sheet name; for docx, section
    fields: list[FieldDescriptor] = field(default_factory=list)
    sample_rows: list[dict[str, Any]] = field(default_factory=list)
    row_count_sampled: int = 0
    row_count_total: int | None = None  # if known
    reader_notes: list[str] = field(default_factory=list)
    detected_at: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "format": self.format,
            "filename": self.filename,
            "sheet_or_section": self.sheet_or_section,
            "fields": [
                {
                    "name": f.name,
                    "path": f.path,
                    "position": f.position,
                    "width": f.width,
                    "inferred_type": f.inferred_type,
                    "nullable": f.nullable,
                    "sample_values": f.sample_values,
                    "notes": f.notes,
                }
                for f in self.fields
            ],
            "sample_rows": self.sample_rows,
            "row_count_sampled": self.row_count_sampled,
            "row_count_total": self.row_count_total,
            "reader_notes": self.reader_notes,
            "detected_at": self.detected_at,
        }


# ---------------------------------------------------------------------------
# Encoding-fallback helpers — same chain as admin.catalogue_loader.
# CSV / TSV / PSV / JSON files in the wild ship in CP1252, UTF-8-with-BOM,
# and occasionally Latin-1.  Default UTF-8 fails noisily.  We try in order:
# utf-8-sig → utf-8 → cp1252 → latin-1.  Latin-1 NEVER fails — it accepts
# any byte sequence — so the last attempt always succeeds.
# ---------------------------------------------------------------------------


def _read_csv_safe(src, **kwargs) -> tuple[pd.DataFrame, str]:
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


def _decode_bytes_safe(b: bytes) -> tuple[str, str]:
    """Bytes → (text, encoding_used).  Latin-1 fallback never fails."""
    for enc in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            return b.decode(enc), enc
        except UnicodeDecodeError:
            continue
    return b.decode("latin-1", errors="replace"), "latin-1"


# ---------------------------------------------------------------------------
# Type inference + helpers
# ---------------------------------------------------------------------------


def _infer_logical_type_from_series(s: pd.Series) -> str:
    """Pandas dtype → canonical Snowflake-friendly type."""
    if s.empty:
        return "TEXT"
    nn = s.dropna()
    if nn.empty:
        return "TEXT"
    dtype = str(s.dtype)
    if "datetime" in dtype:
        return "TIMESTAMP_NTZ"
    if "int" in dtype:
        return "NUMBER"
    if "float" in dtype:
        return "NUMBER"
    if "bool" in dtype:
        return "BOOLEAN"
    if "object" in dtype:
        # Heuristic — try date parse on first non-null
        sample = str(nn.iloc[0])[:64]
        if len(sample) == 8 and sample.isdigit():
            return "DATE"  # YYYYMMDD pattern
        if 8 <= len(sample) <= 10 and (sample.count("-") == 2 or sample.count("/") == 2):
            return "DATE"
        if "T" in sample and ":" in sample:
            return "TIMESTAMP_NTZ"
    return "TEXT"


def _sample_values(s: pd.Series, n: int = 5) -> list[str]:
    """Pick up to N distinct non-null sample values, stringified + truncated."""
    nn = s.dropna().astype(str).head(n * 4)
    seen: list[str] = []
    for v in nn:
        vs = v[:200]
        if vs not in seen:
            seen.append(vs)
        if len(seen) >= n:
            break
    return seen


# ---------------------------------------------------------------------------
# Readers
# ---------------------------------------------------------------------------


def read_csv(
    src: str | Path | IO[bytes],
    *,
    filename: str = "",
    sample_n: int = 1000,
) -> SourceView:
    """CSV / TSV / PSV.  Sniffs separator + encoding (utf-8-sig → utf-8 →
    cp1252 → latin-1), reads up to ``sample_n`` rows."""
    if isinstance(src, (str, Path)):
        filename = filename or Path(str(src)).name
    else:
        filename = filename or "uploaded.csv"

    if isinstance(src, (str, Path)):
        lower = str(src).lower()
        if lower.endswith(".tsv"):
            df, enc = _read_csv_safe(src, sep="\t", nrows=sample_n)
        elif lower.endswith(".psv"):
            df, enc = _read_csv_safe(src, sep="|", nrows=sample_n)
        else:
            df, enc = _read_csv_safe(src, sep=None, engine="python", nrows=sample_n)
    else:
        df, enc = _read_csv_safe(src, sep=None, engine="python", nrows=sample_n)

    sv = _df_to_source_view(df, format_label="csv", filename=filename, sample_n=sample_n)
    if enc not in ("utf-8", "utf-8-sig"):
        sv.reader_notes.append(f"Read using {enc} fallback (UTF-8 failed)")
    return sv


def read_xlsx(
    src: str | Path | IO[bytes],
    *,
    filename: str = "",
    sheet_name: str | None = None,
    sample_n: int = 1000,
) -> SourceView | list[SourceView]:
    """xlsx.  If ``sheet_name`` is given, return one SourceView for that
    sheet.  Otherwise return a LIST of SourceViews — one per sheet — so the
    operator picks which tab maps to which canonical dataset.
    """
    if isinstance(src, (str, Path)):
        filename = filename or Path(str(src)).name
    else:
        filename = filename or "uploaded.xlsx"

    if sheet_name is not None:
        df = pd.read_excel(src, sheet_name=sheet_name, nrows=sample_n)
        view = _df_to_source_view(df, format_label="xlsx", filename=filename, sample_n=sample_n)
        view.sheet_or_section = sheet_name
        return view

    # Multi-sheet: return one view per sheet
    try:
        xl = pd.ExcelFile(src)
    except Exception as exc:
        # Surface as a single SourceView with an error note
        v = SourceView(format="xlsx", filename=filename, detected_at=datetime.now(UTC).isoformat())
        v.reader_notes.append(f"failed to open workbook: {exc}")
        return [v]

    views: list[SourceView] = []
    for sheet in xl.sheet_names:
        try:
            df = pd.read_excel(src, sheet_name=sheet, nrows=sample_n)
        except Exception as exc:
            v = SourceView(
                format="xlsx",
                filename=filename,
                sheet_or_section=sheet,
                detected_at=datetime.now(UTC).isoformat(),
            )
            v.reader_notes.append(f"failed to read sheet '{sheet}': {exc}")
            views.append(v)
            continue
        view = _df_to_source_view(
            df,
            format_label="xlsx",
            filename=filename,
            sample_n=sample_n,
        )
        view.sheet_or_section = sheet
        views.append(view)
    return views


def read_json(
    src: str | Path | IO[bytes],
    *,
    filename: str = "",
    sample_n: int = 1000,
) -> SourceView:
    """JSON — handles flat-list, nested-list, dict-of-lists, JSONL.

    Walks the structure to extract every leaf field with its dot-path.
    Sample values are pulled from the first ``sample_n`` records.
    """
    enc_used: str | None = None
    if isinstance(src, (str, Path)):
        filename = filename or Path(str(src)).name
        raw_bytes = Path(str(src)).read_bytes()
        text, enc_used = _decode_bytes_safe(raw_bytes)
    else:
        if hasattr(src, "seek"):
            src.seek(0)
        raw = src.read()
        if isinstance(raw, bytes):
            text, enc_used = _decode_bytes_safe(raw)
        else:
            text = str(raw)
            enc_used = "utf-8"
        filename = filename or "uploaded.json"

    # Try JSONL first
    records: list[Any]
    if "\n" in text and text.lstrip().startswith("{"):
        try:
            records = [json.loads(line) for line in text.splitlines() if line.strip()]
        except json.JSONDecodeError:
            records = []
    else:
        records = []
    if not records:
        try:
            parsed = json.loads(text)
            if isinstance(parsed, list):
                records = parsed
            elif isinstance(parsed, dict):
                # If dict has a single key whose value is a list, use that list.
                # E.g. {"members": [{...}, ...]} → records = members
                if len(parsed) == 1 and isinstance(next(iter(parsed.values())), list):
                    records = next(iter(parsed.values()))
                else:
                    records = [parsed]
            else:
                records = [parsed]
        except Exception as exc:
            v = SourceView(
                format="json", filename=filename, detected_at=datetime.now(UTC).isoformat()
            )
            v.reader_notes.append(f"could not parse JSON: {exc}")
            return v

    records = records[:sample_n]
    # Use pandas json_normalize to flatten
    try:
        df = pd.json_normalize(records, max_level=4)
    except Exception:
        # Last resort — pull only top-level keys
        df = pd.DataFrame(records)

    view = _df_to_source_view(df, format_label="json", filename=filename, sample_n=sample_n)
    if enc_used and enc_used not in ("utf-8", "utf-8-sig"):
        view.reader_notes.append(f"Read using {enc_used} fallback (UTF-8 failed)")
    # JSON-specific enrichment — every column name is a dot-path
    for f in view.fields:
        if "." in f.name:
            f.path = f.name
    return view


def read_fixed_width(*args, **kwargs):
    """Pass 3 — fixed-width reader.  Still deferred — use CSV/xlsx/JSON or
    pass the spec doc (.docx / .pdf) so the LLM infers positions from text."""
    raise NotImplementedError(
        "Fixed-width reader is Pass 3.  Use CSV / xlsx / JSON for now, or upload "
        "the corresponding spec file (.docx / PDF) so the agent can infer "
        "positions from the spec."
    )


def read_docx(
    src: str | Path | IO[bytes],
    *,
    filename: str = "",
    sample_n: int = 1000,
) -> SourceView:
    """Read a Word .docx mapping spec.

    Strategy:
      1. Open via python-docx.
      2. If the document has tables → take the FIRST table.  Use row 1 as
         field headers, rows 2..N as sample values.  This is the common
         "data dictionary" pattern (Field name | Type | Required | Desc).
      3. Iterate ALL paragraphs + all table cells, dump the full text into
         ``reader_notes`` so the LLM has full context even when the table
         extraction misses something subtle.

    The agent (LLM) reads the SourceView and proposes a Bronze schema
    based on the spec — same as for CSV/xlsx — except sample_values come
    from the spec text rather than real data rows.
    """
    try:
        import docx as _docx  # python-docx
    except ImportError:
        raise NotImplementedError(
            "python-docx is not installed.  Run `pip install python-docx` (or "
            "rebuild the control_tower container) to enable .docx reading."
        )

    filename = filename or (
        Path(str(src)).name if isinstance(src, (str, Path)) else "uploaded.docx"
    )
    # python-docx accepts either path or file-like
    if hasattr(src, "seek"):
        try:
            src.seek(0)
        except Exception:
            pass
        doc = _docx.Document(src)
    else:
        doc = _docx.Document(str(src))

    sv = SourceView(
        format="docx",
        filename=filename,
        detected_at=datetime.now(UTC).isoformat(),
    )

    # 1) Collect ALL text — paragraphs + table cells.  Cap at 50 KB so we
    #    don't blow the LLM context on monster specs.
    text_chunks: list[str] = []
    for para in doc.paragraphs:
        t = (para.text or "").strip()
        if t:
            text_chunks.append(t)
    text_chunks.append("")  # blank line between paragraphs and tables

    # 2) Table-driven field extraction (most data dictionaries are tables)
    if doc.tables:
        table_chunks: list[str] = []
        first_table = doc.tables[0]
        rows = []
        for tr in first_table.rows:
            cells = [c.text.strip() for c in tr.cells]
            rows.append(cells)
            table_chunks.append(" | ".join(cells))
        if rows:
            headers = [h if h else f"col_{i}" for i, h in enumerate(rows[0], 1)]
            data_rows = rows[1 : 1 + sample_n] if len(rows) > 1 else []
            # Detect which header column is the "field name" — first column
            # whose header contains "field" / "name" / "column".  If nothing
            # matches, use column 0.
            name_col_idx = 0
            for i, h in enumerate(headers):
                hl = h.lower()
                if "field" in hl or "name" in hl or "column" in hl or "attribute" in hl:
                    name_col_idx = i
                    break
            for row in data_rows:
                if not row or name_col_idx >= len(row):
                    continue
                fname = row[name_col_idx].strip()
                if not fname:
                    continue
                sv.fields.append(
                    FieldDescriptor(
                        name=fname,
                        inferred_type="TEXT",
                        nullable=True,
                        sample_values=[c[:200] for c in row[:5]],
                        notes=" · ".join(
                            f"{headers[i]}: {row[i]}"
                            for i in range(min(len(row), len(headers)))
                            if i != name_col_idx and row[i].strip()
                        )[:500],
                    )
                )
            sv.sample_rows = [
                {headers[i]: row[i] for i in range(min(len(row), len(headers)))}
                for row in data_rows[:8]
            ]
        # Also dump other tables as text for the LLM
        for idx, tbl in enumerate(doc.tables[1:], start=2):
            table_chunks.append(f"\n— Table {idx} —")
            for tr in tbl.rows:
                table_chunks.append(" | ".join(c.text.strip() for c in tr.cells))
        text_chunks.extend(table_chunks)

    full_text = "\n".join(text_chunks).strip()
    if len(full_text) > 50000:
        full_text = full_text[:50000] + "\n…[truncated at 50KB]"
    sv.reader_notes.append("FULL_TEXT:\n" + full_text)
    sv.row_count_sampled = len(sv.fields)
    if not sv.fields:
        sv.reader_notes.append(
            "No table-style field list detected in the document.  The agent "
            "will reason from the prose spec text in FULL_TEXT above."
        )
    return sv


def read_pdf(
    src: str | Path | IO[bytes],
    *,
    filename: str = "",
    sample_n: int = 1000,
) -> SourceView:
    """Read a PDF mapping spec.

    Strategy (same shape as :func:`read_docx`):
      1. Open via pdfplumber.
      2. Try to extract tables — use first table as field list.
      3. Dump all extracted text into ``reader_notes`` so the LLM has
         full spec context even when table extraction is partial.
    """
    try:
        import pdfplumber as _pp
    except ImportError:
        raise NotImplementedError(
            "pdfplumber is not installed.  Run `pip install pdfplumber` to enable PDF."
        )

    filename = filename or (Path(str(src)).name if isinstance(src, (str, Path)) else "uploaded.pdf")
    if hasattr(src, "seek"):
        try:
            src.seek(0)
        except Exception:
            pass

    sv = SourceView(
        format="pdf",
        filename=filename,
        detected_at=datetime.now(UTC).isoformat(),
    )

    text_chunks: list[str] = []
    all_tables: list[list[list[str]]] = []

    # pdfplumber accepts path or file-like
    with _pp.open(src) as pdf:
        sv.row_count_total = len(pdf.pages)
        for page_no, page in enumerate(pdf.pages, start=1):
            try:
                page_text = page.extract_text() or ""
            except Exception:
                page_text = ""
            if page_text.strip():
                text_chunks.append(f"\n— Page {page_no} —\n{page_text.strip()}")
            try:
                page_tables = page.extract_tables() or []
            except Exception:
                page_tables = []
            for t in page_tables:
                # normalize: list[list[str|None]] → list[list[str]]
                normalized = [[(cell or "").strip() for cell in row] for row in t if row]
                if normalized and any(any(c for c in r) for r in normalized):
                    all_tables.append(normalized)

    # Use the LARGEST table as the field list (usually the data dictionary
    # spans pages and pdfplumber returns it as separate chunks per page —
    # we collapse heuristically by picking the biggest).
    if all_tables:
        biggest = max(all_tables, key=lambda t: sum(len(r) for r in t))
        if biggest:
            headers = [h if h else f"col_{i}" for i, h in enumerate(biggest[0], 1)]
            data_rows = biggest[1 : 1 + sample_n] if len(biggest) > 1 else []
            name_col_idx = 0
            for i, h in enumerate(headers):
                hl = h.lower()
                if "field" in hl or "name" in hl or "column" in hl or "attribute" in hl:
                    name_col_idx = i
                    break
            for row in data_rows:
                if not row or name_col_idx >= len(row):
                    continue
                fname = row[name_col_idx].strip()
                if not fname:
                    continue
                sv.fields.append(
                    FieldDescriptor(
                        name=fname,
                        inferred_type="TEXT",
                        nullable=True,
                        sample_values=[c[:200] for c in row[:5]],
                        notes=" · ".join(
                            f"{headers[i]}: {row[i]}"
                            for i in range(min(len(row), len(headers)))
                            if i != name_col_idx and row[i].strip()
                        )[:500],
                    )
                )
            sv.sample_rows = [
                {headers[i]: row[i] for i in range(min(len(row), len(headers)))}
                for row in data_rows[:8]
            ]

    full_text = "\n".join(text_chunks).strip()
    if len(full_text) > 50000:
        full_text = full_text[:50000] + "\n…[truncated at 50KB]"
    sv.reader_notes.append("FULL_TEXT:\n" + full_text)
    sv.row_count_sampled = len(sv.fields)
    if not sv.fields:
        sv.reader_notes.append(
            "No table-style field list detected in the PDF.  The agent will "
            "reason from the prose spec text in FULL_TEXT above."
        )
    return sv


def read_text(
    spec_text: str,
    *,
    filename: str = "pasted_text",
) -> SourceView:
    """Read a free-text mapping spec — operator pasted into a text area.

    No structure assumed — the entire text becomes the FULL_TEXT context
    that the LLM reasons from.  If the text happens to look CSV-ish on the
    first line (commas + multiple lines), we'll best-effort parse it.
    """
    sv = SourceView(
        format="freetext",
        filename=filename,
        detected_at=datetime.now(UTC).isoformat(),
    )
    spec_text = (spec_text or "").strip()
    if not spec_text:
        sv.reader_notes.append("empty input")
        return sv

    # Best-effort CSV sniff: if first non-empty line has 2+ commas AND there
    # are 2+ more non-empty lines below, parse as CSV.  Otherwise treat as
    # prose spec.
    lines = [ln for ln in spec_text.splitlines() if ln.strip()]
    if len(lines) >= 3 and lines[0].count(",") >= 2 and lines[1].count(",") >= 2:
        try:
            import io as _io

            df = pd.read_csv(_io.StringIO(spec_text))
            sv2 = _df_to_source_view(df, format_label="freetext", filename=filename, sample_n=1000)
            sv2.reader_notes.append(
                "Parsed as CSV-style table (auto-detected from comma + line structure)."
            )
            sv2.reader_notes.append("FULL_TEXT:\n" + spec_text[:50000])
            return sv2
        except Exception:
            pass  # fall through to prose mode

    text = spec_text if len(spec_text) <= 50000 else spec_text[:50000] + "\n…[truncated at 50KB]"
    sv.reader_notes.append("FULL_TEXT:\n" + text)
    sv.reader_notes.append(
        "Prose spec — agent will reason from the description in FULL_TEXT above "
        "(no structured table detected)."
    )
    sv.row_count_sampled = 0
    return sv


# ---------------------------------------------------------------------------
# Internal: build a SourceView from a pandas DataFrame
# ---------------------------------------------------------------------------


def _df_to_source_view(
    df: pd.DataFrame,
    *,
    format_label: str,
    filename: str,
    sample_n: int,
) -> SourceView:
    sv = SourceView(
        format=format_label, filename=filename, detected_at=datetime.now(UTC).isoformat()
    )
    if df is None or df.empty:
        sv.reader_notes.append("empty file or no parseable rows")
        sv.row_count_sampled = 0
        return sv
    sv.row_count_sampled = int(min(len(df), sample_n))
    # Per-field descriptors
    for col in df.columns:
        s = df[col]
        sv.fields.append(
            FieldDescriptor(
                name=str(col),
                inferred_type=_infer_logical_type_from_series(s),
                nullable=bool(s.isna().any()),
                sample_values=_sample_values(s, n=5),
            )
        )
    # Sample rows for context (capped, stringified for JSON safety)
    head = df.head(8).fillna("").astype(str)
    sv.sample_rows = head.to_dict(orient="records")
    return sv


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------


def detect_and_read(
    src: str | Path | IO[bytes],
    *,
    filename: str = "",
    sheet_name: str | None = None,
    sample_n: int = 1000,
) -> SourceView | list[SourceView]:
    """Sniff the file and route to the right reader.

    Returns a single :class:`SourceView` for CSV/JSON/single-sheet xlsx, or
    a list of :class:`SourceView` for multi-tab xlsx (one per tab) so the
    UI can present them with a per-tab selector.
    """
    name = filename or (str(src) if isinstance(src, (str, Path)) else "")
    lower = name.lower()

    if (
        lower.endswith(".csv")
        or lower.endswith(".tsv")
        or lower.endswith(".psv")
        or lower.endswith(".txt")
    ):
        return read_csv(src, filename=filename or name, sample_n=sample_n)
    if lower.endswith(".xlsx") or lower.endswith(".xls") or lower.endswith(".xlsm"):
        return read_xlsx(src, filename=filename or name, sheet_name=sheet_name, sample_n=sample_n)
    if lower.endswith(".json") or lower.endswith(".jsonl") or lower.endswith(".ndjson"):
        return read_json(src, filename=filename or name, sample_n=sample_n)
    if lower.endswith(".docx"):
        return read_docx(src, filename=filename or name)
    if lower.endswith(".pdf"):
        return read_pdf(src, filename=filename or name)

    # Unknown extension — best-effort CSV sniff
    return read_csv(src, filename=filename or name, sample_n=sample_n)
