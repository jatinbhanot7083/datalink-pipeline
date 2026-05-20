"""Build canonical-format sample files (v2 — single-sheet contract).

Generates ONE sample of each supported format into
``data/sample/canonical_templates/`` so operators can download a working
template + open it in their tool of choice.

v2 — May 2026: the canonical xlsx is **a single sheet** named
``08_Field_Catalogue`` (matching what the operational DB architect ships).
Dataset-level metadata is **derived** at load time from per-field rows.

Samples are HARDCODED (not pulled from a live source) — so they:
  * always reflect the canonical SPEC (not whatever state the DB is in)
  * are deterministic across runs
  * survive a CONTROL wipe

Run:

    python scripts/build_canonical_samples.py
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

OUT_DIR = Path(__file__).resolve().parents[1] / "data" / "sample" / "canonical_templates"


# ---------------------------------------------------------------------------
# Sample data — 5 datasets · 4 fields each = 20 field rows
#
# Per-dataset Frequency is set on the FIRST row of each group (matching the
# Excel grouped-layout convention).  Subsequent rows in the same group leave
# Frequency blank — the loader's forward-fill is NOT used for Frequency, so
# we put the value only on the first row of each group on purpose.  (For
# CSV / JSON / JSONL outputs, Frequency is repeated on every row since
# they're flat.)
# ---------------------------------------------------------------------------

SAMPLE_FIELDS: list[dict[str, str]] = [
    # Membership — 4 fields
    {
        "Dataset": "Membership",
        "Field name": "Payer Name",
        "Req / Opt": "Required",
        "Description": "Name of the member's primary insurer",
        "Additional notes": "",
        "Example": "Aetna Health Inc.",
        "Frequency": "Monthly",
    },
    {
        "Dataset": "Membership",
        "Field name": "Member Card ID",
        "Req / Opt": "Required",
        "Description": "Insurer's unique identification number for the member",
        "Additional notes": "Used as canonical member_id downstream",
        "Example": "A12345678",
        "Frequency": "Monthly",
    },
    {
        "Dataset": "Membership",
        "Field name": "DOB",
        "Req / Opt": "Required",
        "Description": "Member's date of birth",
        "Additional notes": "Preferred format is yyyy-mm-dd",
        "Example": "1972-08-15",
        "Frequency": "Monthly",
    },
    {
        "Dataset": "Membership",
        "Field name": "Gender",
        "Req / Opt": "Required",
        "Description": "Member's biological sex",
        "Additional notes": "M / F / U / X allowed",
        "Example": "M",
        "Frequency": "Monthly",
    },
    # Claims-Encounters — 4 fields
    {
        "Dataset": "Claims-Encounters",
        "Field name": "Claim ID",
        "Req / Opt": "Required",
        "Description": "Unique claim identifier from the payer",
        "Additional notes": "Combined with claim_line for line-level uniqueness",
        "Example": "CL0001234567",
        "Frequency": "Monthly",
    },
    {
        "Dataset": "Claims-Encounters",
        "Field name": "Member Card ID",
        "Req / Opt": "Required",
        "Description": "Patient's payer card ID — joins to Membership",
        "Additional notes": "",
        "Example": "A12345678",
        "Frequency": "Monthly",
    },
    {
        "Dataset": "Claims-Encounters",
        "Field name": "Service Start Date",
        "Req / Opt": "Required",
        "Description": "Earliest date of service on the claim line",
        "Additional notes": "Preferred format yyyy-mm-dd",
        "Example": "2026-01-15",
        "Frequency": "Monthly",
    },
    {
        "Dataset": "Claims-Encounters",
        "Field name": "Total Charge",
        "Req / Opt": "Required",
        "Description": "Total charged amount in dollars",
        "Additional notes": "Decimal(12,2); negative allowed for reversals",
        "Example": "1234.56",
        "Frequency": "Monthly",
    },
    # Provider — 4 fields
    {
        "Dataset": "Provider",
        "Field name": "NPI",
        "Req / Opt": "Required",
        "Description": "National Provider Identifier (10 digits)",
        "Additional notes": "Must validate Luhn checksum",
        "Example": "1234567893",
        "Frequency": "Monthly",
    },
    {
        "Dataset": "Provider",
        "Field name": "Provider Name",
        "Req / Opt": "Required",
        "Description": "Legal name of the provider",
        "Additional notes": "",
        "Example": "Dr. Jane Smith, MD",
        "Frequency": "Monthly",
    },
    {
        "Dataset": "Provider",
        "Field name": "Specialty",
        "Req / Opt": "Required",
        "Description": "Primary clinical specialty (taxonomy code or text)",
        "Additional notes": "NUCC taxonomy preferred",
        "Example": "Internal Medicine",
        "Frequency": "Monthly",
    },
    {
        "Dataset": "Provider",
        "Field name": "Network Status",
        "Req / Opt": "Optional",
        "Description": "In-network / Out-of-network / Terminated",
        "Additional notes": "",
        "Example": "In-network",
        "Frequency": "Monthly",
    },
    # Daily Census (ADT) — 4 fields
    {
        "Dataset": "Daily Census (ADT)",
        "Field name": "Encounter ID",
        "Req / Opt": "Required",
        "Description": "Unique encounter identifier from the facility",
        "Additional notes": "",
        "Example": "ENC-2026-0001",
        "Frequency": "Daily",
    },
    {
        "Dataset": "Daily Census (ADT)",
        "Field name": "Member Card ID",
        "Req / Opt": "Required",
        "Description": "Patient identifier — joins to Membership",
        "Additional notes": "",
        "Example": "A12345678",
        "Frequency": "Daily",
    },
    {
        "Dataset": "Daily Census (ADT)",
        "Field name": "Admit Date",
        "Req / Opt": "Required",
        "Description": "Date of admission",
        "Additional notes": "yyyy-mm-dd",
        "Example": "2026-05-10",
        "Frequency": "Daily",
    },
    {
        "Dataset": "Daily Census (ADT)",
        "Field name": "Discharge Status",
        "Req / Opt": "Optional",
        "Description": "Discharge disposition code; NULL if still admitted",
        "Additional notes": "UB-04 standard codes",
        "Example": "01",
        "Frequency": "Daily",
    },
    # CMS-MMR — 4 fields
    {
        "Dataset": "CMS-MMR",
        "Field name": "HICN",
        "Req / Opt": "Required",
        "Description": "Health Insurance Claim Number (legacy Medicare ID)",
        "Additional notes": "Replaced by MBI for new members; both may appear",
        "Example": "123456789A",
        "Frequency": "Monthly",
    },
    {
        "Dataset": "CMS-MMR",
        "Field name": "MBI",
        "Req / Opt": "Required",
        "Description": "Medicare Beneficiary Identifier",
        "Additional notes": "11-char alphanumeric; replaces HICN",
        "Example": "1EG4-TE5-MK73",
        "Frequency": "Monthly",
    },
    {
        "Dataset": "CMS-MMR",
        "Field name": "Payment Year/Month",
        "Req / Opt": "Required",
        "Description": "Reporting year + month (YYYYMM)",
        "Additional notes": "",
        "Example": "202605",
        "Frequency": "Monthly",
    },
    {
        "Dataset": "CMS-MMR",
        "Field name": "Risk Score",
        "Req / Opt": "Required",
        "Description": "CMS-HCC risk score for the member",
        "Additional notes": "Decimal(6,4) — RAF score",
        "Example": "1.2345",
        "Frequency": "Monthly",
    },
]


# Column order used in xlsx + csv (matches the operator's real artifact)
COL_ORDER = [
    "Dataset",
    "Field name",
    "Req / Opt",
    "Description",
    "Additional notes",
    "Example",
    "Frequency",
]


def _build_flat_df() -> pd.DataFrame:
    """Build the flat DataFrame — every row has every column populated."""
    return pd.DataFrame(SAMPLE_FIELDS, columns=COL_ORDER)


def _build_grouped_df_for_xlsx() -> pd.DataFrame:
    """Build the GROUPED DataFrame — Dataset is filled ONLY on the first row
    of each dataset's group (matching the operator's Excel layout)."""
    df = _build_flat_df().copy()
    prev = None
    for i, row in df.iterrows():
        if row["Dataset"] == prev:
            df.at[i, "Dataset"] = ""
        else:
            prev = row["Dataset"]
    return df


def _write_xlsx(dest: Path) -> None:
    """Single-sheet xlsx — sheet named '08_Field_Catalogue', grouped layout."""
    df = _build_grouped_df_for_xlsx()
    with pd.ExcelWriter(dest, engine="openpyxl") as w:
        df.to_excel(w, sheet_name="08_Field_Catalogue", index=False)


def _write_csv(dest: Path, sep: str) -> None:
    """Flat CSV/TSV/PSV — Dataset filled on EVERY row (no grouping)."""
    df = _build_flat_df()
    df.to_csv(dest, index=False, sep=sep, encoding="utf-8")


def _write_json(dest: Path) -> None:
    """Single `field_catalogue` array — dataset metadata is derived at load time."""
    obj = {
        "field_catalogue": [
            {
                "dataset": str(r.get("Dataset", "") or ""),
                "field_name": str(r.get("Field name", "") or ""),
                "req_opt": str(r.get("Req / Opt", "") or ""),
                "description": str(r.get("Description", "") or ""),
                "additional_notes": str(r.get("Additional notes", "") or ""),
                "example": str(r.get("Example", "") or ""),
                "frequency": str(r.get("Frequency", "") or ""),
            }
            for r in SAMPLE_FIELDS
        ],
    }
    dest.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")


def _write_jsonl(dest: Path) -> None:
    """One JSON object per line — every record is a field record (no _kind)."""
    lines: list[str] = []
    for r in SAMPLE_FIELDS:
        rec = {
            "dataset": r["Dataset"],
            "field_name": r["Field name"],
            "req_opt": r["Req / Opt"],
            "description": r["Description"],
            "additional_notes": r["Additional notes"],
            "example": r["Example"],
            "frequency": r["Frequency"],
        }
        lines.append(json.dumps(rec, ensure_ascii=False))
    dest.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_readme(dest_dir: Path, ds_count: int, fd_count: int) -> None:
    readme = f"""# Canonical Product Catalogue — sample templates (v2)

This directory ships **one canonical sample per supported file format** so
operators see the exact shape DataLink expects before they upload via
**Data Model Designer → 📂 Upload catalogue**.

## v2 contract — single sheet, fields-only

The canonical xlsx ships **one sheet** named `08_Field_Catalogue` with one
row per field.  Dataset-level metadata (Frequency, total Fields, Required
count, Optional count) is **derived** at load time from per-field rows.

The `Dataset` column uses Excel's grouped-layout convention: filled on the
FIRST row of each dataset's block, left blank on subsequent rows.  The
loader forward-fills automatically.

Each sample contains:

- **{ds_count} dataset groups** (Membership, Claims-Encounters, Provider, Daily Census (ADT), CMS-MMR)
- **{fd_count} field rows** (4 fields per dataset)

The real production catalogue has 34 datasets + 943 fields.

## Files

| File | Format | Shape |
|---|---|---|
| `canonical_catalogue.xlsx` | Excel | **Single sheet** `08_Field_Catalogue` — one row per field, Dataset column grouped |
| `canonical_catalogue.csv`  | CSV | Flat — one row per (dataset, field), Dataset on every row |
| `canonical_catalogue.tsv`  | TSV | Tab-separated equivalent of CSV |
| `canonical_catalogue.psv`  | PSV | Pipe-separated equivalent of CSV |
| `canonical_catalogue.json` | JSON | `{{"field_catalogue": [...]}}` — single array |
| `canonical_catalogue.jsonl`| JSONL | One JSON object per line, each describing a field |

## Validation

Every upload runs through `datalink.admin.canonical_validators.smart_validate`
**before** any Snowflake write.  Validation errors are surfaced in the upload
result panel with a clear remediation message.

## Regenerating

Run `python scripts/build_canonical_samples.py` after any change to the
master Product Catalogue source xlsx.
"""
    (dest_dir / "README.md").write_text(readme, encoding="utf-8")


# ---------------------------------------------------------------------------
# Single-dataset sample templates — for operators uploading ONE dataset only
# ---------------------------------------------------------------------------

# Hand-picked illustrative single dataset showing the full column convention.
# The dataset name placeholder ("New Dataset") signals to the user that they
# should rename it before upload.
SINGLE_DATASET_FIELDS: list[dict[str, str]] = [
    {
        "Dataset": "New Dataset",
        "Field name": "Member ID",
        "Req / Opt": "Required",
        "Description": "Unique member identifier from the source system",
        "Additional notes": "Used as the canonical member_id downstream",
        "Example": "M0000123",
        "Frequency": "Monthly",
    },
    {
        "Dataset": "New Dataset",
        "Field name": "Last Name",
        "Req / Opt": "Required",
        "Description": "Member's legal last name",
        "Additional notes": "",
        "Example": "Smith",
        "Frequency": "Monthly",
    },
    {
        "Dataset": "New Dataset",
        "Field name": "Date of Birth",
        "Req / Opt": "Required",
        "Description": "Member's date of birth",
        "Additional notes": "Preferred format yyyy-mm-dd",
        "Example": "1972-08-15",
        "Frequency": "Monthly",
    },
    {
        "Dataset": "New Dataset",
        "Field name": "Postal Code",
        "Req / Opt": "Optional",
        "Description": "Member's mailing ZIP / postal code",
        "Additional notes": "5-digit US ZIP or alphanumeric for non-US",
        "Example": "10001",
        "Frequency": "Monthly",
    },
]


def _write_single_dataset_xlsx(dest: Path) -> None:
    """Single-dataset xlsx — one sheet `08_Field_Catalogue` with one dataset."""
    df = pd.DataFrame(SINGLE_DATASET_FIELDS, columns=COL_ORDER).copy()
    # Apply grouped-layout convention: Dataset filled only on first row
    for i in range(1, len(df)):
        df.at[i, "Dataset"] = ""
    with pd.ExcelWriter(dest, engine="openpyxl") as w:
        df.to_excel(w, sheet_name="08_Field_Catalogue", index=False)


def _write_single_dataset_csv(dest: Path, sep: str) -> None:
    pd.DataFrame(SINGLE_DATASET_FIELDS, columns=COL_ORDER).to_csv(
        dest,
        index=False,
        sep=sep,
        encoding="utf-8",
    )


def _write_single_dataset_json(dest: Path) -> None:
    obj = {
        "field_catalogue": [
            {
                "dataset": r["Dataset"],
                "field_name": r["Field name"],
                "req_opt": r["Req / Opt"],
                "description": r["Description"],
                "additional_notes": r["Additional notes"],
                "example": r["Example"],
                "frequency": r["Frequency"],
            }
            for r in SINGLE_DATASET_FIELDS
        ]
    }
    dest.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")


def _write_single_dataset_jsonl(dest: Path) -> None:
    lines = [
        json.dumps(
            {
                "dataset": r["Dataset"],
                "field_name": r["Field name"],
                "req_opt": r["Req / Opt"],
                "description": r["Description"],
                "additional_notes": r["Additional notes"],
                "example": r["Example"],
                "frequency": r["Frequency"],
            },
            ensure_ascii=False,
        )
        for r in SINGLE_DATASET_FIELDS
    ]
    dest.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    print("Building canonical samples (v2 single-sheet contract)")
    distinct_ds = sorted({r["Dataset"] for r in SAMPLE_FIELDS})
    print(f"  datasets:  {len(distinct_ds)}")
    print(f"  fields:    {len(SAMPLE_FIELDS)}")

    print("Writing multi-dataset samples...")
    _write_xlsx(OUT_DIR / "canonical_catalogue.xlsx")
    _write_csv(OUT_DIR / "canonical_catalogue.csv", sep=",")
    _write_csv(OUT_DIR / "canonical_catalogue.tsv", sep="\t")
    _write_csv(OUT_DIR / "canonical_catalogue.psv", sep="|")
    _write_json(OUT_DIR / "canonical_catalogue.json")
    _write_jsonl(OUT_DIR / "canonical_catalogue.jsonl")

    print("Writing single-dataset samples...")
    _write_single_dataset_xlsx(OUT_DIR / "single_dataset.xlsx")
    _write_single_dataset_csv(OUT_DIR / "single_dataset.csv", sep=",")
    _write_single_dataset_csv(OUT_DIR / "single_dataset.tsv", sep="\t")
    _write_single_dataset_csv(OUT_DIR / "single_dataset.psv", sep="|")
    _write_single_dataset_json(OUT_DIR / "single_dataset.json")
    _write_single_dataset_jsonl(OUT_DIR / "single_dataset.jsonl")

    _write_readme(OUT_DIR, len(distinct_ds), len(SAMPLE_FIELDS))

    print()
    for p in sorted(OUT_DIR.iterdir()):
        print(f"  + {p.name}  ({p.stat().st_size} bytes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
