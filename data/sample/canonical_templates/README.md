# Canonical Product Catalogue — sample templates (v2)

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

- **5 dataset groups** (Membership, Claims-Encounters, Provider, Daily Census (ADT), CMS-MMR)
- **20 field rows** (4 fields per dataset)

The real production catalogue has 34 datasets + 943 fields.

## Files

| File | Format | Shape |
|---|---|---|
| `canonical_catalogue.xlsx` | Excel | **Single sheet** `08_Field_Catalogue` — one row per field, Dataset column grouped |
| `canonical_catalogue.csv`  | CSV | Flat — one row per (dataset, field), Dataset on every row |
| `canonical_catalogue.tsv`  | TSV | Tab-separated equivalent of CSV |
| `canonical_catalogue.psv`  | PSV | Pipe-separated equivalent of CSV |
| `canonical_catalogue.json` | JSON | `{"field_catalogue": [...]}` — single array |
| `canonical_catalogue.jsonl`| JSONL | One JSON object per line, each describing a field |

## Validation

Every upload runs through `datalink.admin.canonical_validators.smart_validate`
**before** any Snowflake write.  Validation errors are surfaced in the upload
result panel with a clear remediation message.

## Regenerating

Run `python scripts/build_canonical_samples.py` after any change to the
master Product Catalogue source xlsx.
