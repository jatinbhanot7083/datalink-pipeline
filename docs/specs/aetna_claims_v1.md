# Aetna Claims Data Contract — Bronze Layer

## Overview
This contract defines the structure and validation rules for Aetna claims data ingested via CSV file feed. The schema is anchored to FHIR R4 Claim Resource with medium-strictness conformance.

## File Specifications
- **Format**: CSV (comma-delimited)
- **Encoding**: UTF-8
- **Header**: Yes (column names in first row)
- **Expected Frequency**: Daily or batch
- **Sample File**: `aetna_claims_test.csv`

## Column Specifications

| Column | Type | Nullable | FHIR Standard | Validation Rule | Example |
|--------|------|----------|---------------|-----------------|----------|
| `claim_id` | VARCHAR | NO | Claim.id | Unique per load; non-empty | `C001` |
| `member_id` | VARCHAR | NO | Claim.patient.identifier.value | Non-empty; alphanumeric | `M001` |
| `provider_npi` | VARCHAR | YES | Claim.provider.identifier.value | Regex: `^[0-9]{10}$` (NPI format) | `1234567890` |
| `cpt_code` | VARCHAR | YES | Claim.item[0].productOrService.coding[0].code | Regex: `^[0-9]{5}([A-Z]{2})?$` (CPT-4 format) | `99213` |
| `icd10_primary` | VARCHAR | YES | Claim.diagnosis[0].diagnosis.coding[0].code | Regex: `^[A-Z][0-9]{2}(\.[A-Z0-9]{1,4})?$` (ICD-10-CM format) | `E11.9` |

## Audit Columns (Injected by Runtime)
- `_load_dt`: TIMESTAMP — UTC timestamp of load execution
- `_source_file`: VARCHAR — Source file name (e.g., `aetna_claims_test.csv`)
- `_batch_id`: VARCHAR — Unique batch identifier for this load
- `_record_source`: VARCHAR — Source system identifier (e.g., `aetna`)
- `_load_type`: VARCHAR — Load mode (e.g., `FILE_DRIVEN`)
- `_file_row_number`: INTEGER — Row number in source file (1-indexed, excluding header)
- `_record_hash`: VARCHAR — SHA-256 hash of record for idempotency detection

## Data Quality Rules
1. **Primary Key**: `(claim_id, member_id, _load_dt)` — No duplicates within a load batch.
2. **Referential Integrity**: `member_id` must exist in the member master (validated downstream in Silver layer).
3. **Format Validation**: NPI, CPT, and ICD-10 codes must conform to regex patterns (see table above).
4. **Completeness**: `claim_id` and `member_id` are mandatory; other columns may be null if not applicable to the claim type.

## Deviations from FHIR R4 Standard
- **Scope**: This Bronze table captures only the primary diagnosis and first procedure code. FHIR Claim.diagnosis and Claim.item are arrays; full array support is deferred to Silver/Gold layers.
- **Missing Fields**: The following FHIR Claim fields are not present in this vendor feed and should be sourced from supplementary files or systems:
  - `Claim.billablePeriod.start` (service date)
  - `Claim.insurance[0].coverage.identifier.value` (plan ID)
  - `Claim.item[0].net.value` (billed amount)
  - `Claim.status` (claim status)
  - `Claim.related[0].reference.value` (prior authorization reference)
  - **Action**: Operator to confirm whether these fields are available in a separate feed or should be sourced from a claims adjudication system.

## Load Idempotency
The `_record_hash` column enables idempotent reloads. If a record with the same hash is detected in a subsequent load, it is skipped (no duplicate insertion).

## Contact
- **Data Steward**: [Aetna Claims Integration Team]
- **Last Updated**: [YYYY-MM-DD]
- **Version**: 1.0
