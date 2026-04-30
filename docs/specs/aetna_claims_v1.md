# Aetna Claims Data Contract Specification

## Overview
This Bronze-layer data contract defines the schema for Aetna professional health care claims (837P equivalent) ingested via CSV file feed.

## File Characteristics
- **Format**: CSV (comma-separated values)
- **Encoding**: UTF-8 (assumed)
- **Header**: Present (7 columns)
- **Sample file**: `aetna_claims_smoke.csv`
- **Expected frequency**: Daily or batch-driven

## Column Specifications

| Column | Vendor Name | Standard | Type | Nullable | Validation | Notes |
|--------|-------------|----------|------|----------|------------|-------|
| claim_id | ClaimNumber | X12 837P CLM01; FHIR Claim.identifier | VARCHAR | NO | Unique per claim; alphanumeric | Primary key; must be unique across all loads |
| member_id | MbrNo | X12 837P Loop 2010BA NM109; FHIR Claim.patient | VARCHAR | NO | Alphanumeric; matches member master | Foreign key to member dimension |
| provider_npi | ProviderNPI | X12 837P Loop 2310B NM109; FHIR Claim.provider | VARCHAR | NO | Exactly 10 digits; valid NPI | Rendering provider; must be valid NPI |
| cpt_code | CPT | X12 837P Loop 2400 SV101-2; FHIR Claim.item.productOrService | VARCHAR | NO | 5 digits + optional 2-char modifier; matches AMA CPT | Procedure code; must be valid CPT-4 |
| icd10_primary | ICD10 | X12 837P Loop 2300 HI01-2 (ABK); FHIR Claim.diagnosis[0] | VARCHAR | YES | ICD-10-CM format (e.g., E11.9); matches CMS ICD-10 | Primary diagnosis; nullable if not provided |
| icd10_secondary | (not in vendor file) | X12 837P Loop 2300 HI02-2 (ABF); FHIR Claim.diagnosis[1..n] | VARCHAR | YES | ICD-10-CM format; matches CMS ICD-10 | Secondary diagnosis; **MISSING from vendor file—recommend requesting** |
| service_date | SvcDt | X12 837P Loop 2400 DTP*472; FHIR Claim.item.servicedDate | DATE | NO | ISO 8601 (YYYY-MM-DD); must be ≤ claim submission date | Date service was rendered |
| billed_amount | BilledAmt | X12 837P Loop 2400 SV102; FHIR Claim.item.unitPrice | DECIMAL(12,2) | NO | Numeric; 0.00 to 999,999.99; 2 decimal places | Total billed charge for line item |
| claim_status | (not in vendor file) | X12 837P CLM05-3; FHIR Claim.status | VARCHAR | YES | Enum: active, cancelled, draft, entered-in-error | **MISSING from vendor file—recommend deriving or requesting** |
| plan_id | (not in vendor file) | X12 837P Loop 2010BB NM109 + Loop 2330B SBR; FHIR Claim.insurer | VARCHAR | YES | Alphanumeric; matches plan master | **MISSING from vendor file—recommend requesting** |
| prior_auth_ref | (not in vendor file) | X12 837P Loop 2300 REF*9F; FHIR Claim.preAuthRef | VARCHAR | YES | Alphanumeric; matches prior auth system | **MISSING from vendor file—recommend requesting** |

## Standards Alignment

### X12 837P (Professional Health Care Claim — 5010)
- **Claim ID**: CLM01 segment
- **Member ID**: Loop 2010BA NM109 (subscriber)
- **Provider NPI**: Loop 2310B NM109 (rendering provider)
- **Procedure Code**: Loop 2400 SV101-2 (CPT/HCPCS)
- **Diagnosis**: Loop 2300 HI segment (ABK = primary, ABF = secondary)
- **Service Date**: Loop 2400 DTP*472
- **Billed Amount**: Loop 2400 SV102
- **Claim Status**: CLM05-3 (frequency code) + adjudication
- **Plan/Payer**: Loop 2010BB NM109 + Loop 2330B SBR
- **Prior Auth**: Loop 2300 REF*9F or Loop 2400 REF*9F

### FHIR R4 Claim Resource
- **Claim.identifier**: Unique claim ID
- **Claim.patient**: Member/subscriber reference
- **Claim.provider**: Rendering provider reference (NPI)
- **Claim.item.productOrService**: CPT/HCPCS code (CodeSystem: http://www.ama-assn.org/go/cpt)
- **Claim.diagnosis[].diagnosis**: ICD-10-CM code (CodeSystem: http://hl7.org/fhir/sid/icd-10-cm)
- **Claim.item.servicedDate**: Date service rendered
- **Claim.item.unitPrice**: Billed charge
- **Claim.status**: active | cancelled | draft | entered-in-error
- **Claim.insurer**: Payer/plan reference
- **Claim.preAuthRef**: Prior authorization reference

## Data Quality Rules

1. **Uniqueness**: `claim_id` must be unique within each load batch.
2. **Referential Integrity**: `member_id` should exist in the member master; `provider_npi` should be a valid 10-digit NPI.
3. **Code Validity**: `cpt_code` must be a valid AMA CPT-4 code; `icd10_primary` and `icd10_secondary` must be valid CMS ICD-10-CM codes.
4. **Date Logic**: `service_date` must be ≤ claim submission date (inferred from file load date).
5. **Amount Validation**: `billed_amount` must be ≥ 0.00 and ≤ 999,999.99.
6. **Enum Compliance**: `claim_status` (when populated) must be one of: active, cancelled, draft, entered-in-error.

## Missing Fields (Vendor Exception)

The following standard fields are **not present** in the vendor's current file:
- `icd10_secondary` (X12 HI02-2, FHIR Claim.diagnosis[1..n])
- `claim_status` (X12 CLM05-3, FHIR Claim.status)
- `plan_id` (X12 Loop 2010BB + 2330B, FHIR Claim.insurer)
- `prior_auth_ref` (X12 REF*9F, FHIR Claim.preAuthRef)

**Recommendation**: Request these fields from Aetna or document as vendor exceptions with a plan to backfill via a separate lookup or enrichment process.

## Audit Columns (Injected by Runtime)

The following 7 columns are automatically appended by the DataLink ingestion framework:
- `_load_dt` (TIMESTAMP): UTC timestamp of load execution
- `_source_file` (VARCHAR): Name of the source file (e.g., `aetna_claims_smoke.csv`)
- `_batch_id` (VARCHAR): Unique batch identifier for this load
- `_record_source` (VARCHAR): Source system identifier (e.g., `aetna`)
- `_load_type` (VARCHAR): Load type (e.g., `FILE_DRIVEN`, `API_PULL`)
- `_file_row_number` (INTEGER): Row number in the source file
- `_record_hash` (VARCHAR): SHA-256 hash of the record for deduplication

## Example Record

```
claim_id,member_id,provider_npi,cpt_code,icd10_primary,service_date,billed_amount
CLM00001,MBR1001,1234567890,99213,E11.9,2026-01-15,125.00
```

## Contact & Governance

- **Data Owner**: Aetna Claims Operations
- **Technical Contact**: DataLink Engineering
- **Last Updated**: 2026-01-15
- **Version**: 1.0 (DRAFT)
