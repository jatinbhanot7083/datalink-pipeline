# CMS Data Dictionaries — Medicare FFS, MA, Medicaid

CMS publishes data dictionaries for Medicare Fee-for-Service (FFS) claims,
Medicare Advantage (MA) encounters, and Medicaid State Plans. These are
the authoritative reference for federal-program claim structure, beyond
the X12 wire format.

## Medicare FFS Claim Source Tables (CCW / RIF)

The Chronic Conditions Warehouse (CCW) Research Identifiable Files (RIF)
are the canonical CMS source for FFS claims research. Five claim files:

| File | Description | Key Fields |
|---|---|---|
| `INPATIENT_BASE_CLAIMS` | Inpatient hospital stays (Part A institutional) | `BENE_ID`, `CLM_ID`, `NCH_NEAR_LINE_REC_IDENT_CD`, `CLM_FROM_DT`, `CLM_THRU_DT`, `PRVDR_NUM`, `CLM_PMT_AMT`, `NCH_PRMRY_PYR_CLM_PD_AMT`, `CLM_DRG_CD`, `ICD_DGNS_CD1`..`25`, `ICD_PRCDR_CD1`..`25` |
| `OUTPATIENT_BASE_CLAIMS` | Outpatient department services (Part B institutional) | `BENE_ID`, `CLM_ID`, `CLM_FROM_DT`, `CLM_THRU_DT`, `PRVDR_NUM`, `CLM_PMT_AMT`, `ICD_DGNS_CD1`..`25` |
| `CARRIER_BASE_CLAIMS` | Physician/supplier claims (Part B professional) | `BENE_ID`, `CLM_ID`, `LINE_NUM`, `CLM_FROM_DT`, `PRF_PHYSN_NPI`, `LINE_CMS_TYPE_SRVC_CD`, `LINE_PLACE_OF_SRVC_CD`, `HCPCS_CD`, `HCPCS_1ST_MDFR_CD`..`5TH`, `LINE_NCH_PMT_AMT`, `LINE_BENE_PRMRY_PYR_PD_AMT`, `LINE_BENE_PTB_DDCTBL_AMT`, `LINE_COINSRNC_AMT`, `LINE_SBMTD_CHRG_AMT`, `LINE_ALOWD_CHRG_AMT` |
| `DME_BASE_CLAIMS` | Durable Medical Equipment claims | similar to Carrier with HCPCS codes |
| `HHA_BASE_CLAIMS` | Home Health Agency | similar to Outpatient with HHA-specific fields |
| `HOSPICE_BASE_CLAIMS` | Hospice (Part A) | similar to Inpatient |
| `SNF_BASE_CLAIMS` | Skilled Nursing Facility (Part A) | similar to Inpatient |
| `MEDPAR` | Inpatient and SNF stays consolidated by stay (research-friendly) | one row per stay |

## Beneficiary Master File

`BENEFICIARY_SUMMARY` (BSF) — annual snapshots:

| Field | Description |
|---|---|
| `BENE_ID` | Encrypted unique beneficiary identifier (NOT raw HICN/MBI) |
| `BENE_BIRTH_DT` | Birth date |
| `BENE_DEATH_DT` | Death date if applicable |
| `BENE_SEX_IDENT_CD` | 1=Male, 2=Female, 0=Unknown |
| `BENE_RACE_CD` | 0..6 race codes (CMS race code set) |
| `BENE_RTI_RACE_CD` | RTI-corrected race code (more accurate) |
| `STATE_CODE` | SSA state code (NOT FIPS) |
| `BENE_COUNTY_CD` | SSA county code |
| `ZIP_CD` | First 5 digits |
| `BENE_ENROLLMT_REF_YR` | Reference year |
| `BENE_HMO_CVRAGE_TOT_MONS` | Months in HMO during year |
| `BENE_PTA_CVRAGE_TOT_MONS` | Part A months |
| `BENE_PTB_CVRAGE_TOT_MONS` | Part B months |
| `BENE_PTD_CVRAGE_TOT_MONS` | Part D months |

## Medicare Advantage (MA) Encounter Data

MA plans submit encounter data to CMS via 837P/I + a CMS-specific
"Encounter Data Submission" (EDS) wrapper. The EDS layer adds:

- `ENCOUNTER_ID` — payer-assigned per-encounter unique ID
- `MA_PLAN_ID` — H-number + plan ID (e.g. H1234-001)
- `EDPS_BATCH_ID` — Encounter Data Processing System batch
- Standard 837P/I content per the X12 specs

CMS-specific edits include:
- HCC risk-adjustment-eligible diagnosis flags
- Rejection codes from MAO-002 / MAO-004 reports
- HEDIS-relevance flags

## Medicaid State Plans (T-MSIS)

The Transformed Medicaid Statistical Information System (T-MSIS) is the
canonical Medicaid claims/eligibility submission. State Plans submit:

- **ELIGIBILITY** — beneficiary enrollment month-by-month
- **CLAIM IP** — Medicaid inpatient
- **CLAIM OT** — Medicaid outpatient
- **CLAIM RX** — Medicaid pharmacy
- **CLAIM LT** — Long-term care
- **PROVIDER** — Medicaid-credentialed provider master
- **MANAGED CARE** — MCO enrollment

T-MSIS uses CMS's mandated record formats per the T-MSIS Data Dictionary
(currently v3.x). Key beneficiary fields:

| Field | Description |
|---|---|
| `MSIS_ID` | State-assigned Medicaid ID |
| `STATE_PLAN_ID` | State + plan code |
| `ELIG_BEGIN_DATE` | Eligibility begin |
| `ELIG_END_DATE` | Eligibility end |
| `RACE_ETHNICITY_CODE` | OMB race + ethnicity |
| `DUAL_ELIG_CODE` | 00=Not dual, 01-08=various dual eligibility (Medicare + Medicaid) |
| `RESTRICTED_BENEFITS_CODE` | 1=Full, 2=Family planning only, 3-7=other limited |

## Common Validations for CMS-source Bronze

1. `BENE_ID` must be 8-9 alphanumeric (CMS-encrypted, NOT raw SSN)
2. `CLM_FROM_DT`, `CLM_THRU_DT` must be parseable, `CLM_FROM_DT <= CLM_THRU_DT`
3. `HCPCS_CD` must match `^[A-V][0-9]{4}$` for HCPCS Level II or `^[0-9]{5}[A-Z]?$` for CPT
4. `ICD_DGNS_CD1` must match ICD-10 pattern `^[A-TV-Z][0-9][0-9AB](\.[0-9A-Z]{0,4})?$` or be ICD-9 for pre-2015 claims
5. `PRVDR_NUM` (Medicare CCN provider number) must be 6 digits
6. `PRF_PHYSN_NPI` must be 10 digits, Luhn-mod-10 valid
7. `CLM_DRG_CD` (DRG) must be 3 digits, in current MS-DRG table
8. `LINE_NCH_PMT_AMT` (paid amount) must be >= 0
9. `STATE_CODE` is SSA code 1-53 (NOT FIPS) — common error to confuse with FIPS

## Bronze RAW_CLAIMS Mapping (CMS RIF → DataLink)

| Bronze column | CMS RIF source | Notes |
|---|---|---|
| `claim_id` | `CLM_ID` | |
| `member_id` | `BENE_ID` | encrypted |
| `provider_npi` | `PRF_PHYSN_NPI` | rendering |
| `provider_ccn` | `PRVDR_NUM` | facility CCN |
| `cpt_code` | `HCPCS_CD` | line-level |
| `cpt_modifier_1` | `HCPCS_1ST_MDFR_CD` | |
| `icd10_primary` | `ICD_DGNS_CD1` | |
| `icd10_secondary` | `ICD_DGNS_CD2` | (and 3..25) |
| `service_date` | `CLM_FROM_DT` | |
| `service_end_date` | `CLM_THRU_DT` | |
| `billed_amount` | `LINE_SBMTD_CHRG_AMT` | line-level |
| `allowed_amount` | `LINE_ALOWD_CHRG_AMT` | |
| `medicare_paid` | `LINE_NCH_PMT_AMT` | |
| `place_of_service` | `LINE_PLACE_OF_SRVC_CD` | |
| `claim_type` | derived from file source: INPT/OUTPT/CARR/DME/HHA/HOSP/SNF |
| `drg_code` | `CLM_DRG_CD` | inpatient only |
| `plan_id` | `MA_PLAN_ID` | MA encounter only |
