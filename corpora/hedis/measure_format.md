# HEDIS — Measure Specification Format

The Healthcare Effectiveness Data and Information Set (HEDIS) is the most
widely-used set of healthcare quality performance measures, published
annually by NCQA. Health plans report HEDIS results to NCQA, CMS Star
Ratings, state Medicaid agencies, and accreditation bodies.

## Measure Specification Anatomy

Every HEDIS measure follows this template:

```
Measure ID:   e.g. CDC (Comprehensive Diabetes Care)
Domain:       Effectiveness of Care | Access/Availability | Experience |
              Utilization | Risk Adjusted Utilization | Health Plan Descriptive
Year:         e.g. MY 2026 (Measurement Year)
Description:  one-line summary of what is measured

Eligible Population:
  Product Lines:        Commercial | Medicaid | Medicare
  Ages:                 e.g. 18-75 as of Dec 31 of measurement year
  Continuous Enrollment: minimum continuous enrollment (e.g. measurement year + prior year)
  Allowable Gap:        days of gap permitted (e.g. ≤ 45 days)
  Anchor Date:          the date a member must be enrolled (e.g. Dec 31 of MY)
  Benefit:              required benefits (Medical, Pharmacy)
  Event/Diagnosis:      condition that qualifies (e.g. diabetes Dx)

Denominator:  the denominator (= eligible population minus exclusions)

Numerator:    members who received the recommended care/screening within
              measurement window

Exclusions:
  Required:   conditions that automatically remove from denom (e.g. hospice)
  Optional:   conditions plans MAY exclude (e.g. palliative care)

Codes:        the LOINC, CPT, HCPCS, ICD-10, NDC, SNOMED, RxNorm code sets
              used to identify denominator events and numerator services
```

## Key Code Set Categories

HEDIS measures heavily rely on standardized code sets. NCQA publishes the
"Value Set Directory" (VSD) — a flat file of all code lists referenced by
measures. Common code system identifiers:

| System | OID | Used For |
|---|---|---|
| CPT | `2.16.840.1.113883.6.12` | Services, procedures |
| HCPCS | `2.16.840.1.113883.6.285` | Services, supplies |
| ICD-10-CM | `2.16.840.1.113883.6.90` | Diagnoses |
| ICD-10-PCS | `2.16.840.1.113883.6.4` | Procedures (institutional) |
| LOINC | `2.16.840.1.113883.6.1` | Lab tests, observations |
| SNOMED CT | `2.16.840.1.113883.6.96` | Clinical concepts |
| RxNorm | `2.16.840.1.113883.6.88` | Medications |
| NDC | `2.16.840.1.113883.6.69` | Specific drug products |
| UB-04 Revenue | `2.16.840.1.113883.6.21` | Institutional revenue codes |
| CDC Race | `2.16.840.1.113883.6.238` | Race ethnicity |

## Examples of Common Measures

### CDC — Comprehensive Diabetes Care

Eligible Population: members 18-75 with Type 1 or Type 2 diabetes during
measurement year (identified by 2+ outpatient visits, 1+ acute inpatient,
or 1+ ED visit with diabetes Dx in either MY or year prior, OR by
dispensing diabetes medication).

Sub-measures (sub-numerators):
- HbA1c testing
- HbA1c poor control (>9%) — inverse measure
- HbA1c control (<8%)
- HbA1c control (<7%) — for elderly subset
- Eye exam (retinal)
- Medical attention for nephropathy
- BP control (<140/90)

### COL — Colorectal Cancer Screening

Eligible Population: members 50-75 with continuous enrollment.
Numerator: appropriate screening within timeframe — gFOBT/FIT (annual),
sDNA-FIT (every 3 years), flexible sigmoidoscopy (every 5 years), CT
colonography (every 5 years), colonoscopy (every 10 years).

### BCS — Breast Cancer Screening

Eligible Population: women 50-74 with continuous enrollment.
Numerator: mammogram in measurement year or prior year.

### CIS — Childhood Immunization Status

Eligible Population: members turning 2 in measurement year.
Sub-numerators (combinations 1-10): DTaP, IPV, MMR, HiB, HepB, VZV,
Pneumococcal, HepA, RV, Influenza.

### AMM — Antidepressant Medication Management

Eligible Population: members 18+ with new episode of depression diagnosis
+ antidepressant dispensing.
Sub-numerators: 12 weeks acute phase treatment; 6 months continuation.

## DataLink Bronze for HEDIS Inputs

HEDIS measures consume claims, pharmacy, lab, and member data. Typical
DataLink Bronze tables feeding HEDIS:

| Bronze table | HEDIS Use |
|---|---|
| `RAW_CLAIMS` | denominator events (Dx codes), numerator services (CPT, HCPCS) |
| `RAW_MEMBERSHIP` | eligibility/enrollment, demographics, product line |
| `RAW_PHARMACY_CLAIMS` | medication dispensing for AMM, ADHD-A, SAA, etc. |
| `RAW_LAB_RESULTS` | LOINC-coded lab values for CDC HbA1c, BP, eGFR; HPV; etc. |
| `RAW_PROVIDER` | provider taxonomy for site-of-care identification |

## DQ Implications for HEDIS Sources

If your platform supports HEDIS reporting, DQ checks must be tighter:

1. ICD-10 codes: complete (no truncation), in CMS-current tables
2. CPT/HCPCS: exact match against NCQA value sets (mod CPT modifier rules)
3. Service dates: must be parseable, within or before measurement year
4. Provider NPI: 10 digits, valid in NPPES at service date (HEDIS audit will check)
5. NDC codes: 11 digits in 5-4-2 format, mappable to RxNorm via FDA NDC database
6. LOINC: full 7-digit (with check digit) format `^[0-9]{1,5}-[0-9]$`
7. Continuous enrollment: requires complete eligibility history with no unreported gaps
8. Member identifiers: stable across measurement year and prior year (NCQA hashes for de-dup)

## Audit and Reproducibility

HEDIS results are audited annually by NCQA-licensed auditors. The audit
trail requires:

- Source claim/eligibility detail must be retrievable for any member
  appearing in the rate
- Code logic must match the published Technical Specifications letter-by-letter
- "Hybrid" measures (BCS, CDC eye exam, etc.) require medical record
  abstraction — chart review with source documentation
- All transformations must be reproducible — same input + same MY = same result
