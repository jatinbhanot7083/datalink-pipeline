# X12 837P — Professional Health Care Claim (5010)

The 837P transaction set is the X12 standard for submitting professional
healthcare claims (CMS-1500 equivalent) electronically between providers
and payers under HIPAA mandate.

## Transaction Set ID

`837` with implementation guide `005010X222A1` (Professional)

## Document Hierarchy

```
ISA - Interchange Control Header
GS  - Functional Group Header
ST  - Transaction Set Header (one ST/SE pair per claim batch)
BHT - Beginning of Hierarchical Transaction
Loop 1000A - Submitter Name
Loop 1000B - Receiver Name
Loop 2000A - Billing Provider Hierarchical Level
  Loop 2010AA - Billing Provider Name
  Loop 2010AB - Pay-To Address
  Loop 2000B - Subscriber Hierarchical Level
    Loop 2010BA - Subscriber Name
    Loop 2010BB - Payer Name
    Loop 2300 - Claim Information (one per claim)
      Loop 2310A - Referring Provider Name
      Loop 2310B - Rendering Provider Name
      Loop 2310C - Service Facility Location
      Loop 2310D - Supervising Provider Name
      Loop 2320 - Other Subscriber Information (COB)
      Loop 2400 - Service Line Number (one per service line)
        Loop 2410 - Drug Identification (NDC)
        Loop 2420A - Rendering Provider (line-level)
        Loop 2420B - Purchased Service Provider
        Loop 2420C - Service Facility Location (line-level)
        Loop 2420D - Supervising Provider (line-level)
        Loop 2420E - Ordering Provider
        Loop 2420F - Referring Provider (line-level)
        Loop 2420G - Ambulance Pick-up Location
        Loop 2420H - Ambulance Drop-off Location
        Loop 2430 - Line Adjudication Information (COB)
        Loop 2440 - Form Identification Code
SE  - Transaction Set Trailer
GE  - Functional Group Trailer
IEA - Interchange Control Trailer
```

## Critical Segments and Elements

### Loop 2300 (Claim Information) — one per claim

| Segment | Element | Description | DataLink Bronze column |
|---|---|---|---|
| CLM01 | Patient Control Number | Provider's claim ID | `claim_id` |
| CLM02 | Total Claim Charge | Decimal, e.g. `123.45` | `total_charge` (sum of service line charges) |
| CLM05-1 | Place of Service | 2-digit POS code (11=office, 21=inpt, etc.) | `place_of_service` |
| CLM05-3 | Claim Frequency Code | 1=original, 7=replacement, 8=void | `claim_frequency` |
| CLM06 | Provider Signature on File | Y/N | `provider_signature_on_file` |
| CLM07 | Provider Accept Assignment | A=assigned, C=NotAssigned, P=PartiallyAssigned | `assignment_indicator` |
| CLM08 | Benefits Assignment Certification | Y/N | `benefits_assignment` |
| CLM09 | Release of Information Code | A/I/M/N/O/Y | `release_of_information` |
| DTP | Service Date | DTP*472=accident, *431=onset, *454=admission | `service_start_date` / `service_end_date` |
| HI01-1 | Health Care Diagnosis Code Qualifier | ABK=ICD-10 principal, ABF=ICD-10 secondary | (filter) |
| HI01-2 | Health Care Diagnosis Code | ICD-10-CM code | `icd10_primary` (first), `icd10_secondary` |
| REF*9F | Referral Number | Prior authorization or referral # | `prior_auth_ref` |
| REF*EW | Mammography Certification Number | | `mammography_cert` |
| REF*F8 | Original Reference Number | For replacement claims | `original_claim_id` |
| AMT*F5 | Patient Amount Paid | | `patient_paid_amount` |

### Loop 2010BA (Subscriber Name)

| Segment | Element | Description | DataLink Bronze column |
|---|---|---|---|
| NM103 | Subscriber Last Name | | `member_last_name` |
| NM104 | Subscriber First Name | | `member_first_name` |
| NM108 | Identification Code Qualifier | MI=Member ID | (filter) |
| NM109 | Subscriber Primary Identifier | Member ID | `member_id` |
| N301 | Address Line 1 | | `address_1` |
| N302 | Address Line 2 | | `address_2` |
| N401 | City | | `city` |
| N402 | State | | `state` |
| N403 | ZIP | | `zip` |
| DMG02 | Date of Birth | CCYYMMDD | `dob` |
| DMG03 | Gender | M/F/U | `gender` |

### Loop 2010BB (Payer Name)

| Segment | Element | Description | DataLink Bronze column |
|---|---|---|---|
| NM103 | Payer Name | | `payer_name` |
| NM108 | Identification Code Qualifier | PI=Payer ID, XV=NAIC | (filter) |
| NM109 | Payer Identifier | | `payer_id` |
| REF*G2 | Provider Commercial Number | | `commercial_number` |

### Loop 2310A/B (Referring/Rendering Provider)

| Segment | Element | Description | DataLink Bronze column |
|---|---|---|---|
| NM101 | Entity Identifier Code | DN=Referring, 82=Rendering | (filter) |
| NM103 | Provider Last Name | | `referring_provider_last` / `rendering_provider_last` |
| NM104 | Provider First Name | | `referring_provider_first` / `rendering_provider_first` |
| NM108 | Identification Code Qualifier | XX=NPI | (filter) |
| NM109 | Provider NPI | 10 digits | `referring_provider_npi` / `rendering_provider_npi` |
| PRV03 | Provider Specialty | NUCC taxonomy | `provider_taxonomy` |

### Loop 2400 (Service Line) — one per CPT/HCPCS

| Segment | Element | Description | DataLink Bronze column |
|---|---|---|---|
| LX01 | Service Line Number | 1, 2, 3, ... | `service_line_number` |
| SV101-1 | Product/Service ID Qualifier | HC=HCPCS, HP=HIPPS | (filter) |
| SV101-2 | Product/Service ID | CPT or HCPCS code | `cpt_code` |
| SV101-3 | Procedure Modifier 1 | 2-char modifier | `cpt_modifier_1` |
| SV101-4 | Procedure Modifier 2 | | `cpt_modifier_2` |
| SV102 | Line Item Charge Amount | Decimal | `billed_amount` |
| SV103 | Unit/Basis Code | UN=Unit, MJ=Minutes | `unit_basis` |
| SV104 | Service Unit Count | | `units` |
| SV105 | Place of Service | overrides claim POS if present | `line_pos` |
| SV107-1 | Diagnosis Code Pointer 1 | 1, 2, 3, 4 (ref to HI segment) | `dx_pointer_1` |
| DTP*472 | Service Date | CCYYMMDD or CCYYMMDD-CCYYMMDD | `service_date` |
| REF*6R | Provider Control Number (line) | | `line_control_number` |
| REF*9F | Referral Number (line) | | `line_referral_number` |
| AMT*B6 | Allowed Amount | | `allowed_amount` |
| HCP01 | Pricing Methodology | 00=Zero, 01=Priced as Submitted, 02=Repriced | `pricing_methodology` |

## Common DQ Validations for 837P

1. ISA09 (interchange date) must be within last 30 days
2. CLM01 must match payer's claim numbering format
3. CLM02 must equal sum of SV102 across all Loop 2400 service lines (within rounding tolerance)
4. Loop 2010BA NM109 (member_id) must resolve to active Coverage at DTP*472 service date
5. NM109 NPI fields (rendering, referring, billing) must be 10 digits matching `^[0-9]{10}$`
6. NPIs must pass Luhn-mod-10 checksum validation
7. ICD-10 codes in HI segment must match `^[A-TV-Z][0-9][0-9AB](\.[0-9A-Z]{0,4})?$`
8. CPT codes in SV101-2 must match `^[0-9]{5}[A-Z]?$`
9. HCPCS Level II codes must match `^[A-V][0-9]{4}$`
10. CPT modifiers must be in current CMS modifier table
11. Place of Service (CLM05-1) must be in CMS POS code set
12. DTP*472 service date must be <= today and >= patient DOB
13. SV104 units must be > 0
14. SV102 billed amount must be >= 0
15. Loop 2300 HI segments: HI01 must be principal diagnosis (ABK qualifier), HI02..HI12 secondary

## Bronze RAW_CLAIMS Column Mapping (X12 837P → DataLink)

| Bronze column | X12 source |
|---|---|
| `claim_id` | CLM01 |
| `member_id` | Loop 2010BA NM109 |
| `provider_npi` | Loop 2310B NM109 (rendering) |
| `cpt_code` | Loop 2400 SV101-2 |
| `icd10_primary` | Loop 2300 HI01-2 (qualifier ABK) |
| `icd10_secondary` | Loop 2300 HI02-2 (qualifier ABF) |
| `service_date` | Loop 2400 DTP*472 |
| `billed_amount` | Loop 2400 SV102 |
| `claim_status` | derived from CLM05-3 frequency + adjudication |
| `plan_id` | derived from Loop 2010BB NM109 + Loop 2330B SBR |
| `prior_auth_ref` | Loop 2300 REF*9F or Loop 2400 REF*9F (line override) |

## Differences from 837I (Institutional)

837I uses Loop 2300 with **revenue codes** (UB-04 form) instead of CPT primarily,
plus Loop 2400 SV2 instead of SV1 segment. Bronze ingest should detect the
implementation guide qualifier in GS08 (`005010X222A1`=P, `005010X223A2`=I)
and route to the appropriate column mapper.
