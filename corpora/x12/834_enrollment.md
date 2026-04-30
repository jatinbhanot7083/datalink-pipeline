# X12 834 — Benefit Enrollment and Maintenance (5010)

The 834 transaction set is the X12 standard for transferring enrollment
information about benefits between sponsoring entities (employers,
government agencies) and insurance carriers under HIPAA mandate. It is the
primary EDI feed driving membership/eligibility data into payer systems.

## Transaction Set ID

`834` with implementation guide `005010X220A1`

## Document Hierarchy

```
ISA - Interchange Control Header
GS  - Functional Group Header
ST  - Transaction Set Header (834 ID)
BGN - Beginning Segment (purpose, reference, date)
REF - Transaction Set Policy Number
DTP*007 - Effective Date
QTY - Subscriber/Member Counts
Loop 1000A - Sponsor Name (employer)
  N1 - Sponsor identification
Loop 1000B - Payer
  N1 - Payer identification
Loop 1000C - TPA / Broker (optional)
Loop 2000 - Member Level Detail (one per member transaction)
  INS - Member Level Detail (relationship, benefit status, employment status)
  REF*0F - Subscriber Number
  REF*1L - Group/Policy Number
  REF*17 - Client Number
  REF*23 - Client Reporting Category
  REF*3H - Case Number
  REF*4A - Cross Reference Identifier
  REF*ABB - Personal Identification Number
  REF*DX - Department/Agency Number
  REF*F6 - Health Insurance Claim Number (HIC)
  REF*Q4 - Prior Identifier Number
  REF*ZZ - Mutually Defined
  DTP*356 - Eligibility Begin Date
  DTP*357 - Eligibility End Date
  DTP*336 - Employment Begin Date
  DTP*337 - Retirement Date
  DTP*303 - Maintenance Effective Date
  DTP*338 - Medicare Begin
  DTP*339 - Medicare End
  Loop 2100A - Member Name
    NM1*IL - Member identification (subscriber or dependent)
    PER - Communications (phone, email)
    N3, N4 - Member address
    DMG - Demographic Information (DOB, gender, marital, race, ethnicity)
    EC - Employment Class
    ICM - Member Income
    AMT - Member Policy Amounts
    HLH - Member Health Information (height, weight, dependents, smoker, etc.)
    LUI - Languages
  Loop 2100B - Incorrect Member Name (for corrections)
  Loop 2100C - Member Mailing Address (if different from primary)
  Loop 2100D - Member Employer (if separate from sponsor)
  Loop 2100E - Member School
  Loop 2100F - Custodial Parent
  Loop 2100G - Responsible Person
  Loop 2100H - Drop Off Location
  Loop 2200 - Disability Information
  Loop 2300 - Health Coverage (one per coverage assigned to member)
    HD - Health Coverage segment
      HD01 - Maintenance Type Code (021=Add, 024=Cancel, 025=Reinstate, 030=Audit)
      HD03 - Insurance Line Code (HLT=Health, DEN=Dental, VIS=Vision, RX=Pharm)
      HD04 - Plan Coverage Description
      HD05 - Coverage Level Code (CHD=Child only, ECH=Employee+Children,
              EMP=Employee only, ESP=Employee+Spouse, FAM=Family,
              IND=Individual, SPC=Spouse+Children, SPO=Spouse only)
    DTP*348 - Benefit Begin Date
    DTP*349 - Benefit End Date
    AMT*B9 - Coverage Amount
    REF*1L - Group/Policy Number
    REF*X9 - Cross Reference Number
    Loop 2310 - Provider Information (PCP)
      LX*1 - Loop Counter
      NM1*P3 - Provider name
      N3, N4 - Provider address
      PER - Provider communications
      PRV - Provider speciality (taxonomy)
    Loop 2320 - Coordination of Benefits (other coverages)
      COB - Coordination of Benefits
      REF*60 - Member Group / Policy Number (other carrier)
      DTP*344 - Coordination of Benefits Begin
      DTP*345 - Coordination of Benefits End
      Loop 2330 - COB Related Entity
SE - Transaction Set Trailer
GE - Functional Group Trailer
IEA - Interchange Control Trailer
```

## Critical Fields for Membership Bronze (RAW_MEMBERSHIP)

| X12 source | Bronze column | Description |
|---|---|---|
| Loop 2000 INS01 | `relationship_to_subscriber` | Y=Subscriber, N=Dependent (with INS02) |
| Loop 2000 INS02 | `dependent_relationship_code` | 01=Spouse, 19=Child, 17=Stepson, 18=Stepdaughter, etc. (when INS01=N) |
| Loop 2000 INS03 | `maintenance_type_code` | 001=Change, 021=Add, 024=Cancel, 025=Reinstate, 030=Audit |
| Loop 2000 INS04 | `maintenance_reason_code` | 25=Change in identifier, 28=Initial enrollment |
| Loop 2000 INS05 | `benefit_status_code` | A=Active, C=COBRA, S=Surviving Insured, T=Terminated |
| Loop 2000 REF*0F | `subscriber_id` | The subscriber's primary identifier |
| Loop 2000 REF*1L | `group_id` | Group/policy number |
| Loop 2000 DTP*356 | `eligibility_start_date` | CCYYMMDD |
| Loop 2000 DTP*357 | `eligibility_end_date` | CCYYMMDD |
| Loop 2100A NM103 | `member_last_name` | |
| Loop 2100A NM104 | `member_first_name` | |
| Loop 2100A NM105 | `member_middle_name` | |
| Loop 2100A NM109 | `member_id` | Member identifier |
| Loop 2100A PER03 | `home_phone` | when PER02=HP |
| Loop 2100A PER05 | `email` | when PER04=EM |
| Loop 2100A PER06 | `mobile_phone` | when PER05=CP (Cellular Phone) |
| Loop 2100A N301 | `address_1` | |
| Loop 2100A N302 | `address_2` | |
| Loop 2100A N401 | `city` | |
| Loop 2100A N402 | `state` | 2-letter |
| Loop 2100A N403 | `zip` | |
| Loop 2100A DMG02 | `dob` | CCYYMMDD |
| Loop 2100A DMG03 | `gender` | M, F, U |
| Loop 2100A DMG04 | `marital_status` | A=Single, B=Common Law, D=Divorced, I=Living Together, M=Married, R=Registered Domestic Partner, S=Single, U=Unreported, W=Widowed |
| Loop 2100A DMG05-1 | `race_ethnicity_code` | 7=Not Provided, 8=Not Applicable, A=American Indian or Alaska Native, B=Black, ... |
| Loop 2100A LUI02 | `preferred_language` | ISO 639 language code |
| Loop 2100A HLH02 | `height_inches` | |
| Loop 2100A HLH03 | `weight_pounds` | |
| Loop 2100A HLH04 | `tobacco_use` | N=No, U=Unknown, Y=Yes |
| Loop 2300 HD03 | `coverage_line` | HLT=Health, DEN=Dental, VIS=Vision, RX=Pharmacy |
| Loop 2300 HD05 | `coverage_level` | EMP, ESP, ECH, FAM, etc. |
| Loop 2300 DTP*348 | `benefit_start_date` | |
| Loop 2300 DTP*349 | `benefit_end_date` | |
| Loop 2300 REF*1L | `plan_id` | Group/policy number for this coverage line |
| Loop 2310 NM109 | `pcp_npi` | Primary Care Physician NPI |

## Common DQ Validations for 834

1. INS03 maintenance_type_code must be in valid set: 001, 021, 024, 025, 030
2. INS05 benefit_status_code must be in: A, C, S, T
3. DTP*356 eligibility_start_date must be parseable as CCYYMMDD
4. DTP*356 eligibility_start_date must be <= DTP*357 eligibility_end_date when both present
5. NM109 member_id must be present and unique within tenant + scope
6. DMG02 dob must be in past, after 1900-01-01
7. DMG03 gender must be one of M, F, U
8. N402 state must be 2-letter US state/territory if N404 country=US
9. N403 zip must match `^[0-9]{5}(-?[0-9]{4})?$` if N404 country=US
10. HD03 coverage_line must be valid: HLT, DEN, VIS, RX
11. HD05 coverage_level must be valid: CHD, ECH, EMP, ESP, FAM, IND, SPC, SPO
12. PER03 (phone) when PER02=HP must be 10 digits after stripping non-numeric
13. INS01=Y subscribers must have a valid REF*0F subscriber_id
14. INS01=N dependents must have INS02 relationship code

## Bronze RAW_MEMBERSHIP from 834

| `member_id` | required, primary |
| `subscriber_id` | required when INS01=N (link to subscriber row) |
| `relationship_to_subscriber` | derived from INS01 + INS02 |
| `member_first_name`, `member_last_name`, `member_middle_name` | from Loop 2100A NM1 |
| `dob`, `gender` | from Loop 2100A DMG |
| `address_1`, `address_2`, `city`, `state`, `zip` | from Loop 2100A N3/N4 |
| `home_phone`, `mobile_phone`, `email` | from Loop 2100A PER |
| `eligibility_start_date`, `eligibility_end_date` | from Loop 2000 DTP |
| `benefit_status_code` | from Loop 2000 INS05 |
| `plan_id`, `coverage_level`, `coverage_line` | from Loop 2300 |
| `group_id` | from Loop 2000 REF*1L |
| `pcp_npi` | from Loop 2310 NM109 |
| `preferred_language` | from Loop 2100A LUI02 |
| `race_ethnicity_code` | from Loop 2100A DMG05-1 |
| `marital_status` | from Loop 2100A DMG04 |

Mandatory audit columns: `_load_dt`, `_source_file`, `_batch_id`, `_record_source`, `_load_type`, `_file_row_number`, `_record_hash`.
