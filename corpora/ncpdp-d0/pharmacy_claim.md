# NCPDP D.0 — Pharmacy Claim Standard

The National Council for Prescription Drug Programs (NCPDP) Telecommunication
Standard D.0 is the standard for real-time pharmacy claim transactions
between pharmacies and pharmacy benefit managers (PBMs) / payers under HIPAA
mandate. It governs B1 billing requests and N1/D1 reversals.

## Transaction Types

| Code | Name | Description |
|---|---|---|
| `B1` | Billing Transaction | Submit pharmacy claim for adjudication |
| `B2` | Reversal | Reverse a previously billed B1 |
| `B3` | Rebill | Resubmit a corrected claim |
| `D1` | Predetermination of Benefits | Eligibility/coverage check |
| `E1` | Eligibility Verification | Patient eligibility check |
| `N1`, `N2`, `N3` | Information Reporting | Various |
| `P1`, `P2`, `P3`, `P4` | Pre-determination, Prior auth | Pharmacy auth flow |

## Segment Hierarchy

```
Header (transaction header — required)
Patient Segment (member demographics — required)
Insurance Segment (member insurance — required for B1)
Claim Segment (the prescription — required)
Pharmacy Provider Segment (NPI — required for B1)
Prescriber Segment (writing physician)
COB Segment (other coverage)
DUR/PPS Segment (drug utilization review)
Pricing Segment (cost detail)
Compound Segment (for compounded drugs)
Coupon Segment (manufacturer coupon)
Workers Comp Segment (WC claims only)
```

## Critical Fields for Pharmacy Claim Bronze (RAW_PHARMACY_CLAIMS)

### Header

| NCPDP Field | Field Name | Bronze Column |
|---|---|---|
| 1Ø1-A1 | BIN Number | `bin_number` (6 digits, e.g. 012345 = Caremark) |
| 1Ø2-A2 | Version/Release Number | `version` (D0=DZero, DØ format) |
| 1Ø3-A3 | Transaction Code | `transaction_code` (B1, B2, ...) |
| 1Ø4-A4 | Processor Control Number | `pcn` (PBM-specific) |
| 1Ø9-A9 | Transaction Count | `transaction_count` |
| 2Ø2-B2 | Service Provider ID Qualifier | (filter, 01=NPI) |
| 2Ø1-B1 | Service Provider ID | `pharmacy_npi` (10 digits) |
| 4Ø1-D1 | Date of Service | `service_date` (CCYYMMDD) |

### Patient Segment

| NCPDP Field | Field Name | Bronze Column |
|---|---|---|
| 3Ø4-C4 | Date of Birth | `dob` (CCYYMMDD) |
| 3Ø5-C5 | Patient Gender Code | `gender` (1=Not Specified, 2=Male, 3=Female) |
| 3Ø7-C7 | Place of Service | `place_of_service` |
| 31Ø-CA | Patient First Name | `member_first_name` |
| 311-CB | Patient Last Name | `member_last_name` |
| 322-CM | Patient Street Address | `address_1` |
| 323-CN | Patient City | `city` |
| 324-CO | Patient State | `state` |
| 325-CP | Patient ZIP/Postal Code | `zip` |
| 326-CQ | Patient Phone Number | `home_phone` |

### Insurance Segment

| NCPDP Field | Field Name | Bronze Column |
|---|---|---|
| 3Ø2-C2 | Cardholder ID | `member_id` (subscriber's primary ID) |
| 3Ø1-C1 | Group ID | `group_id` |
| 3Ø3-C3 | Person Code | `person_code` (01=cardholder, 02=spouse, 03+=dep) |
| 3Ø6-C6 | Patient Relationship Code | `relationship_to_subscriber` (1=Cardholder, 2=Spouse, 3=Child, 4=Other) |
| 313-CD | Cardholder First Name | `subscriber_first_name` |
| 314-CE | Cardholder Last Name | `subscriber_last_name` |
| 524-FO | Plan ID | `plan_id` |

### Claim Segment

| NCPDP Field | Field Name | Bronze Column |
|---|---|---|
| 4Ø2-D2 | Prescription/Service Reference Number | `rx_number` |
| 436-E1 | Product/Service ID Qualifier | (filter, 03=NDC) |
| 4Ø7-D7 | Product/Service ID | `ndc_code` (11-digit National Drug Code) |
| 442-E7 | Quantity Dispensed | `quantity` |
| 4Ø3-D3 | Fill Number | `fill_number` (00=original, 01-99=refill) |
| 4Ø5-D5 | Days Supply | `days_supply` |
| 4Ø6-D6 | Compound Code | `compound_code` (1=No, 2=Yes) |
| 4Ø8-D8 | Dispense As Written / Product Selection Code | `daw_code` (0-9) |
| 414-DE | Date Prescription Written | `date_prescribed` |
| 415-DF | Number of Refills Authorized | `refills_authorized` |
| 419-DJ | Prescription Origin Code | `prescription_origin` (0=Not Known, 1=Written, 2=Telephone, 3=Electronic, 4=Facsimile, 5=Pharmacy) |
| 461-EU | Prior Authorization Type Code | `prior_auth_type` |
| 462-EV | Prior Authorization Number Submitted | `prior_auth_ref` |

### Pharmacy Pricing

| NCPDP Field | Field Name | Bronze Column |
|---|---|---|
| 4Ø9-D9 | Ingredient Cost Submitted | `ingredient_cost` |
| 412-DC | Dispensing Fee Submitted | `dispensing_fee` |
| 433-DX | Patient Paid Amount Submitted | `patient_paid` |
| 438-E3 | Incentive Amount Submitted | `incentive_amount` |
| 478-H7 | Other Amount Claimed Submitted | `other_amount_claimed` |
| 481-HA | Flat Sales Tax Amount Submitted | `flat_sales_tax` |
| 482-GE | Percentage Sales Tax Amount Submitted | `pct_sales_tax_amount` |
| 426-DQ | Usual and Customary Charge | `usual_customary` |
| 43Ø-DU | Gross Amount Due | `gross_amount_due` |
| 423-DN | Basis of Cost Determination | `cost_basis` |

### Prescriber Segment

| NCPDP Field | Field Name | Bronze Column |
|---|---|---|
| 466-EZ | Prescriber ID Qualifier | (filter, 01=NPI, 12=DEA) |
| 411-DB | Prescriber ID | `prescriber_npi` |
| 427-DR | Prescriber Last Name | `prescriber_last_name` |
| 364-2J | Prescriber First Name | `prescriber_first_name` |

## Common DQ Validations for NCPDP D.0

1. `bin_number` must be 6 digits
2. `pharmacy_npi` must be 10 digits, Luhn-mod-10 valid
3. `prescriber_npi` if qualifier=01 must be 10 digits
4. `ndc_code` must be 11 digits in 5-4-2 format (or 4-4-2 zero-padded to 11)
5. `service_date` must be parseable as CCYYMMDD, not in future, after dob
6. `quantity` must be > 0
7. `days_supply` must be > 0 and < 366
8. `fill_number` must be 0-99
9. `refills_authorized` must be 0-99
10. `daw_code` must be 0-9
11. `gender` must be 1, 2, or 3
12. `relationship_to_subscriber` must be 1-4
13. `plan_id` must match the payer's plan code format
14. `member_id` must resolve to active membership at `service_date`

## Bronze RAW_PHARMACY_CLAIMS Schema (DataLink convention)

Standard business columns (use the table above as the canonical mapping)
plus mandatory audit:

```
_load_dt           TIMESTAMP
_source_file       VARCHAR
_batch_id          VARCHAR
_record_source     VARCHAR  -- e.g. 'CAREMARK_NCPDP_D0'
_load_type         VARCHAR  -- 'FULL' | 'INCREMENTAL'
_file_row_number   BIGINT
_record_hash       VARCHAR  -- MD5 of business columns
```
