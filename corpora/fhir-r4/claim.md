# FHIR R4 — Claim Resource

The Claim resource represents a request to an insurer for a payment for the
provision of healthcare-related goods and services. It is a fundamental
resource for healthcare claims processing.

## Resource Path

`Claim`

## Maturity Level

3 (Trial Use)

## Cardinality and Key Fields

| Field | Type | Cardinality | Description |
|---|---|---|---|
| `id` | string | 0..1 | Logical id of the resource. Globally unique business identifier. Pattern `[A-Za-z0-9\-\.]{1,64}`. |
| `identifier` | Identifier[] | 0..* | Business identifier assigned by the claim creator (provider). Should include `system` (assigning authority URI) and `value`. |
| `status` | code | 1..1 | Required. Values: `active` \| `cancelled` \| `draft` \| `entered-in-error`. |
| `type` | CodeableConcept | 1..1 | Required. The category of the claim. Common values: `professional` (837P), `institutional` (837I), `pharmacy` (NCPDP), `oral`, `vision`. Bound to ClaimType valueset. |
| `subType` | CodeableConcept | 0..1 | More specific subtype, e.g., `emergency`. |
| `use` | code | 1..1 | Required. Values: `claim` \| `preauthorization` \| `predetermination`. |
| `patient` | Reference(Patient) | 1..1 | Required. The party to whom the professional services and/or products have been supplied. |
| `billablePeriod` | Period | 0..1 | Service start and end dates. `start` is required when present. |
| `created` | dateTime | 1..1 | Required. Date the claim was created. |
| `enterer` | Reference(Practitioner \| PractitionerRole) | 0..1 | Person who created the claim. |
| `insurer` | Reference(Organization) | 0..1 | Target payer organization. |
| `provider` | Reference(Practitioner \| PractitionerRole \| Organization) | 1..1 | Required. The party responsible for the claim. |
| `priority` | CodeableConcept | 1..1 | Required. Bound to ProcessPriority. Values: `stat` \| `normal` \| `deferred`. |
| `fundsReserve` | CodeableConcept | 0..1 | For whom funds are to be reserved. |
| `related` | BackboneElement[] | 0..* | Prior or corollary claims. Each has `claim` Reference, `relationship` CodeableConcept, `reference` Identifier. |
| `prescription` | Reference(MedicationRequest \| VisionPrescription) | 0..1 | Pharmacy or vision prescription referenced. |
| `originalPrescription` | Reference | 0..1 | Original prescription if a substitute. |
| `payee` | BackboneElement | 0..1 | Recipient of benefits payable. Has `type` (provider/subscriber/other) and `party` Reference. |
| `referral` | Reference(ServiceRequest) | 0..1 | Treatment referral. |
| `facility` | Reference(Location) | 0..1 | Service location. |
| `careTeam` | BackboneElement[] | 0..* | Each item: `sequence` (positiveInt), `provider` Reference, `responsible` boolean, `role` CodeableConcept, `qualification` CodeableConcept. |
| `supportingInfo` | BackboneElement[] | 0..* | Supporting clinical information for adjudication. Each: `sequence`, `category`, `code`, value[x] (string/Quantity/Attachment/Reference), `reason`. |
| `diagnosis` | BackboneElement[] | 0..* | Each diagnosis: `sequence` (positiveInt), `diagnosis[x]` (CodeableConcept ICD-10-CM | Reference), `type` CodeableConcept (admitting/principal/secondary), `onAdmission`, `packageCode`. |
| `procedure` | BackboneElement[] | 0..* | Each: `sequence`, `type` (primary/secondary), `date` dateTime, `procedure[x]` (CPT/HCPCS CodeableConcept | Reference), `udi` Reference[]. |
| `insurance` | BackboneElement[] | 1..* | Required, at least one. Each: `sequence`, `focal` boolean (which is primary), `identifier` Identifier (subscriber id), `coverage` Reference(Coverage), `businessArrangement`, `preAuthRef` string[], `claimResponse` Reference. |
| `accident` | BackboneElement | 0..1 | If accident-related. `date` date, `type` CodeableConcept, `location[x]`. |
| `item` | BackboneElement[] | 0..* | Service line items. Each: `sequence` (positiveInt), `careTeamSequence` positiveInt[], `diagnosisSequence` positiveInt[], `procedureSequence` positiveInt[], `informationSequence` positiveInt[], `revenue` CodeableConcept (UB-04 revenue code), `category` CodeableConcept, `productOrService` CodeableConcept (CPT/HCPCS — required), `modifier` CodeableConcept[] (CPT modifiers), `programCode` CodeableConcept[], `serviced[x]` (date | Period), `location[x]` (CodeableConcept | Address | Reference), `quantity` SimpleQuantity, `unitPrice` Money, `factor` decimal, `net` Money, `udi` Reference[], `bodySite` CodeableConcept, `subSite` CodeableConcept[], `encounter` Reference[], `detail` BackboneElement[]. |
| `total` | Money | 0..1 | The total value of all items. |

## Relationships

- `Claim.patient` → `Patient` resource (member/beneficiary)
- `Claim.insurer` → `Organization` (payer)
- `Claim.provider` → `Practitioner` | `PractitionerRole` | `Organization` (rendering or billing provider)
- `Claim.insurance.coverage` → `Coverage` resource (benefit plan)
- `Claim.referral` → `ServiceRequest`
- `Claim.facility` → `Location`
- `Claim.careTeam.provider` → `Practitioner` | `PractitionerRole` | `Organization`

## Key Code Sets

| Field | Value Set | Notes |
|---|---|---|
| `Claim.type` | `http://terminology.hl7.org/CodeSystem/claim-type` | professional, institutional, oral, pharmacy, vision |
| `Claim.use` | `http://hl7.org/fhir/claim-use` | claim, preauthorization, predetermination |
| `Claim.priority` | `http://terminology.hl7.org/CodeSystem/processpriority` | stat, normal, deferred |
| `Claim.diagnosis.diagnosis[x]` | ICD-10-CM | `http://hl7.org/fhir/sid/icd-10-cm` |
| `Claim.procedure.procedure[x]` | CPT | `http://www.ama-assn.org/go/cpt` |
| | HCPCS | `https://www.cms.gov/Medicare/Coding/HCPCSReleaseCodeSets` |
| `Claim.item.productOrService` | CPT/HCPCS | Same as above |
| `Claim.item.modifier` | CPT modifiers | 2-character codes |
| `Claim.item.revenue` | UB-04 revenue codes | Institutional only |

## Common Identifier Systems

- **Member ID**: payer-specific, e.g., `urn:oid:2.16.840.1.113883.4.349` for Medicare HIC
- **NPI (National Provider Identifier)**: `http://hl7.org/fhir/sid/us-npi` — 10-digit numeric
- **Tax ID (EIN)**: `urn:oid:2.16.840.1.113883.4.4`
- **Claim Number**: payer-assigned, system varies

## Validation Rules (typical claim DQ checks)

1. `Claim.identifier[0].value` must match payer's claim numbering pattern
2. `Claim.patient` must resolve to an existing `Patient` resource
3. `Claim.insurance[0].coverage` must resolve to active `Coverage` with status=active
4. `Claim.diagnosis[*].diagnosis.coding[0].code` must match ICD-10-CM regex `^[A-TV-Z][0-9][0-9AB](\.[0-9A-Z]{0,4})?$`
5. `Claim.procedure[*].procedure.coding[0].code` if CPT must match `^[0-9]{5}[A-Z]?$`
6. `Claim.item[*].productOrService.coding[0].code` if CPT must match `^[0-9]{5}[A-Z]?$`, if HCPCS Level II must match `^[A-V][0-9]{4}$`
7. `Claim.item[*].quantity.value` must be > 0
8. `Claim.item[*].unitPrice.value` must be >= 0; currency normalised to `USD`
9. `Claim.total.value` must equal sum of `Claim.item[*].net.value` (within rounding tolerance)
10. `Claim.billablePeriod.start` must be <= `Claim.billablePeriod.end`
11. `Claim.created` must not be in the future
12. NPI on `Claim.provider` and `Claim.careTeam[*].provider` must be 10 digits; checksum validated via Luhn-mod-10 with NPI prefix
13. Provider taxonomy (when supplied via `Claim.careTeam[*].qualification`) must come from NUCC Healthcare Provider Taxonomy

## Bronze → Silver Mapping Hints (Data Vault 2.0)

When mapping FHIR Claim into Data Vault Silver:

- `HUB_CLAIM` natural key: `Claim.identifier.value` (with `Claim.identifier.system` for tenant-scoping)
- `HUB_PATIENT` natural key: `Claim.patient.identifier.value`
- `HUB_PROVIDER` natural key: `Claim.provider.identifier.value` (NPI)
- `HUB_COVERAGE` natural key: `Claim.insurance[*].coverage.identifier.value`
- `LINK_CLAIM_PATIENT` between HUB_CLAIM and HUB_PATIENT
- `LINK_CLAIM_PROVIDER` between HUB_CLAIM and HUB_PROVIDER (one per careTeam member)
- `LINK_CLAIM_COVERAGE` between HUB_CLAIM and HUB_COVERAGE
- `SAT_CLAIM_HEADER` descriptors: status, type, subType, use, billablePeriod, created, priority, total
- `SAT_CLAIM_ITEM` (one row per `Claim.item[*]`): sequence, productOrService.code, modifier, serviced, quantity, unitPrice, net
- `SAT_CLAIM_DIAGNOSIS` (one row per `Claim.diagnosis[*]`): sequence, code, type, onAdmission

## Naming Conventions for Bronze RAW Tables

When creating Bronze for an FHIR-source vendor:

- Table: `BRONZE_<CLIENT>.RAW_CLAIMS`
- Column names use snake_case derived from FHIR field paths:
  - `Claim.id` → `claim_id`
  - `Claim.patient.identifier.value` → `member_id`
  - `Claim.provider.identifier.value` → `provider_npi`
  - `Claim.diagnosis[0].diagnosis.coding[0].code` → `icd10_primary`
  - `Claim.item[0].productOrService.coding[0].code` → `cpt_code`
  - `Claim.billablePeriod.start` → `service_date`
  - `Claim.item[0].net.value` → `billed_amount`
  - `Claim.status` → `claim_status`
  - `Claim.insurance[0].coverage.identifier.value` → `plan_id`
  - `Claim.related[0].reference.value` (when relationship=prior) → `prior_auth_ref`
- Mandatory audit columns (DataLink convention): `_load_dt`, `_source_file`, `_batch_id`, `_record_source`, `_load_type`, `_file_row_number`, `_record_hash`
