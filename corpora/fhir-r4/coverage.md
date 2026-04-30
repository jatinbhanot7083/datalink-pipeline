# FHIR R4 — Coverage Resource

The Coverage resource represents financial instruments (insurance plan or
self-pay) that pay for healthcare services. It is the FHIR equivalent of an
insurance card / member benefit plan.

## Resource Path

`Coverage`

## Maturity Level

3 (Trial Use)

## Cardinality and Key Fields

| Field | Type | Cardinality | Description |
|---|---|---|---|
| `id` | string | 0..1 | Logical id. Globally unique. |
| `identifier` | Identifier[] | 0..* | Member ID, group ID, plan ID. Each has `system` (assigning authority) + `value`. Most common: subscriber ID. |
| `status` | code | 1..1 | Required. `active` \| `cancelled` \| `draft` \| `entered-in-error`. |
| `type` | CodeableConcept | 0..1 | Coverage category — bound to `http://terminology.hl7.org/CodeSystem/v3-ActCode`. Examples: `EHCPOL` (extended healthcare), `PUBLICPOL` (public healthcare), `MCPOL` (managed care). |
| `policyHolder` | Reference(Patient \| RelatedPerson \| Organization) | 0..1 | Owner of the policy. |
| `subscriber` | Reference(Patient \| RelatedPerson) | 0..1 | Subscriber to the policy. Often same as Patient. |
| `subscriberId` | string | 0..1 | ID assigned to subscriber. Typical "Member ID" on insurance cards. |
| `beneficiary` | Reference(Patient) | 1..1 | Required. Who receives benefits — the actual patient. |
| `dependent` | string | 0..1 | Dependent number for differentiation. |
| `relationship` | CodeableConcept | 0..1 | Relationship of beneficiary to subscriber. `self`, `spouse`, `child`, etc. |
| `period` | Period | 0..1 | Coverage start/end dates. |
| `payor` | Reference(Organization \| Patient \| RelatedPerson) | 1..* | Required. Issuer(s) of the policy. |
| `class` | BackboneElement[] | 0..* | Identifier of the plan provider, etc. Each: `type` CodeableConcept, `value` string, `name` string. Common types: `group`, `plan`, `subgroup`, `class`, `subclass`. |
| `order` | positiveInt | 0..1 | The order of applicability of this coverage relative to others. 1 = primary. |
| `network` | string | 0..1 | The plan's network identifier. |
| `costToBeneficiary` | BackboneElement[] | 0..* | Patient payments for coverage. Each: `type` (copay/deductible/coinsurance), `value[x]`, `exception`. |
| `subrogation` | boolean | 0..1 | True if reimbursement is to provider, false if to subscriber. |
| `contract` | Reference(Contract)[] | 0..* | Underlying contract. |

## Relationships

- `Coverage.beneficiary` → `Patient` (required)
- `Coverage.subscriber` → `Patient` | `RelatedPerson`
- `Coverage.payor` → `Organization` (the payer/insurer)
- `Coverage.policyHolder` → `Patient` | `RelatedPerson` | `Organization`

## Common Class Types

| Type code | Meaning | Example value |
|---|---|---|
| `group` | Group ID | "GRP-12345" |
| `plan` | Plan ID | "BCBS-PPO-2026" |
| `subplan` | Sub-plan | "FAMILY-DEDUCTIBLE-1500" |
| `class` | Coverage class | "GOLD" |
| `subclass` | Coverage sub-class | "GOLD-PLUS" |
| `division` | Division | "NORTHEAST" |
| `subdivision` | Sub-division | "NY-METRO" |
| `sequence` | Sequence | "1" |
| `rxbin` | Pharmacy BIN | "012345" |
| `rxpcn` | Pharmacy PCN | "MEDD" |
| `rxgroup` | Pharmacy group | "RX-GROUP-A" |
| `rxid` | Pharmacy ID | "M123456789" |

## Validation Rules (typical coverage DQ checks)

1. `Coverage.beneficiary` must resolve to an existing Patient
2. `Coverage.subscriberId` must be present for active coverage
3. `Coverage.period.start` must be <= `Coverage.period.end` if both present
4. `Coverage.status=active` requires `Coverage.period.start` to be <= today
5. `Coverage.payor` must resolve; for Medicare/Medicaid, the payor's NCPDP/HIOS identifier must be valid
6. `Coverage.class[type=plan].value` must match plan ID format for the issuing payer
7. `Coverage.relationship.coding[0].code` from `http://terminology.hl7.org/CodeSystem/v3-RoleCode` (SubscriberCovered, etc.)

## Bronze → Silver Mapping Hints (Data Vault 2.0)

- `HUB_COVERAGE` natural key: `Coverage.subscriberId` + payer system (multi-tenant scoping)
- `HUB_MEMBER` natural key: `Coverage.beneficiary.identifier.value` (member ID)
- `HUB_PAYER` natural key: `Coverage.payor[0].identifier.value` (organization NPI / NAIC)
- `LINK_MEMBER_COVERAGE` between HUB_MEMBER and HUB_COVERAGE
- `LINK_COVERAGE_PAYER` between HUB_COVERAGE and HUB_PAYER
- `SAT_COVERAGE` descriptors: status, type, period.start, period.end, relationship, network, order
- `SAT_COVERAGE_CLASS` (one row per `Coverage.class[*]`): type, value, name

## Bronze RAW_MEMBERSHIP Column Naming

When creating Bronze for an FHIR-source membership feed:

- `Coverage.subscriberId` → `member_id`
- `Coverage.beneficiary.identifier.value` → `member_id` (often same as above)
- `Coverage.policyHolder.identifier.value` → `subscriber_id`
- `Coverage.period.start` → `eligibility_start_date`
- `Coverage.period.end` → `eligibility_end_date`
- `Coverage.relationship.coding[0].code` → `relationship_to_subscriber`
- `Coverage.payor[0].identifier.value` → `payer_id`
- `Coverage.class[type=plan].value` → `plan_id`
- `Coverage.class[type=group].value` → `group_id`
- `Coverage.class[type=rxbin].value` → `rx_bin`
- `Coverage.class[type=rxpcn].value` → `rx_pcn`
- `Coverage.network` → `network_id`
