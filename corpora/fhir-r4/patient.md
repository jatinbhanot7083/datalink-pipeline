# FHIR R4 — Patient Resource

The Patient resource demographic information about an individual receiving
care. It is the foundational resource for member/beneficiary identity in
healthcare claims and membership feeds.

## Resource Path

`Patient`

## Maturity Level

5 (Normative)

## Cardinality and Key Fields

| Field | Type | Cardinality | Description |
|---|---|---|---|
| `id` | string | 0..1 | Logical id. |
| `identifier` | Identifier[] | 0..* | Business identifiers. Common: MRN, member ID, SSN. Each: `system` URI + `value` string. |
| `active` | boolean | 0..1 | Whether record is in active use. |
| `name` | HumanName[] | 0..* | A name associated with the patient. Each: `use` (usual/official/temp/nickname/maiden), `text` (full name), `family` (last name), `given` string[] (first + middle), `prefix`, `suffix`, `period`. |
| `telecom` | ContactPoint[] | 0..* | Contact details. Each: `system` (phone/fax/email/sms), `value`, `use` (home/work/mobile), `rank`, `period`. |
| `gender` | code | 0..1 | `male` \| `female` \| `other` \| `unknown`. Bound to AdministrativeGender. |
| `birthDate` | date | 0..1 | Date of birth. Format `YYYY-MM-DD`. |
| `deceased[x]` | boolean \| dateTime | 0..1 | Indicates if/when patient died. |
| `address` | Address[] | 0..* | Addresses. Each: `use` (home/work/billing), `type` (postal/physical/both), `text`, `line` string[], `city`, `district` (county), `state`, `postalCode`, `country`, `period`. |
| `maritalStatus` | CodeableConcept | 0..1 | Marital status. Bound to MaritalStatus. |
| `multipleBirth[x]` | boolean \| integer | 0..1 | Birth order if multiple. |
| `photo` | Attachment[] | 0..* | Image. |
| `contact` | BackboneElement[] | 0..* | Contact party for the patient (next of kin, emergency). |
| `communication` | BackboneElement[] | 0..* | Languages spoken. Each: `language` CodeableConcept (BCP-47), `preferred` boolean. |
| `generalPractitioner` | Reference(Organization \| Practitioner \| PractitionerRole)[] | 0..* | Patient's nominated PCP. |
| `managingOrganization` | Reference(Organization) | 0..1 | Organization that is the custodian of the record. |
| `link` | BackboneElement[] | 0..* | Links to other patient resources (merges, replaces). Each: `other` Reference, `type` (replaced-by/replaces/refer/seealso). |

## Common Identifier Systems

| System URI | Description | Format |
|---|---|---|
| `http://hl7.org/fhir/sid/us-ssn` | US Social Security Number | 9 digits |
| `urn:oid:2.16.840.1.113883.4.1` | US SSN (OID form) | 9 digits |
| `http://hl7.org/fhir/sid/us-medicare` | Medicare HICN/MBI | 11 alphanumeric for MBI |
| Payer-specific URIs | Member ID | varies |
| MRN (per facility) | Medical record number | facility-specific |

## Validation Rules (typical patient/membership DQ checks)

1. `Patient.name[0].family` must be present (non-null, non-empty)
2. `Patient.birthDate` must be present and parseable as `YYYY-MM-DD`
3. `Patient.birthDate` must be in the past (DOB cannot be in future)
4. `Patient.birthDate` must be within reasonable range (e.g., not before 1900)
5. `Patient.gender` must be one of `male` \| `female` \| `other` \| `unknown`
6. `Patient.telecom[system=phone].value` if US must match `^\(?[0-9]{3}\)?[-. ]?[0-9]{3}[-. ]?[0-9]{4}$` after normalization
7. `Patient.telecom[system=email].value` if present must be RFC 5322 valid
8. `Patient.address[use=home].postalCode` if US must match `^[0-9]{5}(-[0-9]{4})?$`
9. `Patient.address[use=home].state` if US must be one of 50 states + DC + territories (2-letter abbreviation)
10. `Patient.address[use=home].country` if US must be `US` (ISO-3166-1)
11. SSN if present must match `^[0-9]{3}-?[0-9]{2}-?[0-9]{4}$` (and not be 000-00-0000 or 999-99-9999)
12. Patient.identifier values across systems must not collide within tenant
13. Multiple Patient.identifier rows with same system must have unique values

## Bronze → Silver Mapping Hints (Data Vault 2.0)

- `HUB_PATIENT` natural key: payer-issued member identifier (most stable identifier)
  - For Medicare: MBI from `http://hl7.org/fhir/sid/us-medicare`
  - For commercial: member ID from `Coverage.subscriberId` resolution
- `HUB_PERSON` (organisation-wide MDM): SSN if available, else internal master person ID
- `LINK_PATIENT_PERSON` between HUB_PATIENT and HUB_PERSON (handles same person across coverages)
- `SAT_PATIENT_DEMOGRAPHICS` descriptors: name.family, name.given, gender, birthDate, deceasedBoolean, deceasedDateTime, maritalStatus
- `SAT_PATIENT_CONTACT` (one row per `Patient.telecom[*]`): system, value, use, rank
- `SAT_PATIENT_ADDRESS` (one row per `Patient.address[*]`): use, type, line[*], city, state, postalCode, country
- `SAT_PATIENT_COMMUNICATION` (one row per `Patient.communication[*]`): language, preferred

## Bronze RAW Column Naming

When creating Bronze for an FHIR-source membership/patient feed:

- `Patient.identifier[?].value` → `member_id` (with `system` filtering for primary id)
- `Patient.name[0].family` → `last_name`
- `Patient.name[0].given[0]` → `first_name`
- `Patient.name[0].given[1]` → `middle_name`
- `Patient.gender` → `gender`
- `Patient.birthDate` → `dob`
- `Patient.deceasedDateTime` → `dod`
- `Patient.telecom[system=phone].value` → `home_phone` / `mobile_phone`
- `Patient.telecom[system=email].value` → `email`
- `Patient.address[use=home].line[0]` → `address_1`
- `Patient.address[use=home].line[1]` → `address_2`
- `Patient.address[use=home].city` → `city`
- `Patient.address[use=home].state` → `state`
- `Patient.address[use=home].postalCode` → `zip`
- `Patient.address[use=home].country` → `country`
- `Patient.communication[preferred=true].language.coding[0].code` → `preferred_language`

## Common PHI Considerations

Patient resource fields that ARE PHI under HIPAA Safe Harbor:
- All name fields (`name.family`, `name.given`)
- All identifiers (`identifier.value`, including SSN, MBI, MRN, member ID)
- Date of birth (full date — only year is non-PHI for ages < 89)
- All address detail (street, ZIP+4, county, etc. — only state + first 3 digits of ZIP for areas > 20K population are non-PHI)
- All telecom values (phone, email)
- Photo attachments (face)
- Vehicle/license plate identifiers

When emitting Bronze, mark these columns with the audit metadata so downstream
masking/hashing rules know which to redact for non-PHI consumers.
