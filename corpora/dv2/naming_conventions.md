# Data Vault 2.0 — Naming Conventions and Patterns

Data Vault 2.0 (DV 2.0) by Dan Linstedt is the methodology used at the
Silver tier of the DataLink medallion architecture. Three table types,
strict naming, and hash-based primary keys for parallel loading.

## Three Table Types

### Hub (HUB_)

Stores **business keys** for an entity. One Hub per business concept.
Append-only — once a business key arrives, it stays forever (even after
"deletion" in source — Data Vault tracks deletion via Sat).

**Standard columns:**

```
<HUB>_HK            VARBINARY  -- hash key, deterministic from natural keys
<NK_columns>        ...        -- the natural / business keys
LOAD_DT             TIMESTAMP  -- when this hash key was first seen
RECORD_SOURCE       VARCHAR    -- upstream system tag (e.g. 'CANO_HEALTH_837P')
```

**Examples:**

| Hub Table | Business Key Columns | Example Row |
|---|---|---|
| `HUB_PATIENT` | `MEMBER_ID` (+ payer system) | `(0xa3..., 'MBR0000123', 'AETNA', 2026-04-15, 'AETNA_834')` |
| `HUB_CLAIM` | `CLAIM_ID` (+ payer system) | `(0xb1..., 'CLM0001', 'AETNA', 2026-04-15, 'AETNA_837P')` |
| `HUB_PROVIDER` | `PROVIDER_NPI` | `(0xc7..., '1234567890', 2026-04-15, 'NPPES')` |
| `HUB_COVERAGE` | `SUBSCRIBER_ID`, `PLAN_ID` | |
| `HUB_ENCOUNTER` | `ENCOUNTER_ID` (+ facility) | |
| `HUB_DIAGNOSIS` | `ICD10_CODE` | natural key |
| `HUB_PROCEDURE` | `CPT_CODE` | natural key |

### Link (LINK_)

Stores **relationships** between Hubs. One Link per relationship type.
Append-only.

**Standard columns:**

```
<LINK>_HK           VARBINARY  -- hash of all parent HKs
<HUB_A>_HK          VARBINARY  -- FK to Hub A
<HUB_B>_HK          VARBINARY  -- FK to Hub B
... (more parent Hub HKs as needed)
LOAD_DT             TIMESTAMP
RECORD_SOURCE       VARCHAR
```

**Examples:**

| Link Table | Parent Hubs | Description |
|---|---|---|
| `LINK_CLAIM_PATIENT` | HUB_CLAIM, HUB_PATIENT | Each claim references one patient |
| `LINK_CLAIM_PROVIDER` | HUB_CLAIM, HUB_PROVIDER | Claim's rendering provider (1) and care team (N) |
| `LINK_CLAIM_COVERAGE` | HUB_CLAIM, HUB_COVERAGE | Which coverage paid the claim |
| `LINK_CLAIM_DIAGNOSIS` | HUB_CLAIM, HUB_DIAGNOSIS | One per ICD-10 listed |
| `LINK_CLAIM_PROCEDURE` | HUB_CLAIM, HUB_PROCEDURE | One per CPT/HCPCS line |
| `LINK_PATIENT_COVERAGE` | HUB_PATIENT, HUB_COVERAGE | Effective dates, primary/secondary |
| `LINK_PATIENT_PROVIDER` | HUB_PATIENT, HUB_PROVIDER | PCP, specialist relationships |

### Satellite (SAT_)

Stores **descriptive attributes** of a Hub or Link, with full history.
Append-only with `LOAD_DT` — old rows are not deleted, new rows are
appended when attributes change.

**Standard columns:**

```
<HUB or LINK>_HK    VARBINARY  -- FK to parent Hub/Link
LOAD_DT             TIMESTAMP  -- composite PK with HK
LOAD_END_DT         TIMESTAMP  -- nullable; populated when superseded
HASHDIFF            VARBINARY  -- hash of all attribute columns; detect changes
RECORD_SOURCE       VARCHAR
<attr1>             ...        -- the descriptive attributes
<attr2>             ...
...
```

**Examples:**

| Satellite Table | Parent | Attributes |
|---|---|---|
| `SAT_PATIENT_DEMOGRAPHICS` | HUB_PATIENT | first_name, last_name, dob, gender, marital_status |
| `SAT_PATIENT_ADDRESS` | HUB_PATIENT | address_1, address_2, city, state, zip, address_type |
| `SAT_PATIENT_CONTACT` | HUB_PATIENT | home_phone, mobile_phone, email |
| `SAT_CLAIM_HEADER` | HUB_CLAIM | claim_status, billed_amount, service_start, service_end, place_of_service |
| `SAT_CLAIM_LINE` | HUB_CLAIM (with line_seq) | line_number, cpt_code, modifier, units, billed, allowed |
| `SAT_PROVIDER_DEMOGRAPHICS` | HUB_PROVIDER | first_name, last_name, taxonomy, license, credentials |
| `SAT_COVERAGE_DETAILS` | HUB_COVERAGE | plan_id, coverage_line, level, eligibility_start, eligibility_end |
| `SAT_PATIENT_PROVIDER_RELATIONSHIP` | LINK_PATIENT_PROVIDER | relationship_type, start_date, end_date, primary |

### Optional: Reference Tables (REF_)

Stable code sets — ICD-10, CPT, NPI lookup, NUCC taxonomy, etc.
Replicated, not append-only. Used for joins with Hubs.

```
REF_ICD10_CODES
REF_CPT_CODES
REF_HCPCS_CODES
REF_NUCC_TAXONOMY
REF_PLACE_OF_SERVICE
REF_NPI_REGISTRY
```

## Key Naming Rules

1. **Singular** entity names: `HUB_PATIENT`, not `HUB_PATIENTS`
2. **No abbreviations** for first words: `HUB_DIAGNOSIS` not `HUB_DX`
3. **Underscore-separated** uppercase: `SAT_CLAIM_LINE_DETAIL`
4. **Schema prefix per tenant**: `SILVER_silver_AETNA.HUB_PATIENT`
5. **Multi-Sat per Hub** for change-velocity isolation: `SAT_PATIENT_DEMOGRAPHICS` (changes monthly), `SAT_PATIENT_ADDRESS` (changes annually) — separate so high-velocity changes don't drag low-velocity ones
6. **Link names** = `LINK_<HUB_A>_<HUB_B>` alphabetical: `LINK_CLAIM_PATIENT` not `LINK_PATIENT_CLAIM`

## Hash Key Computation

Every Hub/Link/Sat HK is a deterministic hash so multiple loaders can
populate in parallel without coordination. Standard:

```sql
-- Hub HK
HUB_PATIENT.PATIENT_HK = MD5(UPPER(TRIM(MEMBER_ID)) || '|' || UPPER(TRIM(PAYER_SYSTEM)))

-- Link HK
LINK_CLAIM_PATIENT.LINK_HK = MD5(UPPER(TRIM(MEMBER_ID)) || '|' || UPPER(TRIM(PAYER_SYSTEM))
                              || '|' || UPPER(TRIM(CLAIM_ID)) || '|' || UPPER(TRIM(CLAIM_PAYER_SYSTEM)))

-- Sat HASHDIFF (for change detection)
SAT_PATIENT_DEMOGRAPHICS.HASHDIFF = MD5(
    COALESCE(UPPER(TRIM(FIRST_NAME)), '') || '|' ||
    COALESCE(UPPER(TRIM(LAST_NAME)), '') || '|' ||
    COALESCE(TO_VARCHAR(DOB), '') || '|' ||
    COALESCE(UPPER(TRIM(GENDER)), '')
)
```

Use UPPER + TRIM to normalize case + whitespace before hashing — otherwise
the same business key with different casing produces different hashes.

## Insert Patterns

**Hub insert** (only inserts new business keys):

```sql
INSERT INTO HUB_PATIENT (PATIENT_HK, MEMBER_ID, PAYER_SYSTEM, LOAD_DT, RECORD_SOURCE)
SELECT
    MD5(UPPER(TRIM(s.member_id)) || '|' || UPPER(TRIM(s.payer_system))) AS PATIENT_HK,
    UPPER(TRIM(s.member_id)),
    UPPER(TRIM(s.payer_system)),
    CURRENT_TIMESTAMP,
    'AETNA_834'
FROM BRONZE_AETNA.RAW_MEMBERSHIP s
WHERE NOT EXISTS (
    SELECT 1 FROM SILVER_silver_AETNA.HUB_PATIENT h
    WHERE h.PATIENT_HK = MD5(UPPER(TRIM(s.member_id)) || '|' || UPPER(TRIM(s.payer_system)))
);
```

**Sat insert** (only when HASHDIFF changes):

```sql
INSERT INTO SAT_PATIENT_DEMOGRAPHICS (PATIENT_HK, LOAD_DT, HASHDIFF, RECORD_SOURCE,
                                       FIRST_NAME, LAST_NAME, DOB, GENDER)
SELECT
    h.PATIENT_HK,
    CURRENT_TIMESTAMP,
    MD5(COALESCE(UPPER(TRIM(s.first_name)),'') || '|' || COALESCE(UPPER(TRIM(s.last_name)),''),
        || ... ) AS NEW_HASHDIFF,
    'AETNA_834',
    s.first_name, s.last_name, s.dob, s.gender
FROM BRONZE_AETNA.RAW_MEMBERSHIP s
JOIN SILVER_silver_AETNA.HUB_PATIENT h ON h.MEMBER_ID = UPPER(TRIM(s.member_id))
WHERE NOT EXISTS (
    SELECT 1
    FROM SILVER_silver_AETNA.SAT_PATIENT_DEMOGRAPHICS prior
    WHERE prior.PATIENT_HK = h.PATIENT_HK
      AND prior.HASHDIFF = MD5(... same expr ...)
      AND prior.LOAD_END_DT IS NULL
);
```

This idempotency pattern means re-running the Silver build with the same
Bronze rows is a no-op — perfect for the schedule-and-replay world.

## Why Data Vault for Healthcare

- **Auditability**: every change history-tracked. Critical for HIPAA / CMS audits.
- **Late-arriving data**: a claim from 30 days ago arriving today doesn't break the model — just a new Sat row.
- **Multi-source ambiguity**: the same patient appearing in 837P and 834 with slight name spelling difference resolves to the same HUB_PATIENT (via SSN or normalized member_id).
- **Schema evolution**: vendors changing their column structure adds new Sats without restructuring existing tables.
- **Massively parallel load**: hash-based PKs let independent loaders populate Hubs/Sats simultaneously.
