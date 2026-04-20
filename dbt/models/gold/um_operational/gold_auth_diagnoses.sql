-- Gold: GOLD_AUTH_DIAGNOSES — ICD-10 diagnosis codes per auth.
-- One row per (PatientAuth, ICD code). Primary + secondary split.

{{ config(materialized='table', schema='gold_um') }}

WITH unioned AS (
    -- Primary diagnosis
    SELECT
        pa.patient_auth_id,
        scd.icd10_primary AS diagnosis_code,
        1 AS sequence_number,
        true AS is_primary
    FROM {{ ref('gold_patient_auth') }} pa
    JOIN {{ ref('hub_claim') }} hc ON hc.claim_id = pa.source_claim_id
    JOIN {{ ref('sat_claim_details') }} scd ON scd.hub_claim_hk = hc.hub_claim_hk
    WHERE scd.icd10_primary IS NOT NULL AND scd.icd10_primary != ''

    UNION ALL

    -- Secondary diagnosis (when present)
    SELECT
        pa.patient_auth_id,
        scd.icd10_secondary AS diagnosis_code,
        2 AS sequence_number,
        false AS is_primary
    FROM {{ ref('gold_patient_auth') }} pa
    JOIN {{ ref('hub_claim') }} hc ON hc.claim_id = pa.source_claim_id
    JOIN {{ ref('sat_claim_details') }} scd ON scd.hub_claim_hk = hc.hub_claim_hk
    WHERE scd.icd10_secondary IS NOT NULL AND scd.icd10_secondary != ''
)

SELECT
    CAST(ROW_NUMBER() OVER (ORDER BY patient_auth_id, sequence_number) AS INTEGER) AS auth_diagnosis_id,
    patient_auth_id,
    diagnosis_code,
    sequence_number,
    is_primary,
    CURRENT_TIMESTAMP AS created_on,
    'SILVER.DV2.0' AS record_source
FROM unioned
