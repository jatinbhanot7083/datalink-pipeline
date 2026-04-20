-- Gold: GOLD_AUTH_CODE — CPT/HCPCS procedure code per auth.
-- One row per (PatientAuth, CPT). For demo we have 1 CPT per auth (from
-- the originating claim); real UM can have many.

{{ config(materialized='table', schema='gold_um') }}

SELECT
    CAST(ROW_NUMBER() OVER (ORDER BY pa.patient_auth_id) AS INTEGER) AS auth_code_id,
    pa.patient_auth_id,
    CASE
        WHEN scd.cpt_code LIKE 'J____' THEN 'HCPCS'
        ELSE 'CPT'
    END AS auth_code_type,
    scd.cpt_code                   AS auth_code_ref,
    1                              AS requested_units,  -- demo: 1 unit per auth
    CASE
        WHEN scd.claim_status = 'APPROVED' THEN 1
        WHEN scd.claim_status = 'DENIED'   THEN 0
        ELSE NULL
    END                            AS approved_units,
    pa.auth_from_date              AS from_date,
    pa.auth_due_date               AS to_date,
    true                           AS is_primary,
    CURRENT_TIMESTAMP              AS created_on,
    'SILVER.DV2.0'                 AS record_source
FROM {{ ref('gold_patient_auth') }} pa
JOIN {{ ref('hub_claim') }} hc ON hc.claim_id = pa.source_claim_id
JOIN {{ ref('sat_claim_details') }} scd ON scd.hub_claim_hk = hc.hub_claim_hk
