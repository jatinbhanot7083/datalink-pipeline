-- Gold: GOLD_AUTH_DECISION — one decision event per PatientAuth.
-- Real UM: a single PatientAuth can have multiple decisions (initial,
-- extension, appeal). For demo we generate one decision per auth.

{{ config(materialized='table', schema='gold_um') }}

SELECT
    CAST(ROW_NUMBER() OVER (ORDER BY pa.patient_auth_id) AS INTEGER) AS auth_decision_id,
    pa.patient_auth_id,
    CASE
        WHEN pa.auth_status_id = 5 THEN 3  -- PENDED auth → PENDED decision
        WHEN scd.claim_status = 'APPROVED' THEN 1   -- APPROVED
        WHEN scd.claim_status = 'DENIED'   THEN 2   -- DENIED
        WHEN scd.claim_status = 'SUBMITTED' THEN 3  -- pending → PENDED
        ELSE 3
    END AS decision_status_id,
    pa.auth_from_date AS decision_date,
    pa.requested_amount AS total_requested,
    CASE
        WHEN scd.claim_status = 'APPROVED' THEN pa.requested_amount
        WHEN scd.claim_status = 'DENIED'   THEN 0
        ELSE NULL  -- pending
    END AS total_approved,
    CASE WHEN scd.claim_status = 'DENIED' THEN 'MEDICAL_NECESSITY_NOT_MET' END AS denial_reason,
    CURRENT_TIMESTAMP AS created_on,
    'SILVER.DV2.0' AS record_source
FROM {{ ref('gold_patient_auth') }} pa
JOIN {{ ref('hub_claim') }} hc ON hc.claim_id = pa.source_claim_id
JOIN {{ ref('sat_claim_details') }} scd ON scd.hub_claim_hk = hc.hub_claim_hk
