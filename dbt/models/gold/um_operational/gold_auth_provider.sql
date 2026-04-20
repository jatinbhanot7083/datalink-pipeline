-- Gold: GOLD_AUTH_PROVIDER — provider attached to each auth.
-- For demo: one provider per auth (the claim's billing provider, role = "Attending").
-- Real UM: can have Attending + Referring + Facility + Specialist etc.

{{ config(materialized='table', schema='gold_um') }}

SELECT
    CAST(ROW_NUMBER() OVER (ORDER BY pa.patient_auth_id) AS INTEGER) AS auth_provider_id,
    pa.patient_auth_id,
    pa.provider_npi,
    spi.provider_name,
    spi.specialty_code,
    spi.network_status,
    'ATTENDING' AS provider_role,
    CURRENT_TIMESTAMP AS created_on,
    'SILVER.DV2.0' AS record_source
FROM {{ ref('gold_patient_auth') }} pa
LEFT JOIN {{ ref('hub_provider') }} hp ON hp.npi = pa.provider_npi
LEFT JOIN {{ ref('sat_provider_info') }} spi ON spi.hub_provider_hk = hp.hub_provider_hk
