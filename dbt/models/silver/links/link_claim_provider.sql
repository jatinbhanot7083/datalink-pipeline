-- Silver: LINK_CLAIM_PROVIDER — associates each claim with its billing provider.

{{ config(materialized='incremental', incremental_strategy='append') }}

WITH candidates AS (
    SELECT
        claim_id,
        provider_npi,
        MIN(_load_dt) AS first_seen_dts,
        MIN(_record_source) AS first_record_source
    FROM {{ source('bronze', 'raw_claims') }}
    WHERE claim_id IS NOT NULL
      AND provider_npi IS NOT NULL
    GROUP BY claim_id, provider_npi
)

SELECT
    {{ dv_hash_key(['claim_id', 'provider_npi']) }} AS link_claim_provider_hk,
    {{ dv_hash_key(['claim_id']) }} AS hub_claim_hk,
    {{ dv_hash_key(['provider_npi']) }} AS hub_provider_hk,
    first_seen_dts AS load_dts,
    first_record_source AS record_source
FROM candidates
{% if is_incremental() %}
WHERE {{ dv_hash_key(['claim_id', 'provider_npi']) }} NOT IN (
    SELECT link_claim_provider_hk FROM {{ this }}
)
{% endif %}
