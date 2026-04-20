-- Silver: HUB_CLAIM — one row per unique claim_id, ever.
-- Per DV2.0: INSERT-ONLY. Never DELETE, never UPDATE.
-- Load timestamp = earliest _load_dt seen in Bronze for this business key.

{{ config(materialized='incremental', incremental_strategy='append') }}

WITH candidates AS (
    SELECT
        claim_id,
        MIN(_load_dt) AS first_seen_dts,
        MIN(_record_source) AS first_record_source
    FROM {{ source('bronze', 'raw_claims') }}
    WHERE claim_id IS NOT NULL
    GROUP BY claim_id
)

SELECT
    {{ dv_hash_key(['claim_id']) }} AS hub_claim_hk,
    claim_id,
    first_seen_dts AS load_dts,
    first_record_source AS record_source
FROM candidates
{% if is_incremental() %}
WHERE claim_id NOT IN (SELECT claim_id FROM {{ this }})
{% endif %}
