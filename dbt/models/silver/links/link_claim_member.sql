-- Silver: LINK_CLAIM_MEMBER — associates each claim with its member enrollment.
-- INSERT-ONLY. A unique combination of (claim_id, member_id, plan_id) → one row.

{{ config(materialized='incremental', incremental_strategy='append') }}

WITH candidates AS (
    SELECT
        claim_id,
        member_id,
        plan_id,
        MIN(_load_dt) AS first_seen_dts,
        MIN(_record_source) AS first_record_source
    FROM {{ source('bronze', 'raw_claims') }}
    WHERE claim_id IS NOT NULL
      AND member_id IS NOT NULL
      AND plan_id IS NOT NULL
    GROUP BY claim_id, member_id, plan_id
)

SELECT
    {{ dv_hash_key(['claim_id', 'member_id', 'plan_id']) }} AS link_claim_member_hk,
    {{ dv_hash_key(['claim_id']) }} AS hub_claim_hk,
    {{ dv_hash_key(['member_id', 'plan_id']) }} AS hub_member_hk,
    first_seen_dts AS load_dts,
    first_record_source AS record_source
FROM candidates
{% if is_incremental() %}
WHERE {{ dv_hash_key(['claim_id', 'member_id', 'plan_id']) }} NOT IN (
    SELECT link_claim_member_hk FROM {{ this }}
)
{% endif %}
