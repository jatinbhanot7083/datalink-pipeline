-- Silver: LINK_MEMBER_PLAN — connects HUB_MEMBER ↔ HUB_PLAN.
-- Each (member_id, plan_id) → 1 link row. Supports multi-plan member histories
-- (a member enrolled in multiple plans has multiple link rows).

{{ config(materialized='incremental', incremental_strategy='append') }}

WITH candidates AS (
    SELECT
        member_id,
        plan_id,
        MIN(_load_dt) AS first_seen_dts,
        MIN(_record_source) AS first_record_source
    FROM {{ source('bronze', 'raw_membership') }}
    WHERE member_id IS NOT NULL
      AND plan_id IS NOT NULL
    GROUP BY member_id, plan_id
)

SELECT
    {{ dv_hash_key(['member_id', 'plan_id']) }} AS link_member_plan_hk,
    {{ dv_hash_key(['member_id', 'plan_id']) }} AS hub_member_hk,
    {{ dv_hash_key(['plan_id']) }} AS hub_plan_hk,
    first_seen_dts AS load_dts,
    first_record_source AS record_source
FROM candidates
{% if is_incremental() %}
WHERE {{ dv_hash_key(['member_id', 'plan_id']) }} NOT IN (
    SELECT link_member_plan_hk FROM {{ this }}
)
{% endif %}
