-- Silver: HUB_MEMBER — business key (member_id, plan_id).
-- A member can be enrolled in multiple plans; each (member_id, plan_id)
-- combination is a distinct Hub row.

{{ config(materialized='incremental', incremental_strategy='append') }}

WITH candidates AS (
    SELECT
        member_id,
        plan_id,
        MIN(_load_dt) AS first_seen_dts,
        MIN(_record_source) AS first_record_source
    FROM {{ source('bronze', 'raw_membership') }}
    WHERE member_id IS NOT NULL AND plan_id IS NOT NULL
    GROUP BY member_id, plan_id
)

SELECT
    {{ dv_hash_key(['member_id', 'plan_id']) }} AS hub_member_hk,
    member_id,
    plan_id,
    first_seen_dts AS load_dts,
    first_record_source AS record_source
FROM candidates
{% if is_incremental() %}
WHERE (member_id, plan_id) NOT IN (
    SELECT member_id, plan_id FROM {{ this }}
)
{% endif %}
