-- Silver: HUB_PLAN — business key plan_id.
-- Added in Phase 3 to make LINK_MEMBER_PLAN a proper 2-hub link per DV2.0.
-- Sources plan_ids from both raw_claims AND raw_membership to capture
-- every plan ever mentioned (even if no members are currently enrolled).

{{ config(materialized='incremental', incremental_strategy='append') }}

WITH from_claims AS (
    SELECT plan_id, MIN(_load_dt) AS first_seen_dts, MIN(_record_source) AS first_record_source
    FROM {{ source('bronze', 'raw_claims') }}
    WHERE plan_id IS NOT NULL
    GROUP BY plan_id
),
from_membership AS (
    SELECT plan_id, MIN(_load_dt) AS first_seen_dts, MIN(_record_source) AS first_record_source
    FROM {{ source('bronze', 'raw_membership') }}
    WHERE plan_id IS NOT NULL
    GROUP BY plan_id
),
unioned AS (
    SELECT * FROM from_claims
    UNION
    SELECT * FROM from_membership
),
candidates AS (
    SELECT
        plan_id,
        MIN(first_seen_dts) AS load_dts,
        MIN(first_record_source) AS record_source
    FROM unioned
    GROUP BY plan_id
)

SELECT
    {{ dv_hash_key(['plan_id']) }} AS hub_plan_hk,
    plan_id,
    load_dts,
    record_source
FROM candidates
{% if is_incremental() %}
WHERE plan_id NOT IN (SELECT plan_id FROM {{ this }})
{% endif %}
