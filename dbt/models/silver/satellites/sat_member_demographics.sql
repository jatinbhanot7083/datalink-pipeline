-- Silver: SAT_MEMBER_DEMOGRAPHICS — descriptive attrs of each member, historized.

{{ config(materialized='incremental', incremental_strategy='append') }}

WITH source AS (
    SELECT
        {{ dv_hash_key(['member_id', 'plan_id']) }} AS hub_member_hk,
        _load_dt AS load_dts,
        {{ dv_hash_diff([
            'subscriber_id', 'dob', 'gender', 'group_id',
            'effective_date', 'termination_date', 'coverage_type', 'state'
        ]) }} AS hash_diff,
        subscriber_id,
        dob,
        gender,
        group_id,
        effective_date,
        termination_date,
        coverage_type,
        state,
        _record_source AS record_source,
        ROW_NUMBER() OVER (
            PARTITION BY member_id, plan_id, _load_dt
            ORDER BY _source_file, _batch_id
        ) AS _rn
    FROM {{ source('bronze', 'raw_membership') }}
    WHERE member_id IS NOT NULL AND plan_id IS NOT NULL
)

SELECT
    hub_member_hk,
    load_dts,
    hash_diff,
    subscriber_id,
    dob,
    gender,
    group_id,
    effective_date,
    termination_date,
    coverage_type,
    state,
    record_source
FROM source
WHERE _rn = 1
{% if is_incremental() %}
  AND NOT EXISTS (
      SELECT 1
      FROM (
          SELECT hub_member_hk, hash_diff,
                 ROW_NUMBER() OVER (PARTITION BY hub_member_hk ORDER BY load_dts DESC) AS latest_rn
          FROM {{ this }}
      ) t
      WHERE t.hub_member_hk = source.hub_member_hk
        AND t.hash_diff = source.hash_diff
        AND t.latest_rn = 1
  )
{% endif %}
