-- Phase 15.7 DV2 Silver — Satellite
-- Sat       : SAT_MEMBER_COVERAGE
-- Hub       : HUB_MEMBER
-- Dataset   : membership
-- Client    : __global__
-- Source    : BRONZE___GLOBAL__.raw_membership
-- Pattern   : Satellite (descriptive, hash-diff change-detected)

{{ config(
    materialized = 'incremental',
    unique_key   = ['hash_key', '_hash_diff'],
    on_schema_change = 'fail',
    tags = ['datalink', 'phase15', 'dv2', 'silver', 'sat', '__global__', 'membership']
) }}

WITH bronze AS (
    SELECT *,
        SHA2_HEX(CONCAT_WS('|', COALESCE(CAST(member_card_id AS VARCHAR), '')), 256) AS hash_key
    FROM {{ source('bronze', 'raw_membership') }}
)
SELECT
    hash_key,
        TRY_CAST((COALESCE(member_card_id, member_medicare_id, member_medicaid_id)) AS VARCHAR) AS member_id,
        TRY_CAST((member_coverage_effective_date) AS DATE) AS coverage_effective_from,
        TRY_CAST((member_coverage_end_date) AS DATE) AS coverage_effective_to,
        TRY_CAST((member_health_plan_begin_date) AS DATE) AS health_plan_effective_from,
        TRY_CAST((member_enrollment_date) AS DATE) AS enrollment_date,
        TRY_CAST((refresh_date) AS DATE) AS data_refresh_date,
        TRY_CAST((CURRENT_TIMESTAMP()) AS TIMESTAMP) AS dw_load_date,
        TRY_CAST((COALESCE(refresh_date, CURRENT_DATE())) AS DATE) AS dw_effective_from,
        TRY_CAST((NULL) AS DATE) AS dw_effective_to,
        TRY_CAST((TRUE) AS BOOLEAN) AS dw_is_current,
    _load_dt,
    _record_source,
    _batch_id,
    SHA2_HEX(CONCAT_WS('|', COALESCE(CAST((COALESCE(member_card_id, member_medicare_id, member_medicaid_id)) AS VARCHAR), ''), COALESCE(CAST((member_coverage_effective_date) AS VARCHAR), ''), COALESCE(CAST((member_coverage_end_date) AS VARCHAR), ''), COALESCE(CAST((member_health_plan_begin_date) AS VARCHAR), ''), COALESCE(CAST((member_enrollment_date) AS VARCHAR), ''), COALESCE(CAST((refresh_date) AS VARCHAR), ''), COALESCE(CAST((CURRENT_TIMESTAMP()) AS VARCHAR), ''), COALESCE(CAST((COALESCE(refresh_date, CURRENT_DATE())) AS VARCHAR), ''), COALESCE(CAST((NULL) AS VARCHAR), ''), COALESCE(CAST((TRUE) AS VARCHAR), '')), 256) AS _hash_diff
FROM bronze
WHERE member_card_id IS NOT NULL
{% if is_incremental() %}
  AND (SHA2_HEX(CONCAT_WS('|', COALESCE(CAST((COALESCE(member_card_id, member_medicare_id, member_medicaid_id)) AS VARCHAR), ''), COALESCE(CAST((member_coverage_effective_date) AS VARCHAR), ''), COALESCE(CAST((member_coverage_end_date) AS VARCHAR), ''), COALESCE(CAST((member_health_plan_begin_date) AS VARCHAR), ''), COALESCE(CAST((member_enrollment_date) AS VARCHAR), ''), COALESCE(CAST((refresh_date) AS VARCHAR), ''), COALESCE(CAST((CURRENT_TIMESTAMP()) AS VARCHAR), ''), COALESCE(CAST((COALESCE(refresh_date, CURRENT_DATE())) AS VARCHAR), ''), COALESCE(CAST((NULL) AS VARCHAR), ''), COALESCE(CAST((TRUE) AS VARCHAR), '')), 256)) NOT IN (
      SELECT _hash_diff FROM {{ this }} WHERE hash_key = bronze.hash_key
  )
{% endif %}
