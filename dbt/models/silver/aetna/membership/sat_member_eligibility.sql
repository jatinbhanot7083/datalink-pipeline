-- Phase 15.7 DV2 Silver — Satellite
-- Sat       : SAT_MEMBER_ELIGIBILITY
-- Hub       : HUB_MEMBER
-- Dataset   : membership
-- Client    : aetna
-- Source    : BRONZE_AETNA.raw_membership
-- Pattern   : Satellite (descriptive, hash-diff change-detected)

{{ config(
    materialized = 'incremental',
    unique_key   = ['hash_key', '_hash_diff'],
    on_schema_change = 'fail',
    tags = ['datalink', 'phase15', 'dv2', 'silver', 'sat', 'aetna', 'membership']
) }}

WITH bronze AS (
    SELECT *,
        SHA2_HEX(CONCAT_WS('|', COALESCE(CAST(member_card_id AS VARCHAR), '')), 256) AS hash_key
    FROM {{ source('bronze_aetna', 'raw_membership') }}
)
SELECT
    hash_key,
        TRY_CAST((CASE LOWER(member_active_status) WHEN 'active' THEN 'active' WHEN 'inactive' THEN 'inactive' WHEN 'terminated' THEN 'terminated' WHEN 'suspended' THEN 'suspended' ELSE 'unknown' END) AS VARCHAR) AS active_status,
        TRY_CAST((member_medicaid_indicator) AS BOOLEAN) AS medicaid_indicator,
        TRY_CAST((member_medicaid_dual_eligibility_indicator) AS BOOLEAN) AS dual_eligible_indicator,
        TRY_CAST((NULLIF(member_dual_eligibility_begin_date, '1900-01-01')) AS DATE) AS dual_eligible_effective_date,
        TRY_CAST((NULLIF(member_dual_eligibility_end_date, '9999-12-31')) AS DATE) AS dual_eligible_end_date,
        TRY_CAST((member_coverage_effective_date) AS DATE) AS coverage_effective_date,
        TRY_CAST((NULLIF(member_coverage_end_date, '9999-12-31')) AS DATE) AS coverage_end_date,
    _load_dt,
    _record_source,
    _batch_id,
    SHA2_HEX(CONCAT_WS('|', COALESCE(CAST((CASE LOWER(member_active_status) WHEN 'active' THEN 'active' WHEN 'inactive' THEN 'inactive' WHEN 'terminated' THEN 'terminated' WHEN 'suspended' THEN 'suspended' ELSE 'unknown' END) AS VARCHAR), ''), COALESCE(CAST((member_medicaid_indicator) AS VARCHAR), ''), COALESCE(CAST((member_medicaid_dual_eligibility_indicator) AS VARCHAR), ''), COALESCE(CAST((NULLIF(member_dual_eligibility_begin_date, '1900-01-01')) AS VARCHAR), ''), COALESCE(CAST((NULLIF(member_dual_eligibility_end_date, '9999-12-31')) AS VARCHAR), ''), COALESCE(CAST((member_coverage_effective_date) AS VARCHAR), ''), COALESCE(CAST((NULLIF(member_coverage_end_date, '9999-12-31')) AS VARCHAR), '')), 256) AS _hash_diff
FROM bronze
WHERE member_card_id IS NOT NULL
{% if is_incremental() %}
  AND (SHA2_HEX(CONCAT_WS('|', COALESCE(CAST((CASE LOWER(member_active_status) WHEN 'active' THEN 'active' WHEN 'inactive' THEN 'inactive' WHEN 'terminated' THEN 'terminated' WHEN 'suspended' THEN 'suspended' ELSE 'unknown' END) AS VARCHAR), ''), COALESCE(CAST((member_medicaid_indicator) AS VARCHAR), ''), COALESCE(CAST((member_medicaid_dual_eligibility_indicator) AS VARCHAR), ''), COALESCE(CAST((NULLIF(member_dual_eligibility_begin_date, '1900-01-01')) AS VARCHAR), ''), COALESCE(CAST((NULLIF(member_dual_eligibility_end_date, '9999-12-31')) AS VARCHAR), ''), COALESCE(CAST((member_coverage_effective_date) AS VARCHAR), ''), COALESCE(CAST((NULLIF(member_coverage_end_date, '9999-12-31')) AS VARCHAR), '')), 256)) NOT IN (
      SELECT _hash_diff FROM {{ this }} WHERE hash_key = bronze.hash_key
  )
{% endif %}
