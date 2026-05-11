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
    FROM {{ source('bronze', 'raw_membership') }}
)
SELECT
    hash_key,
        TRY_CAST(CAST((member_active_status) AS VARCHAR) AS VARCHAR) AS active_status,
        TRY_CAST(CAST((member_medicaid_indicator) AS VARCHAR) AS BOOLEAN) AS medicaid_indicator,
        TRY_CAST(CAST((member_medicaid_dual_eligibility_indicator) AS VARCHAR) AS BOOLEAN) AS dual_eligible_indicator,
        TRY_CAST(CAST((member_dual_eligibility_begin_date) AS VARCHAR) AS DATE) AS dual_eligibility_effective_from,
        TRY_CAST(CAST((member_dual_eligibility_end_date) AS VARCHAR) AS DATE) AS dual_eligibility_effective_to,
        TRY_CAST(CAST((member_coverage_effective_date) AS VARCHAR) AS DATE) AS coverage_effective_from,
        TRY_CAST(CAST((member_coverage_end_date) AS VARCHAR) AS DATE) AS coverage_effective_to,
    _load_dt,
    _record_source,
    _batch_id,
    SHA2_HEX(CONCAT_WS('|', COALESCE(CAST((member_active_status) AS VARCHAR), ''), COALESCE(CAST((member_medicaid_indicator) AS VARCHAR), ''), COALESCE(CAST((member_medicaid_dual_eligibility_indicator) AS VARCHAR), ''), COALESCE(CAST((member_dual_eligibility_begin_date) AS VARCHAR), ''), COALESCE(CAST((member_dual_eligibility_end_date) AS VARCHAR), ''), COALESCE(CAST((member_coverage_effective_date) AS VARCHAR), ''), COALESCE(CAST((member_coverage_end_date) AS VARCHAR), '')), 256) AS _hash_diff,
    -- Phase 17.2: propagate vendor-overflow JSON from Bronze. Default
    -- behaviour is pass-through; clients who want to filter or rename
    -- specific keys should override this dbt model.
    _extra AS _extensions
FROM bronze
WHERE member_card_id IS NOT NULL
{% if is_incremental() %}
  AND (SHA2_HEX(CONCAT_WS('|', COALESCE(CAST((member_active_status) AS VARCHAR), ''), COALESCE(CAST((member_medicaid_indicator) AS VARCHAR), ''), COALESCE(CAST((member_medicaid_dual_eligibility_indicator) AS VARCHAR), ''), COALESCE(CAST((member_dual_eligibility_begin_date) AS VARCHAR), ''), COALESCE(CAST((member_dual_eligibility_end_date) AS VARCHAR), ''), COALESCE(CAST((member_coverage_effective_date) AS VARCHAR), ''), COALESCE(CAST((member_coverage_end_date) AS VARCHAR), '')), 256)) NOT IN (
      SELECT _hash_diff FROM {{ this }} WHERE hash_key = bronze.hash_key
  )
{% endif %}
