-- Phase 15.7 DV2 Silver — Satellite
-- Sat       : SAT_MEMBER_ELIGIBILITY
-- Hub       : HUB_MEMBER
-- Dataset   : membership
-- Client    : global_corp
-- Source    : BRONZE_GLOBAL_CORP.raw_membership
-- Pattern   : Satellite (descriptive, hash-diff change-detected)

{{ config(
    materialized = 'incremental',
    unique_key   = ['hash_key', '_hash_diff'],
    on_schema_change = 'fail',
    tags = ['datalink', 'phase15', 'dv2', 'silver', 'sat', 'global_corp', 'membership']
) }}

WITH bronze AS (
    SELECT *,
        SHA2_HEX(CONCAT_WS('|', COALESCE(CAST(member_card_id AS VARCHAR), '')), 256) AS hash_key
    FROM {{ source('bronze', 'raw_membership') }}
)
SELECT
    hash_key,
        TRY_CAST((COALESCE(member_card_id, member_medicare_id, member_medicaid_id)) AS VARCHAR) AS member_id,
        TRY_CAST((UPPER(member_active_status)) AS VARCHAR) AS active_status,
        TRY_CAST((UPPER(member_relationship_code)) AS VARCHAR) AS relationship_code,
        TRY_CAST((member_medicaid_indicator) AS BOOLEAN) AS medicaid_indicator,
        TRY_CAST((member_medicaid_dual_eligibility_indicator) AS BOOLEAN) AS dual_eligible_indicator,
        TRY_CAST((member_dual_eligibility_begin_date) AS DATE) AS dual_eligibility_effective_from,
        TRY_CAST((member_dual_eligibility_end_date) AS DATE) AS dual_eligibility_effective_to,
        TRY_CAST((CURRENT_TIMESTAMP()) AS TIMESTAMP) AS dw_load_date,
        TRY_CAST((COALESCE(refresh_date, CURRENT_DATE())) AS DATE) AS dw_effective_from,
        TRY_CAST((NULL) AS DATE) AS dw_effective_to,
        TRY_CAST((TRUE) AS BOOLEAN) AS dw_is_current,
    _load_dt,
    _record_source,
    _batch_id,
    SHA2_HEX(CONCAT_WS('|', COALESCE(CAST((COALESCE(member_card_id, member_medicare_id, member_medicaid_id)) AS VARCHAR), ''), COALESCE(CAST((UPPER(member_active_status)) AS VARCHAR), ''), COALESCE(CAST((UPPER(member_relationship_code)) AS VARCHAR), ''), COALESCE(CAST((member_medicaid_indicator) AS VARCHAR), ''), COALESCE(CAST((member_medicaid_dual_eligibility_indicator) AS VARCHAR), ''), COALESCE(CAST((member_dual_eligibility_begin_date) AS VARCHAR), ''), COALESCE(CAST((member_dual_eligibility_end_date) AS VARCHAR), ''), COALESCE(CAST((CURRENT_TIMESTAMP()) AS VARCHAR), ''), COALESCE(CAST((COALESCE(refresh_date, CURRENT_DATE())) AS VARCHAR), ''), COALESCE(CAST((NULL) AS VARCHAR), ''), COALESCE(CAST((TRUE) AS VARCHAR), '')), 256) AS _hash_diff,
    -- Phase 17.2: vendor-overflow JSON propagated from Bronze
    _extra AS _extensions
FROM bronze
WHERE member_card_id IS NOT NULL
{% if is_incremental() %}
  AND (SHA2_HEX(CONCAT_WS('|', COALESCE(CAST((COALESCE(member_card_id, member_medicare_id, member_medicaid_id)) AS VARCHAR), ''), COALESCE(CAST((UPPER(member_active_status)) AS VARCHAR), ''), COALESCE(CAST((UPPER(member_relationship_code)) AS VARCHAR), ''), COALESCE(CAST((member_medicaid_indicator) AS VARCHAR), ''), COALESCE(CAST((member_medicaid_dual_eligibility_indicator) AS VARCHAR), ''), COALESCE(CAST((member_dual_eligibility_begin_date) AS VARCHAR), ''), COALESCE(CAST((member_dual_eligibility_end_date) AS VARCHAR), ''), COALESCE(CAST((CURRENT_TIMESTAMP()) AS VARCHAR), ''), COALESCE(CAST((COALESCE(refresh_date, CURRENT_DATE())) AS VARCHAR), ''), COALESCE(CAST((NULL) AS VARCHAR), ''), COALESCE(CAST((TRUE) AS VARCHAR), '')), 256)) NOT IN (
      SELECT _hash_diff FROM {{ this }} WHERE hash_key = bronze.hash_key
  )
{% endif %}
