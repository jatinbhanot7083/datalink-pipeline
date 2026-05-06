-- Phase 15.7 DV2 Silver — Satellite
-- Sat       : SAT_MEMBER_CONTACT
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
        TRY_CAST(CAST((COALESCE(member_card_id, member_medicare_id, member_medicaid_id)) AS VARCHAR) AS VARCHAR) AS member_id,
        TRY_CAST(CAST((member_phone) AS VARCHAR) AS VARCHAR) AS phone,
        TRY_CAST(CAST((CURRENT_TIMESTAMP()) AS VARCHAR) AS TIMESTAMP) AS dw_load_date,
        TRY_CAST(CAST((COALESCE(refresh_date, CURRENT_DATE())) AS VARCHAR) AS DATE) AS dw_effective_from,
        TRY_CAST(CAST((NULL) AS VARCHAR) AS DATE) AS dw_effective_to,
        TRY_CAST(CAST((TRUE) AS VARCHAR) AS BOOLEAN) AS dw_is_current,
    _load_dt,
    _record_source,
    _batch_id,
    SHA2_HEX(CONCAT_WS('|', COALESCE(CAST((COALESCE(member_card_id, member_medicare_id, member_medicaid_id)) AS VARCHAR), ''), COALESCE(CAST((member_phone) AS VARCHAR), ''), COALESCE(CAST((CURRENT_TIMESTAMP()) AS VARCHAR), ''), COALESCE(CAST((COALESCE(refresh_date, CURRENT_DATE())) AS VARCHAR), ''), COALESCE(CAST((NULL) AS VARCHAR), ''), COALESCE(CAST((TRUE) AS VARCHAR), '')), 256) AS _hash_diff,
    -- Phase 17.2: vendor-overflow JSON propagated from Bronze
    _extra AS _extensions
FROM bronze
WHERE member_card_id IS NOT NULL
{% if is_incremental() %}
  AND (SHA2_HEX(CONCAT_WS('|', COALESCE(CAST((COALESCE(member_card_id, member_medicare_id, member_medicaid_id)) AS VARCHAR), ''), COALESCE(CAST((member_phone) AS VARCHAR), ''), COALESCE(CAST((CURRENT_TIMESTAMP()) AS VARCHAR), ''), COALESCE(CAST((COALESCE(refresh_date, CURRENT_DATE())) AS VARCHAR), ''), COALESCE(CAST((NULL) AS VARCHAR), ''), COALESCE(CAST((TRUE) AS VARCHAR), '')), 256)) NOT IN (
      SELECT _hash_diff FROM {{ this }} WHERE hash_key = bronze.hash_key
  )
{% endif %}
