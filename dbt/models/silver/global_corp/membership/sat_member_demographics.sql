-- Phase 15.7 DV2 Silver — Satellite
-- Sat       : SAT_MEMBER_DEMOGRAPHICS
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
        TRY_CAST(CAST((member_first_name) AS VARCHAR) AS VARCHAR) AS member_first_name,
        TRY_CAST(CAST((member_last_name) AS VARCHAR) AS VARCHAR) AS member_last_name,
        TRY_CAST(CAST((TRIM(CONCAT_WS(' ', member_first_name, member_middle_name, member_last_name))) AS VARCHAR) AS VARCHAR) AS full_name,
        TRY_CAST(CAST((CASE WHEN UPPER(member_gender) IN ('M', 'MALE') THEN 'M' WHEN UPPER(member_gender) IN ('F', 'FEMALE') THEN 'F' WHEN UPPER(member_gender) IN ('U', 'UNKNOWN') THEN 'U' WHEN UPPER(member_gender) IN ('O', 'OTHER') THEN 'O' ELSE 'U' END) AS VARCHAR) AS VARCHAR) AS gender,
        TRY_CAST(CAST((member_birth_date) AS VARCHAR) AS DATE) AS birth_date,
        TRY_CAST(CAST((CURRENT_TIMESTAMP()) AS VARCHAR) AS TIMESTAMP) AS dw_load_date,
        TRY_CAST(CAST((COALESCE(refresh_date, CURRENT_DATE())) AS VARCHAR) AS DATE) AS dw_effective_from,
        TRY_CAST(CAST((NULL) AS VARCHAR) AS DATE) AS dw_effective_to,
        TRY_CAST(CAST((TRUE) AS VARCHAR) AS BOOLEAN) AS dw_is_current,
    _load_dt,
    _record_source,
    _batch_id,
    SHA2_HEX(CONCAT_WS('|', COALESCE(CAST((COALESCE(member_card_id, member_medicare_id, member_medicaid_id)) AS VARCHAR), ''), COALESCE(CAST((member_first_name) AS VARCHAR), ''), COALESCE(CAST((member_last_name) AS VARCHAR), ''), COALESCE(CAST((TRIM(CONCAT_WS(' ', member_first_name, member_middle_name, member_last_name))) AS VARCHAR), ''), COALESCE(CAST((CASE WHEN UPPER(member_gender) IN ('M', 'MALE') THEN 'M' WHEN UPPER(member_gender) IN ('F', 'FEMALE') THEN 'F' WHEN UPPER(member_gender) IN ('U', 'UNKNOWN') THEN 'U' WHEN UPPER(member_gender) IN ('O', 'OTHER') THEN 'O' ELSE 'U' END) AS VARCHAR), ''), COALESCE(CAST((member_birth_date) AS VARCHAR), ''), COALESCE(CAST((CURRENT_TIMESTAMP()) AS VARCHAR), ''), COALESCE(CAST((COALESCE(refresh_date, CURRENT_DATE())) AS VARCHAR), ''), COALESCE(CAST((NULL) AS VARCHAR), ''), COALESCE(CAST((TRUE) AS VARCHAR), '')), 256) AS _hash_diff,
    -- Phase 17.2: vendor-overflow JSON propagated from Bronze
    _extra AS _extensions
FROM bronze
WHERE member_card_id IS NOT NULL
{% if is_incremental() %}
  AND (SHA2_HEX(CONCAT_WS('|', COALESCE(CAST((COALESCE(member_card_id, member_medicare_id, member_medicaid_id)) AS VARCHAR), ''), COALESCE(CAST((member_first_name) AS VARCHAR), ''), COALESCE(CAST((member_last_name) AS VARCHAR), ''), COALESCE(CAST((TRIM(CONCAT_WS(' ', member_first_name, member_middle_name, member_last_name))) AS VARCHAR), ''), COALESCE(CAST((CASE WHEN UPPER(member_gender) IN ('M', 'MALE') THEN 'M' WHEN UPPER(member_gender) IN ('F', 'FEMALE') THEN 'F' WHEN UPPER(member_gender) IN ('U', 'UNKNOWN') THEN 'U' WHEN UPPER(member_gender) IN ('O', 'OTHER') THEN 'O' ELSE 'U' END) AS VARCHAR), ''), COALESCE(CAST((member_birth_date) AS VARCHAR), ''), COALESCE(CAST((CURRENT_TIMESTAMP()) AS VARCHAR), ''), COALESCE(CAST((COALESCE(refresh_date, CURRENT_DATE())) AS VARCHAR), ''), COALESCE(CAST((NULL) AS VARCHAR), ''), COALESCE(CAST((TRUE) AS VARCHAR), '')), 256)) NOT IN (
      SELECT _hash_diff FROM {{ this }} WHERE hash_key = bronze.hash_key
  )
{% endif %}
