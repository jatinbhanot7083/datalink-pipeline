-- Phase 15.7 DV2 Silver — Satellite
-- Sat       : SAT_MEMBER_DEMOGRAPHICS
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
        TRY_CAST((member_first_name) AS VARCHAR) AS name_first,
        TRY_CAST((NULLIF(member_middle_name, '')) AS VARCHAR) AS name_middle,
        TRY_CAST((member_last_name) AS VARCHAR) AS name_last,
        TRY_CAST((TRIM(CONCAT_WS(' ', member_first_name, member_middle_name, member_last_name))) AS VARCHAR) AS name_full,
        TRY_CAST((member_birth_date) AS DATE) AS birth_date,
        TRY_CAST((CASE LOWER(member_gender) WHEN 'male' THEN 'male' WHEN 'female' THEN 'female' WHEN 'other' THEN 'other' WHEN 'unknown' THEN 'unknown' ELSE 'unknown' END) AS VARCHAR) AS gender,
        TRY_CAST((member_phone) AS VARCHAR) AS telecom_phone,
    _load_dt,
    _record_source,
    _batch_id,
    SHA2_HEX(CONCAT_WS('|', COALESCE(CAST((member_first_name) AS VARCHAR), ''), COALESCE(CAST((NULLIF(member_middle_name, '')) AS VARCHAR), ''), COALESCE(CAST((member_last_name) AS VARCHAR), ''), COALESCE(CAST((TRIM(CONCAT_WS(' ', member_first_name, member_middle_name, member_last_name))) AS VARCHAR), ''), COALESCE(CAST((member_birth_date) AS VARCHAR), ''), COALESCE(CAST((CASE LOWER(member_gender) WHEN 'male' THEN 'male' WHEN 'female' THEN 'female' WHEN 'other' THEN 'other' WHEN 'unknown' THEN 'unknown' ELSE 'unknown' END) AS VARCHAR), ''), COALESCE(CAST((member_phone) AS VARCHAR), '')), 256) AS _hash_diff
FROM bronze
WHERE member_card_id IS NOT NULL
{% if is_incremental() %}
  AND (SHA2_HEX(CONCAT_WS('|', COALESCE(CAST((member_first_name) AS VARCHAR), ''), COALESCE(CAST((NULLIF(member_middle_name, '')) AS VARCHAR), ''), COALESCE(CAST((member_last_name) AS VARCHAR), ''), COALESCE(CAST((TRIM(CONCAT_WS(' ', member_first_name, member_middle_name, member_last_name))) AS VARCHAR), ''), COALESCE(CAST((member_birth_date) AS VARCHAR), ''), COALESCE(CAST((CASE LOWER(member_gender) WHEN 'male' THEN 'male' WHEN 'female' THEN 'female' WHEN 'other' THEN 'other' WHEN 'unknown' THEN 'unknown' ELSE 'unknown' END) AS VARCHAR), ''), COALESCE(CAST((member_phone) AS VARCHAR), '')), 256)) NOT IN (
      SELECT _hash_diff FROM {{ this }} WHERE hash_key = bronze.hash_key
  )
{% endif %}
