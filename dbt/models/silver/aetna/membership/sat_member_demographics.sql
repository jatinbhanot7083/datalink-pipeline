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
    FROM {{ source('bronze', 'raw_membership') }}
)
SELECT
    hash_key,
        TRY_CAST(CAST((member_first_name) AS VARCHAR) AS VARCHAR) AS member_first_name,
        TRY_CAST(CAST((member_last_name) AS VARCHAR) AS VARCHAR) AS member_last_name,
        TRY_CAST(CAST((member_birth_date) AS VARCHAR) AS DATE) AS birth_date,
        TRY_CAST(CAST((member_gender) AS VARCHAR) AS VARCHAR) AS gender,
        TRY_CAST(CAST((member_phone) AS VARCHAR) AS VARCHAR) AS phone_number,
    _load_dt,
    _record_source,
    _batch_id,
    SHA2_HEX(CONCAT_WS('|', COALESCE(CAST((member_first_name) AS VARCHAR), ''), COALESCE(CAST((member_last_name) AS VARCHAR), ''), COALESCE(CAST((member_birth_date) AS VARCHAR), ''), COALESCE(CAST((member_gender) AS VARCHAR), ''), COALESCE(CAST((member_phone) AS VARCHAR), '')), 256) AS _hash_diff,
    -- Phase 17.2: propagate vendor-overflow JSON from Bronze. Default
    -- behaviour is pass-through; clients who want to filter or rename
    -- specific keys should override this dbt model.
    _extra AS _extensions
FROM bronze
WHERE member_card_id IS NOT NULL
{% if is_incremental() %}
  AND (SHA2_HEX(CONCAT_WS('|', COALESCE(CAST((member_first_name) AS VARCHAR), ''), COALESCE(CAST((member_last_name) AS VARCHAR), ''), COALESCE(CAST((member_birth_date) AS VARCHAR), ''), COALESCE(CAST((member_gender) AS VARCHAR), ''), COALESCE(CAST((member_phone) AS VARCHAR), '')), 256)) NOT IN (
      SELECT _hash_diff FROM {{ this }} WHERE hash_key = bronze.hash_key
  )
{% endif %}
