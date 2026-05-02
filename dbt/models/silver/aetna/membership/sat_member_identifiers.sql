-- Phase 15.7 DV2 Silver — Satellite
-- Sat       : SAT_MEMBER_IDENTIFIERS
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
        TRY_CAST((member_card_id) AS VARCHAR) AS identifier_card_id,
        TRY_CAST((member_medicare_id) AS VARCHAR) AS identifier_medicare_id,
        TRY_CAST((member_medicaid_id) AS VARCHAR) AS identifier_medicaid_id,
    _load_dt,
    _record_source,
    _batch_id,
    SHA2_HEX(CONCAT_WS('|', COALESCE(CAST((member_card_id) AS VARCHAR), ''), COALESCE(CAST((member_medicare_id) AS VARCHAR), ''), COALESCE(CAST((member_medicaid_id) AS VARCHAR), '')), 256) AS _hash_diff
FROM bronze
WHERE member_card_id IS NOT NULL
{% if is_incremental() %}
  AND (SHA2_HEX(CONCAT_WS('|', COALESCE(CAST((member_card_id) AS VARCHAR), ''), COALESCE(CAST((member_medicare_id) AS VARCHAR), ''), COALESCE(CAST((member_medicaid_id) AS VARCHAR), '')), 256)) NOT IN (
      SELECT _hash_diff FROM {{ this }} WHERE hash_key = bronze.hash_key
  )
{% endif %}
