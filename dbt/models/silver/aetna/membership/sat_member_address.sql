-- Phase 15.7 DV2 Silver — Satellite
-- Sat       : SAT_MEMBER_ADDRESS
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
        TRY_CAST((member_street_address_line1) AS VARCHAR) AS address_line_1,
        TRY_CAST((NULLIF(member_street_address_line2, '')) AS VARCHAR) AS address_line_2,
        TRY_CAST((member_city) AS VARCHAR) AS address_city,
        TRY_CAST((UPPER(member_state)) AS VARCHAR) AS address_state,
        TRY_CAST((member_zip_code) AS VARCHAR) AS address_postal_code,
        TRY_CAST((NULLIF(member_county_code, '')) AS VARCHAR) AS address_county_code,
        TRY_CAST((NULLIF(member_county_name, '')) AS VARCHAR) AS address_county_name,
    _load_dt,
    _record_source,
    _batch_id,
    SHA2_HEX(CONCAT_WS('|', COALESCE(CAST((member_street_address_line1) AS VARCHAR), ''), COALESCE(CAST((NULLIF(member_street_address_line2, '')) AS VARCHAR), ''), COALESCE(CAST((member_city) AS VARCHAR), ''), COALESCE(CAST((UPPER(member_state)) AS VARCHAR), ''), COALESCE(CAST((member_zip_code) AS VARCHAR), ''), COALESCE(CAST((NULLIF(member_county_code, '')) AS VARCHAR), ''), COALESCE(CAST((NULLIF(member_county_name, '')) AS VARCHAR), '')), 256) AS _hash_diff
FROM bronze
WHERE member_card_id IS NOT NULL
{% if is_incremental() %}
  AND (SHA2_HEX(CONCAT_WS('|', COALESCE(CAST((member_street_address_line1) AS VARCHAR), ''), COALESCE(CAST((NULLIF(member_street_address_line2, '')) AS VARCHAR), ''), COALESCE(CAST((member_city) AS VARCHAR), ''), COALESCE(CAST((UPPER(member_state)) AS VARCHAR), ''), COALESCE(CAST((member_zip_code) AS VARCHAR), ''), COALESCE(CAST((NULLIF(member_county_code, '')) AS VARCHAR), ''), COALESCE(CAST((NULLIF(member_county_name, '')) AS VARCHAR), '')), 256)) NOT IN (
      SELECT _hash_diff FROM {{ this }} WHERE hash_key = bronze.hash_key
  )
{% endif %}
