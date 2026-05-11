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
    FROM {{ source('bronze', 'raw_membership') }}
)
SELECT
    hash_key,
        TRY_CAST(CAST((member_street_address_line1) AS VARCHAR) AS VARCHAR) AS address_line_1,
        TRY_CAST(CAST((member_street_address_line2) AS VARCHAR) AS VARCHAR) AS address_line_2,
        TRY_CAST(CAST((member_city) AS VARCHAR) AS VARCHAR) AS city,
        TRY_CAST(CAST((member_state) AS VARCHAR) AS VARCHAR) AS state,
        TRY_CAST(CAST((member_zip_code) AS VARCHAR) AS VARCHAR) AS zip_code,
        TRY_CAST(CAST((member_county_code) AS VARCHAR) AS VARCHAR) AS county_code,
        TRY_CAST(CAST((member_county_name) AS VARCHAR) AS VARCHAR) AS county_name,
    _load_dt,
    _record_source,
    _batch_id,
    SHA2_HEX(CONCAT_WS('|', COALESCE(CAST((member_street_address_line1) AS VARCHAR), ''), COALESCE(CAST((member_street_address_line2) AS VARCHAR), ''), COALESCE(CAST((member_city) AS VARCHAR), ''), COALESCE(CAST((member_state) AS VARCHAR), ''), COALESCE(CAST((member_zip_code) AS VARCHAR), ''), COALESCE(CAST((member_county_code) AS VARCHAR), ''), COALESCE(CAST((member_county_name) AS VARCHAR), '')), 256) AS _hash_diff,
    -- Phase 17.2: propagate vendor-overflow JSON from Bronze. Default
    -- behaviour is pass-through; clients who want to filter or rename
    -- specific keys should override this dbt model.
    _extra AS _extensions
FROM bronze
WHERE member_card_id IS NOT NULL
{% if is_incremental() %}
  AND (SHA2_HEX(CONCAT_WS('|', COALESCE(CAST((member_street_address_line1) AS VARCHAR), ''), COALESCE(CAST((member_street_address_line2) AS VARCHAR), ''), COALESCE(CAST((member_city) AS VARCHAR), ''), COALESCE(CAST((member_state) AS VARCHAR), ''), COALESCE(CAST((member_zip_code) AS VARCHAR), ''), COALESCE(CAST((member_county_code) AS VARCHAR), ''), COALESCE(CAST((member_county_name) AS VARCHAR), '')), 256)) NOT IN (
      SELECT _hash_diff FROM {{ this }} WHERE hash_key = bronze.hash_key
  )
{% endif %}
