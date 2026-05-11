-- Phase 15.7 DV2 Silver — Satellite
-- Sat       : SAT_PROVIDER_IDENTITY
-- Hub       : HUB_PROVIDER
-- Dataset   : provider
-- Client    : aetna
-- Source    : BRONZE_AETNA.raw_provider
-- Pattern   : Satellite (descriptive, hash-diff change-detected)

{{ config(
    materialized = 'incremental',
    unique_key   = ['hash_key', '_hash_diff'],
    on_schema_change = 'fail',
    tags = ['datalink', 'phase15', 'dv2', 'silver', 'sat', 'aetna', 'provider']
) }}

WITH bronze AS (
    SELECT *,
        SHA2_HEX(CONCAT_WS('|', COALESCE(CAST(national_provider_identifier_npi AS VARCHAR), '')), 256) AS hash_key
    FROM {{ source('bronze', 'raw_provider') }}
)
SELECT
    hash_key,
        TRY_CAST(CAST((provider_identification_number_id) AS VARCHAR) AS VARCHAR) AS provider_id,
        TRY_CAST(CAST((provider_first_name) AS VARCHAR) AS VARCHAR) AS provider_first_name,
        TRY_CAST(CAST((NULLIF(provider_middle_name, '')) AS VARCHAR) AS VARCHAR) AS provider_middle_name,
        TRY_CAST(CAST((provider_last_name) AS VARCHAR) AS VARCHAR) AS provider_last_name,
        TRY_CAST(CAST((TRIM(CONCAT_WS(' ', provider_first_name, NULLIF(provider_middle_name, ''), provider_last_name))) AS VARCHAR) AS VARCHAR) AS provider_full_name,
        TRY_CAST(CAST((provider_tax_id) AS VARCHAR) AS VARCHAR) AS tax_id,
        TRY_CAST(CAST((provider_tax_id_name) AS VARCHAR) AS VARCHAR) AS tax_id_entity_name,
    _load_dt,
    _record_source,
    _batch_id,
    SHA2_HEX(CONCAT_WS('|', COALESCE(CAST((provider_identification_number_id) AS VARCHAR), ''), COALESCE(CAST((provider_first_name) AS VARCHAR), ''), COALESCE(CAST((NULLIF(provider_middle_name, '')) AS VARCHAR), ''), COALESCE(CAST((provider_last_name) AS VARCHAR), ''), COALESCE(CAST((TRIM(CONCAT_WS(' ', provider_first_name, NULLIF(provider_middle_name, ''), provider_last_name))) AS VARCHAR), ''), COALESCE(CAST((provider_tax_id) AS VARCHAR), ''), COALESCE(CAST((provider_tax_id_name) AS VARCHAR), '')), 256) AS _hash_diff,
    -- Phase 17.2: propagate vendor-overflow JSON from Bronze. Default
    -- behaviour is pass-through; clients who want to filter or rename
    -- specific keys should override this dbt model.
    _extra AS _extensions
FROM bronze
WHERE national_provider_identifier_npi IS NOT NULL
{% if is_incremental() %}
  AND (SHA2_HEX(CONCAT_WS('|', COALESCE(CAST((provider_identification_number_id) AS VARCHAR), ''), COALESCE(CAST((provider_first_name) AS VARCHAR), ''), COALESCE(CAST((NULLIF(provider_middle_name, '')) AS VARCHAR), ''), COALESCE(CAST((provider_last_name) AS VARCHAR), ''), COALESCE(CAST((TRIM(CONCAT_WS(' ', provider_first_name, NULLIF(provider_middle_name, ''), provider_last_name))) AS VARCHAR), ''), COALESCE(CAST((provider_tax_id) AS VARCHAR), ''), COALESCE(CAST((provider_tax_id_name) AS VARCHAR), '')), 256)) NOT IN (
      SELECT _hash_diff FROM {{ this }} WHERE hash_key = bronze.hash_key
  )
{% endif %}
