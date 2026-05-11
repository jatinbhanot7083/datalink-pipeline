-- Phase 15.7 DV2 Silver — Satellite
-- Sat       : SAT_PROVIDER_ADDRESS
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
        TRY_CAST(CAST((NULLIF(provider_primary_servicing_address_line_1, '')) AS VARCHAR) AS VARCHAR) AS primary_address_line_1,
        TRY_CAST(CAST((NULLIF(provider_primary_servicing_address_line_2, '')) AS VARCHAR) AS VARCHAR) AS primary_address_line_2,
        TRY_CAST(CAST((NULLIF(provider_primary_servicing_address_city, '')) AS VARCHAR) AS VARCHAR) AS primary_address_city,
        TRY_CAST(CAST((NULLIF(UPPER(provider_primary_servicing_address_state), '')) AS VARCHAR) AS VARCHAR) AS primary_address_state,
        TRY_CAST(CAST((NULLIF(provider_primary_servicing_address_zip_code, '')) AS VARCHAR) AS VARCHAR) AS primary_address_zip_code,
    _load_dt,
    _record_source,
    _batch_id,
    SHA2_HEX(CONCAT_WS('|', COALESCE(CAST((NULLIF(provider_primary_servicing_address_line_1, '')) AS VARCHAR), ''), COALESCE(CAST((NULLIF(provider_primary_servicing_address_line_2, '')) AS VARCHAR), ''), COALESCE(CAST((NULLIF(provider_primary_servicing_address_city, '')) AS VARCHAR), ''), COALESCE(CAST((NULLIF(UPPER(provider_primary_servicing_address_state), '')) AS VARCHAR), ''), COALESCE(CAST((NULLIF(provider_primary_servicing_address_zip_code, '')) AS VARCHAR), '')), 256) AS _hash_diff,
    -- Phase 17.2: propagate vendor-overflow JSON from Bronze. Default
    -- behaviour is pass-through; clients who want to filter or rename
    -- specific keys should override this dbt model.
    _extra AS _extensions
FROM bronze
WHERE national_provider_identifier_npi IS NOT NULL
{% if is_incremental() %}
  AND (SHA2_HEX(CONCAT_WS('|', COALESCE(CAST((NULLIF(provider_primary_servicing_address_line_1, '')) AS VARCHAR), ''), COALESCE(CAST((NULLIF(provider_primary_servicing_address_line_2, '')) AS VARCHAR), ''), COALESCE(CAST((NULLIF(provider_primary_servicing_address_city, '')) AS VARCHAR), ''), COALESCE(CAST((NULLIF(UPPER(provider_primary_servicing_address_state), '')) AS VARCHAR), ''), COALESCE(CAST((NULLIF(provider_primary_servicing_address_zip_code, '')) AS VARCHAR), '')), 256)) NOT IN (
      SELECT _hash_diff FROM {{ this }} WHERE hash_key = bronze.hash_key
  )
{% endif %}
