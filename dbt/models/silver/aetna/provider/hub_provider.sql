-- Phase 15.7 DV2 Silver — Hub
-- Hub      : HUB_PROVIDER
-- Dataset  : provider
-- Client   : aetna
-- Source   : BRONZE_AETNA.raw_provider
-- Pattern  : Hub (immutable business-key registry)

{{ config(
    materialized = 'incremental',
    unique_key   = 'hash_key',
    on_schema_change = 'fail',
    tags = ['datalink', 'phase15', 'dv2', 'silver', 'hub', 'aetna', 'provider']
) }}

SELECT
    SHA2_HEX(CONCAT_WS('|', COALESCE(CAST(national_provider_identifier_npi AS VARCHAR), '')), 256) AS hash_key,
    CAST(national_provider_identifier_npi AS VARCHAR) AS provider_npi,
    _load_dt,
    _record_source
FROM {{ source('bronze', 'raw_provider') }}
WHERE national_provider_identifier_npi IS NOT NULL
{% if is_incremental() %}
  AND SHA2_HEX(CONCAT_WS('|', COALESCE(CAST(national_provider_identifier_npi AS VARCHAR), '')), 256) NOT IN (SELECT hash_key FROM {{ this }})
{% endif %}
GROUP BY hash_key, provider_npi, _load_dt, _record_source
