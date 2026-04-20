-- Silver: SAT_PROVIDER_INFO — descriptive attrs of each provider, historized.

{{ config(materialized='incremental', incremental_strategy='append') }}

WITH source AS (
    SELECT
        {{ dv_hash_key(['npi']) }} AS hub_provider_hk,
        _load_dt AS load_dts,
        {{ dv_hash_diff([
            'provider_name', 'entity_type', 'specialty_code',
            'tin', 'network_status', 'address_line1', 'city',
            'state', 'license_state'
        ]) }} AS hash_diff,
        provider_name,
        entity_type,
        specialty_code,
        tin,
        network_status,
        address_line1,
        city,
        state,
        license_state,
        _record_source AS record_source,
        ROW_NUMBER() OVER (
            PARTITION BY npi, _load_dt
            ORDER BY _source_file, _batch_id
        ) AS _rn
    FROM {{ source('bronze', 'raw_provider') }}
    WHERE npi IS NOT NULL
)

SELECT
    hub_provider_hk,
    load_dts,
    hash_diff,
    provider_name,
    entity_type,
    specialty_code,
    tin,
    network_status,
    address_line1,
    city,
    state,
    license_state,
    record_source
FROM source
WHERE _rn = 1
{% if is_incremental() %}
  AND NOT EXISTS (
      SELECT 1
      FROM (
          SELECT hub_provider_hk, hash_diff,
                 ROW_NUMBER() OVER (PARTITION BY hub_provider_hk ORDER BY load_dts DESC) AS latest_rn
          FROM {{ this }}
      ) t
      WHERE t.hub_provider_hk = source.hub_provider_hk
        AND t.hash_diff = source.hash_diff
        AND t.latest_rn = 1
  )
{% endif %}
