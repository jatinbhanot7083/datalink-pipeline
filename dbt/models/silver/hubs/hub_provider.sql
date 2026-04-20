-- Silver: HUB_PROVIDER — business key (NPI).

{{ config(materialized='incremental', incremental_strategy='append') }}

WITH candidates AS (
    SELECT
        npi,
        MIN(_load_dt) AS first_seen_dts,
        MIN(_record_source) AS first_record_source
    FROM {{ source('bronze', 'raw_provider') }}
    WHERE npi IS NOT NULL
    GROUP BY npi
)

SELECT
    {{ dv_hash_key(['npi']) }} AS hub_provider_hk,
    npi,
    first_seen_dts AS load_dts,
    first_record_source AS record_source
FROM candidates
{% if is_incremental() %}
WHERE npi NOT IN (SELECT npi FROM {{ this }})
{% endif %}
