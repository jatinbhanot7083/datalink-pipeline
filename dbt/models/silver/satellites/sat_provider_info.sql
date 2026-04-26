-- Silver: SAT_PROVIDER_INFO
--
-- Phase 9.2: SCD Type 2 + soft-delete on FULL Bronze batches. Provider
-- networks shift over time — providers leave, new ones join, addresses
-- change. ``is_active=TRUE`` returns the current network. When a FULL
-- provider registry file lands and a previously-known NPI is missing,
-- the soft-delete logic flips that provider's latest Sat row to
-- ``is_active=FALSE``.
--
-- Materialization is ``table`` (full refresh). See sat_member_demographics
-- header for the rationale.

{{ config(materialized='table') }}

WITH source AS (
    SELECT
        {{ dv_hash_key(['npi']) }} AS hub_provider_hk,
        npi,
        _load_dt AS load_dts,
        _batch_id,
        _load_type,
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
),

true_changes AS (
    SELECT
        hub_provider_hk, npi, load_dts, _batch_id, _load_type,
        hash_diff, provider_name, entity_type, specialty_code, tin,
        network_status, address_line1, city, state, license_state,
        record_source
    FROM source
    WHERE _rn = 1
    QUALIFY hash_diff IS DISTINCT FROM
        LAG(hash_diff) OVER (PARTITION BY hub_provider_hk ORDER BY load_dts)
),

with_scd2_window AS (
    SELECT *,
        load_dts AS effective_start_date,
        LEAD(load_dts) OVER (PARTITION BY hub_provider_hk ORDER BY load_dts) AS effective_end_date,
        (LEAD(load_dts) OVER (PARTITION BY hub_provider_hk ORDER BY load_dts) IS NULL)
            AS is_latest_version
    FROM true_changes
),

latest_full_batch AS (
    -- The most recent FULL Bronze batch's NPIs. Empty if no FULL has
    -- landed (incremental-only world). Drives soft-delete.
    SELECT DISTINCT npi
    FROM {{ source('bronze', 'raw_provider') }}
    WHERE _batch_id = (
        SELECT _batch_id
        FROM {{ source('bronze', 'raw_provider') }}
        WHERE UPPER(_load_type) = 'FULL'
        ORDER BY _load_dt DESC
        LIMIT 1
    )
)

SELECT
    s.hub_provider_hk,
    s.load_dts,
    s.hash_diff,
    s.provider_name,
    s.entity_type,
    s.specialty_code,
    s.tin,
    s.network_status,
    s.address_line1,
    s.city,
    s.state,
    s.license_state,
    s.record_source,
    s.effective_start_date,
    s.effective_end_date,
    CASE
        WHEN NOT s.is_latest_version
            THEN FALSE
        WHEN (SELECT COUNT(*) FROM latest_full_batch) > 0
             AND NOT EXISTS (
                 SELECT 1 FROM latest_full_batch lfb WHERE lfb.npi = s.npi
             )
            THEN FALSE  -- soft-deleted: missing from latest FULL batch
        ELSE TRUE
    END AS is_active
FROM with_scd2_window s
