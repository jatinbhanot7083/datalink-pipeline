-- Phase 15 Pipeline Architect — Gold dbt model
-- Source : SILVER_AETNA.provider_clean
-- Target : GOLD_AETNA.provider
-- Pattern: latest-per-business-key snapshot (UM_OPERATIONAL)

{{ config(
    materialized = 'table',
    cluster_by   = ['_record_hash']
) }}

WITH ranked AS (
    SELECT
        national_provider_identifier_npi,
        provider_identification_number_id,
        provider_first_name,
        provider_middle_name,
        provider_last_name,
        provider_tax_id,
        provider_tax_id_name,
        provider_primary_servicing_address_line_1,
        provider_primary_servicing_address_line_2,
        provider_primary_servicing_address_city,
        provider_primary_servicing_address_state,
        provider_primary_servicing_address_zip_code,
        provider_contracted_entity_name,
        refresh_date,
        _load_dt,
        _batch_id,
        _record_source,
        _record_hash,
        ROW_NUMBER() OVER (
            PARTITION BY _record_hash
            ORDER BY _load_dt DESC, _record_hash DESC
        ) AS _rn
    FROM {{ source('silver_aetna', 'provider_clean') }}
)
SELECT
    national_provider_identifier_npi,
        provider_identification_number_id,
        provider_first_name,
        provider_middle_name,
        provider_last_name,
        provider_tax_id,
        provider_tax_id_name,
        provider_primary_servicing_address_line_1,
        provider_primary_servicing_address_line_2,
        provider_primary_servicing_address_city,
        provider_primary_servicing_address_state,
        provider_primary_servicing_address_zip_code,
        provider_contracted_entity_name,
        refresh_date,
    _load_dt,
    _batch_id,
    _record_source,
    _record_hash,
    _batch_id              AS _silver_run_id,
    CURRENT_TIMESTAMP()    AS _gold_effective_dt
FROM ranked
WHERE _rn = 1
