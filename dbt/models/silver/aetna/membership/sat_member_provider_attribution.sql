-- Phase 15.7 DV2 Silver — Satellite
-- Sat       : SAT_MEMBER_PROVIDER_ATTRIBUTION
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
        TRY_CAST((NULLIF(attributed_provider_id, '')) AS VARCHAR) AS attributed_provider_id,
        TRY_CAST((NULLIF(atrributed_provider_npi, '')) AS VARCHAR) AS attributed_provider_npi,
        TRY_CAST((NULLIF(member_provider_pcp_begin_date, '1900-01-01')) AS DATE) AS attributed_provider_effective_date,
        TRY_CAST((CAST(attributed_provider_contract_id AS TEXT)) AS VARCHAR) AS attributed_provider_contract_id,
        TRY_CAST((NULLIF(managed_care_organization_contract_number, '')) AS VARCHAR) AS managed_care_organization_contract_number,
    _load_dt,
    _record_source,
    _batch_id,
    SHA2_HEX(CONCAT_WS('|', COALESCE(CAST((NULLIF(attributed_provider_id, '')) AS VARCHAR), ''), COALESCE(CAST((NULLIF(atrributed_provider_npi, '')) AS VARCHAR), ''), COALESCE(CAST((NULLIF(member_provider_pcp_begin_date, '1900-01-01')) AS VARCHAR), ''), COALESCE(CAST((CAST(attributed_provider_contract_id AS TEXT)) AS VARCHAR), ''), COALESCE(CAST((NULLIF(managed_care_organization_contract_number, '')) AS VARCHAR), '')), 256) AS _hash_diff
FROM bronze
WHERE member_card_id IS NOT NULL
{% if is_incremental() %}
  AND (SHA2_HEX(CONCAT_WS('|', COALESCE(CAST((NULLIF(attributed_provider_id, '')) AS VARCHAR), ''), COALESCE(CAST((NULLIF(atrributed_provider_npi, '')) AS VARCHAR), ''), COALESCE(CAST((NULLIF(member_provider_pcp_begin_date, '1900-01-01')) AS VARCHAR), ''), COALESCE(CAST((CAST(attributed_provider_contract_id AS TEXT)) AS VARCHAR), ''), COALESCE(CAST((NULLIF(managed_care_organization_contract_number, '')) AS VARCHAR), '')), 256)) NOT IN (
      SELECT _hash_diff FROM {{ this }} WHERE hash_key = bronze.hash_key
  )
{% endif %}
