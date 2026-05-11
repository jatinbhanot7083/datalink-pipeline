-- Phase 15.7 DV2 Silver — Link
-- Link      : LINK_MEMBER_PROVIDER
-- Dataset   : membership
-- Client    : aetna
-- Source    : BRONZE_AETNA.raw_membership
-- Hubs      : HUB_MEMBER, HUB_PROVIDER

{{ config(
    materialized = 'incremental',
    unique_key   = 'link_hash_key',
    on_schema_change = 'fail',
    tags = ['datalink', 'phase15', 'dv2', 'silver', 'link', 'aetna', 'membership']
) }}

WITH bronze AS (
    SELECT *,
        SHA2_HEX(CONCAT_WS('|', COALESCE(CAST(member_card_id AS VARCHAR), '')), 256) AS hub_member_hash_key,
        SHA2_HEX(CONCAT_WS('|', COALESCE(CAST(attributed_provider_id AS VARCHAR), '')), 256) AS hub_provider_hash_key
    FROM {{ source('bronze', 'raw_membership') }}
)
SELECT
    SHA2_HEX(CONCAT_WS('|', hub_member_hash_key, hub_provider_hash_key), 256) AS link_hash_key,
    hub_member_hash_key, hub_provider_hash_key,
    _load_dt,
    _record_source,
    _batch_id
FROM bronze
WHERE member_card_id IS NOT NULL AND attributed_provider_id IS NOT NULL
{% if is_incremental() %}
  AND SHA2_HEX(CONCAT_WS('|', hub_member_hash_key, hub_provider_hash_key), 256) NOT IN (SELECT link_hash_key FROM {{ this }})
{% endif %}
