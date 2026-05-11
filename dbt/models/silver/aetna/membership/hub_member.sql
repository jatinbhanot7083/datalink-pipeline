-- Phase 15.7 DV2 Silver — Hub
-- Hub      : HUB_MEMBER
-- Dataset  : membership
-- Client   : aetna
-- Source   : BRONZE_AETNA.raw_membership
-- Pattern  : Hub (immutable business-key registry)

{{ config(
    materialized = 'incremental',
    unique_key   = 'hash_key',
    on_schema_change = 'fail',
    tags = ['datalink', 'phase15', 'dv2', 'silver', 'hub', 'aetna', 'membership']
) }}

SELECT
    SHA2_HEX(CONCAT_WS('|', COALESCE(CAST(member_card_id AS VARCHAR), '')), 256) AS hash_key,
    CAST(member_card_id AS VARCHAR) AS member_id,
    _load_dt,
    _record_source
FROM {{ source('bronze', 'raw_membership') }}
WHERE member_card_id IS NOT NULL
{% if is_incremental() %}
  AND SHA2_HEX(CONCAT_WS('|', COALESCE(CAST(member_card_id AS VARCHAR), '')), 256) NOT IN (SELECT hash_key FROM {{ this }})
{% endif %}
GROUP BY hash_key, member_id, _load_dt, _record_source
