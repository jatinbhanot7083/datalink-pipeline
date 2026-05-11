-- Phase 15.7 DV2 Silver — Satellite
-- Sat       : SAT_MEMBER_ENROLLMENT
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
    FROM {{ source('bronze', 'raw_membership') }}
)
SELECT
    hash_key,
        TRY_CAST(CAST((payer_name) AS VARCHAR) AS VARCHAR) AS payer_name,
        TRY_CAST(CAST((member_line_of_business) AS VARCHAR) AS VARCHAR) AS line_of_business,
        TRY_CAST(CAST((member_product) AS VARCHAR) AS VARCHAR) AS product_code,
        TRY_CAST(CAST((product_description) AS VARCHAR) AS VARCHAR) AS product_description,
        TRY_CAST(CAST((member_product_begin_date) AS VARCHAR) AS DATE) AS product_effective_from,
        TRY_CAST(CAST((member_enrollment_date) AS VARCHAR) AS DATE) AS enrollment_date,
        TRY_CAST(CAST((member_health_plan_begin_date) AS VARCHAR) AS DATE) AS health_plan_effective_from,
    _load_dt,
    _record_source,
    _batch_id,
    SHA2_HEX(CONCAT_WS('|', COALESCE(CAST((payer_name) AS VARCHAR), ''), COALESCE(CAST((member_line_of_business) AS VARCHAR), ''), COALESCE(CAST((member_product) AS VARCHAR), ''), COALESCE(CAST((product_description) AS VARCHAR), ''), COALESCE(CAST((member_product_begin_date) AS VARCHAR), ''), COALESCE(CAST((member_enrollment_date) AS VARCHAR), ''), COALESCE(CAST((member_health_plan_begin_date) AS VARCHAR), '')), 256) AS _hash_diff,
    -- Phase 17.2: propagate vendor-overflow JSON from Bronze. Default
    -- behaviour is pass-through; clients who want to filter or rename
    -- specific keys should override this dbt model.
    _extra AS _extensions
FROM bronze
WHERE member_card_id IS NOT NULL
{% if is_incremental() %}
  AND (SHA2_HEX(CONCAT_WS('|', COALESCE(CAST((payer_name) AS VARCHAR), ''), COALESCE(CAST((member_line_of_business) AS VARCHAR), ''), COALESCE(CAST((member_product) AS VARCHAR), ''), COALESCE(CAST((product_description) AS VARCHAR), ''), COALESCE(CAST((member_product_begin_date) AS VARCHAR), ''), COALESCE(CAST((member_enrollment_date) AS VARCHAR), ''), COALESCE(CAST((member_health_plan_begin_date) AS VARCHAR), '')), 256)) NOT IN (
      SELECT _hash_diff FROM {{ this }} WHERE hash_key = bronze.hash_key
  )
{% endif %}
