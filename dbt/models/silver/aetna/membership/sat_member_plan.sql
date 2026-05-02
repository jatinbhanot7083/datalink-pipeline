-- Phase 15.7 DV2 Silver — Satellite
-- Sat       : SAT_MEMBER_PLAN
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
        TRY_CAST((payer_name) AS VARCHAR) AS payer_name,
        TRY_CAST((CASE UPPER(member_line_of_business) WHEN 'HMO' THEN 'HMO' WHEN 'PPO' THEN 'PPO' WHEN 'POS' THEN 'POS' WHEN 'INDEMNITY' THEN 'INDEMNITY' WHEN 'HDHP' THEN 'HDHP' ELSE UPPER(member_line_of_business) END) AS VARCHAR) AS line_of_business,
        TRY_CAST((member_product) AS VARCHAR) AS product_code,
        TRY_CAST((NULLIF(product_description, '')) AS VARCHAR) AS product_description,
        TRY_CAST((member_product_begin_date) AS DATE) AS product_effective_date,
        TRY_CAST((CAST(plan_benefit_package_id AS TEXT)) AS VARCHAR) AS plan_benefit_package_id,
        TRY_CAST((plan_benefit_package_name) AS VARCHAR) AS plan_benefit_package_name,
        TRY_CAST((NULLIF(segment_id_plan_benefit_package, 0)) AS INTEGER) AS segment_id,
        TRY_CAST((member_health_plan_begin_date) AS DATE) AS health_plan_effective_date,
        TRY_CAST((NULLIF(member_enrollment_date, '1900-01-01')) AS DATE) AS enrollment_date,
    _load_dt,
    _record_source,
    _batch_id,
    SHA2_HEX(CONCAT_WS('|', COALESCE(CAST((payer_name) AS VARCHAR), ''), COALESCE(CAST((CASE UPPER(member_line_of_business) WHEN 'HMO' THEN 'HMO' WHEN 'PPO' THEN 'PPO' WHEN 'POS' THEN 'POS' WHEN 'INDEMNITY' THEN 'INDEMNITY' WHEN 'HDHP' THEN 'HDHP' ELSE UPPER(member_line_of_business) END) AS VARCHAR), ''), COALESCE(CAST((member_product) AS VARCHAR), ''), COALESCE(CAST((NULLIF(product_description, '')) AS VARCHAR), ''), COALESCE(CAST((member_product_begin_date) AS VARCHAR), ''), COALESCE(CAST((CAST(plan_benefit_package_id AS TEXT)) AS VARCHAR), ''), COALESCE(CAST((plan_benefit_package_name) AS VARCHAR), ''), COALESCE(CAST((NULLIF(segment_id_plan_benefit_package, 0)) AS VARCHAR), ''), COALESCE(CAST((member_health_plan_begin_date) AS VARCHAR), ''), COALESCE(CAST((NULLIF(member_enrollment_date, '1900-01-01')) AS VARCHAR), '')), 256) AS _hash_diff
FROM bronze
WHERE member_card_id IS NOT NULL
{% if is_incremental() %}
  AND (SHA2_HEX(CONCAT_WS('|', COALESCE(CAST((payer_name) AS VARCHAR), ''), COALESCE(CAST((CASE UPPER(member_line_of_business) WHEN 'HMO' THEN 'HMO' WHEN 'PPO' THEN 'PPO' WHEN 'POS' THEN 'POS' WHEN 'INDEMNITY' THEN 'INDEMNITY' WHEN 'HDHP' THEN 'HDHP' ELSE UPPER(member_line_of_business) END) AS VARCHAR), ''), COALESCE(CAST((member_product) AS VARCHAR), ''), COALESCE(CAST((NULLIF(product_description, '')) AS VARCHAR), ''), COALESCE(CAST((member_product_begin_date) AS VARCHAR), ''), COALESCE(CAST((CAST(plan_benefit_package_id AS TEXT)) AS VARCHAR), ''), COALESCE(CAST((plan_benefit_package_name) AS VARCHAR), ''), COALESCE(CAST((NULLIF(segment_id_plan_benefit_package, 0)) AS VARCHAR), ''), COALESCE(CAST((member_health_plan_begin_date) AS VARCHAR), ''), COALESCE(CAST((NULLIF(member_enrollment_date, '1900-01-01')) AS VARCHAR), '')), 256)) NOT IN (
      SELECT _hash_diff FROM {{ this }} WHERE hash_key = bronze.hash_key
  )
{% endif %}
