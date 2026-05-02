-- Phase 15 Pipeline Architect — Silver dbt model
-- Source : BRONZE_AETNA.raw_membership  (bronze_anchor=FLAT_FILE)
-- Target : SILVER_AETNA.membership_clean
-- Pattern: NORMALIZED (TRY_CAST + required-not-null filter)

{{ config(
    materialized = 'incremental',
    unique_key   = '_record_hash',
    on_schema_change = 'fail'
) }}

SELECT
    TRY_CAST(payer_name AS VARCHAR)  AS payer_name,
    TRY_CAST(member_card_id AS VARCHAR)  AS member_card_id,
    TRY_CAST(member_medicare_id AS VARCHAR)  AS member_medicare_id,
    TRY_CAST(member_medicaid_id AS VARCHAR)  AS member_medicaid_id,
    TRY_CAST(member_first_name AS VARCHAR)  AS member_first_name,
    TRY_CAST(member_middle_name AS VARCHAR)  AS member_middle_name,
    TRY_CAST(member_last_name AS VARCHAR)  AS member_last_name,
    TRY_CAST(member_gender AS VARCHAR)  AS member_gender,
    TRY_CAST(member_birth_date AS DATE)  AS member_birth_date,
    TRY_CAST(member_phone AS VARCHAR)  AS member_phone,
    TRY_CAST(member_street_address_line1 AS VARCHAR)  AS member_street_address_line1,
    TRY_CAST(member_street_address_line2 AS VARCHAR)  AS member_street_address_line2,
    TRY_CAST(member_county_code AS VARCHAR)  AS member_county_code,
    TRY_CAST(member_county_name AS VARCHAR)  AS member_county_name,
    TRY_CAST(member_city AS VARCHAR)  AS member_city,
    TRY_CAST(member_state AS VARCHAR)  AS member_state,
    TRY_CAST(member_zip_code AS VARCHAR)  AS member_zip_code,
    TRY_CAST(member_active_status AS VARCHAR)  AS member_active_status,
    TRY_CAST(member_relationship_code AS VARCHAR)  AS member_relationship_code,
    TRY_CAST(member_medicaid_indicator AS BOOLEAN)  AS member_medicaid_indicator,
    TRY_CAST(member_medicaid_dual_eligibility_indicator AS BOOLEAN)  AS member_medicaid_dual_eligibility_indicator,
    TRY_CAST(member_dual_eligibility_begin_date AS DATE)  AS member_dual_eligibility_begin_date,
    TRY_CAST(member_dual_eligibility_end_date AS DATE)  AS member_dual_eligibility_end_date,
    TRY_CAST(member_coverage_effective_date AS DATE)  AS member_coverage_effective_date,
    TRY_CAST(member_coverage_end_date AS DATE)  AS member_coverage_end_date,
    TRY_CAST(member_provider_pcp_begin_date AS DATE)  AS member_provider_pcp_begin_date,
    TRY_CAST(member_health_plan_begin_date AS DATE)  AS member_health_plan_begin_date,
    TRY_CAST(member_enrollment_date AS DATE)  AS member_enrollment_date,
    TRY_CAST(member_line_of_business AS VARCHAR)  AS member_line_of_business,
    TRY_CAST(member_product AS VARCHAR)  AS member_product,
    TRY_CAST(product_description AS VARCHAR)  AS product_description,
    TRY_CAST(plan_benenfit_package_id AS INTEGER)  AS plan_benenfit_package_id,
    TRY_CAST(plan_benefit_package_name AS VARCHAR)  AS plan_benefit_package_name,
    TRY_CAST(segment_id_plan_benefit_package AS INTEGER)  AS segment_id_plan_benefit_package,
    TRY_CAST(member_product_begin_date AS DATE)  AS member_product_begin_date,
    TRY_CAST(managed_care_organization_contract_number AS VARCHAR)  AS managed_care_organization_contract_number,
    TRY_CAST(attributed_provider_id AS VARCHAR)  AS attributed_provider_id,
    TRY_CAST(atrributed_provider_npi AS VARCHAR)  AS atrributed_provider_npi,
    TRY_CAST(attributed_provider_contract_id AS DECIMAL(20, 4))  AS attributed_provider_contract_id,
    TRY_CAST(plan_benefit_package_id AS VARCHAR)  AS plan_benefit_package_id,
    TRY_CAST(refresh_date AS DATE)  AS refresh_date,
    _load_dt,
    _batch_id,
    _record_source,
    _record_hash
FROM {{ source('bronze_aetna', 'raw_membership') }}
WHERE payer_name IS NOT NULL AND member_card_id IS NOT NULL AND member_medicare_id IS NOT NULL AND member_medicaid_id IS NOT NULL AND member_first_name IS NOT NULL AND member_middle_name IS NOT NULL AND member_last_name IS NOT NULL AND member_gender IS NOT NULL  /* truncated to first 8 NOT NULL guards; full set in GX suite */
{% if is_incremental() %}
  AND _load_dt > (SELECT COALESCE(MAX(_load_dt), '1900-01-01') FROM {{ this }})
{% endif %}
