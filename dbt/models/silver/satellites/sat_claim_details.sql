-- Silver: SAT_CLAIM_DETAILS
--
-- Phase 9.2: SCD Type 2 with explicit ``effective_start_date`` /
-- ``effective_end_date`` / ``is_active`` columns. Claims are
-- transactional events, so the SCD2 here tracks status evolution
-- (SUBMITTED → APPROVED/DENIED/PENDED) and any other attribute changes.
--
-- ``is_active = TRUE`` means "this is the most recent version of this
-- claim's record we have observed". Soft-delete on FULL batch is NOT
-- implemented for claims because claims are events, not state — once a
-- claim exists it does not "disappear" from the source. (If a payer
-- somehow REVOKES a claim filing, that's a separate UM-layer concern,
-- handled in Gold.)
--
-- Materialization is ``table`` (full refresh). See sat_member_demographics
-- header for the rationale on accepting rebuild cost in exchange for clean
-- single-pass SCD2 semantics.

{{ config(materialized='table') }}

WITH source AS (
    SELECT
        {{ dv_hash_key(['claim_id']) }} AS hub_claim_hk,
        claim_id,
        _load_dt AS load_dts,
        _batch_id,
        {{ dv_hash_diff([
            'cpt_code', 'icd10_primary', 'icd10_secondary',
            'billed_amount', 'claim_status', 'prior_auth_ref', 'service_date'
        ]) }} AS hash_diff,
        cpt_code,
        icd10_primary,
        icd10_secondary,
        service_date,
        billed_amount,
        claim_status,
        prior_auth_ref,
        _record_source AS record_source,
        ROW_NUMBER() OVER (
            PARTITION BY claim_id, _load_dt
            ORDER BY _source_file, _batch_id
        ) AS _rn
    FROM {{ source('bronze', 'raw_claims') }}
    WHERE claim_id IS NOT NULL
),

true_changes AS (
    SELECT
        hub_claim_hk, claim_id, load_dts, _batch_id, hash_diff,
        cpt_code, icd10_primary, icd10_secondary, service_date,
        billed_amount, claim_status, prior_auth_ref, record_source
    FROM source
    WHERE _rn = 1
    QUALIFY hash_diff IS DISTINCT FROM
        LAG(hash_diff) OVER (PARTITION BY hub_claim_hk ORDER BY load_dts)
)

SELECT
    hub_claim_hk,
    load_dts,
    hash_diff,
    cpt_code,
    icd10_primary,
    icd10_secondary,
    service_date,
    billed_amount,
    claim_status,
    prior_auth_ref,
    record_source,
    load_dts AS effective_start_date,
    LEAD(load_dts) OVER (PARTITION BY hub_claim_hk ORDER BY load_dts) AS effective_end_date,
    (LEAD(load_dts) OVER (PARTITION BY hub_claim_hk ORDER BY load_dts) IS NULL)
        AS is_active
FROM true_changes
