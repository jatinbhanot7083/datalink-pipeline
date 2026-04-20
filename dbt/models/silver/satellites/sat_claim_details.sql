-- Silver: SAT_CLAIM_DETAILS — descriptive attrs of each claim, historized.
-- INSERT-ONLY. A new row lands only when hash_diff changes for a given hub_claim_hk
-- (SCD Type 2 via load_dts).

{{ config(materialized='incremental', incremental_strategy='append') }}

WITH source AS (
    SELECT
        {{ dv_hash_key(['claim_id']) }} AS hub_claim_hk,
        _load_dt AS load_dts,
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
        -- Dedupe: for the same (hub_hk, load_dts), keep one row deterministically.
        ROW_NUMBER() OVER (
            PARTITION BY claim_id, _load_dt
            ORDER BY _source_file, _batch_id
        ) AS _rn
    FROM {{ source('bronze', 'raw_claims') }}
    WHERE claim_id IS NOT NULL
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
    record_source
FROM source
WHERE _rn = 1
{% if is_incremental() %}
  -- Skip rows whose hash_diff is already the latest-known for this hub_hk.
  AND NOT EXISTS (
      SELECT 1
      FROM (
          SELECT hub_claim_hk, hash_diff,
                 ROW_NUMBER() OVER (PARTITION BY hub_claim_hk ORDER BY load_dts DESC) AS latest_rn
          FROM {{ this }}
      ) t
      WHERE t.hub_claim_hk = source.hub_claim_hk
        AND t.hash_diff = source.hash_diff
        AND t.latest_rn = 1
  )
{% endif %}
