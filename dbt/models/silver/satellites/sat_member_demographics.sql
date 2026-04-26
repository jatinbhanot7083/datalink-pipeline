-- Silver: SAT_MEMBER_DEMOGRAPHICS
--
-- Phase 9.2: full SCD Type 2. Each row carries explicit
-- ``effective_start_date`` / ``effective_end_date`` / ``is_active`` so
-- downstream queries don't need window-function gymnastics — a simple
-- ``WHERE is_active = TRUE`` returns current truth.
--
-- Materialization is ``table`` (full refresh) on every dbt run. We accept
-- the rebuild cost (sub-second at <100k rows, sub-minute at <10M) in
-- exchange for clean SCD2 semantics computable in one SQL pass. Bronze
-- append-only (Phase 9.1) guarantees we can always reconstruct, so a
-- destructive rebuild is non-lossy by design.
--
-- ROW LIFECYCLE:
--   1. New (member_id, plan_id) appears in Bronze
--      → Sat row inserted. is_active=TRUE. effective_end_date=NULL.
--   2. Same (member_id, plan_id) re-appears with changed attributes
--      (hash_diff differs)
--      → Previous Sat row's effective_end_date set to new load_dts.
--      → New Sat row inserted with is_active=TRUE.
--   3. Same (member_id, plan_id) re-appears with IDENTICAL hash_diff
--      → No new Sat row (idempotent).
--   4. (member_id, plan_id) absent from a FULL Bronze batch
--      → Most-recent Sat row's is_active flips to FALSE (soft-delete).
--      → Soft-delete fires ONLY when ``_load_type='FULL'``.

{{ config(materialized='table') }}

WITH source AS (
    -- Read every Bronze row, dedupe per (natural_key, load_dts) so the
    -- same file dropped twice in one second doesn't cause double-Sat.
    SELECT
        {{ dv_hash_key(['member_id', 'plan_id']) }} AS hub_member_hk,
        member_id,
        plan_id,
        _load_dt AS load_dts,
        _batch_id,
        _load_type,
        {{ dv_hash_diff([
            'subscriber_id', 'dob', 'gender', 'group_id',
            'effective_date', 'termination_date', 'coverage_type', 'state'
        ]) }} AS hash_diff,
        subscriber_id,
        dob,
        gender,
        group_id,
        effective_date,
        termination_date,
        coverage_type,
        state,
        _record_source AS record_source,
        ROW_NUMBER() OVER (
            PARTITION BY member_id, plan_id, _load_dt
            ORDER BY _source_file, _batch_id
        ) AS _rn
    FROM {{ source('bronze', 'raw_membership') }}
    WHERE member_id IS NOT NULL AND plan_id IS NOT NULL
),

true_changes AS (
    -- DV 2.0 SCD: keep a Sat row only when the attribute hash changes
    -- since the previous version for the same hub. Collapses identical
    -- back-to-back batches.
    SELECT
        hub_member_hk, member_id, plan_id, load_dts, _batch_id, _load_type,
        hash_diff, subscriber_id, dob, gender, group_id, effective_date,
        termination_date, coverage_type, state, record_source
    FROM source
    WHERE _rn = 1
    QUALIFY hash_diff IS DISTINCT FROM
        LAG(hash_diff) OVER (PARTITION BY hub_member_hk ORDER BY load_dts)
),

with_scd2_window AS (
    -- effective_end_date is the load_dts of the row that supersedes this
    -- one — NULL for the most recent version per hub. is_latest_version
    -- captures the "naive" current-state flag; soft-delete may override.
    SELECT *,
        load_dts AS effective_start_date,
        LEAD(load_dts) OVER (PARTITION BY hub_member_hk ORDER BY load_dts) AS effective_end_date,
        (LEAD(load_dts) OVER (PARTITION BY hub_member_hk ORDER BY load_dts) IS NULL)
            AS is_latest_version
    FROM true_changes
),

latest_full_batch AS (
    -- The most recent FULL Bronze batch's natural keys. Empty if no FULL
    -- batch has ever landed (incremental-only world). When non-empty, we
    -- use it to detect terminations: hubs whose last-known Sat row exists
    -- but are absent from this FULL → mark inactive.
    SELECT DISTINCT member_id, plan_id
    FROM {{ source('bronze', 'raw_membership') }}
    WHERE _batch_id = (
        SELECT _batch_id
        FROM {{ source('bronze', 'raw_membership') }}
        WHERE UPPER(_load_type) = 'FULL'
        ORDER BY _load_dt DESC
        LIMIT 1
    )
)

SELECT
    s.hub_member_hk,
    s.load_dts,
    s.hash_diff,
    s.subscriber_id,
    s.dob,
    s.gender,
    s.group_id,
    s.effective_date,
    s.termination_date,
    s.coverage_type,
    s.state,
    s.record_source,
    s.effective_start_date,
    s.effective_end_date,
    -- is_active = "is this the latest version AND, if a FULL batch has
    -- landed, is the hub still present in that FULL batch?"
    CASE
        WHEN NOT s.is_latest_version
            THEN FALSE
        WHEN (SELECT COUNT(*) FROM latest_full_batch) > 0
             AND NOT EXISTS (
                 SELECT 1 FROM latest_full_batch lfb
                 WHERE lfb.member_id = s.member_id AND lfb.plan_id = s.plan_id
             )
            THEN FALSE  -- soft-deleted: missing from latest FULL batch
        ELSE TRUE
    END AS is_active
FROM with_scd2_window s
