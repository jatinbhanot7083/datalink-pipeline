-- Gold OUTBOX: outbox_gold_patient_auth (Phase 9.3)
--
-- Computes the DELTA payload to push to On-Prem this run, vs whatever
-- was last successfully pushed. The orchestration layer
-- (datalink.pipeline.router.outbox_egress) reads this table, ships rows
-- across the network to SQL Server / Postgres, then records the outcome
-- in CONTROL.egress_batch_log.
--
-- Why an outbox (not direct push):
--   1. Every PatientAuth row that crossed the network is queryable in
--      Snowflake forever — perfect audit trail.
--   2. Network outage halfway through a push? Rerun = idempotent. The
--      outbox knows which rows are still PENDING vs PUSHED.
--   3. On-Prem schema can be upgraded independently — outbox is the
--      contract between Snowflake and the operational DBs.
--
-- Egress action vocabulary:
--   INSERT     — net-new auth_code; never seen on On-Prem before
--   UPDATE     — auth_code exists on On-Prem but Silver carries newer
--                attribute values (status, due_date, provider, etc.)
--   DEACTIVATE — auth_code was active on On-Prem; Silver now has it
--                marked inactive (e.g. claim_status flipped to DENIED).
--
-- Watermark mechanics:
--   The outbox includes EVERY row whose source Silver `effective_start_date`
--   is greater than the most recent cutoff_dts in CONTROL.egress_batch_log
--   (per client_id, entity='gold_patient_auth'). On Day 1 (no prior log)
--   that cutoff is 1900-01-01 → every row is INSERT-fresh.

{{ config(materialized='table', schema='gold_um') }}

{% set log_table = 'CONTROL.egress_batch_log' %}

WITH last_cutoff AS (
    -- Most recent successful watermark for this entity. NULL on first run
    -- → coalesce to a far-past sentinel so every row qualifies.
    SELECT COALESCE(
        MAX(cutoff_dts),
        CAST('1900-01-01' AS TIMESTAMP)
    ) AS cutoff_dts
    FROM {{ log_table }}
    WHERE entity = 'gold_patient_auth'
      AND status = 'PUSHED'
),

base_with_scd2 AS (
    -- Read the gold model + its primary Silver source (sat_claim_details)
    -- so we can pick up effective_start_date and is_active for delta
    -- computation. PatientAuth keys off claim_id → join via Silver Hub.
    SELECT
        gpa.*,
        scd.effective_start_date,
        scd.effective_end_date,
        scd.is_active
    FROM {{ ref('gold_patient_auth') }} gpa
    JOIN {{ ref('hub_claim') }} hc
      ON hc.claim_id = gpa.source_claim_id
    JOIN {{ ref('sat_claim_details') }} scd
      ON scd.hub_claim_hk = hc.hub_claim_hk
)

-- Final SELECT: every target column from operational `patient_auth`
-- (so bulk_upsert receives a correctly-shaped row), PLUS outbox-audit
-- columns that the egress code strips before sending across the wire.
-- Numeric cols are explicitly CAST to the target's precision so the
-- pyodbc → SQL Server bridge can't trip on "Converting decimal loses
-- precision" — Snowflake's materialised table can otherwise widen the
-- inferred precision past DECIMAL(18,2).
SELECT
    -- Deterministic egress_batch_id per (auth_code, effective_start_date)
    -- so the same delta isn't double-pushed if the dbt model rebuilds
    -- between push attempts.
    {{ dv_hash_key(['auth_code', 'effective_start_date']) }} AS egress_batch_id,
    -- 13 target columns (matches GOLD_UM_TABLES['patient_auth'].columns):
    CAST(patient_auth_id AS INTEGER)              AS patient_auth_id,
    auth_code,
    patient_id_text,
    hierarchy_id,
    CAST(auth_type_id   AS INTEGER)               AS auth_type_id,
    CAST(auth_status_id AS INTEGER)               AS auth_status_id,
    CAST(auth_from_date AS DATE)                  AS auth_from_date,
    CAST(auth_due_date  AS TIMESTAMP)             AS auth_due_date,
    provider_npi,
    CAST(requested_amount AS DECIMAL(18, 2))      AS requested_amount,
    source_claim_id,
    CAST(created_on AS TIMESTAMP)                 AS created_on,
    record_source,
    -- Outbox-audit (stripped before sending to On-Prem):
    effective_start_date,
    CASE
        WHEN NOT is_active THEN 'DEACTIVATE'
        WHEN effective_start_date = (
            SELECT MIN(effective_start_date)
            FROM base_with_scd2 b2
            WHERE b2.auth_code = base_with_scd2.auth_code
        ) THEN 'INSERT'
        ELSE 'UPDATE'
    END AS egress_action,
    (SELECT cutoff_dts FROM last_cutoff) AS prior_cutoff_dts
FROM base_with_scd2
WHERE effective_start_date > (SELECT cutoff_dts FROM last_cutoff)
   OR (NOT is_active AND effective_end_date > (SELECT cutoff_dts FROM last_cutoff))
