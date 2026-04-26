-- Gold: GOLD_PATIENT_AUTH — one row per authorization.
-- Source: Silver Hub/Sat/Link — joins claim, member, provider to build the
-- authorization header that feeds [UM].[PatientAuth] in SQL Server.
--
-- Scope: we derive a PatientAuth row for every claim that had a prior_auth_ref
-- (≈40% of our 10k synthetic claims). Real UM systems would have separate
-- auth-request feeds; this mapping is good enough for the Phase 4 demo.
--
-- Materialization: table (full refresh). IDs are generated via ROW_NUMBER()
-- so they are stable within a run but regenerated on each refresh — fine
-- for push-to-ops where the target does a natural-key MERGE on AuthCode.

{{ config(materialized='table', schema='gold_um') }}

WITH claims_with_auth AS (
    SELECT
        sc.hub_claim_hk,
        sc.claim_id,
        scd.service_date,
        scd.cpt_code,
        scd.claim_status,
        scd.prior_auth_ref,
        scd.billed_amount
    FROM {{ ref('hub_claim') }} sc
    JOIN {{ ref('sat_claim_details') }} scd USING (hub_claim_hk)
    WHERE scd.prior_auth_ref IS NOT NULL AND scd.prior_auth_ref != ''
),
-- Derive auth type from CPT code using simple buckets.
-- In prod this mapping lives in LuProcedureCodeMapping.
typed AS (
    SELECT
        c.*,
        lcm.link_claim_member_hk,
        lcp.link_claim_provider_hk,
        hm.hub_member_hk,
        hp.hub_provider_hk,
        hm.member_id,
        hm.plan_id,
        hp.npi,
        CASE
            WHEN c.cpt_code LIKE '2744%' THEN 1  -- INPATIENT (surgery)
            WHEN c.cpt_code LIKE '9949%' THEN 5  -- BH (care mgmt proxy)
            WHEN c.cpt_code LIKE 'J____' THEN 3  -- PHARMACY (J codes)
            ELSE 2                                -- OUTPATIENT default
        END AS lu_auth_type_id,
        CASE
            WHEN c.claim_status = 'APPROVED' THEN 2  -- CLOSED
            WHEN c.claim_status = 'DENIED'   THEN 2  -- CLOSED
            WHEN c.claim_status = 'SUBMITTED' THEN 1 -- OPEN
            WHEN c.claim_status = 'PENDED'   THEN 5  -- PENDED
            ELSE 1
        END AS lu_auth_status_id
    FROM claims_with_auth c
    LEFT JOIN {{ ref('link_claim_member') }} lcm ON lcm.hub_claim_hk = c.hub_claim_hk
    LEFT JOIN {{ ref('hub_member') }} hm ON hm.hub_member_hk = lcm.hub_member_hk
    LEFT JOIN {{ ref('link_claim_provider') }} lcp ON lcp.hub_claim_hk = c.hub_claim_hk
    LEFT JOIN {{ ref('hub_provider') }} hp ON hp.hub_provider_hk = lcp.hub_provider_hk
)

SELECT
    -- Surrogate PK — real UM would use IDENTITY/SEQUENCE; ROW_NUMBER() is stable in a single run.
    CAST(ROW_NUMBER() OVER (ORDER BY prior_auth_ref) AS INTEGER) AS patient_auth_id,
    prior_auth_ref                     AS auth_code,
    member_id                          AS patient_id_text,      -- VARCHAR member_id in Silver; prod casts to INT
    plan_id                            AS hierarchy_id,
    lu_auth_type_id                    AS auth_type_id,
    lu_auth_status_id                  AS auth_status_id,
    service_date                       AS auth_from_date,
    DATEADD(HOUR, 72, service_date)    AS auth_due_date,        -- 72h TAT per UM-Gold-v2; DATEADD works on both DuckDB + Snowflake
    npi                                AS provider_npi,
    billed_amount                      AS requested_amount,
    claim_id                           AS source_claim_id,
    CURRENT_TIMESTAMP                  AS created_on,
    'SILVER.DV2.0'                     AS record_source
FROM typed
