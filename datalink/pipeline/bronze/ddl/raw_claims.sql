-- Bronze: RAW_CLAIMS
--
-- Source: EDI 837P / 837I (Professional / Institutional) or CSV export.
-- All columns VARCHAR at Bronze (no type coercion — per Medallion doc §3.1).
-- Dialect: DDL is shared across DuckDB (local) and Snowflake (prod) — both
-- accept VARCHAR, NUMBER, DATE, TIMESTAMP. Schema qualifier is resolved at
-- runtime by the warehouse adapter.
--
-- Pipeline-added metadata (always last four columns, populated on load):
--   _load_dt        TIMESTAMP  — when this row was MERGE'd
--   _source_file    VARCHAR    — filename this row came from
--   _batch_id       VARCHAR    — one per pipeline run
--   _record_source  VARCHAR    — upstream system identifier (UHC_SFTP, etc.)

CREATE TABLE IF NOT EXISTS {schema}.RAW_CLAIMS (
    claim_id          VARCHAR,
    member_id         VARCHAR,
    provider_npi      VARCHAR,
    cpt_code          VARCHAR,
    icd10_primary     VARCHAR,
    icd10_secondary   VARCHAR,
    service_date      DATE,
    billed_amount     DECIMAL(18, 2),  -- DECIMAL is standard SQL; works in DuckDB AND Snowflake (Snowflake aliases NUMBER → DECIMAL)
    claim_status      VARCHAR,
    plan_id           VARCHAR,
    prior_auth_ref    VARCHAR,
    _load_dt          TIMESTAMP,
    _source_file      VARCHAR,
    _batch_id         VARCHAR,
    _record_source    VARCHAR,
    PRIMARY KEY (claim_id)
);
