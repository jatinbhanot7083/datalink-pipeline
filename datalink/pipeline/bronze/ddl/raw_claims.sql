-- Bronze: RAW_CLAIMS
--
-- Source: EDI 837P / 837I (Professional / Institutional) or CSV export.
-- All business columns VARCHAR at Bronze (no type coercion — per
-- Medallion doc §3.1). Dialect is shared across DuckDB and Snowflake
-- (both accept VARCHAR, NUMBER, DATE, TIMESTAMP). Schema qualifier is
-- resolved at runtime by the warehouse adapter.
--
-- Phase 9.1: Bronze is now an APPEND-ONLY ledger. The natural-key PK
-- (claim_id) was removed because the same claim can legitimately appear
-- in multiple batches across days. Silver Hub dedups via
-- QUALIFY ROW_NUMBER OVER(PARTITION BY claim_id ORDER BY _load_dt DESC)=1.
--
-- 7 audit columns enforced (order matters):
--   _load_dt           CURRENT_TIMESTAMP at batch ingest
--   _source_file       original SFTP path
--   _batch_id          Airflow run_id
--   _record_source     upstream system tag
--   _load_type         'FULL' | 'INCREMENTAL' | 'UNKNOWN'  (Phase 9.1)
--   _file_row_number   1-based row position in source file (Phase 9.1)
--   _record_hash       MD5(CONCAT_WS('|', business cols))   (Phase 9.1)

CREATE TABLE IF NOT EXISTS {schema}.RAW_CLAIMS (
    claim_id           VARCHAR,
    member_id          VARCHAR,
    provider_npi       VARCHAR,
    cpt_code           VARCHAR,
    icd10_primary      VARCHAR,
    icd10_secondary    VARCHAR,
    service_date       DATE,
    billed_amount      DECIMAL(18, 2),  -- DECIMAL standard; Snowflake aliases NUMBER → DECIMAL
    claim_status       VARCHAR,
    plan_id            VARCHAR,
    prior_auth_ref     VARCHAR,
    _load_dt           TIMESTAMP,
    _source_file       VARCHAR,
    _batch_id          VARCHAR,
    _record_source     VARCHAR,
    _load_type         VARCHAR,
    _file_row_number   BIGINT,
    _record_hash       VARCHAR
);
