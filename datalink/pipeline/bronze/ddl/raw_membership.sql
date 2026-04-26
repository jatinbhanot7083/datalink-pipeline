-- Bronze: RAW_MEMBERSHIP
-- Source: EDI 834 (Enrollment) or monthly/delta CSV.
--
-- Phase 9.1: Bronze is now an APPEND-ONLY ledger. The natural-key PK was
-- removed because the same (member_id, plan_id) can legitimately appear in
-- multiple batches (e.g. a Sunday full + 6 weekday incrementals + next
-- Sunday full). Silver Hub dedups via:
--   QUALIFY ROW_NUMBER() OVER (PARTITION BY member_id, plan_id
--                              ORDER BY _load_dt DESC) = 1
--
-- 7 audit columns enforced (order matters — the ingest pipeline asserts
-- the trailing 7 cols match this exact list, lower-case):
--   _load_dt           CURRENT_TIMESTAMP at batch ingest
--   _source_file       original SFTP path
--   _batch_id          Airflow run_id (uniquely identifies a batch)
--   _record_source     upstream system tag
--   _load_type         'FULL' | 'INCREMENTAL' | 'UNKNOWN'  (Phase 9.1)
--   _file_row_number   1-based row position within source file (Phase 9.1)
--   _record_hash       MD5(CONCAT_WS('|', business cols))   (Phase 9.1)

CREATE TABLE IF NOT EXISTS {schema}.RAW_MEMBERSHIP (
    member_id          VARCHAR,
    subscriber_id      VARCHAR,
    dob                DATE,
    gender             VARCHAR,
    plan_id            VARCHAR,
    group_id           VARCHAR,
    effective_date     DATE,
    termination_date   DATE,
    coverage_type      VARCHAR,
    state              VARCHAR(2),
    _load_dt           TIMESTAMP,
    _source_file       VARCHAR,
    _batch_id          VARCHAR,
    _record_source     VARCHAR,
    _load_type         VARCHAR,
    _file_row_number   BIGINT,
    _record_hash       VARCHAR
);
