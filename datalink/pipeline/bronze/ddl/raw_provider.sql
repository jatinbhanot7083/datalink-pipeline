-- Bronze: RAW_PROVIDER
-- Source: CMS NPPES NPI Registry (weekly full or real-time FHIR R4 Practitioner).
--
-- Phase 9.1: Bronze is now an APPEND-ONLY ledger. The natural-key PK (npi)
-- was removed because the same provider can legitimately appear in
-- multiple batches. Silver Hub dedups via
-- QUALIFY ROW_NUMBER OVER(PARTITION BY npi ORDER BY _load_dt DESC) = 1.
--
-- 7 audit columns enforced (order matters):
--   _load_dt / _source_file / _batch_id / _record_source / _load_type /
--   _file_row_number / _record_hash

CREATE TABLE IF NOT EXISTS {schema}.RAW_PROVIDER (
    npi                VARCHAR,
    provider_name      VARCHAR,
    entity_type        VARCHAR(1),
    specialty_code     VARCHAR,
    tin                VARCHAR,
    network_status     VARCHAR,
    address_line1      VARCHAR,
    city               VARCHAR,
    state              VARCHAR(2),
    license_state      VARCHAR(2),
    _load_dt           TIMESTAMP,
    _source_file       VARCHAR,
    _batch_id          VARCHAR,
    _record_source     VARCHAR,
    _load_type         VARCHAR,
    _file_row_number   BIGINT,
    _record_hash       VARCHAR
);
