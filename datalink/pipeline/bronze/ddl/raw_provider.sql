-- Bronze: RAW_PROVIDER
-- Source: CMS NPPES NPI Registry (weekly full file or real-time FHIR R4 Practitioner).
-- MERGE key: NPI (10 digits, Luhn-validated at Silver).

CREATE TABLE IF NOT EXISTS {schema}.RAW_PROVIDER (
    npi              VARCHAR,
    provider_name    VARCHAR,
    entity_type      VARCHAR(1),
    specialty_code   VARCHAR,
    tin              VARCHAR,
    network_status   VARCHAR,
    address_line1    VARCHAR,
    city             VARCHAR,
    state            VARCHAR(2),
    license_state    VARCHAR(2),
    _load_dt         TIMESTAMP,
    _source_file     VARCHAR,
    _batch_id        VARCHAR,
    _record_source   VARCHAR,
    PRIMARY KEY (npi)
);
