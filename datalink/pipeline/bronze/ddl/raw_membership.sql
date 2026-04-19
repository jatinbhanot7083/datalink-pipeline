-- Bronze: RAW_MEMBERSHIP
-- Source: EDI 834 (Enrollment) or monthly/delta CSV.
-- MERGE key: (member_id, plan_id) — a member can be enrolled in multiple plans.

CREATE TABLE IF NOT EXISTS {schema}.RAW_MEMBERSHIP (
    member_id        VARCHAR,
    subscriber_id    VARCHAR,
    dob              DATE,
    gender           VARCHAR,
    plan_id          VARCHAR,
    group_id         VARCHAR,
    effective_date   DATE,
    termination_date DATE,
    coverage_type    VARCHAR,
    state            VARCHAR(2),
    _load_dt         TIMESTAMP,
    _source_file     VARCHAR,
    _batch_id        VARCHAR,
    _record_source   VARCHAR,
    PRIMARY KEY (member_id, plan_id)
);
