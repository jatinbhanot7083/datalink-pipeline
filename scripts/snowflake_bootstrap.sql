-- ============================================================================
-- DataLink Snowflake bootstrap — Phase 7, Step 3.
-- ============================================================================
-- Purpose: provision the database, role, warehouse, service user, and internal
-- stage that the DataLink pipeline needs before any DAG can run against
-- Snowflake. Idempotent — safe to re-run. Destructive-op-free — no DROPs.
--
-- How to run:
--   1. Log into https://app.snowflake.com with your admin account
--   2. Open a fresh Worksheet (top-left: "+" → "SQL Worksheet")
--   3. Role selector (top-right of worksheet): pick ACCOUNTADMIN
--   4. BEFORE PASTING: change REPLACE_WITH_YOUR_STRONG_PASSWORD below
--      to a 16+ character password you generate in a password manager.
--      DO NOT paste that password in any chat.
--   5. Paste this entire file into the worksheet
--   6. Click "Run All" (the blue dropdown arrow → "Run All")
--   7. Scroll to the bottom — you should see
--      "DataLink Snowflake bootstrap complete. Proceed to Step 4."
-- ============================================================================

USE ROLE ACCOUNTADMIN;

-- ----------------------------------------------------------------------------
-- 1. Compute warehouse — MEDIUM size with 1-hour warm window for snappy UI.
--    Recipe per docs/demo_journal.md §"5th ingredient — Snowflake-side
--    warehouse tuning": MEDIUM is ~5× faster than XSMALL on the metadata
--    queries the dashboards issue. Demo cost is ~$0.10 per session — the
--    UX gain is worth it. AUTO_SUSPEND=3600 keeps the warehouse hot for
--    a full hour of idle time before it cools off (otherwise every click
--    after a coffee break pays a 3-10s spin-up).
-- ----------------------------------------------------------------------------
CREATE WAREHOUSE IF NOT EXISTS DATALINK_WH
  WITH WAREHOUSE_SIZE = 'MEDIUM'
       AUTO_SUSPEND  = 3600                          -- 1-hour warm window
       AUTO_RESUME   = TRUE                          -- wake on first query
       STATEMENT_TIMEOUT_IN_SECONDS        = 300     -- fail rogue queries at 5 min
       STATEMENT_QUEUED_TIMEOUT_IN_SECONDS = 60      -- fail queue waits at 1 min
       INITIALLY_SUSPENDED = TRUE
       COMMENT = 'Compute warehouse for DataLink dev pipelines';

-- If the warehouse already exists from a prior bootstrap, ALTER it to the
-- canonical recipe (CREATE IF NOT EXISTS won't update an existing one).
ALTER WAREHOUSE DATALINK_WH SET
  WAREHOUSE_SIZE = 'MEDIUM',
  AUTO_SUSPEND   = 3600,
  AUTO_RESUME    = TRUE,
  STATEMENT_TIMEOUT_IN_SECONDS        = 300,
  STATEMENT_QUEUED_TIMEOUT_IN_SECONDS = 60;

-- ----------------------------------------------------------------------------
-- 2. Database + schemas. Per-client schemas (BRONZE_AETNA, etc) are created
--    dynamically by the pipeline at ingest time; we only pre-create the
--    shared base schemas here.
-- ----------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS DATALINK_DEV
  COMMENT = 'DataLink development database — Bronze/Silver/Gold/Control';

USE DATABASE DATALINK_DEV;

CREATE SCHEMA IF NOT EXISTS BRONZE  COMMENT = 'Raw ingested data';
CREATE SCHEMA IF NOT EXISTS SILVER  COMMENT = 'Cleaned + conformed (DV2.0)';
CREATE SCHEMA IF NOT EXISTS CONTROL COMMENT = 'Pipeline metadata + DQ suites';

-- ----------------------------------------------------------------------------
-- 3. Role — the pipeline's least-privilege identity. Not ACCOUNTADMIN.
-- ----------------------------------------------------------------------------
CREATE ROLE IF NOT EXISTS DATALINK_ENGINEER
  COMMENT = 'Role used by the DataLink pipeline service account';

-- Warehouse usage (needed to run queries at all)
GRANT USAGE, OPERATE ON WAREHOUSE DATALINK_WH TO ROLE DATALINK_ENGINEER;

-- Database + schema usage
GRANT USAGE ON DATABASE DATALINK_DEV                     TO ROLE DATALINK_ENGINEER;
GRANT CREATE SCHEMA ON DATABASE DATALINK_DEV             TO ROLE DATALINK_ENGINEER;
GRANT USAGE ON ALL SCHEMAS IN DATABASE DATALINK_DEV      TO ROLE DATALINK_ENGINEER;
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE DATALINK_DEV   TO ROLE DATALINK_ENGINEER;

-- Make the pipeline role the OWNER of the base schemas so it can create /
-- drop tables inside them without needing per-table grants. REVOKE CURRENT
-- GRANTS detaches the original ownership from ACCOUNTADMIN.
GRANT OWNERSHIP ON SCHEMA DATALINK_DEV.BRONZE  TO ROLE DATALINK_ENGINEER REVOKE CURRENT GRANTS;
GRANT OWNERSHIP ON SCHEMA DATALINK_DEV.SILVER  TO ROLE DATALINK_ENGINEER REVOKE CURRENT GRANTS;
GRANT OWNERSHIP ON SCHEMA DATALINK_DEV.CONTROL TO ROLE DATALINK_ENGINEER REVOKE CURRENT GRANTS;

-- ----------------------------------------------------------------------------
-- 4. Internal stage — where Bronze CSVs land via PUT from the airflow
--    container before COPY INTO the RAW_* tables.
-- ----------------------------------------------------------------------------
USE SCHEMA DATALINK_DEV.BRONZE;

CREATE STAGE IF NOT EXISTS DATALINK_STAGE
  COMMENT = 'Internal stage for CSV files ingested from SFTP drop zone';

GRANT READ, WRITE ON STAGE DATALINK_STAGE TO ROLE DATALINK_ENGINEER;

-- ----------------------------------------------------------------------------
-- 5. Service user — the identity used by the airflow + control_tower
--    containers. Separate from your human admin account.
--
-- IMPORTANT: change REPLACE_WITH_YOUR_STRONG_PASSWORD below before running.
--
-- To generate a strong password you can use any password manager, OR run
-- this in Snowflake to have it generate one for you (then copy the result):
--      SELECT RANDSTR(24, RANDOM());
-- ----------------------------------------------------------------------------
CREATE USER IF NOT EXISTS DATALINK_SVC
  PASSWORD            = 'REPLACE_WITH_YOUR_STRONG_PASSWORD'
  DEFAULT_ROLE        = DATALINK_ENGINEER
  DEFAULT_WAREHOUSE   = DATALINK_WH
  DEFAULT_NAMESPACE   = DATALINK_DEV.CONTROL
  MUST_CHANGE_PASSWORD = FALSE
  COMMENT = 'Service account for DataLink pipeline — do not use for human login';

GRANT ROLE DATALINK_ENGINEER TO USER DATALINK_SVC;

-- ----------------------------------------------------------------------------
-- 6. Verification — you should see one row per object, confirming creation.
-- ----------------------------------------------------------------------------
SHOW WAREHOUSES LIKE 'DATALINK_WH';
SHOW DATABASES  LIKE 'DATALINK_DEV';
SHOW SCHEMAS    IN DATABASE DATALINK_DEV;
SHOW ROLES      LIKE 'DATALINK_ENGINEER';
SHOW USERS      LIKE 'DATALINK_SVC';
SHOW STAGES     IN SCHEMA DATALINK_DEV.BRONZE;

SELECT 'DataLink Snowflake bootstrap complete. Proceed to Step 4.' AS status;
