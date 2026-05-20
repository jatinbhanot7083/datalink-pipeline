-- ============================================================================
-- DataLink — new-account bootstrap (Snowflake web UI · ACCOUNTADMIN)
-- ----------------------------------------------------------------------------
-- Run this AS A WHOLE in the Snowflake web UI worksheet on the NEW account
-- (WFDIQAH-WCA64552, AZURE, Enterprise) before any Python/Airflow connects.
--
-- Migration history:
--   v1 — built for RVLWGTB-MOA19214 → BJAQCMS-GKA58836
--   v2 — re-used for BJAQCMS-GKA58836 → WFDIQAH-WCA64552 (May 2026)
--
-- It rebuilds the warehouse / database / role / grants the previous
-- account had.  At the end, also grants the new role to your user.
--
-- Idempotent: CREATE … IF NOT EXISTS / ALTER … SET — safe to re-run.
-- ============================================================================

USE ROLE ACCOUNTADMIN;

-- ----------------------------------------------------------------------------
-- 1. Warehouse — identical config to the old account
--      MEDIUM · 1h auto-suspend · 5min statement timeout · starts suspended
-- ----------------------------------------------------------------------------
CREATE WAREHOUSE IF NOT EXISTS DATALINK_WH
    WAREHOUSE_SIZE                       = 'MEDIUM'
    AUTO_SUSPEND                         = 3600
    AUTO_RESUME                          = TRUE
    STATEMENT_TIMEOUT_IN_SECONDS         = 300
    STATEMENT_QUEUED_TIMEOUT_IN_SECONDS  = 60
    INITIALLY_SUSPENDED                  = TRUE
    COMMENT                              = 'DataLink primary warehouse — Bronze/Silver/Gold + dbt + UI queries';

-- If the warehouse already existed (e.g. from a partial run), force its
-- parameters back to canonical so we don't drift.
ALTER WAREHOUSE DATALINK_WH SET
    WAREHOUSE_SIZE                       = 'MEDIUM'
    AUTO_SUSPEND                         = 3600
    AUTO_RESUME                          = TRUE
    STATEMENT_TIMEOUT_IN_SECONDS         = 300
    STATEMENT_QUEUED_TIMEOUT_IN_SECONDS  = 60;

-- ----------------------------------------------------------------------------
-- 2. Database (dev) + the prod placeholder dbt profile references
-- ----------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS DATALINK_DEV
    COMMENT = 'DataLink dev — Bronze/Silver/Gold + CONTROL schemas live here';

-- The prod profile in dbt/profiles.yml expects DATALINK_PROD to exist.
-- Create as an empty placeholder so dbt-prod is a no-op until you cut over.
CREATE DATABASE IF NOT EXISTS DATALINK_PROD
    COMMENT = 'DataLink prod — placeholder, populated by promotion only';

-- ----------------------------------------------------------------------------
-- 3. Role + grants — DATALINK_ENGINEER is what the app actually uses
-- ----------------------------------------------------------------------------
CREATE ROLE IF NOT EXISTS DATALINK_ENGINEER
    COMMENT = 'App service role for the DataLink platform';

-- Warehouse + database privileges (current AND future objects)
GRANT USAGE  ON WAREHOUSE DATALINK_WH      TO ROLE DATALINK_ENGINEER;
GRANT ALL    ON DATABASE  DATALINK_DEV     TO ROLE DATALINK_ENGINEER;
GRANT ALL    ON ALL    SCHEMAS IN DATABASE DATALINK_DEV TO ROLE DATALINK_ENGINEER;
GRANT ALL    ON FUTURE SCHEMAS IN DATABASE DATALINK_DEV TO ROLE DATALINK_ENGINEER;
GRANT ALL    ON FUTURE TABLES  IN DATABASE DATALINK_DEV TO ROLE DATALINK_ENGINEER;
GRANT ALL    ON FUTURE VIEWS   IN DATABASE DATALINK_DEV TO ROLE DATALINK_ENGINEER;
GRANT ALL    ON FUTURE STAGES  IN DATABASE DATALINK_DEV TO ROLE DATALINK_ENGINEER;
GRANT ALL    ON FUTURE FILE FORMATS IN DATABASE DATALINK_DEV TO ROLE DATALINK_ENGINEER;

-- Same for the prod placeholder so day-1 promotion just works.
GRANT ALL    ON DATABASE  DATALINK_PROD    TO ROLE DATALINK_ENGINEER;
GRANT ALL    ON ALL    SCHEMAS IN DATABASE DATALINK_PROD TO ROLE DATALINK_ENGINEER;
GRANT ALL    ON FUTURE SCHEMAS IN DATABASE DATALINK_PROD TO ROLE DATALINK_ENGINEER;
GRANT ALL    ON FUTURE TABLES  IN DATABASE DATALINK_PROD TO ROLE DATALINK_ENGINEER;
GRANT ALL    ON FUTURE VIEWS   IN DATABASE DATALINK_PROD TO ROLE DATALINK_ENGINEER;

-- The role needs to be able to create new schemas under DATALINK_DEV — the
-- CONTROL schema bootstrap, factory DAGs, and Pipeline Architect all do this.
GRANT CREATE SCHEMA ON DATABASE DATALINK_DEV  TO ROLE DATALINK_ENGINEER;
GRANT CREATE SCHEMA ON DATABASE DATALINK_PROD TO ROLE DATALINK_ENGINEER;

-- ----------------------------------------------------------------------------
-- 4. Bind the role to your login user
-- ----------------------------------------------------------------------------
GRANT ROLE DATALINK_ENGINEER TO USER JATINBHANOT7083;

-- Set DATALINK_ENGINEER as the default role + warehouse so any tool that
-- forgets to pass them still works.
ALTER USER JATINBHANOT7083 SET
    DEFAULT_ROLE      = DATALINK_ENGINEER
    DEFAULT_WAREHOUSE = DATALINK_WH
    DEFAULT_NAMESPACE = DATALINK_DEV.PUBLIC;

-- ----------------------------------------------------------------------------
-- 5. Sanity probes — paste these one at a time after the block above
-- ----------------------------------------------------------------------------
-- USE ROLE DATALINK_ENGINEER;
-- USE WAREHOUSE DATALINK_WH;
-- USE DATABASE DATALINK_DEV;
-- SHOW WAREHOUSES LIKE 'DATALINK_WH';
-- SHOW DATABASES  LIKE 'DATALINK_%';
-- SHOW GRANTS TO ROLE DATALINK_ENGINEER;
-- SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_DATABASE();
