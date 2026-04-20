-- SQL Server UM-schema bootstrap. Applied by the `sqlserver_init`
-- one-shot container after `sqlserver` is healthy.
--
-- Creates:
--   DB:      DataLinkUM
--   Schema:  [UM]                  (per UM-Gold-v2 doc — [Schema].[PascalCase])
--   Login/User: datalink / datalink_local_only
--
-- The router's PostgresOperationalDb / SqlServerOperationalDb adapters
-- handle the actual Gold UM DDL at push time (see
-- datalink.pipeline.router.schema.sqlserver_ddl). This script only
-- makes sure the database + user + schema exist.

USE master;
GO

IF DB_ID(N'DataLinkUM') IS NULL
BEGIN
    CREATE DATABASE DataLinkUM;
END
GO

-- Login at server scope.
IF NOT EXISTS (SELECT 1 FROM sys.sql_logins WHERE name = N'datalink')
BEGIN
    CREATE LOGIN datalink WITH PASSWORD = N'datalink_local_only',
        CHECK_POLICY = OFF, CHECK_EXPIRATION = OFF, DEFAULT_DATABASE = DataLinkUM;
END
GO

USE DataLinkUM;
GO

-- User inside the DB, mapped to the login.
IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'datalink')
BEGIN
    CREATE USER datalink FOR LOGIN datalink;
END
GO

-- Grant full control inside DataLinkUM — ops DB, not a prod lockdown.
ALTER ROLE db_owner ADD MEMBER datalink;
GO

-- [UM] schema used by every Gold UM table.
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'UM')
BEGIN
    EXEC('CREATE SCHEMA [UM] AUTHORIZATION [datalink]');
END
GO

PRINT 'DataLink UM schema bootstrap complete.';
GO
