-- =============================================================
-- Snowflake Cloud Data Platform - Database & Schema DDL
-- =============================================================
-- DEV Database (Transient - no fail-safe, saves storage cost)
-- QA Database (Transient - no fail-safe, saves storage cost)
-- PROD Database (Permanent - 7 days time travel for recovery)
-- =============================================================

-- =====================
-- DEV Environment
-- =====================
CREATE OR REPLACE TRANSIENT DATABASE SALES_DEV
    COMMENT = 'Sales development database - transient, used for development and testing of new features';

CREATE OR REPLACE TRANSIENT SCHEMA SALES_DEV.BRONZE
    COMMENT = 'Raw ingestion layer - stores unprocessed source data as-is';
CREATE OR REPLACE TRANSIENT SCHEMA SALES_DEV.SILVER
    COMMENT = 'Cleansed and curated layer - validated, deduped and conformed data';
CREATE OR REPLACE TRANSIENT SCHEMA SALES_DEV.GOLD
    COMMENT = 'Business-ready layer - aggregated data optimized for reporting and analytics';
CREATE OR REPLACE TRANSIENT SCHEMA SALES_DEV.COMMON
    COMMENT = 'Shared utilities - common functions, stored procedures and reusable objects';

-- =====================
-- QA Environment
-- =====================
CREATE OR REPLACE TRANSIENT DATABASE SALES_QA
    COMMENT = 'Sales QA database - transient, used for quality assurance and integration testing';

CREATE OR REPLACE TRANSIENT SCHEMA SALES_QA.BRONZE
    COMMENT = 'Raw ingestion layer - stores unprocessed source data as-is';
CREATE OR REPLACE TRANSIENT SCHEMA SALES_QA.SILVER
    COMMENT = 'Cleansed and curated layer - validated, deduped and conformed data';
CREATE OR REPLACE TRANSIENT SCHEMA SALES_QA.GOLD
    COMMENT = 'Business-ready layer - aggregated data optimized for reporting and analytics';
CREATE OR REPLACE TRANSIENT SCHEMA SALES_QA.COMMON
    COMMENT = 'Shared utilities - common functions, stored procedures and reusable objects';

-- =====================
-- PROD Environment
-- =====================
CREATE OR REPLACE DATABASE SALES_PROD
    DATA_RETENTION_TIME_IN_DAYS = 7
    COMMENT = 'Sales production database - permanent with 7-day time travel for data protection';

CREATE OR REPLACE SCHEMA SALES_PROD.BRONZE
    DATA_RETENTION_TIME_IN_DAYS = 7
    COMMENT = 'Raw ingestion layer - stores unprocessed source data as-is';
CREATE OR REPLACE SCHEMA SALES_PROD.SILVER
    DATA_RETENTION_TIME_IN_DAYS = 7
    COMMENT = 'Cleansed and curated layer - validated, deduped and conformed data';
CREATE OR REPLACE SCHEMA SALES_PROD.GOLD
    DATA_RETENTION_TIME_IN_DAYS = 7
    COMMENT = 'Business-ready layer - aggregated data optimized for reporting and analytics';
CREATE OR REPLACE SCHEMA SALES_PROD.COMMON
    DATA_RETENTION_TIME_IN_DAYS = 7
    COMMENT = 'Shared utilities - common functions, stored procedures and reusable objects';
