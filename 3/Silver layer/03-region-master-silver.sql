-- =====================================================
-- Silver Layer - REGION_MASTER Dynamic Table
-- Deduplication, data quality checks, incremental refresh
-- =====================================================

CREATE OR REPLACE DYNAMIC TABLE SALES_DEV.SILVER.REGION_MASTER
    TARGET_LAG = DOWNSTREAM
    WAREHOUSE = COMPUTE_WH
    REFRESH_MODE = INCREMENTAL
    INITIALIZE = ON_CREATE
    COMMENT = 'Silver layer region master with deduplication and data quality validation'
AS
SELECT
    REGION_CODE,
    REGION_NAME,
    IS_ACTIVE,
    EFFECTIVE_START_DATE,
    EFFECTIVE_END_DATE,
    CREATED_AT,
    SOURCE_SYSTEM,
    
    -- Data Quality Flags
    CASE 
        WHEN REGION_CODE IS NULL THEN FALSE
        WHEN REGION_NAME IS NULL OR TRIM(REGION_NAME) = '' THEN FALSE
        WHEN IS_ACTIVE NOT IN ('Y', 'N') THEN FALSE
        WHEN EFFECTIVE_START_DATE IS NULL THEN FALSE
        WHEN EFFECTIVE_END_DATE < EFFECTIVE_START_DATE THEN FALSE
        ELSE TRUE
    END AS IS_VALID_RECORD,
    
    -- Audit Columns
    __FILE_NAME,
    __ROW_NUMBER AS __SOURCE_ROW_NUMBER,
    __LOAD_TS AS __BRONZE_LOAD_TS

FROM SALES_DEV.BRONZE.REGION_MASTER

-- Deduplication: Keep latest record per REGION_CODE based on load timestamp
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY REGION_CODE 
    ORDER BY __LOAD_TS DESC, __ROW_NUMBER DESC
) = 1;
