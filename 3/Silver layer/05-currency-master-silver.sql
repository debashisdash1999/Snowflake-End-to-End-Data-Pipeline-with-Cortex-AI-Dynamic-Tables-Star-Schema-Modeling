-- =====================================================
-- Silver Layer - CURRENCY_MASTER Dynamic Table
-- Deduplication, data quality checks, incremental refresh
-- =====================================================

CREATE OR REPLACE DYNAMIC TABLE SALES_DEV.SILVER.CURRENCY_MASTER
    TARGET_LAG = DOWNSTREAM
    WAREHOUSE = COMPUTE_WH
    REFRESH_MODE = INCREMENTAL
    INITIALIZE = ON_CREATE
    COMMENT = 'Silver layer currency master with deduplication and data quality validation'
AS
SELECT
    CURRENCY_CODE,
    CURRENCY_NAME,
    CURRENCY_SYMBOL,
    MINOR_UNIT,
    IS_ACTIVE,
    EFFECTIVE_START_DATE,
    EFFECTIVE_END_DATE,
    CREATED_AT,
    SOURCE_SYSTEM,
    
    -- Data Quality Flag
    CASE 
        WHEN CURRENCY_CODE IS NULL THEN FALSE
        WHEN CURRENCY_NAME IS NULL OR TRIM(CURRENCY_NAME) = '' THEN FALSE
        WHEN IS_ACTIVE NOT IN ('Y', 'N') THEN FALSE
        WHEN EFFECTIVE_START_DATE IS NULL THEN FALSE
        WHEN EFFECTIVE_END_DATE < EFFECTIVE_START_DATE THEN FALSE
        ELSE TRUE
    END AS IS_VALID_RECORD,
    
    -- Audit Columns
    __FILE_NAME,
    __ROW_NUMBER AS __SOURCE_ROW_NUMBER,
    __LOAD_TS AS __BRONZE_LOAD_TS

FROM SALES_DEV.BRONZE.CURRENCY_MASTER

-- Deduplication: Keep latest record per CURRENCY_CODE
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY CURRENCY_CODE 
    ORDER BY __LOAD_TS DESC, __ROW_NUMBER DESC
) = 1;
