-- =====================================================
-- Silver Layer - TAX_MASTER Dynamic Table
-- Deduplication, data quality checks, incremental refresh
-- =====================================================

CREATE OR REPLACE DYNAMIC TABLE SALES_DEV.SILVER.TAX_MASTER
    TARGET_LAG = DOWNSTREAM
    WAREHOUSE = COMPUTE_WH
    REFRESH_MODE = INCREMENTAL
    INITIALIZE = ON_CREATE
    COMMENT = 'Silver layer tax master with deduplication and data quality validation'
AS
SELECT
    TAX_CODE,
    TAX_TYPE,
    TAX_RATE,
    TAX_INCLUSIVE_FLAG,
    EFFECTIVE_START_DATE,
    EFFECTIVE_END_DATE,
    IS_ACTIVE,
    CREATED_AT,
    SOURCE_SYSTEM,
    
    -- Data Quality Flag
    CASE 
        WHEN TAX_CODE IS NULL THEN FALSE
        WHEN TAX_TYPE IS NULL OR TRIM(TAX_TYPE) = '' THEN FALSE
        WHEN TAX_RATE IS NULL OR TAX_RATE < 0 OR TAX_RATE > 1 THEN FALSE
        WHEN TAX_INCLUSIVE_FLAG NOT IN ('Y', 'N') THEN FALSE
        WHEN IS_ACTIVE NOT IN ('Y', 'N') THEN FALSE
        WHEN EFFECTIVE_START_DATE IS NULL THEN FALSE
        WHEN EFFECTIVE_END_DATE < EFFECTIVE_START_DATE THEN FALSE
        ELSE TRUE
    END AS IS_VALID_RECORD,
    
    -- Audit Columns
    __FILE_NAME,
    __ROW_NUMBER AS __SOURCE_ROW_NUMBER,
    __LOAD_TS AS __BRONZE_LOAD_TS

FROM SALES_DEV.BRONZE.TAX_MASTER

-- Deduplication: Keep latest record per TAX_CODE
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY TAX_CODE 
    ORDER BY __LOAD_TS DESC, __ROW_NUMBER DESC
) = 1;
