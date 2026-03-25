-- =====================================================
-- Silver Layer - COUNTRY_MASTER Dynamic Table
-- Deduplication, data quality checks, incremental refresh
-- =====================================================

CREATE OR REPLACE DYNAMIC TABLE SALES_DEV.SILVER.COUNTRY_MASTER
    TARGET_LAG = DOWNSTREAM
    WAREHOUSE = COMPUTE_WH
    REFRESH_MODE = INCREMENTAL
    INITIALIZE = ON_CREATE
    COMMENT = 'Silver layer country master with deduplication and data quality validation'
AS
SELECT
    COUNTRY_CODE,
    COUNTRY_NAME,
    REGION_CODE,
    CURRENCY_CODE,
    TAX_CODE,
    PRIMARY_LANGUAGE,
    TIMEZONE,
    ECOMMERCE_SUPPORTED,
    RETAIL_STORE_SUPPORTED,
    MARKET_TIER,
    
    -- Data Quality Flag
    CASE 
        WHEN COUNTRY_CODE IS NULL THEN FALSE
        WHEN COUNTRY_NAME IS NULL OR TRIM(COUNTRY_NAME) = '' THEN FALSE
        WHEN REGION_CODE IS NULL THEN FALSE
        WHEN CURRENCY_CODE IS NULL THEN FALSE
        WHEN ECOMMERCE_SUPPORTED NOT IN ('Y', 'N') THEN FALSE
        WHEN RETAIL_STORE_SUPPORTED NOT IN ('Y', 'N') THEN FALSE
        ELSE TRUE
    END AS IS_VALID_RECORD,
    
    -- Audit Columns
    __FILE_NAME,
    __ROW_NUMBER AS __SOURCE_ROW_NUMBER,
    __LOAD_TS AS __BRONZE_LOAD_TS

FROM SALES_DEV.BRONZE.COUNTRY_MASTER

-- Deduplication: Keep latest record per COUNTRY_CODE
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY COUNTRY_CODE 
    ORDER BY __LOAD_TS DESC, __ROW_NUMBER DESC
) = 1;
