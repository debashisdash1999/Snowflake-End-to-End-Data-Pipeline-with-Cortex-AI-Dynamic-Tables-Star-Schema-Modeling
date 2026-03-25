-- =====================================================
-- Silver Layer - Product Master Dynamic Tables
-- Deduplication, data quality checks, incremental refresh
-- =====================================================

-- =====================================================
-- PRODUCT_CATEGORY_MASTER
-- =====================================================
CREATE OR REPLACE DYNAMIC TABLE SALES_DEV.SILVER.PRODUCT_CATEGORY_MASTER
    TARGET_LAG = DOWNSTREAM
    WAREHOUSE = COMPUTE_WH
    REFRESH_MODE = INCREMENTAL
    INITIALIZE = ON_CREATE
    COMMENT = 'Silver layer product category master with deduplication and quality validation'
AS
SELECT
    CATEGORY_CODE,
    CATEGORY_NAME,
    REPORTING_SEGMENT,
    IS_ACTIVE,
    EFFECTIVE_START_DATE,
    EFFECTIVE_END_DATE,
    CREATED_AT,
    SOURCE_SYSTEM,
    
    -- Data Quality Flag
    CASE 
        WHEN CATEGORY_CODE IS NULL THEN FALSE
        WHEN CATEGORY_NAME IS NULL OR TRIM(CATEGORY_NAME) = '' THEN FALSE
        WHEN IS_ACTIVE NOT IN ('Y', 'N') THEN FALSE
        WHEN EFFECTIVE_END_DATE < EFFECTIVE_START_DATE THEN FALSE
        ELSE TRUE
    END AS IS_VALID_RECORD,
    
    -- Audit Columns
    __FILE_NAME,
    __ROW_NUMBER AS __SOURCE_ROW_NUMBER,
    __LOAD_TS AS __BRONZE_LOAD_TS

FROM SALES_DEV.BRONZE.PRODUCT_CATEGORY_MASTER

QUALIFY ROW_NUMBER() OVER (
    PARTITION BY CATEGORY_CODE 
    ORDER BY __LOAD_TS DESC, __ROW_NUMBER DESC
) = 1;

-- =====================================================
-- PRODUCT_FAMILY_MASTER
-- =====================================================
CREATE OR REPLACE DYNAMIC TABLE SALES_DEV.SILVER.PRODUCT_FAMILY_MASTER
    TARGET_LAG = DOWNSTREAM
    WAREHOUSE = COMPUTE_WH
    REFRESH_MODE = INCREMENTAL
    INITIALIZE = ON_CREATE
    COMMENT = 'Silver layer product family master with deduplication and quality validation'
AS
SELECT
    FAMILY_CODE,
    FAMILY_NAME,
    CATEGORY_CODE,
    LAUNCH_YEAR,
    IS_ACTIVE,
    LIFECYCLE_STATUS,
    CREATED_AT,
    SOURCE_SYSTEM,
    
    -- Data Quality Flag
    CASE 
        WHEN FAMILY_CODE IS NULL THEN FALSE
        WHEN FAMILY_NAME IS NULL OR TRIM(FAMILY_NAME) = '' THEN FALSE
        WHEN CATEGORY_CODE IS NULL THEN FALSE
        WHEN IS_ACTIVE NOT IN ('Y', 'N') THEN FALSE
        WHEN LAUNCH_YEAR < 1900 OR LAUNCH_YEAR > 2100 THEN FALSE
        ELSE TRUE
    END AS IS_VALID_RECORD,
    
    -- Audit Columns
    __FILE_NAME,
    __ROW_NUMBER AS __SOURCE_ROW_NUMBER,
    __LOAD_TS AS __BRONZE_LOAD_TS

FROM SALES_DEV.BRONZE.PRODUCT_FAMILY_MASTER

QUALIFY ROW_NUMBER() OVER (
    PARTITION BY FAMILY_CODE 
    ORDER BY __LOAD_TS DESC, __ROW_NUMBER DESC
) = 1;

-- =====================================================
-- PRODUCT_MODEL_MASTER
-- =====================================================
CREATE OR REPLACE DYNAMIC TABLE SALES_DEV.SILVER.PRODUCT_MODEL_MASTER
    TARGET_LAG = DOWNSTREAM
    WAREHOUSE = COMPUTE_WH
    REFRESH_MODE = INCREMENTAL
    INITIALIZE = ON_CREATE
    COMMENT = 'Silver layer product model master with deduplication and quality validation'
AS
SELECT
    MODEL_CODE,
    MODEL_NAME,
    FAMILY_CODE,
    LAUNCH_DATE,
    DISCONTINUE_DATE,
    LIFECYCLE_STATUS,
    IS_ACTIVE,
    CREATED_AT,
    SOURCE_SYSTEM,
    
    -- Data Quality Flag
    CASE 
        WHEN MODEL_CODE IS NULL THEN FALSE
        WHEN MODEL_NAME IS NULL OR TRIM(MODEL_NAME) = '' THEN FALSE
        WHEN FAMILY_CODE IS NULL THEN FALSE
        WHEN IS_ACTIVE NOT IN ('Y', 'N') THEN FALSE
        WHEN DISCONTINUE_DATE IS NOT NULL AND DISCONTINUE_DATE < LAUNCH_DATE THEN FALSE
        ELSE TRUE
    END AS IS_VALID_RECORD,
    
    -- Audit Columns
    __FILE_NAME,
    __ROW_NUMBER AS __SOURCE_ROW_NUMBER,
    __LOAD_TS AS __BRONZE_LOAD_TS

FROM SALES_DEV.BRONZE.PRODUCT_MODEL_MASTER

QUALIFY ROW_NUMBER() OVER (
    PARTITION BY MODEL_CODE 
    ORDER BY __LOAD_TS DESC, __ROW_NUMBER DESC
) = 1;

-- =====================================================
-- PRODUCT_SKU_MASTER
-- =====================================================
CREATE OR REPLACE DYNAMIC TABLE SALES_DEV.SILVER.PRODUCT_SKU_MASTER
    TARGET_LAG = DOWNSTREAM
    WAREHOUSE = COMPUTE_WH
    REFRESH_MODE = INCREMENTAL
    INITIALIZE = ON_CREATE
    COMMENT = 'Silver layer product SKU master with deduplication and quality validation'
AS
SELECT
    SKU_CODE,
    MODEL_CODE,
    VARIANT,
    PRICE_TIER,
    GLOBAL_LAUNCH_DATE,
    IS_ACTIVE,
    CREATED_AT,
    SOURCE_SYSTEM,
    
    -- Data Quality Flag
    CASE 
        WHEN SKU_CODE IS NULL THEN FALSE
        WHEN MODEL_CODE IS NULL THEN FALSE
        WHEN VARIANT IS NULL OR TRIM(VARIANT) = '' THEN FALSE
        WHEN IS_ACTIVE NOT IN ('Y', 'N') THEN FALSE
        ELSE TRUE
    END AS IS_VALID_RECORD,
    
    -- Audit Columns
    __FILE_NAME,
    __ROW_NUMBER AS __SOURCE_ROW_NUMBER,
    __LOAD_TS AS __BRONZE_LOAD_TS

FROM SALES_DEV.BRONZE.PRODUCT_SKU_MASTER

QUALIFY ROW_NUMBER() OVER (
    PARTITION BY SKU_CODE 
    ORDER BY __LOAD_TS DESC, __ROW_NUMBER DESC
) = 1;

-- =====================================================
-- PRODUCT_COUNTRY_AVAILABILITY
-- =====================================================
CREATE OR REPLACE DYNAMIC TABLE SALES_DEV.SILVER.PRODUCT_COUNTRY_AVAILABILITY
    TARGET_LAG = DOWNSTREAM
    WAREHOUSE = COMPUTE_WH
    REFRESH_MODE = INCREMENTAL
    INITIALIZE = ON_CREATE
    COMMENT = 'Silver layer product country availability with deduplication and quality validation'
AS
SELECT
    SKU_CODE,
    COUNTRY_CODE,
    LOCAL_LAUNCH_DATE,
    LOCAL_DISCONTINUE_DATE,
    IS_AVAILABLE,
    CREATED_AT,
    SOURCE_SYSTEM,
    
    -- Data Quality Flag
    CASE 
        WHEN SKU_CODE IS NULL THEN FALSE
        WHEN COUNTRY_CODE IS NULL THEN FALSE
        WHEN IS_AVAILABLE NOT IN ('Y', 'N') THEN FALSE
        WHEN LOCAL_DISCONTINUE_DATE IS NOT NULL AND LOCAL_DISCONTINUE_DATE < LOCAL_LAUNCH_DATE THEN FALSE
        ELSE TRUE
    END AS IS_VALID_RECORD,
    
    -- Audit Columns
    __FILE_NAME,
    __ROW_NUMBER AS __SOURCE_ROW_NUMBER,
    __LOAD_TS AS __BRONZE_LOAD_TS

FROM SALES_DEV.BRONZE.PRODUCT_COUNTRY_AVAILABILITY

QUALIFY ROW_NUMBER() OVER (
    PARTITION BY SKU_CODE, COUNTRY_CODE 
    ORDER BY __LOAD_TS DESC, __ROW_NUMBER DESC
) = 1;
