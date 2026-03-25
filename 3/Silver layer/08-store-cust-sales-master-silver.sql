-- =====================================================
-- Silver Layer - STORE_MASTER Dynamic Table
-- Deduplication, data quality checks, incremental refresh
-- =====================================================

CREATE OR REPLACE DYNAMIC TABLE SALES_DEV.SILVER.STORE_MASTER
    TARGET_LAG = DOWNSTREAM
    WAREHOUSE = COMPUTE_WH
    REFRESH_MODE = INCREMENTAL
    INITIALIZE = ON_CREATE
    COMMENT = 'Silver layer store master with deduplication and quality validation'
AS
SELECT
    STORE_CODE,
    STORE_NAME,
    COUNTRY_CODE,
    REGION_CODE,
    TAX_JURISDICTION_CODE,
    FORMAT_CODE,
    CITY,
    STATE_CODE,
    POSTAL_CODE,
    ADDRESS_LINE1,
    LATITUDE,
    LONGITUDE,
    STORE_OPEN_DATE,
    STORE_CLOSE_DATE,
    LIFECYCLE_STATUS,
    FLOOR_AREA_SQFT,
    ANNUAL_RENT_USD,
    IS_ACTIVE,
    EFFECTIVE_START_DATE,
    EFFECTIVE_END_DATE,
    
    -- Data Quality Flag
    CASE 
        WHEN STORE_CODE IS NULL THEN FALSE
        WHEN STORE_NAME IS NULL OR TRIM(STORE_NAME) = '' THEN FALSE
        WHEN COUNTRY_CODE IS NULL THEN FALSE
        WHEN IS_ACTIVE NOT IN ('Y', 'N') THEN FALSE
        WHEN LATITUDE < -90 OR LATITUDE > 90 THEN FALSE
        WHEN LONGITUDE < -180 OR LONGITUDE > 180 THEN FALSE
        WHEN STORE_CLOSE_DATE IS NOT NULL AND STORE_CLOSE_DATE < STORE_OPEN_DATE THEN FALSE
        WHEN EFFECTIVE_END_DATE < EFFECTIVE_START_DATE THEN FALSE
        WHEN FLOOR_AREA_SQFT < 0 THEN FALSE
        WHEN ANNUAL_RENT_USD < 0 THEN FALSE
        ELSE TRUE
    END AS IS_VALID_RECORD,
    
    -- Audit Columns
    __FILE_NAME,
    __ROW_NUMBER AS __SOURCE_ROW_NUMBER,
    __LOAD_TS AS __BRONZE_LOAD_TS

FROM SALES_DEV.BRONZE.STORE_MASTER

-- Deduplication: Keep latest record per STORE_CODE
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY STORE_CODE 
    ORDER BY __LOAD_TS DESC, __ROW_NUMBER DESC
) = 1;


-- =====================================================
-- Silver Layer - CUSTOMER_MASTER Dynamic Table
-- Deduplication, data quality checks, incremental refresh
-- =====================================================

CREATE OR REPLACE DYNAMIC TABLE SALES_DEV.SILVER.CUSTOMER_MASTER
    TARGET_LAG = DOWNSTREAM
    WAREHOUSE = COMPUTE_WH
    REFRESH_MODE = INCREMENTAL
    INITIALIZE = ON_CREATE
    COMMENT = 'Silver layer customer master with deduplication and quality validation'
AS
SELECT
    CUSTOMER_ID,
    CUSTOMER_NUMBER,
    FIRST_NAME,
    LAST_NAME,
    FULL_NAME,
    GENDER,
    DATE_OF_BIRTH,
    EMAIL,
    PHONE_NUMBER,
    STREET_ADDRESS,
    CITY,
    STATE_PROVINCE,
    POSTAL_CODE,
    COUNTRY_CODE,
    COUNTRY_NAME,
    REGION,
    PREFERRED_LANGUAGE,
    CUSTOMER_SEGMENT,
    LOYALTY_TIER,
    REGISTRATION_DATE,
    IS_ACTIVE,
    SOURCE_SYSTEM,
    RECORD_SOURCE,
    CREATED_AT,
    UPDATED_AT,
    
    -- Data Quality Flag
    CASE 
        WHEN CUSTOMER_ID IS NULL THEN FALSE
        WHEN CUSTOMER_NUMBER IS NULL THEN FALSE
        WHEN EMAIL IS NULL OR EMAIL NOT LIKE '%@%.%' THEN FALSE
        WHEN DATE_OF_BIRTH > '2026-03-02'::DATE THEN FALSE
        WHEN REGISTRATION_DATE > '2026-03-02'::DATE THEN FALSE
        WHEN COUNTRY_CODE IS NULL THEN FALSE
        ELSE TRUE
    END AS IS_VALID_RECORD,
    
    -- Audit Columns
    __FILE_NAME,
    __ROW_NUMBER AS __SOURCE_ROW_NUMBER,
    __LOAD_TS AS __BRONZE_LOAD_TS

FROM SALES_DEV.BRONZE.CUSTOMER_MASTER

-- Deduplication: Keep latest record per CUSTOMER_ID
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY CUSTOMER_ID 
    ORDER BY __LOAD_TS DESC, __ROW_NUMBER DESC
) = 1;


-- =====================================================
-- Silver Layer - Sales Transaction Dynamic Tables
-- Deduplication, data quality checks, incremental refresh
-- =====================================================

-- =====================================================
-- SALES_HEADER
-- =====================================================
CREATE OR REPLACE DYNAMIC TABLE SALES_DEV.SILVER.SALES_HEADER
    TARGET_LAG = DOWNSTREAM
    WAREHOUSE = COMPUTE_WH
    REFRESH_MODE = INCREMENTAL
    INITIALIZE = ON_CREATE
    COMMENT = 'Silver layer sales header with deduplication and quality validation'
AS
SELECT
    TRANSACTION_ID,
    TRANSACTION_NUMBER,
    TRANSACTION_TIMESTAMP,
    CUSTOMER_ID,
    STORE_ID,
    CHANNEL_ID,
    PAYMENT_METHOD,
    CURRENCY,
    GROSS_AMOUNT,
    TOTAL_DISCOUNT,
    TOTAL_TAX,
    NET_TOTAL,
    CREATED_AT,
    
    -- Data Quality Flag
    CASE 
        WHEN TRANSACTION_ID IS NULL THEN FALSE
        WHEN TRANSACTION_NUMBER IS NULL THEN FALSE
        WHEN CUSTOMER_ID IS NULL THEN FALSE
        WHEN STORE_ID IS NULL THEN FALSE
        WHEN CURRENCY IS NULL THEN FALSE
        WHEN GROSS_AMOUNT < 0 THEN FALSE
        WHEN TOTAL_DISCOUNT < 0 THEN FALSE
        WHEN TOTAL_TAX < 0 THEN FALSE
        WHEN NET_TOTAL < 0 THEN FALSE
        ELSE TRUE
    END AS IS_VALID_RECORD,
    
    -- Audit Columns
    __FILE_NAME,
    __ROW_NUMBER AS __SOURCE_ROW_NUMBER,
    __LOAD_TS AS __BRONZE_LOAD_TS

FROM SALES_DEV.BRONZE.SALES_HEADER

-- Deduplication: Keep latest record per TRANSACTION_ID
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY TRANSACTION_ID 
    ORDER BY __LOAD_TS DESC, __ROW_NUMBER DESC
) = 1;

-- =====================================================
-- SALES_ITEM
-- =====================================================
CREATE OR REPLACE DYNAMIC TABLE SALES_DEV.SILVER.SALES_ITEM
    TARGET_LAG = DOWNSTREAM
    WAREHOUSE = COMPUTE_WH
    REFRESH_MODE = INCREMENTAL
    INITIALIZE = ON_CREATE
    COMMENT = 'Silver layer sales item with deduplication and quality validation'
AS
SELECT
    TRANSACTION_LINE_ID,
    TRANSACTION_ID,
    SKU_CODE,
    QUANTITY,
    UNIT_PRICE,
    DISCOUNT_AMOUNT,
    TAX_AMOUNT,
    LINE_TOTAL,
    CREATED_AT,
    
    -- Data Quality Flag
    CASE 
        WHEN TRANSACTION_LINE_ID IS NULL THEN FALSE
        WHEN TRANSACTION_ID IS NULL THEN FALSE
        WHEN SKU_CODE IS NULL THEN FALSE
        WHEN QUANTITY IS NULL OR QUANTITY <= 0 THEN FALSE
        WHEN UNIT_PRICE < 0 THEN FALSE
        WHEN DISCOUNT_AMOUNT < 0 THEN FALSE
        WHEN TAX_AMOUNT < 0 THEN FALSE
        WHEN LINE_TOTAL < 0 THEN FALSE
        ELSE TRUE
    END AS IS_VALID_RECORD,
    
    -- Audit Columns
    __FILE_NAME,
    __ROW_NUMBER AS __SOURCE_ROW_NUMBER,
    __LOAD_TS AS __BRONZE_LOAD_TS

FROM SALES_DEV.BRONZE.SALES_ITEM

-- Deduplication: Keep latest record per TRANSACTION_LINE_ID
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY TRANSACTION_LINE_ID 
    ORDER BY __LOAD_TS DESC, __ROW_NUMBER DESC
) = 1;
