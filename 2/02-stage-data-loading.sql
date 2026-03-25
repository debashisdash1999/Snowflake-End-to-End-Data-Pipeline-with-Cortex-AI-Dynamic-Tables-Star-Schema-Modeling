DROP FILE FORMAT IF EXISTS SALES_DEV.COMMON.CSV_FORMAT;

DROP STAGE IF EXISTS SALES_DEV.BRONZE.SALES_ANALYTICS_STAGE;

-- File Format for CSV files with header row
CREATE OR REPLACE FILE FORMAT SALES_DEV.COMMON.CSV_FORMAT
    TYPE = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1
    COMMENT = 'CSV file format with header row support';
    
-- internal stage for csv file uploads in bronze layer
CREATE OR REPLACE STAGE SALES_DEV.BRONZE.SALES_ANALYTICS_STAGE
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Internal stage for uploading sales analytics CSV files';

-- =====================================================
-- REGION_MASTER Table
-- =====================================================
CREATE OR REPLACE TABLE SALES_DEV.BRONZE.REGION_MASTER (
    REGION_CODE             VARCHAR(10)     COMMENT 'Unique region identifier code',
    REGION_NAME             VARCHAR(100)    COMMENT 'Full name of the region',
    IS_ACTIVE               VARCHAR(1)      COMMENT 'Active status flag (Y/N)',
    EFFECTIVE_START_DATE    DATE            COMMENT 'Date when region became active',
    EFFECTIVE_END_DATE      DATE            COMMENT 'Date when region becomes inactive',
    CREATED_AT              TIMESTAMP_NTZ   COMMENT 'Record creation timestamp from source',
    SOURCE_SYSTEM           VARCHAR(50)     COMMENT 'Originating source system name',
    __FILE_NAME             VARCHAR(500)    COMMENT 'Source file name from stage',
    __ROW_NUMBER            NUMBER          COMMENT 'Row number within source file',
    __LOAD_TS               TIMESTAMP_NTZ   COMMENT 'Timestamp when record was loaded'
) COMMENT = 'Bronze layer region master data containing geographic region definitions';

-- =====================================================
-- CURRENCY_MASTER Table
-- =====================================================
CREATE OR REPLACE TABLE SALES_DEV.BRONZE.CURRENCY_MASTER (
    CURRENCY_CODE           VARCHAR(3)      COMMENT 'ISO 4217 currency code',
    CURRENCY_NAME           VARCHAR(100)    COMMENT 'Full currency name',
    CURRENCY_SYMBOL         VARCHAR(5)      COMMENT 'Currency display symbol',
    MINOR_UNIT              NUMBER(1)       COMMENT 'Number of decimal places',
    IS_ACTIVE               VARCHAR(1)      COMMENT 'Active status flag (Y/N)',
    EFFECTIVE_START_DATE    DATE            COMMENT 'Date when currency became active',
    EFFECTIVE_END_DATE      DATE            COMMENT 'Date when currency becomes inactive',
    CREATED_AT              TIMESTAMP_NTZ   COMMENT 'Record creation timestamp from source',
    SOURCE_SYSTEM           VARCHAR(50)     COMMENT 'Originating source system name',
    __FILE_NAME             VARCHAR(500)    COMMENT 'Source file name from stage',
    __ROW_NUMBER            NUMBER          COMMENT 'Row number within source file',
    __LOAD_TS               TIMESTAMP_NTZ   COMMENT 'Timestamp when record was loaded'
) COMMENT = 'Bronze layer currency master data with exchange rate reference information';

-- =====================================================
-- TAX_MASTER Table
-- =====================================================
CREATE OR REPLACE TABLE SALES_DEV.BRONZE.TAX_MASTER (
    TAX_CODE                VARCHAR(20)     COMMENT 'Unique tax configuration code',
    TAX_TYPE                VARCHAR(20)     COMMENT 'Type of tax (VAT, SALES_TAX, etc.)',
    TAX_RATE                NUMBER(5,4)     COMMENT 'Tax rate as decimal (e.g., 0.07 for 7%)',
    TAX_INCLUSIVE_FLAG      VARCHAR(1)      COMMENT 'Whether prices include tax (Y/N)',
    EFFECTIVE_START_DATE    DATE            COMMENT 'Date when tax rate became effective',
    EFFECTIVE_END_DATE      DATE            COMMENT 'Date when tax rate expires',
    IS_ACTIVE               VARCHAR(1)      COMMENT 'Active status flag (Y/N)',
    CREATED_AT              TIMESTAMP_NTZ   COMMENT 'Record creation timestamp from source',
    SOURCE_SYSTEM           VARCHAR(50)     COMMENT 'Originating source system name',
    __FILE_NAME             VARCHAR(500)    COMMENT 'Source file name from stage',
    __ROW_NUMBER            NUMBER          COMMENT 'Row number within source file',
    __LOAD_TS               TIMESTAMP_NTZ   COMMENT 'Timestamp when record was loaded'
) COMMENT = 'Bronze layer tax master data containing tax rates and configurations by region';

-- =====================================================
-- COUNTRY_MASTER Table
-- =====================================================
CREATE OR REPLACE TABLE SALES_DEV.BRONZE.COUNTRY_MASTER (
    COUNTRY_CODE            VARCHAR(3)      COMMENT 'ISO 3166-1 alpha-2 country code',
    COUNTRY_NAME            VARCHAR(100)    COMMENT 'Full country name',
    REGION_CODE             VARCHAR(10)     COMMENT 'Reference to parent region',
    CURRENCY_CODE           VARCHAR(3)      COMMENT 'Default currency for the country',
    TAX_CODE                VARCHAR(20)     COMMENT 'Default tax configuration code',
    PRIMARY_LANGUAGE        VARCHAR(50)     COMMENT 'Primary language spoken',
    TIMEZONE                VARCHAR(50)     COMMENT 'Primary timezone identifier',
    ECOMMERCE_SUPPORTED     VARCHAR(1)      COMMENT 'E-commerce availability flag (Y/N)',
    RETAIL_STORE_SUPPORTED  VARCHAR(1)      COMMENT 'Physical store availability flag (Y/N)',
    MARKET_TIER             VARCHAR(10)     COMMENT 'Market classification tier',
    __FILE_NAME             VARCHAR(500)    COMMENT 'Source file name from stage',
    __ROW_NUMBER            NUMBER          COMMENT 'Row number within source file',
    __LOAD_TS               TIMESTAMP_NTZ   COMMENT 'Timestamp when record was loaded'
) COMMENT = 'Bronze layer country master data with market and operational attributes';

-- =====================================================
-- Data Loading - REGION_MASTER
-- =====================================================
COPY INTO SALES_DEV.BRONZE.REGION_MASTER
FROM (
    SELECT 
        $1, $2, $3, $4, $5, $6, $7,
        METADATA$FILENAME,
        METADATA$FILE_ROW_NUMBER,
        CURRENT_TIMESTAMP()
    FROM @SALES_DEV.BRONZE.SALES_ANALYTICS_STAGE/initial-load/country-master/region_master.csv
)
FILE_FORMAT = SALES_DEV.COMMON.CSV_FORMAT;

-- =====================================================
-- Data Loading - CURRENCY_MASTER
-- =====================================================
COPY INTO SALES_DEV.BRONZE.CURRENCY_MASTER
FROM (
    SELECT 
        $1, $2, $3, $4, $5, $6, $7, $8, $9,
        METADATA$FILENAME,
        METADATA$FILE_ROW_NUMBER,
        CURRENT_TIMESTAMP()
    FROM @SALES_DEV.BRONZE.SALES_ANALYTICS_STAGE/initial-load/country-master/currency_master.csv
)
FILE_FORMAT = SALES_DEV.COMMON.CSV_FORMAT;

-- =====================================================
-- Data Loading - TAX_MASTER
-- =====================================================
COPY INTO SALES_DEV.BRONZE.TAX_MASTER
FROM (
    SELECT 
        $1, $2, $3, $4, $5, $6, $7, $8, $9,
        METADATA$FILENAME,
        METADATA$FILE_ROW_NUMBER,
        CURRENT_TIMESTAMP()
    FROM @SALES_DEV.BRONZE.SALES_ANALYTICS_STAGE/initial-load/country-master/tax_master.csv
)
FILE_FORMAT = SALES_DEV.COMMON.CSV_FORMAT;

-- =====================================================
-- Data Loading - COUNTRY_MASTER
-- =====================================================
COPY INTO SALES_DEV.BRONZE.COUNTRY_MASTER
FROM (
    SELECT 
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
        METADATA$FILENAME,
        METADATA$FILE_ROW_NUMBER,
        CURRENT_TIMESTAMP()
    FROM @SALES_DEV.BRONZE.SALES_ANALYTICS_STAGE/initial-load/country-master/country_master.csv
)
FILE_FORMAT = SALES_DEV.COMMON.CSV_FORMAT;

-- =====================================================
-- PRODUCT_CATEGORY_MASTER Table
-- =====================================================
CREATE OR REPLACE TABLE SALES_DEV.BRONZE.PRODUCT_CATEGORY_MASTER (
    CATEGORY_CODE           VARCHAR(10)     COMMENT 'Unique product category identifier',
    CATEGORY_NAME           VARCHAR(100)    COMMENT 'Full category name',
    REPORTING_SEGMENT       VARCHAR(50)     COMMENT 'Financial reporting segment classification',
    IS_ACTIVE               VARCHAR(1)      COMMENT 'Active status flag (Y/N)',
    EFFECTIVE_START_DATE    DATE            COMMENT 'Date when category became active',
    EFFECTIVE_END_DATE      DATE            COMMENT 'Date when category becomes inactive',
    CREATED_AT              TIMESTAMP_NTZ   COMMENT 'Record creation timestamp from source',
    SOURCE_SYSTEM           VARCHAR(50)     COMMENT 'Originating source system name',
    __FILE_NAME             VARCHAR(500)    COMMENT 'Source file name from stage',
    __ROW_NUMBER            NUMBER          COMMENT 'Row number within source file',
    __LOAD_TS               TIMESTAMP_NTZ   COMMENT 'Timestamp when record was loaded'
) COMMENT = 'Bronze layer product category master defining top-level product classifications';

-- =====================================================
-- PRODUCT_FAMILY_MASTER Table
-- =====================================================
CREATE OR REPLACE TABLE SALES_DEV.BRONZE.PRODUCT_FAMILY_MASTER (
    FAMILY_CODE             VARCHAR(20)     COMMENT 'Unique product family identifier',
    FAMILY_NAME             VARCHAR(100)    COMMENT 'Full product family name',
    CATEGORY_CODE           VARCHAR(10)     COMMENT 'Parent category reference',
    LAUNCH_YEAR             NUMBER(4)       COMMENT 'Year when family was launched',
    IS_ACTIVE               VARCHAR(1)      COMMENT 'Active status flag (Y/N)',
    LIFECYCLE_STATUS        VARCHAR(20)     COMMENT 'Current product lifecycle stage',
    CREATED_AT              TIMESTAMP_NTZ   COMMENT 'Record creation timestamp from source',
    SOURCE_SYSTEM           VARCHAR(50)     COMMENT 'Originating source system name',
    __FILE_NAME             VARCHAR(500)    COMMENT 'Source file name from stage',
    __ROW_NUMBER            NUMBER          COMMENT 'Row number within source file',
    __LOAD_TS               TIMESTAMP_NTZ   COMMENT 'Timestamp when record was loaded'
) COMMENT = 'Bronze layer product family master grouping related product models by generation';

-- =====================================================
-- PRODUCT_MODEL_MASTER Table
-- =====================================================
CREATE OR REPLACE TABLE SALES_DEV.BRONZE.PRODUCT_MODEL_MASTER (
    MODEL_CODE              VARCHAR(20)     COMMENT 'Unique product model identifier',
    MODEL_NAME              VARCHAR(100)    COMMENT 'Full product model name',
    FAMILY_CODE             VARCHAR(20)     COMMENT 'Parent product family reference',
    LAUNCH_DATE             DATE            COMMENT 'Global launch date',
    DISCONTINUE_DATE        DATE            COMMENT 'Date when model was discontinued',
    LIFECYCLE_STATUS        VARCHAR(20)     COMMENT 'Current product lifecycle stage',
    IS_ACTIVE               VARCHAR(1)      COMMENT 'Active status flag (Y/N)',
    CREATED_AT              TIMESTAMP_NTZ   COMMENT 'Record creation timestamp from source',
    SOURCE_SYSTEM           VARCHAR(50)     COMMENT 'Originating source system name',
    __FILE_NAME             VARCHAR(500)    COMMENT 'Source file name from stage',
    __ROW_NUMBER            NUMBER          COMMENT 'Row number within source file',
    __LOAD_TS               TIMESTAMP_NTZ   COMMENT 'Timestamp when record was loaded'
) COMMENT = 'Bronze layer product model master containing specific product model definitions';

-- =====================================================
-- PRODUCT_SKU_MASTER Table
-- =====================================================
CREATE OR REPLACE TABLE SALES_DEV.BRONZE.PRODUCT_SKU_MASTER (
    SKU_CODE                VARCHAR(30)     COMMENT 'Unique stock keeping unit identifier',
    MODEL_CODE              VARCHAR(20)     COMMENT 'Parent product model reference',
    VARIANT                 VARCHAR(50)     COMMENT 'SKU variant description (storage/color)',
    PRICE_TIER              VARCHAR(20)     COMMENT 'Pricing tier classification',
    GLOBAL_LAUNCH_DATE      DATE            COMMENT 'Global SKU launch date',
    IS_ACTIVE               VARCHAR(1)      COMMENT 'Active status flag (Y/N)',
    CREATED_AT              TIMESTAMP_NTZ   COMMENT 'Record creation timestamp from source',
    SOURCE_SYSTEM           VARCHAR(50)     COMMENT 'Originating source system name',
    __FILE_NAME             VARCHAR(500)    COMMENT 'Source file name from stage',
    __ROW_NUMBER            NUMBER          COMMENT 'Row number within source file',
    __LOAD_TS               TIMESTAMP_NTZ   COMMENT 'Timestamp when record was loaded'
) COMMENT = 'Bronze layer product SKU master with sellable item variants and pricing tiers';

-- =====================================================
-- PRODUCT_COUNTRY_AVAILABILITY Table
-- =====================================================
CREATE OR REPLACE TABLE SALES_DEV.BRONZE.PRODUCT_COUNTRY_AVAILABILITY (
    SKU_CODE                VARCHAR(30)     COMMENT 'Reference to product SKU',
    COUNTRY_CODE            VARCHAR(3)      COMMENT 'Country where SKU is available',
    LOCAL_LAUNCH_DATE       DATE            COMMENT 'Country-specific launch date',
    LOCAL_DISCONTINUE_DATE  DATE            COMMENT 'Country-specific discontinue date',
    IS_AVAILABLE            VARCHAR(1)      COMMENT 'Availability status flag (Y/N)',
    CREATED_AT              TIMESTAMP_NTZ   COMMENT 'Record creation timestamp from source',
    SOURCE_SYSTEM           VARCHAR(50)     COMMENT 'Originating source system name',
    __FILE_NAME             VARCHAR(500)    COMMENT 'Source file name from stage',
    __ROW_NUMBER            NUMBER          COMMENT 'Row number within source file',
    __LOAD_TS               TIMESTAMP_NTZ   COMMENT 'Timestamp when record was loaded'
) COMMENT = 'Bronze layer product availability by country with local launch dates';

-- =====================================================
-- Data Loading - PRODUCT_CATEGORY_MASTER
-- =====================================================
COPY INTO SALES_DEV.BRONZE.PRODUCT_CATEGORY_MASTER
FROM (
    SELECT $1,$2,$3,$4,$5,$6,$7,$8,
        METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, CURRENT_TIMESTAMP()
    FROM @SALES_DEV.BRONZE.SALES_ANALYTICS_STAGE/initial-load/product-master/product_category_master.csv
)
FILE_FORMAT = SALES_DEV.COMMON.CSV_FORMAT;

-- =====================================================
-- Data Loading - PRODUCT_FAMILY_MASTER
-- =====================================================
COPY INTO SALES_DEV.BRONZE.PRODUCT_FAMILY_MASTER
FROM (
    SELECT $1,$2,$3,$4,$5,$6,$7,$8,
        METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, CURRENT_TIMESTAMP()
    FROM @SALES_DEV.BRONZE.SALES_ANALYTICS_STAGE/initial-load/product-master/product_family_master.csv
)
FILE_FORMAT = SALES_DEV.COMMON.CSV_FORMAT;

-- =====================================================
-- Data Loading - PRODUCT_MODEL_MASTER
-- =====================================================
COPY INTO SALES_DEV.BRONZE.PRODUCT_MODEL_MASTER
FROM (
    SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,
        METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, CURRENT_TIMESTAMP()
    FROM @SALES_DEV.BRONZE.SALES_ANALYTICS_STAGE/initial-load/product-master/product_model_master.csv
)
FILE_FORMAT = SALES_DEV.COMMON.CSV_FORMAT;

-- =====================================================
-- Data Loading - PRODUCT_SKU_MASTER
-- =====================================================
COPY INTO SALES_DEV.BRONZE.PRODUCT_SKU_MASTER
FROM (
    SELECT $1,$2,$3,$4,$5,$6,$7,$8,
        METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, CURRENT_TIMESTAMP()
    FROM @SALES_DEV.BRONZE.SALES_ANALYTICS_STAGE/initial-load/product-master/product_sku_master.csv
)
FILE_FORMAT = SALES_DEV.COMMON.CSV_FORMAT;

-- =====================================================
-- Data Loading - PRODUCT_COUNTRY_AVAILABILITY
-- =====================================================
COPY INTO SALES_DEV.BRONZE.PRODUCT_COUNTRY_AVAILABILITY
FROM (
    SELECT $1,$2,$3,$4,$5,$6,$7,
        METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, CURRENT_TIMESTAMP()
    FROM @SALES_DEV.BRONZE.SALES_ANALYTICS_STAGE/initial-load/product-master/product_country_availability.csv
)
FILE_FORMAT = SALES_DEV.COMMON.CSV_FORMAT;

-- =====================================================
-- STORE_MASTER Table
-- =====================================================
CREATE OR REPLACE TABLE SALES_DEV.BRONZE.STORE_MASTER (
    STORE_CODE              VARCHAR(20)     COMMENT 'Unique store identifier code',
    STORE_NAME              VARCHAR(100)    COMMENT 'Store display name',
    COUNTRY_CODE            VARCHAR(3)      COMMENT 'Country where store is located',
    REGION_CODE             VARCHAR(10)     COMMENT 'Geographic region reference',
    TAX_JURISDICTION_CODE   VARCHAR(20)     COMMENT 'Tax jurisdiction for the store location',
    FORMAT_CODE             VARCHAR(10)     COMMENT 'Store format type (MALL/MINI/etc.)',
    CITY                    VARCHAR(100)    COMMENT 'City where store is located',
    STATE_CODE              VARCHAR(10)     COMMENT 'State or province code',
    POSTAL_CODE             VARCHAR(20)     COMMENT 'Postal or ZIP code',
    ADDRESS_LINE1           VARCHAR(200)    COMMENT 'Street address',
    LATITUDE                NUMBER(10,6)    COMMENT 'Geographic latitude coordinate',
    LONGITUDE               NUMBER(10,6)    COMMENT 'Geographic longitude coordinate',
    STORE_OPEN_DATE         DATE            COMMENT 'Date store opened for business',
    STORE_CLOSE_DATE        DATE            COMMENT 'Date store closed (if applicable)',
    LIFECYCLE_STATUS        VARCHAR(20)     COMMENT 'Current store operational status',
    FLOOR_AREA_SQFT         NUMBER(10)      COMMENT 'Store floor area in square feet',
    ANNUAL_RENT_USD         NUMBER(12)      COMMENT 'Annual rent amount in USD',
    IS_ACTIVE               VARCHAR(1)      COMMENT 'Active status flag (Y/N)',
    EFFECTIVE_START_DATE    DATE            COMMENT 'Record effective start date',
    EFFECTIVE_END_DATE      DATE            COMMENT 'Record effective end date',
    __FILE_NAME             VARCHAR(500)    COMMENT 'Source file name from stage',
    __ROW_NUMBER            NUMBER          COMMENT 'Row number within source file',
    __LOAD_TS               TIMESTAMP_NTZ   COMMENT 'Timestamp when record was loaded'
) COMMENT = 'Bronze layer store master data with location and operational attributes';

-- =====================================================
-- Data Loading - STORE_MASTER
-- =====================================================
COPY INTO SALES_DEV.BRONZE.STORE_MASTER
FROM (
    SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,
        METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, CURRENT_TIMESTAMP()
    FROM @SALES_DEV.BRONZE.SALES_ANALYTICS_STAGE/initial-load/store-master/store_master.csv
)
FILE_FORMAT = SALES_DEV.COMMON.CSV_FORMAT;

-- =====================================================
-- CUSTOMER_MASTER Table
-- =====================================================
CREATE OR REPLACE TABLE SALES_DEV.BRONZE.CUSTOMER_MASTER (
    CUSTOMER_ID             VARCHAR(50)     COMMENT 'Unique customer UUID identifier',
    CUSTOMER_NUMBER         VARCHAR(20)     COMMENT 'Business customer reference number',
    FIRST_NAME              VARCHAR(100)    COMMENT 'Customer first name',
    LAST_NAME               VARCHAR(100)    COMMENT 'Customer last name',
    FULL_NAME               VARCHAR(200)    COMMENT 'Customer full name',
    GENDER                  VARCHAR(20)     COMMENT 'Customer gender',
    DATE_OF_BIRTH           DATE            COMMENT 'Customer date of birth',
    EMAIL                   VARCHAR(200)    COMMENT 'Customer email address',
    PHONE_NUMBER            VARCHAR(50)     COMMENT 'Customer phone number',
    STREET_ADDRESS          VARCHAR(300)    COMMENT 'Customer street address',
    CITY                    VARCHAR(100)    COMMENT 'Customer city',
    STATE_PROVINCE          VARCHAR(100)    COMMENT 'Customer state or province',
    POSTAL_CODE             VARCHAR(20)     COMMENT 'Customer postal code',
    COUNTRY_CODE            VARCHAR(3)      COMMENT 'Customer country code',
    COUNTRY_NAME            VARCHAR(100)    COMMENT 'Customer country name',
    REGION                  VARCHAR(10)     COMMENT 'Geographic region reference',
    PREFERRED_LANGUAGE      VARCHAR(50)     COMMENT 'Customer preferred language',
    CUSTOMER_SEGMENT        VARCHAR(50)     COMMENT 'Customer segment classification',
    LOYALTY_TIER            VARCHAR(20)     COMMENT 'Customer loyalty program tier',
    REGISTRATION_DATE       DATE            COMMENT 'Date customer registered',
    IS_ACTIVE               BOOLEAN         COMMENT 'Active customer flag',
    SOURCE_SYSTEM           VARCHAR(50)     COMMENT 'Originating source system name',
    RECORD_SOURCE           VARCHAR(50)     COMMENT 'Channel where record originated',
    CREATED_AT              DATE            COMMENT 'Record creation date from source',
    UPDATED_AT              TIMESTAMP_NTZ   COMMENT 'Record last update timestamp',
    __FILE_NAME             VARCHAR(500)    COMMENT 'Source file name from stage',
    __ROW_NUMBER            NUMBER          COMMENT 'Row number within source file',
    __LOAD_TS               TIMESTAMP_NTZ   COMMENT 'Timestamp when record was loaded'
) COMMENT = 'Bronze layer customer master data with demographic and loyalty attributes';

-- =====================================================
-- Data Loading - CUSTOMER_MASTER
-- =====================================================
COPY INTO SALES_DEV.BRONZE.CUSTOMER_MASTER
FROM (
    SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,
        METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, CURRENT_TIMESTAMP()
    FROM @SALES_DEV.BRONZE.SALES_ANALYTICS_STAGE/initial-load/customer-master/customer_master.csv
)
FILE_FORMAT = SALES_DEV.COMMON.CSV_FORMAT;

-- =====================================================
-- SALES_HEADER Table
-- =====================================================
CREATE OR REPLACE TABLE SALES_DEV.BRONZE.SALES_HEADER (
    TRANSACTION_ID          VARCHAR(50)     COMMENT 'Unique transaction UUID identifier',
    TRANSACTION_NUMBER      VARCHAR(20)     COMMENT 'Business transaction reference number',
    TRANSACTION_TIMESTAMP   DATE            COMMENT 'Date and time of transaction',
    CUSTOMER_ID             VARCHAR(50)     COMMENT 'Reference to customer master',
    STORE_ID                VARCHAR(20)     COMMENT 'Reference to store master',
    CHANNEL_ID              VARCHAR(10)     COMMENT 'Sales channel identifier (POS/WEB/etc.)',
    PAYMENT_METHOD          VARCHAR(50)     COMMENT 'Payment method used',
    CURRENCY                VARCHAR(3)      COMMENT 'Transaction currency code',
    GROSS_AMOUNT            NUMBER(12,2)    COMMENT 'Total amount before discounts and tax',
    TOTAL_DISCOUNT          NUMBER(12,2)    COMMENT 'Total discount applied',
    TOTAL_TAX               NUMBER(12,2)    COMMENT 'Total tax amount',
    NET_TOTAL               NUMBER(12,2)    COMMENT 'Final transaction amount',
    CREATED_AT              DATE            COMMENT 'Record creation date from source',
    __FILE_NAME             VARCHAR(500)    COMMENT 'Source file name from stage',
    __ROW_NUMBER            NUMBER          COMMENT 'Row number within source file',
    __LOAD_TS               TIMESTAMP_NTZ   COMMENT 'Timestamp when record was loaded'
) COMMENT = 'Bronze layer sales transaction header with payment and totals';

-- =====================================================
-- SALES_ITEM Table
-- =====================================================
CREATE OR REPLACE TABLE SALES_DEV.BRONZE.SALES_ITEM (
    TRANSACTION_LINE_ID     VARCHAR(20)     COMMENT 'Unique line item identifier',
    TRANSACTION_ID          VARCHAR(50)     COMMENT 'Reference to sales header',
    SKU_CODE                VARCHAR(30)     COMMENT 'Reference to product SKU',
    QUANTITY                NUMBER(10)      COMMENT 'Quantity purchased',
    UNIT_PRICE              NUMBER(12,2)    COMMENT 'Price per unit',
    DISCOUNT_AMOUNT         NUMBER(12,2)    COMMENT 'Discount applied to line',
    TAX_AMOUNT              NUMBER(12,2)    COMMENT 'Tax amount for line',
    LINE_TOTAL              NUMBER(12,2)    COMMENT 'Total amount for line item',
    CREATED_AT              DATE            COMMENT 'Record creation date from source',
    __FILE_NAME             VARCHAR(500)    COMMENT 'Source file name from stage',
    __ROW_NUMBER            NUMBER          COMMENT 'Row number within source file',
    __LOAD_TS               TIMESTAMP_NTZ   COMMENT 'Timestamp when record was loaded'
) COMMENT = 'Bronze layer sales transaction line items with product and pricing details';

-- =====================================================
-- Data Loading - SALES_HEADER
-- =====================================================
COPY INTO SALES_DEV.BRONZE.SALES_HEADER
FROM (
    SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,
        METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, CURRENT_TIMESTAMP()
    FROM @SALES_DEV.BRONZE.SALES_ANALYTICS_STAGE/initial-load/sales-transaction/sales_header.csv
)
FILE_FORMAT = SALES_DEV.COMMON.CSV_FORMAT;

-- =====================================================
-- Data Loading - SALES_ITEM
-- =====================================================
COPY INTO SALES_DEV.BRONZE.SALES_ITEM
FROM (
    SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,
        METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, CURRENT_TIMESTAMP()
    FROM @SALES_DEV.BRONZE.SALES_ANALYTICS_STAGE/initial-load/sales-transaction/sales_item.csv
)
FILE_FORMAT = SALES_DEV.COMMON.CSV_FORMAT;
