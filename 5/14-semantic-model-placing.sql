-- =====================================================
-- Gold Layer - Primary Key and Foreign Key Constraints
-- Informational constraints for query optimization and BI tools
-- =====================================================

-- Note: Snowflake constraints are NOT ENFORCED but provide:
-- 1. Query optimization hints
-- 2. BI tool relationship discovery
-- 3. Self-documenting schema
-- 4. Semantic model metadata

-- =====================================================
-- DIMENSION TABLE PRIMARY KEYS
-- =====================================================
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_CUSTOMER 
ADD CONSTRAINT pk_dim_customer PRIMARY KEY (CUSTOMER_DIM_KEY) NOT ENFORCED;

ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_PRODUCT 
ADD CONSTRAINT pk_dim_product PRIMARY KEY (PRODUCT_DIM_KEY) NOT ENFORCED;

ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_STORE 
ADD CONSTRAINT pk_dim_store PRIMARY KEY (STORE_DIM_KEY) NOT ENFORCED;

ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_COUNTRY 
ADD CONSTRAINT pk_dim_country PRIMARY KEY (COUNTRY_DIM_KEY) NOT ENFORCED;

ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_DATE 
ADD CONSTRAINT pk_dim_date PRIMARY KEY (DATE_DIM_KEY) NOT ENFORCED;

-- =====================================================
-- FACT TABLE PRIMARY KEYS
-- =====================================================
ALTER DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES_HEADER 
ADD CONSTRAINT pk_fact_sales_header PRIMARY KEY (FACT_SALES_HEADER_KEY) NOT ENFORCED;

ALTER DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES_ITEM 
ADD CONSTRAINT pk_fact_sales_item PRIMARY KEY (FACT_SALES_ITEM_KEY) NOT ENFORCED;

-- =====================================================
-- FACT_SALES_HEADER FOREIGN KEYS
-- =====================================================
ALTER DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES_HEADER 
ADD CONSTRAINT fk_header_customer 
FOREIGN KEY (CUSTOMER_DIM_KEY) REFERENCES SALES_DEV.GOLD.DIM_CUSTOMER(CUSTOMER_DIM_KEY) NOT ENFORCED;

ALTER DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES_HEADER 
ADD CONSTRAINT fk_header_store 
FOREIGN KEY (STORE_DIM_KEY) REFERENCES SALES_DEV.GOLD.DIM_STORE(STORE_DIM_KEY) NOT ENFORCED;

ALTER DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES_HEADER 
ADD CONSTRAINT fk_header_country 
FOREIGN KEY (COUNTRY_DIM_KEY) REFERENCES SALES_DEV.GOLD.DIM_COUNTRY(COUNTRY_DIM_KEY) NOT ENFORCED;

ALTER DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES_HEADER 
ADD CONSTRAINT fk_header_date 
FOREIGN KEY (DATE_DIM_KEY) REFERENCES SALES_DEV.GOLD.DIM_DATE(DATE_DIM_KEY) NOT ENFORCED;

-- =====================================================
-- FACT_SALES_ITEM FOREIGN KEYS
-- =====================================================
ALTER DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES_ITEM 
ADD CONSTRAINT fk_item_header 
FOREIGN KEY (FACT_SALES_HEADER_KEY) REFERENCES SALES_DEV.GOLD.FACT_SALES_HEADER(FACT_SALES_HEADER_KEY) NOT ENFORCED;

ALTER DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES_ITEM 
ADD CONSTRAINT fk_item_product 
FOREIGN KEY (PRODUCT_DIM_KEY) REFERENCES SALES_DEV.GOLD.DIM_PRODUCT(PRODUCT_DIM_KEY) NOT ENFORCED;

-- =====================================================
-- Verify Constraints
-- =====================================================
SELECT TABLE_NAME, CONSTRAINT_NAME, CONSTRAINT_TYPE 
FROM SALES_DEV.INFORMATION_SCHEMA.TABLE_CONSTRAINTS 
WHERE TABLE_SCHEMA = 'GOLD' 
ORDER BY TABLE_NAME, CONSTRAINT_TYPE;
