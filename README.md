# ❄️ Snowflake Cortex Code (CoCo) — Apple Retail Sales Data Platform

> A fully automated, AI-assisted, medallion-based Snowflake data platform with star schema modeling and natural language analytics capability.

---

## 📌 Project Summary

This project is an end-to-end **Snowflake-native data engineering pipeline** built for an **Apple Retail Sales** domain. It follows the **Medallion Architecture** (Bronze → Silver → Gold) and leverages **Snowflake Cortex Code (CoCo)** — an AI assistant built into Snowflake — to accelerate DDL generation, transformation logic, dimensional modeling, and semantic layer creation.

The pipeline ingests raw CSV data, cleanses and validates it, builds a star schema with SCD Type 2 dimensions and dual-grain fact tables, pre-computes aggregations at daily/weekly/monthly grains, and exposes the data for **natural language querying** through a Cortex Analyst Semantic Layer.

---

## 🏗️ Architecture Overview

```
13 CSV Files (Source)
      │
      ▼
┌──────────────────────────────────────────────────────────────┐
│                 Snowflake Cloud Data Platform                 │
│                                                              │
│  ┌──────────┐    ┌──────────┐    ┌────────────────────────┐ │
│  │  BRONZE  │───▶│  SILVER  │───▶│         GOLD           │ │
│  │  Schema  │    │  Schema  │    │        Schema          │ │
│  │          │    │          │    │                        │ │
│  │ 13 Raw   │    │ 13 Clean │    │ 5 Dims + 2 Facts +     │ │
│  │ Tables   │    │  Tables  │    │ 1 Bridge + 3 Agg Facts │ │
│  └──────────┘    └──────────┘    └────────────────────────┘ │
│                                             │                │
│               ┌─────────────────────────────┘                │
│               ▼                                              │
│  ┌───────────────────────┐                                   │
│  │    SEMANTIC LAYER     │  ◀── Natural Language Query       │
│  │   (Cortex Analyst)    │                                   │
│  └───────────────────────┘                                   │
│                                                              │
│  ┌──────────┐                                               │
│  │  COMMON  │  (File Formats, Utilities)                    │
│  │  Schema  │                                               │
│  └──────────┘                                               │
└──────────────────────────────────────────────────────────────┘
```

---

## 🧰 Technology Stack

| Component | Technology |
|---|---|
| Cloud Data Platform | Snowflake |
| Storage / Ingestion | Snowflake Internal Stage + COPY INTO |
| Transformation Engine | Dynamic Tables (Incremental + Full) |
| Data Modeling | Star Schema — SCD Type 2 |
| AI Development Assistant | Snowflake Cortex Code (CoCo) |
| Natural Language Analytics | Snowflake Cortex Analyst |
| Semantic Model | YAML-based Semantic Model |
| Constraints | Informational NOT ENFORCED PK/FK |
| Version Control | GitHub |
| Environment Management | SALES_DEV → SALES_QA → SALES_PROD |

---

## 📂 Data Domain — Apple Retail Sales

The domain mirrors Apple's real-world product and business structure.

### Source Entity Groups (13 CSV Files)

| Group | Entities |
|---|---|
| Geographic / Reference | Region, Country, Currency, Tax |
| Product Hierarchy | Product Category → Family → Model → SKU, Product Country Availability |
| Store Master | Store information (name, format, location, lat/long, floor area) |
| Customer Master | Customer demographics, segment, loyalty tier (50,000 customers) |
| Sales Transactions | Sales Header (86,107 transactions), Sales Items (86,107 line items) |

### Product Hierarchy
```
Category  (iPhone, iPad, Mac, Apple Watch, AirPods, Services, Accessories...)
    └── Family  (MacBook Air, MacBook Pro, iMac, Mac mini...)
            └── Model  (specific product model)
                    └── SKU  (variant: storage + colour, price tier, launch date)
```

---

## 🌍 Environment Strategy

| | SALES_DEV | SALES_QA | SALES_PROD |
|---|---|---|---|
| Type | TRANSIENT | TRANSIENT | PERMANENT |
| Time Travel | 1 day | 1 day | 7 days |
| Fail-safe | None | None | Full |
| Purpose | Development | Testing | Production |

> TRANSIENT databases have no Fail-safe storage period — eliminating unnecessary storage costs in non-production environments. All four schemas (BRONZE, SILVER, GOLD, COMMON) exist in each database.

---

## 🔄 Pipeline Layers

### 🟫 Bronze Layer — Raw Ingestion

- 13 CSV files uploaded to **Snowflake Internal Stage** (`SALES_ANALYTICS_STAGE`) via Snowsight
- **COPY INTO** loads data into Bronze tables with positional column mapping
- Every record enriched with three audit metadata columns:

| Column | Source | Purpose |
|---|---|---|
| `__FILE_NAME` | `METADATA$FILENAME` | Tracks exact source file per row |
| `__ROW_NUMBER` | `METADATA$FILE_ROW_NUMBER` | Row position within source file |
| `__LOAD_TS` | `CURRENT_TIMESTAMP()` | Load timestamp — used for deduplication downstream |

- **Append-only** — raw data never modified, full source fidelity preserved
- All tables are **TRANSIENT** in Dev and QA

---

### 🩶 Silver Layer — Cleansing & Transformation

Built entirely using **Snowflake Dynamic Tables** — a modern alternative to Streams + Tasks.

**Configuration applied to all 13 Silver Dynamic Tables:**
```sql
TARGET_LAG    = DOWNSTREAM   -- refreshes only when Gold needs updated data
REFRESH_MODE  = INCREMENTAL  -- processes only new/changed Bronze records
INITIALIZE    = ON_CREATE    -- populated immediately on creation
WAREHOUSE     = COMPUTE_WH
```

**Three key transformations applied in every Silver table:**

**1. Deduplication** — keeps only the latest version of each record:
```sql
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY <business_key>
    ORDER BY __LOAD_TS DESC, __ROW_NUMBER DESC
) = 1
```

> `PRODUCT_COUNTRY_AVAILABILITY` uses a **composite key** `(SKU_CODE, COUNTRY_CODE)` — neither column alone is unique.

**2. Data Quality Flag** — entity-specific validation rules per table:
```sql
CASE
    WHEN <business_key> IS NULL THEN FALSE
    WHEN <domain_rule_violated> THEN FALSE
    ELSE TRUE
END AS IS_VALID_RECORD
```

Examples: TAX_RATE validated between 0–1, STORE lat/lon validated within geographic bounds, CUSTOMER email validated with `LIKE '%@%.%'`, SALES_ITEM QUANTITY validated `> 0`.

**3. Audit column renaming** — `__ROW_NUMBER → __SOURCE_ROW_NUMBER`, `__LOAD_TS → __BRONZE_LOAD_TS`

Only records where `IS_VALID_RECORD = TRUE` flow into the Gold layer.

---

### 🥇 Gold Layer — Business Data Model

Star schema with **SCD Type 2** dimensions, dual-grain fact tables, a bridge table, and pre-computed aggregations. All Gold tables are **Dynamic Tables**.

#### Surrogate Key Pattern (SHA2 Hash)
```sql
SHA2(CONCAT(
    COALESCE(business_key, ''),
    COALESCE(TO_VARCHAR(__BRONZE_LOAD_TS, 'YYYY-MM-DD HH24:MI:SS.FF6'), '')
), 256) AS <entity>_DIM_KEY
```
Deterministic, no sequence objects needed, naturally supports SCD Type 2 versioning.

#### SCD Type 2 Columns (on all 5 dimensions)
```sql
EFFECTIVE_START_TS  TIMESTAMP_NTZ        -- when this version became active
EFFECTIVE_END_TS    TIMESTAMP_NTZ        -- 9999-12-31 for current records
IS_CURRENT          BOOLEAN              -- TRUE = active version
```

#### Dimension Tables

| Dimension | Source | Key Attributes | Rows |
|---|---|---|---|
| `DIM_COUNTRY` | Joins 4 Silver tables (Country + Region + Currency + Tax) | Region, currency symbol, tax type/rate, market tier | 5 |
| `DIM_PRODUCT` | Joins 4 Silver tables (SKU → Model → Family → Category) | Full hierarchy, lifecycle status, price tier, reporting segment | 53 |
| `DIM_STORE` | Silver Store Master | Format (FLAGSHIP/MALL/MINI), lat/long, floor area, annual rent | 80 |
| `DIM_CUSTOMER` | Silver Customer Master | Segment (Consumer/Business), loyalty tier (None/Silver/Gold/Platinum) | 50,000 |
| `DIM_DATE` | Derived from Silver Sales Header dates | Year, quarter, month, week, fiscal year/quarter, weekend flag | 3,443 |

> `DIM_DATE` and both fact tables use `REFRESH_MODE = FULL` — SELECT DISTINCT and complex joins cannot be processed incrementally by Snowflake.

#### Bridge Table

`BRIDGE_PRODUCT_COUNTRY` resolves the **many-to-many** relationship between products and countries — avoiding dimension fan-out and data duplication.

| Column | Purpose |
|---|---|
| `BRIDGE_KEY` | Unique hash key for this bridge record |
| `PRODUCT_DIM_KEY` | FK to DIM_PRODUCT |
| `COUNTRY_DIM_KEY` | FK to DIM_COUNTRY |
| `LOCAL_LAUNCH_DATE` | Country-specific SKU launch date |
| `IS_AVAILABLE` | Availability flag in this country |

**212 rows** (53 SKUs × average 4 countries each)

#### Fact Tables

**`FACT_SALES_HEADER`** — Grain: one row per transaction (86,107 rows)

| | Columns |
|---|---|
| Dimension FKs | CUSTOMER_DIM_KEY, STORE_DIM_KEY, COUNTRY_DIM_KEY, DATE_DIM_KEY |
| Degenerate Dims | TRANSACTION_ID, TRANSACTION_NUMBER, CHANNEL_ID (POS/WEB), PAYMENT_METHOD, CURRENCY |
| Measures | GROSS_AMOUNT, TOTAL_DISCOUNT, TOTAL_TAX, NET_TOTAL, TRANSACTION_COUNT (=1) |

> COUNTRY_DIM_KEY is derived via the store's country — not the customer's address.

**`FACT_SALES_ITEM`** — Grain: one row per line item (86,107 rows)

| | Columns |
|---|---|
| Parent Fact FK | FACT_SALES_HEADER_KEY |
| Inherited FKs | CUSTOMER_DIM_KEY, STORE_DIM_KEY, COUNTRY_DIM_KEY, DATE_DIM_KEY (from header) |
| Product FK | PRODUCT_DIM_KEY |
| Measures | QUANTITY, UNIT_PRICE, DISCOUNT_AMOUNT, TAX_AMOUNT, LINE_TOTAL, LINE_COUNT (=1) |

> Two fact tables by design — Header for transaction/financial analysis, Item for product-level drill-down.

#### Aggregated Fact Tables

| Table | Grain | TARGET_LAG | Extra Measures |
|---|---|---|---|
| `FACT_SALES_DAILY` | Day + Store + Country + Channel | 5 minutes | AVG_TRANSACTION_VALUE, UNIQUE_CUSTOMERS |
| `FACT_SALES_WEEKLY` | Week + Country + Channel | 7 days | ACTIVE_STORES, SELLING_DAYS |
| `FACT_SALES_MONTHLY` | Month + Country + Channel + Fiscal | 30 days | FISCAL_YEAR, FISCAL_QUARTER, ACTIVE_STORES, SELLING_DAYS |

---

## 🔗 Informational Constraints

PK and FK constraints defined on all Gold tables using `NOT ENFORCED` — Snowflake uses these for query optimisation hints and BI tool relationship discovery, without runtime enforcement overhead.

```sql
ALTER DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES_HEADER
ADD CONSTRAINT fk_header_customer
FOREIGN KEY (CUSTOMER_DIM_KEY)
REFERENCES SALES_DEV.GOLD.DIM_CUSTOMER(CUSTOMER_DIM_KEY) NOT ENFORCED;
```

---

## 🤖 Snowflake Cortex Code — AI-Assisted Development

| Stage | What Cortex Code Generated |
|---|---|
| Database & Schema Setup | CREATE DATABASE / SCHEMA DDL with transient + time travel settings + comments |
| Bronze Layer | COPY INTO scripts, file format object, metadata column patterns |
| Silver Layer | Dynamic Table DDL, QUALIFY deduplication logic, IS_VALID_RECORD CASE expressions |
| Gold Dimensions | SCD Type 2 DDL, SHA2 surrogate key patterns, column comments |
| Gold Facts | Fact table DDL, dimension key joins, measure derivations |
| Date Dimension | Full DIM_DATE derivation from Silver Sales Header date range |
| Aggregations | Daily/weekly/monthly aggregated fact table scripts |
| Semantic Layer | Initial YAML semantic model scaffolding |

**Workflow:** Prompt → CoCo generates SQL → Engineer validates & modifies → Integrate into pipeline

---

## 🗣️ Semantic Layer — Cortex Analyst

Natural language querying of the Gold schema — no SQL required. Implemented as a YAML semantic model (`sales_semantic_model.yaml`) uploaded to a Snowflake stage.

### Tables Covered
`dim_customer`, `dim_product`, `dim_store`, `dim_country`, `dim_date`, `fact_sales_header`, `fact_sales_item`

### Key Metrics

| Metric | Expression | Synonyms |
|---|---|---|
| `total_net_sales` | `SUM(NET_TOTAL)` | net sales, net revenue, total sales, sales by region |
| `total_gross_sales` | `SUM(GROSS_AMOUNT)` | gross sales, gross revenue |
| `total_transactions` | `COUNT(DISTINCT TRANSACTION_ID)` | transaction count, order count |
| `average_transaction_value` | `AVG(NET_TOTAL)` | ATV, avg order value, average basket size |
| `total_units_sold` | `SUM(QUANTITY)` | units sold, quantity sold |
| `total_line_revenue` | `SUM(LINE_TOTAL)` | product revenue, revenue by product |

### Verified Queries (Golden SQL)
```
"What are total sales by region?"
"Top 10 products by revenue"
"Show me the monthly sales trend"
"Sales by customer loyalty tier"
"Which stores have the highest sales?"
"Compare POS vs Web channel performance"
"What are sales by product category?"
"What are sales by quarter?"
```

---

## 📊 Key Design Decisions

| Decision | Rationale |
|---|---|
| Dynamic Tables over Streams + Tasks | Built-in incremental processing, no manual stream logic, lower cost |
| TRANSIENT databases for Dev & QA | Eliminates fail-safe storage charges — Dev/QA data is always reproducible |
| 7-day Time Travel for PROD | Maximum recovery window — standard Snowflake best practice for production |
| SHA2 hash surrogate keys | Deterministic (no duplicates on re-run), no sequence objects, SCD2-compatible |
| SCD Type 2 on all 5 dimensions | Preserves historical attribute states for accurate time-based analysis |
| DIM_COUNTRY consolidates 4 Silver tables | Avoids analysts joining 4 tables for every geographic query |
| INNER JOINs in DIM_PRODUCT | Orphaned SKUs excluded from Gold — referential integrity enforced at modeling time |
| LEFT JOINs in DIM_COUNTRY | Country records not dropped if currency/tax reference data is missing |
| FACT_SALES_ITEM inherits FKs from header | Avoids redundant joins — header already resolved all dimension keys |
| Bridge table for Product-Country | Correct many-to-many resolution — no dimension fan-out or measure double-counting |
| NOT ENFORCED constraints | Documents relationships for BI tools and Cortex Analyst without runtime overhead |
| COMMENT on every object and column | Self-documenting schema — critical for team collaboration and Cortex Analyst accuracy |
| Different TARGET_LAG on agg tables | Aligns refresh frequency to business use: 5 min (daily ops) / 7d (weekly) / 30d (monthly exec) |
