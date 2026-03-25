# ❄️ Snowflake Cortex Code (CoCo) — Apple Retail Sales Data Platform

> A fully automated, AI-assisted, medallion-based Snowflake data platform with star schema modeling and natural language analytics capability.

---

## 📌 Project Summary

This project is an end-to-end **Snowflake-native data engineering pipeline** built for an **Apple Retail Sales** domain. It follows the **Medallion Architecture** (Bronze → Silver → Gold) and leverages **Snowflake Cortex Code (CoCo)** — an AI assistant — to accelerate DDL generation, transformation logic, dimensional modeling, and semantic layer creation.

The pipeline ingests raw CSV data, cleanses and validates it, builds a star schema with SCD Type 2 dimensions and dual-grain fact tables, automates incremental loads via a Task DAG, and exposes the data for **natural language querying** through a Cortex Analyst Semantic Layer.

---

## 🏗️ Architecture Overview

```
CSV Files (Source)
      │
      ▼
┌─────────────────────────────────────────────────────────┐
│               Snowflake Cloud Data Platform              │
│                                                         │
│  ┌──────────┐    ┌──────────┐    ┌──────────────────┐  │
│  │  BRONZE  │───▶│  SILVER  │───▶│      GOLD        │  │
│  │  Schema  │    │  Schema  │    │      Schema      │  │
│  │          │    │          │    │                  │  │
│  │ Raw Data │    │  Clean   │    │ Dims + Facts +   │  │
│  │ Landing  │    │ Curated  │    │ Aggregations     │  │
│  └──────────┘    └──────────┘    └──────────────────┘  │
│                                          │              │
│              ┌───────────────────────────┘              │
│              ▼                                          │
│  ┌──────────────────────┐                              │
│  │   SEMANTIC LAYER     │  ◀── Natural Language Query  │
│  │  (Cortex Analyst)    │                              │
│  └──────────────────────┘                              │
│                                                         │
│  ┌──────────┐                                          │
│  │  COMMON  │  (File Formats, Sequences, Utilities)    │
│  │  Schema  │                                          │
│  └──────────┘                                          │
└─────────────────────────────────────────────────────────┘
```

---

## 🧰 Technology Stack

| Component | Technology |
|---|---|
| Cloud Data Platform | Snowflake |
| Storage / Ingestion | Snowflake Internal Stage + COPY INTO |
| Transformation Engine | Dynamic Tables (Incremental) |
| Orchestration | Task DAG (Root + Child Tasks) |
| Data Modeling | Star Schema — SCD Type 2 |
| AI Development Assistant | Snowflake Cortex Code (CoCo) |
| Natural Language Analytics | Snowflake Cortex Analyst |
| Semantic Model | YAML-based Semantic Layer |
| Version Control | GitHub |
| Environment Management | Dev → QA → Prod (Context Promotion) |

---

## 📂 Data Domain — Apple Retail Sales

The domain is modeled after Apple's real-world product and sales structure.

### Source Entity Groups

| Group | Entities |
|---|---|
| Geographic / Reference | Region, Country, Currency, Tax |
| Product Hierarchy | Product Category → Family → Model → SKU, Product Country Availability |
| Store Master | Store information (name, format, location, status) |
| Customer Master | Customer demographics, segment, loyalty tier |
| Sales Transactions | Sales Header (transaction-level), Sales Items (line-level) |

### Product Hierarchy
```
Category  (iPhone, iPad, Mac, Apple Watch, AirPods, Services...)
    └── Family  (MacBook Air, MacBook Pro, iMac, Mac mini...)
            └── Model  (specific product model)
                    └── SKU  (variant: storage + colour, price tier, launch date)
```

---

## 🔄 Pipeline Layers

### 🟫 Bronze Layer — Raw Ingestion

- CSV files uploaded to **Snowflake Internal Stage** via Snowsight
- **COPY INTO** loads raw data into Bronze tables
- **INFER_SCHEMA** detects column structure automatically — no manual DDL per table
- Every record enriched with audit metadata:

| Column | Source | Purpose |
|---|---|---|
| `__FILE_NAME` | `METADATA$FILENAME` | Tracks source file per row |
| `__ROW_NUMBER` | `METADATA$FILE_ROW_NUMBER` | Row position within file |
| `__LOAD_TS` | `CURRENT_TIMESTAMP` | Load timestamp for incremental tracking |

- **Append-only** — raw data never modified, source fidelity preserved
- Tables are **TRANSIENT** in Dev and QA to eliminate fail-safe storage costs

---

### 🩶 Silver Layer — Cleansing & Transformation

Built entirely using **Snowflake Dynamic Tables** — a modern alternative to the traditional Streams + Tasks pattern.

**Dynamic Table Configuration:**
```sql
REFRESH_MODE = INCREMENTAL
TARGET_LAG    = DOWNSTREAM
```

**Key transformations applied:**

- **Deduplication** — keeps latest record per business key:
```sql
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY <business_key>
    ORDER BY __LOAD_TS DESC
) = 1
```

- **Data Quality Flag** — `IS_VALID_RECORD` boolean derived via CASE logic (null checks, domain validation, FK validation)
- **Standardization** — column naming, explicit type casting, string trimming, VARIANT parsing for JSON/Parquet

**Why Dynamic Tables over Streams + Tasks?**

| | Dynamic Tables | Streams + Tasks |
|---|---|---|
| Incremental processing | Built-in | Manual stream logic per table |
| Downstream refresh | Automatic | Requires explicit task chaining |
| Orchestration complexity | Low | High |
| Cost | Efficient | Higher (full refresh risk) |

---

### 🥇 Gold Layer — Business Data Model

Star schema with **SCD Type 2** dimensions and dual-grain fact tables.

#### Dimension Tables

All dimensions implement **Slowly Changing Dimension Type 2**:

```sql
-- Surrogate key pattern
SHA2(CONCAT(<business_key>, <effective_start_ts>), 256) AS <entity>_DIM_KEY

-- SCD Type 2 columns
EFFECTIVE_START_TS  TIMESTAMP
EFFECTIVE_END_TS    TIMESTAMP   -- NULL = current record
IS_CURRENT          BOOLEAN
```

| Dimension | Key Attributes |
|---|---|
| `DIM_COUNTRY` | Region, currency code/symbol, tax type & rate, market tier |
| `DIM_PRODUCT` | Full hierarchy (Category → SKU), lifecycle status, price tier, reporting segment |
| `DIM_STORE` | Format (FLAGSHIP/MALL/MINI), city, country, region, open date, floor area |
| `DIM_CUSTOMER` | Segment (Consumer/Business), loyalty tier (None/Silver/Gold/Platinum), region |
| `DIM_DATE` | Day, week, month, quarter, year, fiscal year/quarter, weekend flag |

#### Bridge Table

`PRODUCT_COUNTRY_BRIDGE` resolves the **many-to-many** relationship between products and the countries where they are available — avoiding dimension fan-out and data duplication.

#### Fact Tables

**`FACT_SALES_HEADER`** — Grain: one row per transaction

| Foreign Keys | Measures |
|---|---|
| CUSTOMER_DIM_KEY, STORE_DIM_KEY, COUNTRY_DIM_KEY, DATE_DIM_KEY | GROSS_AMOUNT, TOTAL_DISCOUNT, TOTAL_TAX, NET_TOTAL |

**`FACT_SALES_ITEM`** — Grain: one row per product line item

| Foreign Keys | Measures |
|---|---|
| FACT_SALES_HEADER_KEY, PRODUCT_DIM_KEY, CUSTOMER_DIM_KEY, STORE_DIM_KEY, COUNTRY_DIM_KEY, DATE_DIM_KEY | QUANTITY, UNIT_PRICE, DISCOUNT_AMOUNT, TAX_AMOUNT, LINE_TOTAL |

> Two fact tables are intentional — Header supports financial/transaction analysis; Item supports product-level drill-down. A single table would force a choice between granularity and measure accuracy.

#### Aggregated Fact Tables

Pre-computed rollups for faster dashboard performance:
- `AGG_FACT_DAILY` — Day-level net sales, transactions, units
- `AGG_FACT_WEEKLY` — Week-level rollup
- `AGG_FACT_MONTHLY` — Month-level rollup

---

## ⚙️ Automation — Task DAG

```
ROOT TASK  (scheduled: every 5 minutes)
     │
     ├──▶ Child Task: Load Region
     ├──▶ Child Task: Load Country
     ├──▶ Child Task: Load Currency
     ├──▶ Child Task: Load Tax
     ├──▶ Child Task: Load Product Category
     ├──▶ Child Task: Load Product Family
     ├──▶ Child Task: Load Product Model
     ├──▶ Child Task: Load Product SKU
     ├──▶ Child Task: Load Product Country Availability
     ├──▶ Child Task: Load Store Master
     ├──▶ Child Task: Load Customer Master
     ├──▶ Child Task: Load Sales Header
     └──▶ Child Task: Load Sales Items
```

- Child Tasks run **in parallel** — one per source entity
- Pattern-based file matching — only **new, unprocessed files** are loaded per run
- Once Bronze is updated, **Dynamic Tables cascade the refresh automatically** through Silver → Gold → Aggregations
- Zero manual intervention required after initial file drop

---

## 🤖 Snowflake Cortex Code — AI-Assisted Development

Cortex Code (CoCo) was used as a development accelerator at every stage:

| Stage | Cortex Code Usage |
|---|---|
| Database & Schema Setup | Generated CREATE DATABASE / SCHEMA DDL with transient properties |
| Bronze Layer | COPY INTO scripts, file format objects, metadata column patterns |
| Silver Layer | Dynamic Table DDL, deduplication logic, data quality CASE expressions |
| Gold Dimensions | SCD Type 2 DDL, SHA2 surrogate key patterns |
| Gold Facts | Fact table DDL, surrogate key joins, measure derivations |
| Date Dimension | Full DIM_DATE population script |
| Aggregations | Daily/weekly/monthly aggregated fact table scripts |
| Semantic Layer | Initial YAML semantic model scaffolding |
| Debugging | Query plan explanation and transformation logic review |

**Workflow:**
1. Provide structured prompt describing entity, columns, and transformation rules
2. Cortex Code generates SQL / DDL
3. Engineer validates, modifies, and tests
4. Integrate into pipeline

---

## 🗣️ Semantic Layer — Cortex Analyst

The Semantic Layer enables **natural language querying** of the Gold schema — no SQL required.

Implemented as a **YAML semantic model** (`sales_semantic_model.yaml`) uploaded to a Snowflake stage and powered by Cortex Analyst.

### What's Defined

**Tables:** `dim_customer`, `dim_product`, `dim_store`, `dim_country`, `dim_date`, `fact_sales_header`, `fact_sales_item`

**Key Metrics:**

| Metric | Expression | Synonyms |
|---|---|---|
| `total_net_sales` | `SUM(NET_TOTAL)` | net sales, net revenue, total sales, sales by region |
| `total_gross_sales` | `SUM(GROSS_AMOUNT)` | gross sales, gross revenue |
| `total_transactions` | `COUNT(DISTINCT TRANSACTION_ID)` | transaction count, order count |
| `average_transaction_value` | `AVG(NET_TOTAL)` | ATV, avg order value, average basket size |
| `total_units_sold` | `SUM(QUANTITY)` | units sold, quantity sold |
| `total_line_revenue` | `SUM(LINE_TOTAL)` | product revenue, revenue by product |

**Relationships:** All dimension-to-fact joins explicitly defined (header ↔ customer, store, country, date; item ↔ header, product, customer, store, country, date)

### Example Natural Language Queries
```
"What are total sales by region?"
"Top 10 products by revenue"
"Show me the monthly sales trend"
"Which stores have the highest sales?"
"Compare POS vs Web channel performance"
"Sales by customer loyalty tier"
```

---

## 🌍 Environment Strategy

```
┌──────────────────────┐
│  Apple Sales (DEV)   │  ◀── Active development
│  TRANSIENT tables    │      All schemas: Bronze, Silver, Gold, Common
└──────────┬───────────┘
           │  All database objects promoted as-is
           ▼
┌──────────────────────┐
│  Apple Sales (QA)    │  ◀── Regression & integration testing
│  TRANSIENT tables    │
└──────────┬───────────┘
           │  All database objects promoted as-is
           ▼
┌──────────────────────┐
│  Apple Sales (PROD)  │  ◀── Final production deployment
└──────────────────────┘
```

> Dev and QA use **TRANSIENT** tables and schemas — this eliminates Snowflake fail-safe storage costs in non-production environments. Common objects (file formats, sequences) live in the **Common Schema** within each context for reusability.

---

## 📊 Key Design Decisions

| Decision | Rationale |
|---|---|
| Dynamic Tables over Streams + Tasks | Simpler architecture, built-in incremental processing, lower cost |
| Two Fact Tables (Header + Item) | Correct grain separation — prevents measure duplication or granularity loss |
| Bridge Table for Product-Country | Resolves many-to-many without dimension fan-out or data duplication |
| SCD Type 2 for all dimensions | Preserves full historical state for accurate time-based analysis |
| Transient tables in Dev & QA | Eliminates fail-safe storage cost in non-production environments |
| Common Schema for utilities | Single maintenance point for shared objects across all layers |
| Informational FK constraints | Documents relationships without runtime enforcement overhead |
| INFER_SCHEMA for Bronze DDL | Eliminates manual table creation — accelerates onboarding new sources |
| Metadata columns in Bronze | Full data lineage from file to row — supports debugging and auditing |
| Cortex Code for development | Reduces repetitive coding — accelerates all pipeline stages |
