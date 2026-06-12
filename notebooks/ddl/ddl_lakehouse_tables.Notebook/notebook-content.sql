-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "synapse_pyspark"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "0aa0a14b-3288-4f9f-93d1-d888edaf7070",
-- META       "default_lakehouse_name": "insurance_lakehouse",
-- META       "default_lakehouse_workspace_id": "e13dac5b-f5b1-4169-bb58-0f6d0bfea366",
-- META       "known_lakehouses": [
-- META         {
-- META           "id": "0aa0a14b-3288-4f9f-93d1-d888edaf7070"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- MARKDOWN ********************

-- #### **A. Create Schema bronze / gold**

-- CELL ********************

CREATE SCHEMA IF NOT EXISTS bronze;
create SCHEMA if NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- #### **B. Bronze Layer**

-- MARKDOWN ********************

-- ##### **1. Create table bronze.crm_agent**

-- CELL ********************

CREATE TABLE IF NOT EXISTS bronze.crm_agent (
    agent_id STRING NOT NULL COMMENT 'PK | Source: agent_id VARCHAR(20)',
    agent_name STRING COMMENT 'Source: agent_name NVARCHAR(200)',
    region STRING COMMENT 'Source: region NVARCHAR(100)',
    branch STRING COMMENT 'Source: branch NVARCHAR(100)',
    manager_name STRING COMMENT 'Source: manager_name NVARCHAR(200)',
    created_date TIMESTAMP COMMENT 'Source: created_date DATETIME',
    updated_at TIMESTAMP COMMENT 'Source: updated_at DATETIME'
);

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- ##### **2. Create table bronze.crm_customer**

-- CELL ********************

CREATE TABLE IF NOT EXISTS bronze.crm_customer (
    customer_id STRING NOT NULL COMMENT 'PK | Source: customer_id VARCHAR(20)',
    full_name STRING COMMENT 'Source: full_name NVARCHAR(200)',
    gender STRING COMMENT 'Source: gender VARCHAR(10)',
    dob DATE COMMENT 'Source: dob DATE — raw value; non-standard M/d/yyyy',
    phone_number STRING COMMENT 'Source: phone_number VARCHAR(20)',
    email STRING COMMENT 'Source: email VARCHAR(200)',
    city STRING COMMENT 'Source: city VARCHAR(100)',
    district STRING COMMENT 'Source: district NVARCHAR(100)',
    created_date TIMESTAMP COMMENT 'Source: created_date DATETIME',
    updated_at TIMESTAMP COMMENT 'Source: updated_at DATETIME'
);

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- ##### **3. Create table bronze.crm_insurance_provider**

-- CELL ********************

CREATE TABLE IF NOT EXISTS bronze.crm_insurance_provider (
    provider_code STRING NOT NULL COMMENT 'PK | Source: provider_code VARCHAR(20)',
    provider_name STRING COMMENT 'Source: provider_name NVARCHAR(200)',
    provider_group STRING COMMENT 'Source: provider_group NVARCHAR(100)',
    active_flag INT COMMENT 'Source: active_flag INT',
    created_date TIMESTAMP COMMENT 'Source: created_date DATETIME',
    updated_at TIMESTAMP COMMENT 'Source: updated_at DATETIME'
);

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- ##### **4. Create table bronze.crm_quotation**

-- CELL ********************

CREATE TABLE IF NOT EXISTS bronze.crm_quotation (
    quotation_id STRING NOT NULL COMMENT 'PK | Source: quotation_id VARCHAR(20)',
    customer_id STRING NOT NULL COMMENT 'FK → crm_customer | Source: customer_id VARCHAR(20)',
    agent_id STRING NOT NULL COMMENT 'FK → crm_agent | Source: agent_id VARCHAR(20)',
    provider_code STRING COMMENT 'FK → crm_insurance_provider | Source: provider_code VARCHAR(20)',
    quotation_date TIMESTAMP COMMENT 'Source: quotation_date DATETIME',
    quotation_status STRING COMMENT 'Source: quotation_status VARCHAR(50)',
    package_code STRING COMMENT 'Source: package_code VARCHAR(50)',
    premium_amount DECIMAL(18, 2) COMMENT 'Source: premium_amount DECIMAL(18,2)',
    quotation_expiry_date STRING COMMENT 'Source: quotation_expiry_date DATETIME — stored as STRING; format varies',
    created_date TIMESTAMP COMMENT 'Source: created_date DATETIME',
    updated_at TIMESTAMP COMMENT 'Source: updated_at DATETIME'
);

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- ##### **5. Create table bronze.crm_quotation_item**

-- CELL ********************

CREATE TABLE IF NOT EXISTS bronze.crm_quotation_item (
    quotation_item_id STRING NOT NULL COMMENT 'PK | Source: quotation_item_id VARCHAR(20)',
    quotation_id STRING NOT NULL COMMENT 'FK → crm_quotation | Source: quotation_id VARCHAR(20)',
    coverage_type STRING COMMENT 'Source: coverage_type NVARCHAR(100)',
    coverage_amount DECIMAL(18, 2) COMMENT 'Source: coverage_amount DECIMAL(18,2)',
    deductible_amount DECIMAL(18, 2) COMMENT 'Source: deductible_amount DECIMAL(18,2)',
    created_date TIMESTAMP COMMENT 'Source: created_date DATETIME',
    updated_at TIMESTAMP COMMENT 'Source: updated_at DATETIME'
);

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- ##### **6. Create table bronze.crm_vehicle**

-- CELL ********************

CREATE TABLE IF NOT EXISTS bronze.crm_vehicle (
    vehicle_id STRING NOT NULL COMMENT 'PK | Source: vehicle_id varchar(20)',
    customer_id STRING NOT NULL COMMENT 'FK → crm_customer | Source: customer_id varchar(20)',
    plate_number STRING COMMENT 'Source: plate_number varchar(20)',
    vehicle_brand STRING COMMENT 'Source: vehicle_brand nvarchar(100)',
    vehicle_model STRING COMMENT 'Source: vehicle_model nvarchar(100)',
    manufacture_year INT COMMENT 'Source: manufacture_year int',
    vehicle_value DECIMAL(18, 2) COMMENT 'Source: vehicle_value decimal(18,2)',
    created_date TIMESTAMP COMMENT 'Source: created_date DATETIME',
    updated_at TIMESTAMP COMMENT 'Source: updated_at DATETIME'
);

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- ##### **7. Create table bronze.policy_policy**

-- CELL ********************

CREATE TABLE IF NOT EXISTS bronze.policy_policy (
    policy_id STRING NOT NULL COMMENT 'PK | Source JSON field: policy_id',
    quotation_id STRING NOT NULL COMMENT 'FK → crm_quotation | Source JSON field: quotation_id',
    customer_id STRING NOT NULL COMMENT 'FK → crm_customer | Source JSON field: customer_id',
    provider_code STRING NOT NULL COMMENT 'FK → crm_insurance_provider | Source JSON field: provider_code',
    policy_number STRING COMMENT 'Source JSON field: policy_number',
    policy_start_date DATE COMMENT 'Source JSON field: policy_start_date',
    policy_end_date DATE COMMENT 'Source JSON field: policy_end_date',
    policy_status STRING COMMENT 'Source JSON field: policy_status',
    premium_amount DECIMAL(18, 2) COMMENT 'Source JSON field: premium_amount',
    issued_date TIMESTAMP COMMENT 'Source JSON field: issued_date (ISO 8601 — T separator)',
    last_updated TIMESTAMP COMMENT 'Source JSON field: last_updated (ISO 8601 — T separator)',
    operation_type STRING COMMENT 'Source JSON field: operation_type (CDC marker: INSERT/UPDATE/DELETE)',
    batch_date DATE COMMENT 'Source JSON field: batch_date (file partition date)',
    source_system STRING COMMENT 'Source JSON field: source_system'
);

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- ##### **8. Create table bronze.policy_payment**

-- CELL ********************

CREATE TABLE IF NOT EXISTS bronze.policy_payment (
    payment_id STRING NOT NULL COMMENT 'PK | Source JSON field: payment_id',
    policy_id STRING NOT NULL COMMENT 'FK → policy_policy | Source JSON field: policy_id',
    payment_date TIMESTAMP COMMENT 'Source JSON field: payment_date',
    payment_method STRING COMMENT 'Source JSON field: payment_method',
    payment_status STRING COMMENT 'Source JSON field: payment_status',
    payment_amount DECIMAL(18, 2) COMMENT 'Source JSON field: payment_amount',
    transaction_reference STRING COMMENT 'Source JSON field: transaction_reference',
    last_updated TIMESTAMP COMMENT 'Source JSON field: last_updated',
    operation_type STRING COMMENT 'Source JSON field: operation_type (CDC marker)',
    batch_date DATE COMMENT 'Source JSON field: batch_date',
    source_system STRING COMMENT 'Source JSON field: source_system'
);

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- ##### **9. Create table bronze.policy_cancellation**

-- CELL ********************

CREATE TABLE IF NOT EXISTS bronze.policy_cancellation (
    cancellation_id STRING NOT NULL COMMENT 'PK | Source JSON field: cancellation_id',
    policy_id STRING NOT NULL COMMENT 'FK → policy_policy | Source JSON field: policy_id',
    cancellation_date TIMESTAMP COMMENT 'Source JSON field: cancellation_date',
    cancellation_reason STRING COMMENT 'Source JSON field: cancellation_reason',
    refund_amount DECIMAL(18, 2) COMMENT 'Source JSON field: refund_amount',
    last_updated TIMESTAMP COMMENT 'Source JSON field: last_updated',
    operation_type STRING COMMENT 'Source JSON field: operation_type (CDC marker)',
    batch_date DATE COMMENT 'Source JSON field: batch_date',
    source_system STRING COMMENT 'Source JSON field: source_system'
);

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- #### **C. Silver Layer**

-- MARKDOWN ********************

-- ##### **1. Create table silver.crm_agent**

-- CELL ********************

CREATE TABLE IF NOT EXISTS silver.crm_agent (
    agent_id STRING NOT NULL COMMENT 'PK | Source: agent_id VARCHAR(20)',
    agent_name STRING COMMENT 'Source: agent_name NVARCHAR(200)',
    region STRING COMMENT 'Source: region NVARCHAR(100)',
    branch STRING COMMENT 'Source: branch NVARCHAR(100)',
    manager_name STRING COMMENT 'Source: manager_name NVARCHAR(200)',
    created_date TIMESTAMP COMMENT 'Source: created_date DATETIME',
    updated_at TIMESTAMP COMMENT 'Source: updated_at DATETIME'
);

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- 
-- ##### **2. Create table silver.crm_customer**

-- CELL ********************

CREATE TABLE IF NOT EXISTS silver.crm_customer (
    customer_id STRING NOT NULL COMMENT 'PK | Source: customer_id VARCHAR(20)',
    full_name STRING COMMENT 'Source: full_name NVARCHAR(200)',
    gender STRING COMMENT 'Source: gender VARCHAR(10)',
    dob DATE COMMENT 'Source: dob DATE — raw value; non-standard M/d/yyyy',
    phone_number STRING COMMENT 'Source: phone_number VARCHAR(20)',
    email STRING COMMENT 'Source: email VARCHAR(200)',
    city STRING COMMENT 'Source: city VARCHAR(100)',
    district STRING COMMENT 'Source: district NVARCHAR(100)',
    created_date TIMESTAMP COMMENT 'Source: created_date DATETIME',
    updated_at TIMESTAMP COMMENT 'Source: updated_at DATETIME'
);

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- 
-- ##### **3. Create table silver.crm_insurance_provider**

-- CELL ********************

CREATE TABLE IF NOT EXISTS silver.crm_insurance_provider (
    provider_code STRING NOT NULL COMMENT 'PK | Source: provider_code VARCHAR(20)',
    provider_name STRING COMMENT 'Source: provider_name NVARCHAR(200)',
    provider_group STRING COMMENT 'Source: provider_group NVARCHAR(100)',
    active_flag INT COMMENT 'Source: active_flag INT',
    created_date TIMESTAMP COMMENT 'Source: created_date DATETIME',
    updated_at TIMESTAMP COMMENT 'Source: updated_at DATETIME'
);

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- 
-- ##### **4. Create table silver.crm_quotation**

-- CELL ********************

CREATE TABLE IF NOT EXISTS silver.crm_quotation (
    quotation_id STRING NOT NULL COMMENT 'PK | Source: quotation_id VARCHAR(20)',
    customer_id STRING NOT NULL COMMENT 'FK → crm_customer | Source: customer_id VARCHAR(20)',
    agent_id STRING NOT NULL COMMENT 'FK → crm_agent | Source: agent_id VARCHAR(20)',
    provider_code STRING COMMENT 'FK → crm_insurance_provider | Source: provider_code VARCHAR(20)',
    quotation_date TIMESTAMP COMMENT 'Source: quotation_date DATETIME',
    quotation_status STRING COMMENT 'Source: quotation_status VARCHAR(50)',
    package_code STRING COMMENT 'Source: package_code VARCHAR(50)',
    premium_amount DECIMAL(18, 2) COMMENT 'Source: premium_amount DECIMAL(18,2)',
    quotation_expiry_date STRING COMMENT 'Source: quotation_expiry_date DATETIME — stored as STRING; format varies',
    created_date TIMESTAMP COMMENT 'Source: created_date DATETIME',
    updated_at TIMESTAMP COMMENT 'Source: updated_at DATETIME'
);

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- 
-- ##### **5. Create table silver.crm_quotation_item**

-- CELL ********************

CREATE TABLE IF NOT EXISTS silver.crm_quotation_item (
    quotation_item_id STRING NOT NULL COMMENT 'PK | Source: quotation_item_id VARCHAR(20)',
    quotation_id STRING NOT NULL COMMENT 'FK → crm_quotation | Source: quotation_id VARCHAR(20)',
    coverage_type STRING COMMENT 'Source: coverage_type NVARCHAR(100)',
    coverage_amount DECIMAL(18, 2) COMMENT 'Source: coverage_amount DECIMAL(18,2)',
    deductible_amount DECIMAL(18, 2) COMMENT 'Source: deductible_amount DECIMAL(18,2)',
    created_date TIMESTAMP COMMENT 'Source: created_date DATETIME',
    updated_at TIMESTAMP COMMENT 'Source: updated_at DATETIME'
);

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- 
-- ##### **6. Create table silver.crm_vehicle**

-- CELL ********************

CREATE TABLE IF NOT EXISTS silver.crm_vehicle (
    vehicle_id STRING NOT NULL COMMENT 'PK | Source: vehicle_id varchar(20)',
    customer_id STRING NOT NULL COMMENT 'FK → crm_customer | Source: customer_id varchar(20)',
    plate_number STRING COMMENT 'Source: plate_number varchar(20)',
    vehicle_brand STRING COMMENT 'Source: vehicle_brand nvarchar(100)',
    vehicle_model STRING COMMENT 'Source: vehicle_model nvarchar(100)',
    manufacture_year INT COMMENT 'Source: manufacture_year int',
    vehicle_value DECIMAL(18, 2) COMMENT 'Source: vehicle_value decimal(18,2)',
    created_date TIMESTAMP COMMENT 'Source: created_date DATETIME',
    updated_at TIMESTAMP COMMENT 'Source: updated_at DATETIME'
);

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- 
-- ##### **7. Create table silver.policy_policy**

-- CELL ********************

CREATE TABLE IF NOT EXISTS silver.policy_policy (
    policy_id STRING NOT NULL COMMENT 'PK | Source JSON field: policy_id',
    quotation_id STRING NOT NULL COMMENT 'FK → crm_quotation | Source JSON field: quotation_id',
    customer_id STRING NOT NULL COMMENT 'FK → crm_customer | Source JSON field: customer_id',
    provider_code STRING NOT NULL COMMENT 'FK → crm_insurance_provider | Source JSON field: provider_code',
    policy_number STRING COMMENT 'Source JSON field: policy_number',
    policy_start_date DATE COMMENT 'Source JSON field: policy_start_date',
    policy_end_date DATE COMMENT 'Source JSON field: policy_end_date',
    policy_status STRING COMMENT 'Source JSON field: policy_status',
    premium_amount DECIMAL(18, 2) COMMENT 'Source JSON field: premium_amount',
    issued_date TIMESTAMP COMMENT 'Source JSON field: issued_date (ISO 8601 — T separator)',
    last_updated TIMESTAMP COMMENT 'Source JSON field: last_updated (ISO 8601 — T separator)',
    operation_type STRING COMMENT 'Source JSON field: operation_type (CDC marker: INSERT/UPDATE/DELETE)',
    batch_date DATE COMMENT 'Source JSON field: batch_date (file partition date)',
    source_system STRING COMMENT 'Source JSON field: source_system'
);

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- 
-- ##### **8. Create table silver.policy_payment**

-- CELL ********************

CREATE TABLE IF NOT EXISTS silver.policy_payment (
    payment_id STRING NOT NULL COMMENT 'PK | Source JSON field: payment_id',
    policy_id STRING NOT NULL COMMENT 'FK → policy_policy | Source JSON field: policy_id',
    payment_date TIMESTAMP COMMENT 'Source JSON field: payment_date',
    payment_method STRING COMMENT 'Source JSON field: payment_method',
    payment_status STRING COMMENT 'Source JSON field: payment_status',
    payment_amount DECIMAL(18, 2) COMMENT 'Source JSON field: payment_amount',
    transaction_reference STRING COMMENT 'Source JSON field: transaction_reference',
    last_updated TIMESTAMP COMMENT 'Source JSON field: last_updated',
    operation_type STRING COMMENT 'Source JSON field: operation_type (CDC marker)',
    batch_date DATE COMMENT 'Source JSON field: batch_date',
    source_system STRING COMMENT 'Source JSON field: source_system'
);

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- 
-- ##### **9. Create table silver.policy_cancellation**

-- CELL ********************

CREATE TABLE IF NOT EXISTS silver.policy_cancellation (
    cancellation_id STRING NOT NULL COMMENT 'PK | Source JSON field: cancellation_id',
    policy_id STRING NOT NULL COMMENT 'FK → policy_policy | Source JSON field: policy_id',
    cancellation_date TIMESTAMP COMMENT 'Source JSON field: cancellation_date',
    cancellation_reason STRING COMMENT 'Source JSON field: cancellation_reason',
    refund_amount DECIMAL(18, 2) COMMENT 'Source JSON field: refund_amount',
    last_updated TIMESTAMP COMMENT 'Source JSON field: last_updated',
    operation_type STRING COMMENT 'Source JSON field: operation_type (CDC marker)',
    batch_date DATE COMMENT 'Source JSON field: batch_date',
    source_system STRING COMMENT 'Source JSON field: source_system'
);

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- #### **D. Gold Layer — Dimensions**

-- MARKDOWN ********************

-- ##### **1. Create table gold.dim_date**

-- CELL ********************

/*
  gold.dim_date — calendar spine (Power BI / reporting)
  Fabric lakehouse · Spark SQL / Delta
  Range: 2015-01-01 to 2035-12-31

  Note: DROP first when adding columns to an existing table.
        CREATE TABLE IF NOT EXISTS alone will not alter an old schema.
*/

DROP TABLE IF EXISTS gold.dim_date;

CREATE TABLE IF NOT EXISTS gold.dim_date (
    -- PK & core
    date_key              INT       NOT NULL COMMENT 'PK | Surrogate key: YYYYMMDD integer — (year*10000 + month*100 + day)',
    full_date             DATE      NOT NULL COMMENT 'Calendar date',

    -- Day
    day_of_month          INT       NOT NULL COMMENT 'Day of month 1–31 — dayofmonth()',
    day_of_year           INT       NOT NULL COMMENT 'Day of year 1–366 — dayofyear()',
    day_of_week           INT       NOT NULL COMMENT 'Day of week (1=Sunday … 7=Saturday) — dayofweek()',
    day_name              STRING    NOT NULL COMMENT 'Full day name e.g. Monday — date_format(full_date, EEEE)',
    day_name_short        STRING    NOT NULL COMMENT 'Short day name e.g. Mon — date_format(full_date, EEE)',
    is_weekend            BOOLEAN   NOT NULL COMMENT 'True if Saturday (7) or Sunday (1)',

    -- Week
    week_of_year          INT       NOT NULL COMMENT 'Week of year — weekofyear()',
    week_start_date       DATE      NOT NULL COMMENT 'Monday of the ISO-style week containing full_date',
    week_end_date         DATE      NOT NULL COMMENT 'Sunday of the week containing full_date',

    -- Month
    month_number          INT       NOT NULL COMMENT 'Month number 1–12 — month()',
    month_name            STRING    NOT NULL COMMENT 'Full month name e.g. January — date_format(full_date, MMMM)',
    month_name_short      STRING    NOT NULL COMMENT 'Short month name e.g. Jan — date_format(full_date, MMM)',
    days_in_month         INT       NOT NULL COMMENT 'Number of days in the month — dayofmonth(last_day(full_date))',
    month_start_date      DATE      NOT NULL COMMENT 'First day of the month — trunc(full_date, MM)',
    month_end_date        DATE      NOT NULL COMMENT 'Last day of the month — last_day(full_date)',
    is_month_start        BOOLEAN   NOT NULL COMMENT 'True when full_date is the 1st of the month',
    is_month_end          BOOLEAN   NOT NULL COMMENT 'True when full_date is the last day of the month',

    -- Year
    year_number           INT       NOT NULL COMMENT 'Calendar year — year()',

    -- Year-month (slicers & chart axes)
    year_month            STRING    NOT NULL COMMENT 'Year-month key e.g. 2026-01 — date_format(full_date, yyyy-MM)',
    year_month_sort_key   INT       NOT NULL COMMENT 'Sort key for year-month e.g. 202601 — use in Power BI Sort by Column',
    year_month_label      STRING    NOT NULL COMMENT 'Display label e.g. Jan 2026 — date_format(full_date, MMM yyyy)',
    year_month_short      STRING    NOT NULL COMMENT 'Short chart label e.g. Jan 26 — date_format(full_date, MMM yy)',

    -- Quarter
    quarter_number        INT       NOT NULL COMMENT 'Quarter 1–4 — quarter()',
    quarter_name          STRING    NOT NULL COMMENT 'Quarter label e.g. Q1 — concat(Q, quarter())',
    quarter_year_label    STRING    NOT NULL COMMENT 'Quarter + year e.g. Q2 2026',
    quarter_year_sort_key INT       NOT NULL COMMENT 'Sort key for quarter e.g. 20262',

    -- Fiscal (calendar = fiscal; change INSERT if FY differs)
    fiscal_year           INT       NOT NULL COMMENT 'Fiscal year (same as calendar year — adjust if FY differs)',
    fiscal_quarter        INT       NOT NULL COMMENT 'Fiscal quarter 1–4 (same as calendar quarter — adjust if FY differs)'
)
USING DELTA
COMMENT 'Conformed calendar dimension — one row per day';


INSERT INTO gold.dim_date
SELECT
    CAST(date_format(d, 'yyyyMMdd') AS INT)                              AS date_key,
    d                                                                     AS full_date,

    dayofmonth(d)                                                         AS day_of_month,
    dayofyear(d)                                                          AS day_of_year,
    dayofweek(d)                                                          AS day_of_week,
    date_format(d, 'EEEE')                                                AS day_name,
    date_format(d, 'EEE')                                                 AS day_name_short,
    dayofweek(d) IN (1, 7)                                                AS is_weekend,

    weekofyear(d)                                                         AS week_of_year,
    date_sub(d, IF(dayofweek(d) = 1, 6, dayofweek(d) - 2))                AS week_start_date,
    date_add(date_sub(d, IF(dayofweek(d) = 1, 6, dayofweek(d) - 2)), 6)   AS week_end_date,

    month(d)                                                              AS month_number,
    date_format(d, 'MMMM')                                                AS month_name,
    date_format(d, 'MMM')                                                 AS month_name_short,
    dayofmonth(last_day(d))                                               AS days_in_month,
    trunc(d, 'MM')                                                        AS month_start_date,
    last_day(d)                                                           AS month_end_date,
    dayofmonth(d) = 1                                                     AS is_month_start,
    d = last_day(d)                                                       AS is_month_end,

    year(d)                                                               AS year_number,

    date_format(d, 'yyyy-MM')                                             AS year_month,
    CAST(date_format(d, 'yyyyMM') AS INT)                                 AS year_month_sort_key,
    date_format(d, 'MMM yyyy')                                            AS year_month_label,
    date_format(d, 'MMM yy')                                              AS year_month_short,

    quarter(d)                                                            AS quarter_number,
    concat('Q', CAST(quarter(d) AS STRING))                               AS quarter_name,
    concat('Q', CAST(quarter(d) AS STRING), ' ', CAST(year(d) AS STRING)) AS quarter_year_label,
    CAST(concat(CAST(year(d) AS STRING), CAST(quarter(d) AS STRING)) AS INT) AS quarter_year_sort_key,

    year(d)                                                               AS fiscal_year,
    quarter(d)                                                            AS fiscal_quarter

FROM (
    SELECT explode(sequence(DATE'2015-01-01', DATE'2035-12-31', INTERVAL 1 DAY)) AS d
);


-- Verify
-- SELECT COUNT(*) FROM gold.dim_date;                                    -- expect 7670
-- SELECT * FROM gold.dim_date WHERE full_date = DATE'2026-06-08';
-- SELECT year_month_short, year_month_sort_key, COUNT(*) FROM gold.dim_date GROUP BY 1, 2 ORDER BY 2;


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- ##### **2. Create table gold.dim_customer**

-- CELL ********************

CREATE TABLE IF NOT EXISTS gold.dim_customer (
    customer_key INT NOT NULL COMMENT 'PK | Surrogate key — generated by ETL',
    customer_id STRING NOT NULL COMMENT 'BK | Natural key from source',
    full_name STRING NOT NULL COMMENT 'Customer full name — SCD2 tracked',
    gender STRING COMMENT 'Gender',
    date_of_birth DATE COMMENT 'Date of birth — parsed from bronze.dob_silver (M/d/yyyy → DATE)',
    age INT COMMENT 'Derived: floor(datediff(current_date, date_of_birth) / 365)',
    phone_number STRING COMMENT 'Phone number — SCD2 tracked',
    email STRING COMMENT 'Email address — SCD2 tracked',
    city STRING COMMENT 'City — SCD2 tracked',
    district STRING COMMENT 'District — SCD2 tracked',
    customer_since_date DATE NOT NULL COMMENT 'Derived from bronze.created_date cast to DATE',
    effective_date DATE NOT NULL  COMMENT 'SCD2: row valid from — current_date() on insert',
    expiry_date DATE COMMENT 'SCD2: row valid to — 9999-12-31 for current rows',
    is_current BOOLEAN NOT NULL COMMENT 'SCD2: True = active current record',
    row_hash STRING NOT NULL  COMMENT 'SHA-256 of SCD2-tracked cols for change detection: full_name || phone_number || email || city || district'
);

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- ##### **3. Create table gold.dim_agent**

-- CELL ********************

CREATE TABLE IF NOT EXISTS gold.dim_agent (
    agent_key INT NOT NULL COMMENT 'PK | Surrogate key — generated by ETL',
    agent_id STRING NOT NULL COMMENT 'BK | Natural key from source',
    agent_name STRING NOT NULL COMMENT 'Agent full name — SCD2 tracked',
    region STRING COMMENT 'Sales region — SCD2 tracked',
    branch STRING COMMENT 'Branch — SCD2 tracked',
    manager_name STRING COMMENT 'Manager name — SCD2 tracked',
    effective_date DATE NOT NULL COMMENT 'SCD2: row valid from — current_date() on insert',
    expiry_date DATE COMMENT 'SCD2: row valid to — 9999-12-31 for current rows',
    is_current BOOLEAN NOT NULL COMMENT 'SCD2: True = active current record',
    row_hash STRING NOT NULL COMMENT 'SHA-256 of SCD2-tracked cols: agent_name || region || branch || manager_name'
);

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- ##### **4. Create table gold.dim_insurance_provider**

-- CELL ********************

CREATE TABLE IF NOT EXISTS gold.dim_insurance_provider (
    provider_key INT NOT NULL COMMENT 'PK | Surrogate key — generated by ETL',
    provider_code STRING NOT NULL COMMENT 'BK | Natural key from source',
    provider_name STRING NOT NULL COMMENT 'Provider full name',
    provider_group STRING COMMENT 'Provider group / parent company',
    is_active BOOLEAN NOT NULL COMMENT 'Derived: CAST(active_flag AS BOOLEAN)'
);

INSERT INTO gold.dim_insurance_provider (provider_key, provider_code, provider_name, provider_group, is_active)
SELECT -1, 'UNKNOWN', 'Unknown', 'Unknown', false
WHERE NOT EXISTS (
    SELECT 1 FROM gold.dim_insurance_provider WHERE provider_key = -1
);

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- ##### **5. Create table gold.dim_vehicle**

-- CELL ********************

CREATE TABLE IF NOT EXISTS gold.dim_vehicle (
    vehicle_key INT NOT NULL COMMENT 'PK | Surrogate key — generated by ETL',
    vehicle_id STRING NOT NULL COMMENT 'BK | Natural key from source',
    customer_id STRING NOT NULL COMMENT 'FK reference: links to dim_customer BK',
    plate_number STRING COMMENT 'Vehicle license plate',
    vehicle_brand STRING COMMENT 'Vehicle brand / make',
    vehicle_model STRING COMMENT 'Vehicle model',
    manufacture_year INT COMMENT 'Year of manufacture',
    vehicle_age_years INT COMMENT 'Derived: year(current_date()) - manufacture_year',
    vehicle_value DECIMAL(18, 2) COMMENT 'Insured vehicle value (VND)',
    vehicle_value_band STRING COMMENT 'Derived band: <500M | 500M-1B | >1B'
);

INSERT INTO gold.dim_vehicle (
    vehicle_key, vehicle_id, customer_id, plate_number, vehicle_brand,
    vehicle_model, manufacture_year, vehicle_value, vehicle_age_years, vehicle_value_band)
SELECT 
    -1, 'UNKNOWN', 'UNKNOWN', 'Unknown', 'Unknown',
    'Unknown', NULL, NULL, NULL, 'Unknown'
WHERE NOT EXISTS (
    SELECT 1 FROM gold.dim_vehicle WHERE vehicle_key = -1
);

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- ##### **6. Create table gold.dim_product_package**

-- CELL ********************

CREATE TABLE IF NOT EXISTS gold.dim_product_package (
    product_package_key INT NOT NULL COMMENT 'PK | Surrogate key — generated by ETL',
    product_package_name STRING NOT NULL COMMENT 'BK | Distinct package_code from crm_quotation'
);

INSERT INTO gold.dim_product_package (product_package_key, product_package_name)
SELECT -1, 'UNKNOWN'
WHERE NOT EXISTS (
    SELECT 1 FROM gold.dim_product_package WHERE product_package_key = -1
);

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- ##### **7. Create table gold.dim_quotation_status**

-- CELL ********************

CREATE TABLE IF NOT EXISTS gold.dim_quotation_status (
    quotation_status_key INT NOT NULL COMMENT 'PK | Surrogate key — generated by ETL',
    quotation_status_name STRING NOT NULL COMMENT 'BK | Distinct quotation_status values'
);

INSERT INTO gold.dim_quotation_status (quotation_status_key, quotation_status_name)
SELECT -1, 'UNKNOWN'
WHERE NOT EXISTS (
    SELECT 1 FROM gold.dim_quotation_status WHERE quotation_status_key = -1
);

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- ##### **8. Create table gold.dim_policy_status**

-- CELL ********************

CREATE TABLE IF NOT EXISTS gold.dim_policy_status (
    policy_status_key INT NOT NULL COMMENT 'PK | Surrogate key — generated by ETL',
    policy_status_name STRING NOT NULL COMMENT 'BK | Distinct policy_status values from policy_policy'
);

INSERT INTO gold.dim_policy_status (policy_status_key, policy_status_name)
SELECT -1, 'UNKNOWN'
WHERE NOT EXISTS (
    SELECT 1 FROM gold.dim_policy_status WHERE policy_status_key = -1
);

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- ##### **9. Create table gold.dim_payment_status**

-- CELL ********************

CREATE TABLE IF NOT EXISTS gold.dim_payment_status (
    payment_status_key INT NOT NULL COMMENT 'PK | Surrogate key — generated by ETL',
    payment_status_name STRING NOT NULL COMMENT 'BK | Distinct payment_status values'
);

INSERT INTO gold.dim_payment_status (payment_status_key, payment_status_name)
SELECT -1, 'UNKNOWN'
WHERE NOT EXISTS (
    SELECT 1 FROM gold.dim_payment_status WHERE payment_status_key = -1
);

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- ##### **10. Create table gold.dim_payment_method**

-- CELL ********************

CREATE TABLE IF NOT EXISTS gold.dim_payment_method (
    payment_method_key INT NOT NULL COMMENT 'PK | Surrogate key — generated by ETL',
    payment_method_name STRING NOT NULL COMMENT 'BK | Distinct payment_method values'
);

INSERT INTO gold.dim_payment_method (payment_method_key, payment_method_name)
SELECT -1, 'UNKNOWN'
WHERE NOT EXISTS (
    SELECT 1 FROM gold.dim_payment_method WHERE payment_method_key = -1
);

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- ##### **11. Create table gold.dim_coverage_type**

-- CELL ********************

CREATE TABLE IF NOT EXISTS gold.dim_coverage_type (
    coverage_type_key INT NOT NULL COMMENT 'PK | Surrogate key — generated by ETL',
    coverage_type_name STRING NOT NULL COMMENT 'BK | Distinct coverage_type values (e.g. Physical Damage)'
);

INSERT INTO gold.dim_coverage_type (coverage_type_key, coverage_type_name)
SELECT -1, 'UNKNOWN'
WHERE NOT EXISTS (
    SELECT 1 FROM gold.dim_coverage_type WHERE coverage_type_key = -1
);

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- ##### **12. Create table gold.dim_cancellation_reason**

-- CELL ********************

CREATE TABLE IF NOT EXISTS gold.dim_cancellation_reason (
    cancellation_reason_key INT NOT NULL COMMENT 'PK | Surrogate key — generated by ETL',
    cancellation_reason_name STRING NOT NULL COMMENT 'BK | Distinct cancellation_reason values (e.g. Customer Request)'
);

INSERT INTO gold.dim_cancellation_reason (cancellation_reason_key, cancellation_reason_name)
SELECT -1, 'UNKNOWN'
WHERE NOT EXISTS (
    SELECT 1 FROM gold.dim_cancellation_reason WHERE cancellation_reason_key = -1
);

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- #### **D. Gold Layer — Facts**

-- MARKDOWN ********************

-- ##### **1. Create table gold.fact_quotation**

-- CELL ********************

CREATE TABLE IF NOT EXISTS gold.fact_quotation (
    fact_quotation_key INT NOT NULL COMMENT 'PK | Surrogate key — generated by ETL',
    quotation_id STRING NOT NULL COMMENT 'DD | Degenerate dimension — natural key',
    quotation_date_key INT NOT NULL COMMENT 'FK → dim_date | date_format(quotation_date, yyyyMMdd)',
    quotation_expiry_date_key INT COMMENT 'FK → dim_date | date_format(quotation_expiry_date, yyyyMMdd)',
    customer_key INT NOT NULL COMMENT 'FK → dim_customer (is_current = true)',
    agent_key INT COMMENT 'FK → dim_agent (is_current = true)',
    provider_key INT NOT NULL COMMENT 'FK → dim_insurance_provider',
    vehicle_key INT COMMENT 'FK → dim_vehicle',
    product_package_key INT NOT NULL COMMENT 'FK → dim_product_package',
    quotation_status_key INT NOT NULL COMMENT 'FK → dim_quotation_status',
    quotation_premium_amount DECIMAL(18, 2) NOT NULL  COMMENT 'Quoted premium amount'
);

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- ##### **2. Create table gold.fact_quotation_item**

-- CELL ********************

CREATE TABLE IF NOT EXISTS gold.fact_quotation_item (
    quotation_item_key INT NOT NULL COMMENT 'PK | Surrogate key — generated by ETL',
    quotation_item_id STRING NOT NULL COMMENT 'DD | Degenerate dimension — natural key',
    coverage_type_key INT NOT NULL COMMENT 'FK → dim_coverage_type',
    coverage_amount DECIMAL(18, 2) NOT NULL COMMENT 'Coverage amount (filter: > 0 applied in ETL)',
    deductible_amount DECIMAL(18, 2) COMMENT 'Deductible amount (filter: >= 0 applied in ETL)'
);

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- ##### **3. Create table gold.fact_policy**

-- CELL ********************

CREATE TABLE IF NOT EXISTS gold.fact_policy (
    fact_policy_key INT NOT NULL COMMENT 'PK | Surrogate key — generated by ETL',
    policy_id STRING NOT NULL COMMENT 'DD | Natural key',
    quotation_id STRING COMMENT 'DD | Linked quotation natural key',
    policy_number STRING NOT NULL COMMENT 'DD | Business policy number',
    policy_start_date_key INT NOT NULL COMMENT 'FK → dim_date | date_format(policy_start_date, yyyyMMdd)',
    policy_end_date_key INT COMMENT 'FK → dim_date | date_format(policy_end_date, yyyyMMdd)',
    issued_date_key INT NOT NULL COMMENT 'FK → dim_date | date_format(issued_date, yyyyMMdd)',
    customer_key INT NOT NULL COMMENT 'FK → dim_customer (is_current = true)',
    provider_key INT NOT NULL COMMENT 'FK → dim_insurance_provider',
    policy_status_key INT NOT NULL COMMENT 'FK → dim_policy_status',
    written_premium_amount DECIMAL(18, 2) NOT NULL COMMENT 'Written premium from policy_policy source',
    quoted_premium_amount DECIMAL(18, 2) COMMENT 'Quoted premium — JOIN crm_quotation ON quotation_id',
    premium_variance_amount DECIMAL(18, 2) COMMENT 'Derived: written_premium_amount - quoted_premium_amount',
    is_in_force BOOLEAN NOT NULL COMMENT 'Derived: policy_status_derived = ACTIVE → True, else False',
    policy_term_days INT COMMENT 'Derived: DATEDIFF(policy_end_date, policy_start_date)'
);

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- ##### **4. Create table gold.fact_payment**

-- CELL ********************

CREATE TABLE IF NOT EXISTS gold.fact_payment (
    fact_payment_key INT NOT NULL COMMENT 'PK | Surrogate key — generated by ETL',
    payment_id STRING NOT NULL COMMENT 'DD | Natural key',
    policy_id STRING NOT NULL COMMENT 'DD | Policy natural key',
    payment_date_key INT NOT NULL COMMENT 'FK → dim_date | date_format(payment_date, yyyyMMdd)',
    payment_method_key INT NOT NULL COMMENT 'FK → dim_payment_method',
    payment_status_key INT NOT NULL COMMENT 'FK → dim_payment_status',
    payment_amount DECIMAL(18, 2) NOT NULL COMMENT 'Payment amount',
    is_successful_payment BOOLEAN NOT NULL COMMENT 'Derived: payment_status = PAID → True, else False'
);

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- ##### **5. Create table gold.fact_cancellation**

-- CELL ********************

CREATE TABLE IF NOT EXISTS gold.fact_cancellation (
    fact_cancellation_key INT NOT NULL COMMENT 'PK | Surrogate key — generated by ETL',
    cancellation_id STRING NOT NULL COMMENT 'DD | Natural key',
    policy_id STRING NOT NULL COMMENT 'DD | Policy natural key',
    cancellation_date_key INT NOT NULL COMMENT 'FK → dim_date | date_format(cancellation_date, yyyyMMdd)',
    customer_key INT NOT NULL COMMENT 'FK → dim_customer — resolved via fact_policy JOIN',
    provider_key INT NOT NULL COMMENT 'FK → dim_insurance_provider — resolved via fact_policy JOIN',
    cancellation_reason_key INT COMMENT 'FK → dim_cancellation_reason',
    refund_amount DECIMAL(18, 2) COMMENT 'Refund amount (filter: >= 0 applied in ETL)'
);

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }
