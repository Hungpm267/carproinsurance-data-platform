/*
  Deploy meta schema (DDL + seed + verify)
  T-SQL equivalent of ddl_meta_schema1.ipynb

  Target: Microsoft Fabric Warehouse (SQL analytics endpoint)
  Run in: Fabric portal SQL editor, SSMS, or pipeline Script activity.

  Aligned with ETL_Metadata_Data_Dictionary.xlsx (5 control tables).

  Fabric notes:
    - Persisted string columns use VARCHAR (not NVARCHAR).
    - PRIMARY KEY added via ALTER TABLE ... NOT ENFORCED after CREATE TABLE.

  WARNING: Step 1 drops and recreates all meta.etl_* tables (destructive).
*/

-- -----------------------------------------------------------------------------
-- Step 1 — Create meta tables
-- -----------------------------------------------------------------------------

IF SCHEMA_ID(N'meta') IS NULL
    EXEC(N'CREATE SCHEMA meta');
GO

IF OBJECT_ID(N'meta.error_logging_table', N'U') IS NOT NULL DROP TABLE meta.error_logging_table;
IF OBJECT_ID(N'meta.etl_execution_log', N'U') IS NOT NULL DROP TABLE meta.etl_execution_log;
IF OBJECT_ID(N'meta.etl_transform_config', N'U') IS NOT NULL DROP TABLE meta.etl_transform_config;
IF OBJECT_ID(N'meta.etl_ingestion_config', N'U') IS NOT NULL DROP TABLE meta.etl_ingestion_config;
IF OBJECT_ID(N'meta.etl_pipeline_config', N'U') IS NOT NULL DROP TABLE meta.etl_pipeline_config;

-- Legacy names (re-run safe after rename)
-- IF OBJECT_ID(N'meta.error_log', N'U') IS NOT NULL DROP TABLE meta.error_log;
-- IF OBJECT_ID(N'meta.audit_log', N'U') IS NOT NULL DROP TABLE meta.audit_log;
-- IF OBJECT_ID(N'meta.pipeline_controller', N'U') IS NOT NULL DROP TABLE meta.pipeline_controller;
-- IF OBJECT_ID(N'meta.pipeline_execution', N'U') IS NOT NULL DROP TABLE meta.pipeline_execution;
-- IF OBJECT_ID(N'meta.transform_config', N'U') IS NOT NULL DROP TABLE meta.transform_config;
-- IF OBJECT_ID(N'meta.ingestion_config', N'U') IS NOT NULL DROP TABLE meta.ingestion_config;
-- IF OBJECT_ID(N'meta.pipeline_config', N'U') IS NOT NULL DROP TABLE meta.pipeline_config;
GO

CREATE TABLE meta.etl_pipeline_config (
    pipeline_name              VARCHAR(100)  NOT NULL,  -- PK. Fabric Data Pipeline name
    pipeline_stage             VARCHAR(50)   NOT NULL,  -- ingestion | transformation
    is_active                  BIT           NOT NULL,  -- 1 = active
    retry_count                INT           NOT NULL,
    retry_interval_minutes     INT           NOT NULL,
    timeout_minutes            INT           NOT NULL,
    created_at                 DATETIME2(3)  NOT NULL,
    updated_at                 DATETIME2(3)  NULL,
    created_by                 VARCHAR(100)  NULL,
    updated_by                 VARCHAR(100)  NULL
);

CREATE TABLE meta.etl_ingestion_config (
    ingestion_config_id        BIGINT        NOT NULL,  -- PK
    pipeline_name              VARCHAR(100)  NOT NULL,  -- FK → etl_pipeline_config
    source_system              VARCHAR(50)   NULL,      -- CRM | PolicyJSON
    source_schema              VARCHAR(100)  NULL,      -- dbo | landing
    source_table               VARCHAR(200)  NULL,      -- Source table or landing entity
    source_path                VARCHAR(500)  NULL,      -- Lakehouse path for files
    source_format              VARCHAR(20)   NOT NULL,  -- SQL_TABLE | JSON | CSV | PARQUET
    file_pattern               VARCHAR(200)  NULL,      -- e.g. policy_*.json
    target_layer               VARCHAR(50)   NOT NULL,  -- landing | bronze
    target_schema              VARCHAR(100)  NOT NULL,
    target_table               VARCHAR(200)  NOT NULL,
    load_type                  VARCHAR(20)   NOT NULL,  -- FULL | INCREMENTAL
    watermark_column           VARCHAR(200)  NULL,      -- Incremental column name e.g. updated_at
    last_watermark             VARCHAR(200)  NULL       -- High-water value; updated after successful Copy
);

CREATE TABLE meta.etl_transform_config (
    transform_config_id        BIGINT        NOT NULL,  -- PK
    pipeline_name              VARCHAR(100)  NOT NULL,  -- FK → etl_pipeline_config
    source_layer               VARCHAR(50)   NOT NULL,  -- bronze
    source_schema              VARCHAR(100)  NOT NULL,
    source_table               VARCHAR(200)  NOT NULL,
    target_layer               VARCHAR(50)   NOT NULL,  -- gold
    target_schema              VARCHAR(100)  NOT NULL,
    target_table               VARCHAR(200)  NOT NULL,
    transform_type             VARCHAR(20)   NOT NULL,  -- APPEND | MERGE_SCD1 | MERGE_SCD2 | OVERWRITE
    primary_key_columns        VARCHAR(500)  NULL,
    partition_column           VARCHAR(200)  NULL,
    dependency_pipeline        VARCHAR(100)  NULL,
    notebook_id                VARCHAR(36)   NULL,      -- Fabric notebook artifact GUID
    watermark_column           VARCHAR(200)  NULL,      -- Incremental column name e.g. updated_at
    last_watermark             VARCHAR(200)  NULL       -- High-water value; updated after successful transform
);

CREATE TABLE meta.etl_execution_log (
    log_id                     BIGINT        NOT NULL,  -- PK — generate in bookend notebook / SP
    pipeline_run_id            VARCHAR(64)   NOT NULL,  -- Fabric Pipeline RunId
    controller_id              BIGINT        NOT NULL,  -- Config row id (0 = pipeline-level)
    pipeline_name              VARCHAR(100)  NOT NULL,
    start_time                 DATETIME2(3)  NOT NULL,
    end_time                   DATETIME2(3)  NULL,
    status                     VARCHAR(20)   NOT NULL,  -- Running | Success | Failed
    rows_read                  BIGINT        NULL,
    rows_inserted              BIGINT        NULL,
    rows_updated               BIGINT        NULL,
    rows_rejected              BIGINT        NULL,
    watermark_from             VARCHAR(200)  NULL,
    watermark_to               VARCHAR(200)  NULL,
    dynamic_source_file        VARCHAR(500)  NULL,
    error_message              VARCHAR(MAX)  NULL,
    logged_at                  DATETIME2(3)  NOT NULL
);

CREATE TABLE meta.error_logging_table (
    error_id                   VARCHAR(36)   NOT NULL,  -- PK — UUID
    execution_id               BIGINT        NULL,      -- FK → etl_execution_log.log_id
    error_pipelinename         VARCHAR(100)  NOT NULL,
    error_timestamp            DATETIME2(3)  NOT NULL,
    error_code                 VARCHAR(50)   NOT NULL,
    layer_name                 VARCHAR(50)   NOT NULL,  -- landing | bronze | gold
    target_table               VARCHAR(200)  NOT NULL,
    error_message              VARCHAR(MAX)  NOT NULL,
    error_severity_level       VARCHAR(20)   NOT NULL,  -- INFO | WARNING | ERROR | CRITICAL
    bad_record_content         VARCHAR(MAX)  NULL,      -- JSON row for replay
    status                     VARCHAR(20)   NOT NULL,  -- New | In-Progress | Resolved | Ignored
    updated_at                 DATETIME2(3)  NOT NULL
);
GO

ALTER TABLE meta.etl_pipeline_config
    ADD CONSTRAINT PK_meta_etl_pipeline_config
        PRIMARY KEY NONCLUSTERED (pipeline_name) NOT ENFORCED;
ALTER TABLE meta.etl_ingestion_config
    ADD CONSTRAINT PK_meta_etl_ingestion_config
        PRIMARY KEY NONCLUSTERED (ingestion_config_id) NOT ENFORCED;
ALTER TABLE meta.etl_transform_config
    ADD CONSTRAINT PK_meta_etl_transform_config
        PRIMARY KEY NONCLUSTERED (transform_config_id) NOT ENFORCED;
ALTER TABLE meta.etl_execution_log
    ADD CONSTRAINT PK_meta_etl_execution_log
        PRIMARY KEY NONCLUSTERED (log_id) NOT ENFORCED;
ALTER TABLE meta.error_logging_table
    ADD CONSTRAINT PK_meta_error_logging_table
        PRIMARY KEY NONCLUSTERED (error_id) NOT ENFORCED;
GO

-- -----------------------------------------------------------------------------
-- Step 2 — Seed configuration
-- -----------------------------------------------------------------------------

DELETE FROM meta.etl_transform_config;
DELETE FROM meta.etl_ingestion_config;
DELETE FROM meta.etl_pipeline_config;
GO

INSERT INTO meta.etl_pipeline_config (
    pipeline_name, pipeline_stage, is_active,
    retry_count, retry_interval_minutes, timeout_minutes,
    created_at, created_by
) VALUES
    ('master_pipeline', 'ingestion',
     1, 1, 5, 120, SYSUTCDATETIME(), 'seed'),
    ('ingest_crm_to_bronze', 'ingestion',
     1, 3, 5, 60, SYSUTCDATETIME(), 'seed'),
    ('ingest_policy_to_bronze', 'ingestion',
     1, 3, 5, 60, SYSUTCDATETIME(), 'seed'),
    ('bronze_to_silver', 'transformation',
     1, 3, 5, 90, SYSUTCDATETIME(), 'seed'),
    ('silver_to_gold', 'transformation',
     1, 3, 5, 90, SYSUTCDATETIME(), 'seed');
GO

-- Optional: landing JSON ingestion (commented out in notebook)
-- INSERT INTO meta.etl_ingestion_config (
--     ingestion_config_id, pipeline_name,
--     source_system, source_schema, source_table,
--     source_path, source_format, file_pattern,
--     target_layer, target_schema, target_table,
--     load_type, watermark_column, last_watermark
-- ) VALUES
--     (101, 'ingest_landing_json',
--      'PolicyJSON', NULL, NULL,
--      'Files/landing/policy', 'JSON', 'policy_*.json',
--      'landing', 'landing', 'policy',
--      'FULL', NULL, NULL),
--     (102, 'ingest_landing_json',
--      'PolicyJSON', NULL, NULL,
--      'Files/landing/payment', 'JSON', 'payment_*.json',
--      'landing', 'landing', 'payment',
--      'FULL', NULL, NULL),
--     (103, 'ingest_landing_json',
--      'PolicyJSON', NULL, NULL,
--      'Files/landing/cancellation', 'JSON', 'cancellation_*.json',
--      'landing', 'landing', 'cancellation',
--      'FULL', NULL, NULL);

INSERT INTO meta.etl_ingestion_config (
    ingestion_config_id, pipeline_name,
    source_system, source_schema, source_table,
    source_path, source_format, file_pattern,
    target_layer, target_schema, target_table,
    load_type, watermark_column, last_watermark
) VALUES
    (201, 'ingest_crm_to_bronze',
     'CRM', 'dbo', 'customers',
     NULL, 'SQL_TABLE', NULL,
     'bronze', 'bronze', 'crm_customer',
     'INCREMENTAL', 'updated_at', NULL),
    (202, 'ingest_crm_to_bronze',
     'CRM', 'dbo', 'agents',
     NULL, 'SQL_TABLE', NULL,
     'bronze', 'bronze', 'crm_agent',
     'INCREMENTAL', 'updated_at', NULL),
    (203, 'ingest_crm_to_bronze',
     'CRM', 'dbo', 'insurance_providers',
     NULL, 'SQL_TABLE', NULL,
     'bronze', 'bronze', 'crm_insurance_provider',
     'INCREMENTAL', 'updated_at', NULL),
    (204, 'ingest_crm_to_bronze',
     'CRM', 'dbo', 'vehicle',
     NULL, 'SQL_TABLE', NULL,
     'bronze', 'bronze', 'crm_vehicle',
     'INCREMENTAL', 'updated_at', NULL),
    (205, 'ingest_crm_to_bronze',
     'CRM', 'dbo', 'quotation',
     NULL, 'SQL_TABLE', NULL,
     'bronze', 'bronze', 'crm_quotation',
     'INCREMENTAL', 'updated_at', NULL),
    (206, 'ingest_crm_to_bronze',
     'CRM', 'dbo', 'quotation_item',
     NULL, 'SQL_TABLE', NULL,
     'bronze', 'bronze', 'crm_quotation_item',
     'INCREMENTAL', 'updated_at', NULL);
GO

INSERT INTO meta.etl_ingestion_config (
    ingestion_config_id, pipeline_name,
    source_system, source_schema, source_table,
    source_path, source_format, file_pattern,
    target_layer, target_schema, target_table,
    load_type, watermark_column, last_watermark
) VALUES
    (301, 'ingest_policy_to_bronze',
     'PolicyJSON', 'landing', 'policy',
     'Files/landing/policy', 'JSON', 'policy_*.json',
     'bronze', 'bronze', 'policy_policy',
     'INCREMENTAL', 'last_updated', NULL),
    (302, 'ingest_policy_to_bronze',
     'PolicyJSON', 'landing', 'payment',
     'Files/landing/payment', 'JSON', 'payment_*.json',
     'bronze', 'bronze', 'policy_payment',
     'INCREMENTAL', 'last_updated', NULL),
    (303, 'ingest_policy_to_bronze',
     'PolicyJSON', 'landing', 'cancellation',
     'Files/landing/cancellation', 'JSON', 'cancellation_*.json',
     'bronze', 'bronze', 'policy_cancellation',
     'INCREMENTAL', 'last_updated', NULL);
GO

INSERT INTO meta.etl_transform_config (
    transform_config_id, pipeline_name,
    source_layer, source_schema, source_table,
    target_layer, target_schema, target_table,
    transform_type, primary_key_columns, partition_column,
    dependency_pipeline, notebook_id, watermark_column, last_watermark
) VALUES
    (401, 'silver_to_gold',
     'bronze', 'bronze', 'crm_customer',
     'gold', 'gold', 'dim_customer',
     'MERGE_SCD2', 'customer_id', NULL,
     'ingest_crm_to_bronze', '556c18f8-e01b-41fd-a058-9e73675a8fc3', 'updated_at', NULL),
    (402, 'silver_to_gold',
     'bronze', 'bronze', 'crm_agent',
     'gold', 'gold', 'dim_agent',
     'MERGE_SCD2', 'agent_id', NULL,
     'ingest_crm_to_bronze', 'aaeed0a4-3f7f-489b-9ce5-fc048e110701', 'updated_at', NULL),
    (403, 'silver_to_gold',
     'bronze', 'bronze', 'crm_insurance_provider',
     'gold', 'gold', 'dim_insurance_provider',
     'MERGE_SCD1', 'provider_code', NULL,
     'ingest_crm_to_bronze', 'cc40ac0a-a176-4de7-b745-b9a83d85effa', 'updated_at', NULL),
    (404, 'silver_to_gold',
     'bronze', 'bronze', 'crm_vehicle',
     'gold', 'gold', 'dim_vehicle',
     'MERGE_SCD1', 'vehicle_id', NULL,
     'ingest_crm_to_bronze', '89efc2ff-8bef-4f6e-9cb6-47d8a293d1b4', 'updated_at', NULL),
    (405, 'silver_to_gold',
     'bronze', 'bronze', 'crm_quotation',
     'gold', 'gold', 'dim_product_package',
     'MERGE_SCD1', 'package_code', NULL,
     'ingest_crm_to_bronze', 'efae997c-b2ef-4983-84d2-d9a12fc500e0', 'updated_at', NULL),
    (406, 'silver_to_gold',
     'bronze', 'bronze', 'crm_quotation',
     'gold', 'gold', 'dim_quotation_status',
     'MERGE_SCD1', 'quotation_status_name', NULL,
     'ingest_crm_to_bronze', '427b8b12-03d8-4f55-8584-1ba580c78544', 'updated_at', NULL),
    (407, 'silver_to_gold',
     'bronze', 'bronze', 'crm_quotation_item',
     'gold', 'gold', 'dim_coverage_type',
     'MERGE_SCD1', 'coverage_type_name', NULL,
     'ingest_crm_to_bronze', '1421dbdc-2738-45f0-8b07-0056b176c85d', 'updated_at', NULL),
    (408, 'silver_to_gold',
     'bronze', 'bronze', 'policy_cancellation',
     'gold', 'gold', 'dim_cancellation_reason',
     'MERGE_SCD1', 'cancellation_reason', NULL,
     'ingest_policy_to_bronze', 'ffa51ffd-e4e9-4853-9850-27273384b697', 'updated_at', NULL),
    (409, 'silver_to_gold',
     'bronze', 'bronze', 'policy_policy',
     'gold', 'gold', 'dim_policy_status',
     'MERGE_SCD1', 'policy_status_name', NULL,
     'ingest_policy_to_bronze', '3a4cb4a6-6252-4738-8cff-9a955434d554', 'updated_at', NULL),
    (410, 'silver_to_gold',
     'bronze', 'bronze', 'policy_payment',
     'gold', 'gold', 'dim_payment_method',
     'MERGE_SCD1', 'payment_method_name', NULL,
     'ingest_policy_to_bronze', '2a1050e2-397d-4098-b1c9-a5276222ef72', 'updated_at', NULL),
    (411, 'silver_to_gold',
     'bronze', 'bronze', 'policy_payment',
     'gold', 'gold', 'dim_payment_status',
     'MERGE_SCD1', 'payment_status_name', NULL,
     'ingest_policy_to_bronze', '11592e12-6b31-4d27-a5bc-2d6b3c92f1ac', 'updated_at', NULL),
    (412, 'silver_to_gold',
     'bronze', 'bronze', 'crm_quotation',
     'gold', 'gold', 'fact_quotation',
     'MERGE_SCD1', 'quotation_id', NULL,
     'ingest_crm_to_bronze', 'e242623f-b72f-4a67-9b1f-ae0a809e9ed7', 'updated_at', NULL),
    (413, 'silver_to_gold',
     'bronze', 'bronze', 'crm_quotation_item',
     'gold', 'gold', 'fact_quotation_item',
     'MERGE_SCD1', 'quotation_item_id', NULL,
     'ingest_crm_to_bronze', 'f6089553-49a0-424c-bcc7-d56fbb58d3ba', 'updated_at', NULL),
    (414, 'silver_to_gold',
     'bronze', 'bronze', 'policy_policy',
     'gold', 'gold', 'fact_policy',
     'MERGE_SCD1', 'policy_id', NULL,
     'ingest_policy_to_bronze', '6ea1e77d-19f6-4970-b99d-64e7accd32f4', 'last_updated', NULL),
    (415, 'silver_to_gold',
     'bronze', 'bronze', 'policy_payment',
     'gold', 'gold', 'fact_payment',
     'MERGE_SCD1', 'payment_id', NULL,
     'ingest_policy_to_bronze', '22a7ba56-3105-4ae1-85c2-e790910da73c', 'last_updated', NULL),
    (416, 'silver_to_gold',
     'bronze', 'bronze', 'policy_cancellation',
     'gold', 'gold', 'fact_cancellation',
     'MERGE_SCD1', 'cancellation_id', NULL,
     'ingest_policy_to_bronze', '7fd44046-2e97-43b0-9ce6-612bb44f8a98', 'last_updated', NULL);
GO

INSERT INTO meta.etl_transform_config (
    transform_config_id, pipeline_name,
    source_layer, source_schema, source_table,
    target_layer, target_schema, target_table,
    transform_type, primary_key_columns, partition_column,
    dependency_pipeline, notebook_id, watermark_column, last_watermark
) VALUES
    (301, 'bronze_to_silver', 'bronze', 'bronze', 'crm_agent',
     'silver', 'silver', 'crm_agent',
     'MERGE_SCD1', 'agent_id', NULL, NULL, '4668f82b-aca9-4ff8-a01b-4811a9737334', 'updated_at', NULL),
    (302, 'bronze_to_silver', 'bronze', 'bronze', 'crm_customer',
     'silver', 'silver', 'crm_customer',
     'MERGE_SCD1', 'customer_id', NULL, NULL, 'ca5e2de4-3406-494a-be09-ec7f8c3124b6', 'updated_at', NULL),
    (303, 'bronze_to_silver', 'bronze', 'bronze', 'crm_insurance_provider',
     'silver', 'silver', 'crm_insurance_provider',
     'MERGE_SCD1', 'provider_code', NULL, NULL, '9b6a0452-fdb0-45ac-a6ab-962cef215477', 'updated_at', NULL),
    (304, 'bronze_to_silver', 'bronze', 'bronze', 'crm_quotation',
     'silver', 'silver', 'crm_quotation',
     'MERGE_SCD1', 'quotation_id', NULL, NULL, '9ab3db24-3c43-4513-8255-83f4e5b65e4d', 'updated_at', NULL),
    (305, 'bronze_to_silver', 'bronze', 'bronze', 'crm_quotation_item',
     'silver', 'silver', 'crm_quotation_item',
     'MERGE_SCD1', 'quotation_item_id', NULL, NULL, '6ef9fb86-59de-42ec-abb9-7858b2bf8871', 'updated_at', NULL),
    (306, 'bronze_to_silver', 'bronze', 'bronze', 'crm_vehicle',
     'silver', 'silver', 'crm_vehicle',
     'MERGE_SCD1', 'vehicle_id', NULL, NULL, '3786e290-f02d-411d-994d-383d5185f19b', 'updated_at', NULL),
    (307, 'bronze_to_silver', 'bronze', 'bronze', 'policy_cancellation',
     'silver', 'silver', 'policy_cancellation',
     'MERGE_SCD1', 'cancellation_id', NULL, NULL, '3e43c05c-8548-46a9-bfb6-01c9c7339f63', 'last_updated', NULL),
    (308, 'bronze_to_silver', 'bronze', 'bronze', 'policy_payment',
     'silver', 'silver', 'policy_payment',
     'MERGE_SCD1', 'payment_id', NULL, NULL, 'e7912425-2f19-46d4-92e8-60aaefa08f44', 'last_updated', NULL),
    (309, 'bronze_to_silver', 'bronze', 'bronze', 'policy_policy',
     'silver', 'silver', 'policy_policy',
     'MERGE_SCD1', 'policy_id', NULL, NULL, '02bde05b-9cbf-4a7b-ae54-962ef6d64580', 'last_updated', NULL);
GO

-- -----------------------------------------------------------------------------
-- Step 3 — Verify
-- Expected row counts after full seed:
--   etl_pipeline_config:    5
--   etl_ingestion_config:   9  (12 if optional landing inserts are enabled)
--   etl_transform_config:  25  (16 silver_to_gold + 9 bronze_to_silver)
--   etl_execution_log:      0
--   error_logging_table:    0
-- -----------------------------------------------------------------------------

SELECT pipeline_name, pipeline_stage, is_active, retry_count, timeout_minutes
FROM meta.etl_pipeline_config
ORDER BY pipeline_name;

SELECT pipeline_name, COUNT(*) AS ingestion_rows
FROM meta.etl_ingestion_config
GROUP BY pipeline_name
ORDER BY pipeline_name;

SELECT ingestion_config_id, source_schema, source_table,
       target_schema, target_table, load_type,
       watermark_column, last_watermark
FROM meta.etl_ingestion_config
WHERE pipeline_name = 'ingest_crm_to_bronze'
ORDER BY ingestion_config_id;

SELECT transform_config_id, source_table, target_table,
       transform_type, notebook_id, dependency_pipeline
FROM meta.etl_transform_config
WHERE pipeline_name = 'silver_to_gold'
ORDER BY transform_config_id;

SELECT 'etl_pipeline_config' AS tbl, COUNT(*) AS n FROM meta.etl_pipeline_config
UNION ALL SELECT 'etl_ingestion_config', COUNT(*) FROM meta.etl_ingestion_config
UNION ALL SELECT 'etl_transform_config', COUNT(*) FROM meta.etl_transform_config
UNION ALL SELECT 'etl_execution_log', COUNT(*) FROM meta.etl_execution_log
UNION ALL SELECT 'error_logging_table', COUNT(*) FROM meta.error_logging_table;
