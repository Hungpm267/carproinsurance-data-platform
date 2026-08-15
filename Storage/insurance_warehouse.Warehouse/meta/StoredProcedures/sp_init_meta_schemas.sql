CREATE   PROCEDURE meta.sp_init_meta_schemas
AS
BEGIN
    -- ==========================================================================================
    -- STEP 1 — DROP & CREATE CONTROL TABLES WITH EXPLICIT PRECISION
    -- ==========================================================================================
    
    IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'meta.error_logging_table') AND type in (N'U'))
        DROP TABLE meta.error_logging_table;

    IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'meta.etl_execution_log') AND type in (N'U'))
        DROP TABLE meta.etl_execution_log;

    IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'meta.etl_transform_config') AND type in (N'U'))
        DROP TABLE meta.etl_transform_config;

    IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'meta.etl_ingestion_config') AND type in (N'U'))
        DROP TABLE meta.etl_ingestion_config;

    IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'meta.etl_pipeline_config') AND type in (N'U'))
        DROP TABLE meta.etl_pipeline_config;

    -- 1. Create Table: meta.etl_pipeline_config
    CREATE TABLE meta.etl_pipeline_config (
        pipeline_name              VARCHAR(255) NOT NULL,
        pipeline_stage             VARCHAR(50)  NOT NULL,
        is_active                  BIT          NOT NULL,
        retry_count                INT          NOT NULL,
        retry_interval_minutes     INT          NOT NULL,
        timeout_minutes            INT          NOT NULL,
        created_at                 DATETIME2(6) NOT NULL, -- Ép kiểu scale (6)
        updated_at                 DATETIME2(6),          -- Ép kiểu scale (6)
        created_by                 VARCHAR(100),
        updated_by                 VARCHAR(100)
    );

    -- 2. Create Table: meta.etl_ingestion_config
    CREATE TABLE meta.etl_ingestion_config (
        ingestion_config_id        BIGINT       NOT NULL,
        pipeline_name              VARCHAR(255) NOT NULL,
        source_system              VARCHAR(100),
        source_schema              VARCHAR(100),
        source_table               VARCHAR(255),
        source_path                VARCHAR(500),
        source_format              VARCHAR(50)  NOT NULL,
        file_pattern               VARCHAR(255),
        target_layer               VARCHAR(50)  NOT NULL,
        target_schema              VARCHAR(100) NOT NULL,
        target_table               VARCHAR(255) NOT NULL,
        load_type                  VARCHAR(50)  NOT NULL,
        watermark_column           VARCHAR(100),
        last_watermark             VARCHAR(255)
    );

    -- 3. Create Table: meta.etl_transform_config
    CREATE TABLE meta.etl_transform_config (
        transform_config_id        BIGINT       NOT NULL,
        pipeline_name              VARCHAR(255) NOT NULL,
        source_layer               VARCHAR(50)  NOT NULL,
        source_schema              VARCHAR(100) NOT NULL,
        source_table               VARCHAR(255) NOT NULL,
        target_layer               VARCHAR(50)  NOT NULL,
        target_schema              VARCHAR(100) NOT NULL,
        target_table               VARCHAR(255) NOT NULL,
        transform_type             VARCHAR(50)  NOT NULL,
        primary_key_columns        VARCHAR(255),
        partition_column           VARCHAR(100),
        dependency_pipeline        VARCHAR(255),
        notebook_id                VARCHAR(255),
        watermark_column           VARCHAR(100),
        last_watermark             VARCHAR(255)
    );

    -- 4. Create Table: meta.etl_execution_log
    CREATE TABLE meta.etl_execution_log (
        log_id                     BIGINT       NOT NULL,
        pipeline_run_id            VARCHAR(255) NOT NULL,
        controller_id              BIGINT       NOT NULL,
        pipeline_name              VARCHAR(255) NOT NULL,
        start_time                 DATETIME2(6) NOT NULL, -- Ép kiểu scale (6)
        end_time                   DATETIME2(6),          -- Ép kiểu scale (6)
        status                     VARCHAR(50)  NOT NULL,
        rows_read                  BIGINT,
        rows_inserted              BIGINT,
        rows_updated               BIGINT,
        rows_rejected              BIGINT,
        watermark_from             VARCHAR(255),
        watermark_to               VARCHAR(255),
        dynamic_source_file        VARCHAR(500),
        error_message              VARCHAR(MAX),
        logged_at                  DATETIME2(6) NOT NULL  -- Ép kiểu scale (6)
    );

    -- 5. Create Table: meta.error_logging_table
    CREATE TABLE meta.error_logging_table (
        error_id                   VARCHAR(50)  NOT NULL,
        execution_id               BIGINT,
        error_pipelinename         VARCHAR(255) NOT NULL,
        error_timestamp            DATETIME2(6) NOT NULL, -- Ép kiểu scale (6)
        error_code                 VARCHAR(100) NOT NULL,
        layer_name                 VARCHAR(50)  NOT NULL,
        target_table               VARCHAR(255) NOT NULL,
        error_message              VARCHAR(MAX) NOT NULL,
        error_severity_level       VARCHAR(50)  NOT NULL,
        bad_record_content         VARCHAR(MAX),
        status                     VARCHAR(50)  NOT NULL,
        updated_at                 DATETIME2(6) NOT NULL  -- Ép kiểu scale (6)
    );

    -- ==========================================================================================
    -- STEP 2 — SEED CONFIGURATION (INSERT DATA)
    -- ==========================================================================================
    DELETE FROM meta.etl_transform_config;
    DELETE FROM meta.etl_ingestion_config;
    DELETE FROM meta.etl_pipeline_config;

    -- Dùng SYSDATETIME() đồng bộ toàn luồng hệ thống
    INSERT INTO meta.etl_pipeline_config (
        pipeline_name, pipeline_stage, is_active, retry_count, retry_interval_minutes, timeout_minutes, created_at, created_by
    ) VALUES
        ('master_pipeline', 'ingestion', 1, 1, 5, 120, SYSDATETIME(), 'seed'),
        ('ingest_crm_to_bronze', 'ingestion', 1, 3, 5, 60, SYSDATETIME(), 'seed'),
        ('ingest_policy_to_bronze', 'ingestion', 1, 3, 5, 60, SYSDATETIME(), 'seed'),
        ('silver_to_gold', 'transformation', 1, 3, 5, 90, SYSDATETIME(), 'seed');

    INSERT INTO meta.etl_ingestion_config (
        ingestion_config_id, pipeline_name, source_system, source_schema, source_table, source_path, source_format, file_pattern, target_layer, target_schema, target_table, load_type, watermark_column, last_watermark
    ) VALUES
        (201, 'ingest_crm_to_bronze', 'CRM', 'dbo', 'customers',           NULL, 'SQL_TABLE', NULL, 'bronze', 'bronze', 'crm_customer',           'INCREMENTAL', 'updated_at',   NULL),
        (202, 'ingest_crm_to_bronze', 'CRM', 'dbo', 'agents',              NULL, 'SQL_TABLE', NULL, 'bronze', 'bronze', 'crm_agent',              'INCREMENTAL', 'updated_at',   NULL),
        (203, 'ingest_crm_to_bronze', 'CRM', 'dbo', 'insurance_providers', NULL, 'SQL_TABLE', NULL, 'bronze', 'bronze', 'crm_insurance_provider', 'INCREMENTAL', 'updated_at',   NULL),
        (204, 'ingest_crm_to_bronze', 'CRM', 'dbo', 'vehicle',             NULL, 'SQL_TABLE', NULL, 'bronze', 'bronze', 'crm_vehicle',            'INCREMENTAL', 'updated_at',   NULL),
        (205, 'ingest_crm_to_bronze', 'CRM', 'dbo', 'quotation',           NULL, 'SQL_TABLE', NULL, 'bronze', 'bronze', 'crm_quotation',           'INCREMENTAL', 'updated_at',   NULL),
        (206, 'ingest_crm_to_bronze', 'CRM', 'dbo', 'quotation_item',      NULL, 'SQL_TABLE', NULL, 'bronze', 'bronze', 'crm_quotation_item',      'INCREMENTAL', 'updated_at',   NULL),
        (301, 'ingest_policy_to_bronze', 'PolicyJSON', 'landing', 'policy',       'Files/landing/policy',       'JSON', 'policy_*.json',       'bronze', 'bronze', 'policy_policy',       'INCREMENTAL', 'last_updated', NULL),
        (302, 'ingest_policy_to_bronze', 'PolicyJSON', 'landing', 'payment',      'Files/landing/payment',      'JSON', 'payment_*.json',      'bronze', 'bronze', 'policy_payment',      'INCREMENTAL', 'last_updated', NULL),
        (303, 'ingest_policy_to_bronze', 'PolicyJSON', 'landing', 'cancellation', 'Files/landing/cancellation', 'JSON', 'cancellation_*.json', 'bronze', 'bronze', 'policy_cancellation', 'INCREMENTAL', 'last_updated', NULL);

    INSERT INTO meta.etl_transform_config (
        transform_config_id, pipeline_name, source_layer, source_schema, source_table, target_layer, target_schema, target_table, transform_type, primary_key_columns, partition_column, dependency_pipeline, notebook_id, watermark_column, last_watermark
    ) VALUES
        (401, 'silver_to_gold', 'bronze', 'bronze', 'crm_customer',           'gold', 'gold', 'dim_customer',           'MERGE_SCD2', 'customer_id',          NULL, 'ingest_crm_to_bronze',    '556c18f8-e01b-41fd-a058-9e73675a8fc3', 'updated_at',   NULL),
        (402, 'silver_to_gold', 'bronze', 'bronze', 'crm_agent',              'gold', 'gold', 'dim_agent',              'MERGE_SCD2', 'agent_id',             NULL, 'ingest_crm_to_bronze',    'aaeed0a4-3f7f-489b-9ce5-fc048e110701', 'updated_at',   NULL),
        (403, 'silver_to_gold', 'bronze', 'bronze', 'crm_insurance_provider', 'gold', 'gold', 'dim_insurance_provider', 'MERGE_SCD1', 'provider_code',        NULL, 'ingest_crm_to_bronze',    'cc40ac0a-a176-4de7-b745-b9a83d85effa', 'updated_at',   NULL),
        (404, 'silver_to_gold', 'bronze', 'bronze', 'crm_vehicle',            'gold', 'gold', 'dim_vehicle',            'MERGE_SCD1', 'vehicle_id',           NULL, 'ingest_crm_to_bronze',    '89efc2ff-8bef-4f6e-9cb6-47d8a293d1b4', 'updated_at',   NULL),
        (405, 'silver_to_gold', 'bronze', 'bronze', 'crm_quotation',           'gold', 'gold', 'dim_product_package',     'MERGE_SCD1', 'package_code',         NULL, 'ingest_crm_to_bronze',    'efae997c-b2ef-4983-84d2-d9a12fc500e0', 'updated_at',   NULL),
        (406, 'silver_to_gold', 'bronze', 'bronze', 'crm_quotation',           'gold', 'gold', 'dim_quotation_status',    'MERGE_SCD1', 'quotation_status_name',NULL, 'ingest_crm_to_bronze',    '427b8b12-03d8-4f55-8584-1ba580c78544', 'updated_at',   NULL),
        (407, 'silver_to_gold', 'bronze', 'bronze', 'crm_quotation_item',      'gold', 'gold', 'dim_coverage_type',       'MERGE_SCD1', 'coverage_type_name',   NULL, 'ingest_crm_to_bronze',    '1421dbdc-2738-45f0-8b07-0056b176c85d', 'updated_at',   NULL),
        (408, 'silver_to_gold', 'bronze', 'bronze', 'policy_cancellation',    'gold', 'gold', 'dim_cancellation_reason', 'MERGE_SCD1', 'cancellation_reason',  NULL, 'ingest_policy_to_bronze', 'ffa51ffd-e4e9-4853-9850-27273384b697', 'updated_at',   NULL),
        (409, 'silver_to_gold', 'bronze', 'bronze', 'policy_policy',          'gold', 'gold', 'dim_policy_status',      'MERGE_SCD1', 'policy_status_name',  NULL, 'ingest_policy_to_bronze', '3a4cb4a6-6252-4738-8cff-9a955434d554', 'updated_at',   NULL),
        (410, 'silver_to_gold', 'bronze', 'bronze', 'policy_payment',         'gold', 'gold', 'dim_payment_method',     'MERGE_SCD1', 'payment_method_name', NULL, 'ingest_policy_to_bronze', '2a1050e2-397d-4098-b1c9-a5276222ef72', 'updated_at',   NULL),
        (411, 'silver_to_gold', 'bronze', 'bronze', 'policy_payment',         'gold', 'gold', 'dim_payment_status',     'MERGE_SCD1', 'payment_status_name', NULL, 'ingest_policy_to_bronze', '11592e12-6b31-4d27-a5bc-2d6b3c92f1ac', 'updated_at',   NULL),
        (412, 'silver_to_gold', 'bronze', 'bronze', 'crm_quotation',           'gold', 'gold', 'fact_quotation',          'MERGE_SCD1', 'quotation_id',         NULL, 'ingest_crm_to_bronze',    'e242623f-b72f-4a67-9b1f-ae0a809e9ed7', 'updated_at',   NULL),
        (413, 'silver_to_gold', 'bronze', 'bronze', 'crm_quotation_item',      'gold', 'gold', 'fact_quotation_item',     'MERGE_SCD1', 'quotation_item_id',    NULL, 'ingest_crm_to_bronze',    'f6089553-49a0-424c-bcc7-d56fbb58d3ba', 'updated_at',   NULL),
        (414, 'silver_to_gold', 'bronze', 'bronze', 'policy_policy',          'gold', 'gold', 'fact_policy',            'MERGE_SCD1', 'policy_id',            NULL, 'ingest_policy_to_bronze', '6ea1e77d-19f6-4970-b99d-64e7accd32f4', 'last_updated',   NULL),
        (415, 'silver_to_gold', 'bronze', 'bronze', 'policy_payment',         'gold', 'gold', 'fact_payment',           'MERGE_SCD1', 'payment_id',           NULL, 'ingest_policy_to_bronze', '22a7ba56-3105-4ae1-85c2-e790910da73c', 'last_updated',   NULL),
        (416, 'silver_to_gold', 'bronze', 'bronze', 'policy_cancellation',    'gold', 'gold', 'fact_cancellation',       'MERGE_SCD1', 'cancellation_id',      NULL, 'ingest_policy_to_bronze', '7fd44046-2e97-43b0-9ce6-612bb44f8a98', 'last_updated',   NULL),
        
        (311, 'bronze_to_silver', 'bronze', 'bronze', 'crm_agent',              'silver', 'silver', 'crm_agent',              'MERGE_SCD1', 'agent_id',          NULL, NULL, '4668f82b-aca9-4ff8-a01b-4811a9737334', 'updated_at',   NULL),
        (312, 'bronze_to_silver', 'bronze', 'bronze', 'crm_customer',           'silver', 'silver', 'crm_customer',           'MERGE_SCD1', 'customer_id',       NULL, NULL, 'ca5e2de4-3406-494a-be09-ec7f8c3124b6', 'updated_at',   NULL),
        (313, 'bronze_to_silver', 'bronze', 'bronze', 'crm_insurance_provider', 'silver', 'silver', 'crm_insurance_provider', 'MERGE_SCD1', 'provider_code',     NULL, NULL, '9b6a0452-fdb0-45ac-a6ab-962cef215477', 'updated_at',   NULL),
        (314, 'bronze_to_silver', 'bronze', 'bronze', 'crm_quotation',          'silver', 'silver', 'crm_quotation',          'MERGE_SCD1', 'quotation_id',      NULL, NULL, '9ab3db24-3c43-4513-8255-83f4e5b65e4d', 'updated_at',   NULL),
        (315, 'bronze_to_silver', 'bronze', 'bronze', 'crm_quotation_item',     'silver', 'silver', 'crm_quotation_item',     'MERGE_SCD1', 'quotation_item_id', NULL, NULL, '6ef9fb86-59de-42ec-abb9-7858b2bf8871', 'updated_at',   NULL),
        (316, 'bronze_to_silver', 'bronze', 'bronze', 'crm_vehicle',            'silver', 'silver', 'crm_vehicle',            'MERGE_SCD1', 'vehicle_id',        NULL, NULL, '3786e290-f02d-411d-994d-383d5185f19b', 'updated_at',   NULL),
        (317, 'bronze_to_silver', 'bronze', 'bronze', 'policy_cancellation',    'silver', 'silver', 'policy_cancellation',    'MERGE_SCD1', 'cancellation_id',   NULL, NULL, '3e43c05c-8548-46a9-bfb6-01c9c7339f63', 'last_updated', NULL),
        (318, 'bronze_to_silver', 'bronze', 'bronze', 'policy_payment',         'silver', 'silver', 'policy_payment',         'MERGE_SCD1', 'payment_id',        NULL, NULL, 'e7912425-2f19-46d4-92e8-60aaefa08f44', 'last_updated', NULL),
        (319, 'bronze_to_silver', 'bronze', 'bronze', 'policy_policy',          'silver', 'silver', 'policy_policy',          'MERGE_SCD1', 'policy_id',         NULL, NULL, '02bde05b-9cbf-4a7b-ae54-962ef6d64580', 'last_updated', NULL);
END;