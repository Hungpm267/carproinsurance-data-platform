# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": "",
# META       "known_lakehouses": []
# META     },
# META     "warehouse": {
# META       "default_warehouse": "db297df8-2356-b5dd-4b73-02bfbec559f0",
# META       "known_warehouses": [
# META         {
# META           "id": "db297df8-2356-b5dd-4b73-02bfbec559f0",
# META           "type": "Datawarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Deploy `meta` schema (DDL + seed + verify)
# 
# Aligned with **`ETL_Metadata_Data_Dictionary.xlsx`** (5 control tables).
# 
# **Before run:** attach the lakehouse.
# 
# | Lakehouse table | Excel sheet |
# |-----------------|-------------|
# | `meta.etl_pipeline_config` | `1.etl_pipeline_config` |
# | `meta.etl_ingestion_config` | `2.etl_ingestion_config` |
# | `meta.etl_transform_config` | `3.etl_transform_config` |
# | `meta.etl_execution_log` | `4.etl_execution_log` |
# | `meta.error_logging_table` | `5.error_logging_table` |
# 
# Incremental cursor: **`etl_ingestion_config.last_watermark`** (updated after successful Copy).
# Column name for cutoff: **`watermark_column`** (e.g. `updated_at`).
# 
# > **Warning:** Step 1 drops and recreates all `meta.etl_*` tables (destructive).

# MARKDOWN ********************

# ## Step 1 — Create `meta` tables

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE SCHEMA IF NOT EXISTS meta;
# MAGIC 
# MAGIC DROP TABLE IF EXISTS meta.error_logging_table;
# MAGIC DROP TABLE IF EXISTS meta.etl_execution_log;
# MAGIC DROP TABLE IF EXISTS meta.etl_transform_config;
# MAGIC DROP TABLE IF EXISTS meta.etl_ingestion_config;
# MAGIC DROP TABLE IF EXISTS meta.etl_pipeline_config;
# MAGIC 
# MAGIC -- -- Legacy names (re-run safe after rename)
# MAGIC -- DROP TABLE IF EXISTS meta.error_log;
# MAGIC -- DROP TABLE IF EXISTS meta.audit_log;
# MAGIC -- DROP TABLE IF EXISTS meta.pipeline_controller;
# MAGIC -- DROP TABLE IF EXISTS meta.pipeline_execution;
# MAGIC -- DROP TABLE IF EXISTS meta.transform_config;
# MAGIC -- DROP TABLE IF EXISTS meta.ingestion_config;
# MAGIC -- DROP TABLE IF EXISTS meta.pipeline_config;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE meta.etl_pipeline_config (
# MAGIC     pipeline_name              STRING    NOT NULL COMMENT 'PK. Fabric Data Pipeline name',
# MAGIC     pipeline_stage             STRING    NOT NULL COMMENT 'ingestion | transformation',
# MAGIC     is_active                  BOOLEAN   NOT NULL COMMENT '1 = active',
# MAGIC     retry_count                INT       NOT NULL,
# MAGIC     retry_interval_minutes     INT       NOT NULL,
# MAGIC     timeout_minutes            INT       NOT NULL,
# MAGIC     created_at                 TIMESTAMP NOT NULL,
# MAGIC     updated_at                 TIMESTAMP,
# MAGIC     created_by                 STRING,
# MAGIC     updated_by                 STRING
# MAGIC ) USING DELTA
# MAGIC COMMENT 'etl_pipeline_config — pipeline registry'

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE meta.etl_ingestion_config (
# MAGIC     ingestion_config_id        BIGINT    NOT NULL COMMENT 'PK',
# MAGIC     pipeline_name              STRING    NOT NULL COMMENT 'FK → etl_pipeline_config',
# MAGIC     source_system              STRING             COMMENT 'CRM | PolicyJSON',
# MAGIC     source_schema              STRING             COMMENT 'dbo | landing',
# MAGIC     source_table               STRING             COMMENT 'Source table or landing entity',
# MAGIC     source_path                STRING             COMMENT 'Lakehouse path for files',
# MAGIC     source_format              STRING    NOT NULL COMMENT 'SQL_TABLE | JSON | CSV | PARQUET',
# MAGIC     file_pattern               STRING             COMMENT 'e.g. policy_*.json',
# MAGIC     target_layer               STRING    NOT NULL COMMENT 'landing | bronze',
# MAGIC     target_schema              STRING    NOT NULL,
# MAGIC     target_table               STRING    NOT NULL,
# MAGIC     load_type                  STRING    NOT NULL COMMENT 'FULL | INCREMENTAL',
# MAGIC     watermark_column           STRING             COMMENT 'Incremental column name e.g. updated_at',
# MAGIC     last_watermark             STRING             COMMENT 'High-water value; updated after successful Copy'
# MAGIC ) USING DELTA
# MAGIC COMMENT 'etl_ingestion_config — one row per Copy (source → landing/bronze)'

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE meta.etl_transform_config (
# MAGIC     transform_config_id        BIGINT    NOT NULL COMMENT 'PK',
# MAGIC     pipeline_name              STRING    NOT NULL COMMENT 'FK → etl_pipeline_config',
# MAGIC     source_layer               STRING    NOT NULL COMMENT 'bronze',
# MAGIC     source_schema              STRING    NOT NULL,
# MAGIC     source_table               STRING    NOT NULL,
# MAGIC     target_layer               STRING    NOT NULL COMMENT 'gold',
# MAGIC     target_schema              STRING    NOT NULL,
# MAGIC     target_table               STRING    NOT NULL,
# MAGIC     transform_type             STRING    NOT NULL COMMENT 'APPEND | MERGE_SCD1 | MERGE_SCD2 | OVERWRITE',
# MAGIC     primary_key_columns        STRING,
# MAGIC     partition_column           STRING,
# MAGIC     dependency_pipeline        STRING,
# MAGIC     notebook_id                STRING             COMMENT 'Fabric notebook file name',
# MAGIC     watermark_column           STRING             COMMENT 'Incremental column name e.g. updated_at',
# MAGIC     last_watermark             STRING             COMMENT 'High-water value; updated after successful Copy'
# MAGIC ) USING DELTA
# MAGIC COMMENT 'etl_transform_config — bronze → gold'

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE meta.etl_execution_log (
# MAGIC     log_id                     BIGINT    NOT NULL COMMENT 'PK — generate in bookend notebook',
# MAGIC     pipeline_run_id            STRING    NOT NULL COMMENT 'Fabric Pipeline RunId',
# MAGIC     controller_id              BIGINT    NOT NULL COMMENT 'Config row id (0 = pipeline-level)',
# MAGIC     pipeline_name              STRING    NOT NULL,
# MAGIC     start_time                 TIMESTAMP NOT NULL,
# MAGIC     end_time                   TIMESTAMP,
# MAGIC     status                     STRING    NOT NULL COMMENT 'Running | Success | Failed',
# MAGIC     rows_read                  BIGINT,
# MAGIC     rows_inserted              BIGINT,
# MAGIC     rows_updated               BIGINT,
# MAGIC     rows_rejected              BIGINT,
# MAGIC     watermark_from             STRING,
# MAGIC     watermark_to               STRING,
# MAGIC     dynamic_source_file        STRING,
# MAGIC     error_message              STRING,
# MAGIC     logged_at                  TIMESTAMP NOT NULL
# MAGIC ) USING DELTA
# MAGIC COMMENT 'etl_execution_log — one row per pipeline run'

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE meta.error_logging_table (
# MAGIC     error_id                   STRING    NOT NULL COMMENT 'PK — UUID',
# MAGIC     execution_id               BIGINT             COMMENT 'FK → etl_execution_log.log_id',
# MAGIC     error_pipelinename         STRING    NOT NULL,
# MAGIC     error_timestamp            TIMESTAMP NOT NULL,
# MAGIC     error_code                 STRING    NOT NULL,
# MAGIC     layer_name                 STRING    NOT NULL COMMENT 'landing | bronze | gold',
# MAGIC     target_table               STRING    NOT NULL,
# MAGIC     error_message              STRING    NOT NULL,
# MAGIC     error_severity_level       STRING    NOT NULL COMMENT 'INFO | WARNING | ERROR | CRITICAL',
# MAGIC     bad_record_content         STRING             COMMENT 'JSON row for replay',
# MAGIC     status                     STRING    NOT NULL COMMENT 'New | In-Progress | Resolved | Ignored',
# MAGIC     updated_at                 TIMESTAMP NOT NULL
# MAGIC ) USING DELTA
# MAGIC COMMENT 'error_logging_table — failures and quarantined rows'

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 2 — Seed configuration

# CELL ********************

# MAGIC %%sql
# MAGIC DELETE FROM meta.etl_transform_config;
# MAGIC DELETE FROM meta.etl_ingestion_config;
# MAGIC DELETE FROM meta.etl_pipeline_config;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC INSERT INTO meta.etl_pipeline_config (
# MAGIC     pipeline_name, pipeline_stage, is_active,
# MAGIC     retry_count, retry_interval_minutes, timeout_minutes,
# MAGIC     created_at, created_by
# MAGIC ) VALUES
# MAGIC     ('master_pipeline', 'ingestion',
# MAGIC      true, 1, 5, 120, current_timestamp(), 'seed'),
# MAGIC     ('ingest_crm_to_bronze', 'ingestion',
# MAGIC      true, 3, 5, 60, current_timestamp(), 'seed'),
# MAGIC     ('ingest_policy_to_bronze', 'ingestion',
# MAGIC      true, 3, 5, 60, current_timestamp(), 'seed'),
# MAGIC     ('bronze_to_silver', 'transformation',
# MAGIC      true, 3, 5, 90, current_timestamp(), 'seed'),
# MAGIC     ('silver_to_gold', 'transformation',
# MAGIC      true, 3, 5, 90, current_timestamp(), 'seed')

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -- %%sql
# -- INSERT INTO meta.etl_ingestion_config (
# --     ingestion_config_id, pipeline_name,
# --     source_system, source_schema, source_table,
# --     source_path, source_format, file_pattern,
# --     target_layer, target_schema, target_table,
# --     load_type, watermark_column, last_watermark
# -- ) VALUES
# --     (101, 'ingest_landing_json',
# --      'PolicyJSON', NULL, NULL,
# --      'Files/landing/policy', 'JSON', 'policy_*.json',
# --      'landing', 'landing', 'policy',
# --      'FULL', NULL, NULL),
# --     (102, 'ingest_landing_json',
# --      'PolicyJSON', NULL, NULL,
# --      'Files/landing/payment', 'JSON', 'payment_*.json',
# --      'landing', 'landing', 'payment',
# --      'FULL', NULL, NULL),
# --     (103, 'ingest_landing_json',
# --      'PolicyJSON', NULL, NULL,
# --      'Files/landing/cancellation', 'JSON', 'cancellation_*.json',
# --      'landing', 'landing', 'cancellation',
# --      'FULL', NULL, NULL)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC INSERT INTO meta.etl_ingestion_config (
# MAGIC     ingestion_config_id, pipeline_name,
# MAGIC     source_system, source_schema, source_table,
# MAGIC     source_path, source_format, file_pattern,
# MAGIC     target_layer, target_schema, target_table,
# MAGIC     load_type, watermark_column, last_watermark
# MAGIC ) VALUES
# MAGIC     (201, 'ingest_crm_to_bronze',
# MAGIC      'CRM', 'dbo', 'customers',
# MAGIC      NULL, 'SQL_TABLE', NULL,
# MAGIC      'bronze', 'bronze', 'crm_customer',
# MAGIC      'INCREMENTAL', 'updated_at', NULL),
# MAGIC     (202, 'ingest_crm_to_bronze',
# MAGIC      'CRM', 'dbo', 'agents',
# MAGIC      NULL, 'SQL_TABLE', NULL,
# MAGIC      'bronze', 'bronze', 'crm_agent',
# MAGIC      'INCREMENTAL', 'updated_at', NULL),
# MAGIC     (203, 'ingest_crm_to_bronze',
# MAGIC      'CRM', 'dbo', 'insurance_providers',
# MAGIC      NULL, 'SQL_TABLE', NULL,
# MAGIC      'bronze', 'bronze', 'crm_insurance_provider',
# MAGIC      'INCREMENTAL', 'updated_at', NULL),
# MAGIC     (204, 'ingest_crm_to_bronze',
# MAGIC      'CRM', 'dbo', 'vehicle',
# MAGIC      NULL, 'SQL_TABLE', NULL,
# MAGIC      'bronze', 'bronze', 'crm_vehicle',
# MAGIC      'INCREMENTAL', 'updated_at', NULL),
# MAGIC     (205, 'ingest_crm_to_bronze',
# MAGIC      'CRM', 'dbo', 'quotation',
# MAGIC      NULL, 'SQL_TABLE', NULL,
# MAGIC      'bronze', 'bronze', 'crm_quotation',
# MAGIC      'INCREMENTAL', 'updated_at', NULL),
# MAGIC     (206, 'ingest_crm_to_bronze',
# MAGIC      'CRM', 'dbo', 'quotation_item',
# MAGIC      NULL, 'SQL_TABLE', NULL,
# MAGIC      'bronze', 'bronze', 'crm_quotation_item',
# MAGIC      'INCREMENTAL', 'updated_at', NULL)

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC INSERT INTO meta.etl_ingestion_config (
# MAGIC     ingestion_config_id, pipeline_name,
# MAGIC     source_system, source_schema, source_table,
# MAGIC     source_path, source_format, file_pattern,
# MAGIC     target_layer, target_schema, target_table,
# MAGIC     load_type, watermark_column, last_watermark
# MAGIC ) VALUES
# MAGIC     (301, 'ingest_policy_to_bronze',
# MAGIC      'PolicyJSON', 'landing', 'policy',
# MAGIC      'Files/landing/policy', 'JSON', 'policy_*.json',
# MAGIC      'bronze', 'bronze', 'policy_policy',
# MAGIC      'INCREMENTAL', 'last_updated', NULL),
# MAGIC     (302, 'ingest_policy_to_bronze',
# MAGIC      'PolicyJSON', 'landing', 'payment',
# MAGIC      'Files/landing/payment', 'JSON', 'payment_*.json',
# MAGIC      'bronze', 'bronze', 'policy_payment',
# MAGIC      'INCREMENTAL', 'last_updated', NULL),
# MAGIC     (303, 'ingest_policy_to_bronze',
# MAGIC      'PolicyJSON', 'landing', 'cancellation',
# MAGIC      'Files/landing/cancellation', 'JSON', 'cancellation_*.json',
# MAGIC      'bronze', 'bronze', 'policy_cancellation',
# MAGIC      'INCREMENTAL', 'last_updated', NULL)

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# import notebookutils
# nb = notebookutils.notebook.get("nb_dim_agent")  # display name in workspace
# print(nb.id)
# print(nb.displayName)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import notebookutils
for nb in notebookutils.notebook.list():
    print(nb.displayName, nb.id)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC INSERT INTO meta.etl_transform_config (
# MAGIC     transform_config_id, pipeline_name,
# MAGIC     source_layer, source_schema, source_table,
# MAGIC     target_layer, target_schema, target_table,
# MAGIC     transform_type, primary_key_columns, partition_column,
# MAGIC     dependency_pipeline, notebook_id, watermark_column, last_watermark
# MAGIC ) VALUES
# MAGIC     (401, 'silver_to_gold',
# MAGIC      'bronze', 'bronze', 'crm_customer',
# MAGIC      'gold', 'gold', 'dim_customer',
# MAGIC      'MERGE_SCD2', 'customer_id', NULL,
# MAGIC      'ingest_crm_to_bronze', '556c18f8-e01b-41fd-a058-9e73675a8fc3', 'updated_at', NULL),
# MAGIC     (402, 'silver_to_gold',
# MAGIC      'bronze', 'bronze', 'crm_agent',
# MAGIC      'gold', 'gold', 'dim_agent',
# MAGIC      'MERGE_SCD2', 'agent_id', NULL,
# MAGIC      'ingest_crm_to_bronze', 'aaeed0a4-3f7f-489b-9ce5-fc048e110701', 'updated_at', NULL),
# MAGIC     (403, 'silver_to_gold',
# MAGIC      'bronze', 'bronze', 'crm_insurance_provider',
# MAGIC      'gold', 'gold', 'dim_insurance_provider',
# MAGIC      'MERGE_SCD1', 'provider_code', NULL,
# MAGIC      'ingest_crm_to_bronze', 'cc40ac0a-a176-4de7-b745-b9a83d85effa', 'updated_at', NULL),
# MAGIC     (404, 'silver_to_gold',
# MAGIC      'bronze', 'bronze', 'crm_vehicle',
# MAGIC      'gold', 'gold', 'dim_vehicle',
# MAGIC      'MERGE_SCD1', 'vehicle_id', NULL,
# MAGIC      'ingest_crm_to_bronze', '89efc2ff-8bef-4f6e-9cb6-47d8a293d1b4', 'updated_at', NULL),
# MAGIC     (405, 'silver_to_gold',
# MAGIC      'bronze', 'bronze', 'crm_quotation',
# MAGIC      'gold', 'gold', 'dim_product_package',
# MAGIC      'MERGE_SCD1', 'package_code', NULL,
# MAGIC      'ingest_crm_to_bronze', 'efae997c-b2ef-4983-84d2-d9a12fc500e0', 'updated_at', NULL),
# MAGIC     (406, 'silver_to_gold',
# MAGIC      'bronze', 'bronze', 'crm_quotation',
# MAGIC      'gold', 'gold', 'dim_quotation_status',
# MAGIC      'MERGE_SCD1', 'quotation_status_name', NULL,
# MAGIC      'ingest_crm_to_bronze', '427b8b12-03d8-4f55-8584-1ba580c78544', 'updated_at', NULL),
# MAGIC     (407, 'silver_to_gold',
# MAGIC      'bronze', 'bronze', 'crm_quotation_item',
# MAGIC      'gold', 'gold', 'dim_coverage_type',
# MAGIC      'MERGE_SCD1', 'coverage_type_name', NULL,
# MAGIC      'ingest_crm_to_bronze', '1421dbdc-2738-45f0-8b07-0056b176c85d', 'updated_at', NULL),
# MAGIC     (408, 'silver_to_gold',
# MAGIC      'bronze', 'bronze', 'policy_cancellation',
# MAGIC      'gold', 'gold', 'dim_cancellation_reason',
# MAGIC      'MERGE_SCD1', 'cancellation_reason', NULL,
# MAGIC      'ingest_policy_to_bronze', 'ffa51ffd-e4e9-4853-9850-27273384b697', 'updated_at', NULL),
# MAGIC     (409, 'silver_to_gold',
# MAGIC      'bronze', 'bronze', 'policy_policy',
# MAGIC      'gold', 'gold', 'dim_policy_status',
# MAGIC      'MERGE_SCD1', 'policy_status_name', NULL,
# MAGIC      'ingest_policy_to_bronze', '3a4cb4a6-6252-4738-8cff-9a955434d554', 'updated_at', NULL),
# MAGIC     (410, 'silver_to_gold',
# MAGIC      'bronze', 'bronze', 'policy_payment',
# MAGIC      'gold', 'gold', 'dim_payment_method',
# MAGIC      'MERGE_SCD1', 'payment_method_name', NULL,
# MAGIC      'ingest_policy_to_bronze', '2a1050e2-397d-4098-b1c9-a5276222ef72', 'updated_at', NULL),
# MAGIC     (411, 'silver_to_gold',
# MAGIC      'bronze', 'bronze', 'policy_payment',
# MAGIC      'gold', 'gold', 'dim_payment_status',
# MAGIC      'MERGE_SCD1', 'payment_status_name', NULL,
# MAGIC      'ingest_policy_to_bronze', '11592e12-6b31-4d27-a5bc-2d6b3c92f1ac', 'updated_at', NULL),
# MAGIC     (412, 'silver_to_gold',
# MAGIC      'bronze', 'bronze', 'crm_quotation',
# MAGIC      'gold', 'gold', 'fact_quotation',
# MAGIC      'MERGE_SCD1', 'quotation_id', NULL,
# MAGIC      'ingest_crm_to_bronze', 'e242623f-b72f-4a67-9b1f-ae0a809e9ed7', 'updated_at', NULL),
# MAGIC     (413, 'silver_to_gold',
# MAGIC      'bronze', 'bronze', 'crm_quotation_item',
# MAGIC      'gold', 'gold', 'fact_quotation_item',
# MAGIC      'MERGE_SCD1', 'quotation_item_id', NULL,
# MAGIC      'ingest_crm_to_bronze', 'f6089553-49a0-424c-bcc7-d56fbb58d3ba', 'updated_at', NULL),
# MAGIC     (414, 'silver_to_gold',
# MAGIC      'bronze', 'bronze', 'policy_policy',
# MAGIC      'gold', 'gold', 'fact_policy',
# MAGIC      'MERGE_SCD1', 'policy_id', NULL,
# MAGIC      'ingest_policy_to_bronze', '6ea1e77d-19f6-4970-b99d-64e7accd32f4', 'last_updated', NULL),
# MAGIC     (415, 'silver_to_gold',
# MAGIC      'bronze', 'bronze', 'policy_payment',
# MAGIC      'gold', 'gold', 'fact_payment',
# MAGIC      'MERGE_SCD1', 'payment_id', NULL,
# MAGIC      'ingest_policy_to_bronze', '22a7ba56-3105-4ae1-85c2-e790910da73c', 'last_updated', NULL),
# MAGIC     (416, 'silver_to_gold',
# MAGIC      'bronze', 'bronze', 'policy_cancellation',
# MAGIC      'gold', 'gold', 'fact_cancellation',
# MAGIC      'MERGE_SCD1', 'cancellation_id', NULL,
# MAGIC      'ingest_policy_to_bronze', '7fd44046-2e97-43b0-9ce6-612bb44f8a98', 'last_updated', NULL)

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC INSERT INTO meta.etl_transform_config
# MAGIC (transform_config_id, pipeline_name, source_layer, source_schema, source_table,
# MAGIC  target_layer, target_schema, target_table, transform_type, primary_key_columns,
# MAGIC  partition_column, dependency_pipeline, notebook_id, watermark_column, last_watermark)
# MAGIC VALUES
# MAGIC (301, 'bronze_to_silver', 'bronze', 'bronze', 'crm_agent',             'silver', 'silver', 'crm_agent',             'MERGE_SCD1', 'agent_id',          NULL, NULL, '4668f82b-aca9-4ff8-a01b-4811a9737334', 'updated_at',   NULL),
# MAGIC (302, 'bronze_to_silver', 'bronze', 'bronze', 'crm_customer',          'silver', 'silver', 'crm_customer',          'MERGE_SCD1', 'customer_id',       NULL, NULL, 'ca5e2de4-3406-494a-be09-ec7f8c3124b6', 'updated_at',   NULL),
# MAGIC (303, 'bronze_to_silver', 'bronze', 'bronze', 'crm_insurance_provider','silver', 'silver', 'crm_insurance_provider','MERGE_SCD1', 'provider_code',     NULL, NULL, '9b6a0452-fdb0-45ac-a6ab-962cef215477', 'updated_at',   NULL),
# MAGIC (304, 'bronze_to_silver', 'bronze', 'bronze', 'crm_quotation',         'silver', 'silver', 'crm_quotation',         'MERGE_SCD1', 'quotation_id',      NULL, NULL, '9ab3db24-3c43-4513-8255-83f4e5b65e4d', 'updated_at',   NULL),
# MAGIC (305, 'bronze_to_silver', 'bronze', 'bronze', 'crm_quotation_item',    'silver', 'silver', 'crm_quotation_item',    'MERGE_SCD1', 'quotation_item_id', NULL, NULL, '6ef9fb86-59de-42ec-abb9-7858b2bf8871', 'updated_at',   NULL),
# MAGIC (306, 'bronze_to_silver', 'bronze', 'bronze', 'crm_vehicle',           'silver', 'silver', 'crm_vehicle',           'MERGE_SCD1', 'vehicle_id',        NULL, NULL, '3786e290-f02d-411d-994d-383d5185f19b', 'updated_at',   NULL),
# MAGIC (307, 'bronze_to_silver', 'bronze', 'bronze', 'policy_cancellation',   'silver', 'silver', 'policy_cancellation',   'MERGE_SCD1', 'cancellation_id',   NULL, NULL, '3e43c05c-8548-46a9-bfb6-01c9c7339f63', 'last_updated', NULL),
# MAGIC (308, 'bronze_to_silver', 'bronze', 'bronze', 'policy_payment',        'silver', 'silver', 'policy_payment',        'MERGE_SCD1', 'payment_id',        NULL, NULL, 'e7912425-2f19-46d4-92e8-60aaefa08f44', 'last_updated', NULL),
# MAGIC (309, 'bronze_to_silver', 'bronze', 'bronze', 'policy_policy',         'silver', 'silver', 'policy_policy',         'MERGE_SCD1', 'policy_id',         NULL, NULL, '02bde05b-9cbf-4a7b-ae54-962ef6d64580', 'last_updated', NULL);

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 3 — Verify

# CELL ********************

display(spark.sql("""
SELECT pipeline_name, pipeline_stage, is_active, retry_count, timeout_minutes
FROM meta.etl_pipeline_config
ORDER BY pipeline_name
"""))

display(spark.sql("""
SELECT pipeline_name, COUNT(*) AS ingestion_rows
FROM meta.etl_ingestion_config
GROUP BY pipeline_name
ORDER BY pipeline_name
"""))

display(spark.sql("""
SELECT ingestion_config_id, source_schema, source_table,
       target_schema, target_table, load_type,
       watermark_column, last_watermark
FROM meta.etl_ingestion_config
WHERE pipeline_name = 'ingest_crm_to_bronze'
ORDER BY ingestion_config_id
"""))

display(spark.sql("""
SELECT transform_config_id, source_table, target_table,
       transform_type, notebook_id, dependency_pipeline
FROM meta.etl_transform_config
WHERE pipeline_name = 'silver_to_gold'
ORDER BY transform_config_id
"""))

display(spark.sql("""
SELECT 'etl_execution_log' AS tbl, COUNT(*) AS n FROM meta.etl_execution_log
UNION ALL SELECT 'error_logging_table', COUNT(*) FROM meta.error_logging_table
"""))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

expected = {
    "etl_pipeline_config": 5,
    "etl_ingestion_config": 12,
    "etl_transform_config": 16,
}
for table, want in expected.items():
    got = spark.table(f"meta.{table}").count()
    status = "OK" if got == want else "MISMATCH"
    print(f"{status} meta.{table}: {got} (expected {want})")

for table in ("etl_execution_log", "error_logging_table"):
    got = spark.table(f"meta.{table}").count()
    print(f"OK meta.{table}: {got} rows (runtime only, expect 0 after fresh deploy)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
