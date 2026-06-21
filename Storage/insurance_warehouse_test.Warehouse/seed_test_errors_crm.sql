/*
  Seed 6 open errors for recover_ingest_crm_to_bronze testing.

  Simulates a failed ingest_crm_to_bronze run where all 6 CRM Copy activities failed.
  Recovery lookup (recovery_lookup_errors.sql) joins:
    error_logging_table.execution_id → etl_execution_log.log_id
    etl_execution_log.controller_id  → etl_ingestion_config.ingestion_config_id

  Run in: Fabric Warehouse (insurance_warehouse) after seed_meta_data.sql seed.

  Recovery pipeline parameters (use the same @failed_run_id below):
    failed_ingest_run_id = TEST-FAILED-INGEST-RUN-001
    pipeline_name        = ingest_crm_to_bronze
    error_status         = New
    default_watermark    = 1900-01-01   (or your ingest default)

  Optional cleanup at bottom removes only rows created by this script.
*/

DECLARE @failed_run_id VARCHAR(64) = 'TEST-FAILED-INGEST-RUN-001';
DECLARE @pipeline_name VARCHAR(100) = 'ingest_crm_to_bronze';
DECLARE @base_log_id   BIGINT;
DECLARE @wm_from       VARCHAR(200) = '1900-01-01T00:00:00.000';

SELECT @base_log_id = COALESCE(MAX(log_id), 0) FROM meta.etl_execution_log;

-- ---------------------------------------------------------------------------
-- 1) Entity execution log rows (one per failed CRM table)
-- ---------------------------------------------------------------------------
INSERT INTO meta.etl_execution_log (
    log_id, pipeline_run_id, controller_id, pipeline_name,
    start_time, end_time, status,
    rows_read, rows_inserted, rows_updated, rows_rejected,
    watermark_from, watermark_to, dynamic_source_file, error_message, logged_at
)
VALUES
    (@base_log_id + 1, @failed_run_id, 201, @pipeline_name,
     SYSUTCDATETIME(), SYSUTCDATETIME(), 'Failed',
     0, 0, 0, 0,
     @wm_from, NULL, NULL,
     'Simulated COPY_FAILED: dbo.customers → bronze.crm_customer', SYSUTCDATETIME()),
    (@base_log_id + 2, @failed_run_id, 202, @pipeline_name,
     SYSUTCDATETIME(), SYSUTCDATETIME(), 'Failed',
     0, 0, 0, 0,
     @wm_from, NULL, NULL,
     'Simulated COPY_FAILED: dbo.agents → bronze.crm_agent', SYSUTCDATETIME()),
    (@base_log_id + 3, @failed_run_id, 203, @pipeline_name,
     SYSUTCDATETIME(), SYSUTCDATETIME(), 'Failed',
     0, 0, 0, 0,
     @wm_from, NULL, NULL,
     'Simulated COPY_FAILED: dbo.insurance_providers → bronze.crm_insurance_provider', SYSUTCDATETIME()),
    (@base_log_id + 4, @failed_run_id, 204, @pipeline_name,
     SYSUTCDATETIME(), SYSUTCDATETIME(), 'Failed',
     0, 0, 0, 0,
     @wm_from, NULL, NULL,
     'Simulated COPY_FAILED: dbo.vehicle → bronze.crm_vehicle', SYSUTCDATETIME()),
    (@base_log_id + 5, @failed_run_id, 205, @pipeline_name,
     SYSUTCDATETIME(), SYSUTCDATETIME(), 'Failed',
     0, 0, 0, 0,
     @wm_from, NULL, NULL,
     'Simulated COPY_FAILED: dbo.quotation → bronze.crm_quotation', SYSUTCDATETIME()),
    (@base_log_id + 6, @failed_run_id, 206, @pipeline_name,
     SYSUTCDATETIME(), SYSUTCDATETIME(), 'Failed',
     0, 0, 0, 0,
     @wm_from, NULL, NULL,
     'Simulated COPY_FAILED: dbo.quotation_item → bronze.crm_quotation_item', SYSUTCDATETIME());

-- ---------------------------------------------------------------------------
-- 2) Error rows (status = New — required for recovery lookup)
-- ---------------------------------------------------------------------------
INSERT INTO meta.error_logging_table (
    error_id,
    execution_id,
    error_pipelinename,
    error_timestamp,
    error_code,
    layer_name,
    target_table,
    error_message,
    error_severity_level,
    bad_record_content,
    status,
    updated_at
)
VALUES
    ('e2010000-0000-4000-8000-000000000201', @base_log_id + 1, @pipeline_name,
     SYSUTCDATETIME(), 'COPY_FAILED', 'bronze', 'crm_customer',
     'Test error: Copy activity failed loading dbo.customers',
     'ERROR', NULL, 'New', SYSUTCDATETIME()),
    ('e2020000-0000-4000-8000-000000000202', @base_log_id + 2, @pipeline_name,
     SYSUTCDATETIME(), 'COPY_FAILED', 'bronze', 'crm_agent',
     'Test error: Copy activity failed loading dbo.agents',
     'ERROR', NULL, 'New', SYSUTCDATETIME()),
    ('e2030000-0000-4000-8000-000000000203', @base_log_id + 3, @pipeline_name,
     SYSUTCDATETIME(), 'COPY_FAILED', 'bronze', 'crm_insurance_provider',
     'Test error: Copy activity failed loading dbo.insurance_providers',
     'ERROR', NULL, 'New', SYSUTCDATETIME()),
    ('e2040000-0000-4000-8000-000000000204', @base_log_id + 4, @pipeline_name,
     SYSUTCDATETIME(), 'COPY_FAILED', 'bronze', 'crm_vehicle',
     'Test error: Copy activity failed loading dbo.vehicle',
     'ERROR', NULL, 'New', SYSUTCDATETIME()),
    ('e2050000-0000-4000-8000-000000000205', @base_log_id + 5, @pipeline_name,
     SYSUTCDATETIME(), 'COPY_FAILED', 'bronze', 'crm_quotation',
     'Test error: Copy activity failed loading dbo.quotation',
     'ERROR', NULL, 'New', SYSUTCDATETIME()),
    ('e2060000-0000-4000-8000-000000000206', @base_log_id + 6, @pipeline_name,
     SYSUTCDATETIME(), 'COPY_FAILED', 'bronze', 'crm_quotation_item',
     'Test error: Copy activity failed loading dbo.quotation_item',
     'ERROR', NULL, 'New', SYSUTCDATETIME());

-- ---------------------------------------------------------------------------
-- 3) Verify — same shape as recovery_lookup_errors.sql
-- ---------------------------------------------------------------------------
DECLARE @error_status VARCHAR(64) = 'New';

;WITH errors AS (
    SELECT
        e.error_id,
        e.error_code,
        e.target_table,
        e.error_message,
        l.controller_id AS ingestion_config_id,
        l.pipeline_run_id,
        ROW_NUMBER() OVER (
            PARTITION BY l.controller_id
            ORDER BY e.error_timestamp DESC
        ) AS rn
    FROM meta.error_logging_table e
    INNER JOIN meta.etl_execution_log l ON e.execution_id = l.log_id
    WHERE l.pipeline_run_id = @failed_run_id
      AND l.pipeline_name = @pipeline_name
      AND e.status = @error_status
)
SELECT
    err.error_id,
    err.error_code,
    err.error_message AS original_error_message,
    ic.ingestion_config_id,
    ic.pipeline_name,
    ic.source_system,
    ic.source_schema,
    ic.source_table,
    ic.target_schema,
    ic.target_table,
    ic.load_type,
    ic.watermark_column,
    ic.last_watermark
FROM errors err
INNER JOIN meta.etl_ingestion_config ic
    ON ic.ingestion_config_id = err.ingestion_config_id
WHERE err.rn = 1
ORDER BY ic.ingestion_config_id;
-- Expect 6 rows (201–206).



/*
-- Cleanup (run when finished testing)
DELETE FROM meta.error_logging_table
WHERE error_id IN (
    'e2010000-0000-4000-8000-000000000201',
    'e2020000-0000-4000-8000-000000000202',
    'e2030000-0000-4000-8000-000000000203',
    'e2040000-0000-4000-8000-000000000204',
    'e2050000-0000-4000-8000-000000000205',
    'e2060000-0000-4000-8000-000000000206'
);

DELETE FROM meta.etl_execution_log
WHERE pipeline_run_id = 'TEST-FAILED-INGEST-RUN-001'
  AND pipeline_name = 'ingest_crm_to_bronze';
*/
