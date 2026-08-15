/*
  Seed open errors for recover_silver_to_gold testing.

  Simulates a failed silver_to_gold run where one dim fails and ALL facts are
  cascade-logged (same pattern as the main orchestrator on dim failure).

  Recovery lookup joins:
    error_logging_table.execution_id → etl_execution_log.log_id
    etl_execution_log.controller_id  → etl_transform_config.transform_config_id

  Run in: Fabric Warehouse after ddl_meta_schema1.sql seed.

  Recovery pipeline parameters:
    failed_transform_run_id = TEST-FAILED-SILVER-GOLD-RUN-001
    pipeline_name           = silver_to_gold
    error_status            = New
    recover_pipeline_name   = recover_silver_to_gold

  Seeded scenario:
    dim_customer (401)     → TRANSFORM_FAILED  (root cause)
    fact_quotation (412)   → BLOCKED_BY_DIM_FAILURE
    fact_quotation_item (413)
    fact_policy (414)
    fact_payment (415)
    fact_cancellation (416)
*/

DECLARE @failed_run_id VARCHAR(64) = 'TEST-FAILED-SILVER-GOLD-RUN-001';
DECLARE @pipeline_name VARCHAR(100) = 'silver_to_gold';
DECLARE @base_log_id   BIGINT;
DECLARE @wm_from       VARCHAR(200) = '1900-01-01T00:00:00.000';

SELECT @base_log_id = COALESCE(MAX(log_id), 0) FROM meta.etl_execution_log;

-- ---------------------------------------------------------------------------
-- 1) Entity execution log rows
--    One failed dim + five cascade-blocked facts (6 entities with errors)
-- ---------------------------------------------------------------------------
INSERT INTO meta.etl_execution_log (
    log_id, pipeline_run_id, controller_id, pipeline_name,
    start_time, end_time, status,
    rows_read, rows_inserted, rows_updated, rows_rejected,
    watermark_from, watermark_to, dynamic_source_file, error_message, logged_at
)
VALUES
    (@base_log_id + 1, @failed_run_id, 401, @pipeline_name,
     SYSUTCDATETIME(), SYSUTCDATETIME(), 'Failed',
     0, 0, 0, 0,
     @wm_from, NULL, NULL,
     'Simulated TRANSFORM_FAILED: silver → gold.dim_customer', SYSUTCDATETIME()),
    (@base_log_id + 2, @failed_run_id, 412, @pipeline_name,
     SYSUTCDATETIME(), SYSUTCDATETIME(), 'Failed',
     0, 0, 0, 0,
     @wm_from, NULL, NULL,
     'Simulated BLOCKED_BY_DIM_FAILURE: fact_quotation not run (dim_customer failed)', SYSUTCDATETIME()),
    (@base_log_id + 3, @failed_run_id, 413, @pipeline_name,
     SYSUTCDATETIME(), SYSUTCDATETIME(), 'Failed',
     0, 0, 0, 0,
     @wm_from, NULL, NULL,
     'Simulated BLOCKED_BY_DIM_FAILURE: fact_quotation_item not run (dim_customer failed)', SYSUTCDATETIME()),
    (@base_log_id + 4, @failed_run_id, 414, @pipeline_name,
     SYSUTCDATETIME(), SYSUTCDATETIME(), 'Failed',
     0, 0, 0, 0,
     @wm_from, NULL, NULL,
     'Simulated BLOCKED_BY_DIM_FAILURE: fact_policy not run (dim_customer failed)', SYSUTCDATETIME()),
    (@base_log_id + 5, @failed_run_id, 415, @pipeline_name,
     SYSUTCDATETIME(), SYSUTCDATETIME(), 'Failed',
     0, 0, 0, 0,
     @wm_from, NULL, NULL,
     'Simulated BLOCKED_BY_DIM_FAILURE: fact_payment not run (dim_customer failed)', SYSUTCDATETIME()),
    (@base_log_id + 6, @failed_run_id, 416, @pipeline_name,
     SYSUTCDATETIME(), SYSUTCDATETIME(), 'Failed',
     0, 0, 0, 0,
     @wm_from, NULL, NULL,
     'Simulated BLOCKED_BY_DIM_FAILURE: fact_cancellation not run (dim_customer failed)', SYSUTCDATETIME());

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
    ('e4010000-0000-4000-8000-000000000401', @base_log_id + 1, @pipeline_name,
     SYSUTCDATETIME(), 'TRANSFORM_FAILED', 'gold', 'dim_customer',
     'Test error: Notebook failed loading silver.crm_customer → gold.dim_customer',
     'ERROR', NULL, 'New', SYSUTCDATETIME()),
    ('e4120000-0000-4000-8000-000000000412', @base_log_id + 2, @pipeline_name,
     SYSUTCDATETIME(), 'BLOCKED_BY_DIM_FAILURE', 'gold', 'fact_quotation',
     'Test error: fact_quotation skipped — upstream dim_customer failed',
     'ERROR', NULL, 'New', SYSUTCDATETIME()),
    ('e4130000-0000-4000-8000-000000000413', @base_log_id + 3, @pipeline_name,
     SYSUTCDATETIME(), 'BLOCKED_BY_DIM_FAILURE', 'gold', 'fact_quotation_item',
     'Test error: fact_quotation_item skipped — upstream dim_customer failed',
     'ERROR', NULL, 'New', SYSUTCDATETIME()),
    ('e4140000-0000-4000-8000-000000000414', @base_log_id + 4, @pipeline_name,
     SYSUTCDATETIME(), 'BLOCKED_BY_DIM_FAILURE', 'gold', 'fact_policy',
     'Test error: fact_policy skipped — upstream dim_customer failed',
     'ERROR', NULL, 'New', SYSUTCDATETIME()),
    ('e4150000-0000-4000-8000-000000000415', @base_log_id + 5, @pipeline_name,
     SYSUTCDATETIME(), 'BLOCKED_BY_DIM_FAILURE', 'gold', 'fact_payment',
     'Test error: fact_payment skipped — upstream dim_customer failed',
     'ERROR', NULL, 'New', SYSUTCDATETIME()),
    ('e4160000-0000-4000-8000-000000000416', @base_log_id + 6, @pipeline_name,
     SYSUTCDATETIME(), 'BLOCKED_BY_DIM_FAILURE', 'gold', 'fact_cancellation',
     'Test error: fact_cancellation skipped — upstream dim_customer failed',
     'ERROR', NULL, 'New', SYSUTCDATETIME());

-- ---------------------------------------------------------------------------
-- 3) Verify — same shape as recover_silver_to_gold Lookup_Recovery_Errors
-- ---------------------------------------------------------------------------
DECLARE @error_status VARCHAR(64) = 'New';

;WITH errors AS (
    SELECT
        e.error_id,
        e.error_code,
        e.error_message,
        e.target_table,
        e.layer_name,
        l.controller_id AS transform_config_id,
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
      AND e.layer_name = 'gold'
)
SELECT
    err.error_id,
    err.error_code,
    err.error_message AS original_error_message,
    err.layer_name,
    tc.transform_config_id,
    tc.pipeline_name,
    tc.source_layer,
    tc.source_schema,
    tc.source_table,
    tc.target_layer,
    tc.target_schema,
    tc.target_table,
    tc.transform_type,
    tc.primary_key_columns,
    tc.partition_column,
    tc.dependency_pipeline,
    tc.notebook_id,
    tc.watermark_column,
    tc.last_watermark
FROM errors err
INNER JOIN meta.etl_transform_config tc
    ON tc.transform_config_id = err.transform_config_id
WHERE err.rn = 1
ORDER BY tc.transform_config_id;
-- Expect 6 rows: 1 dim (401) + 5 facts (412–416).


/*
-- Cleanup (run when finished testing)
DELETE FROM meta.error_logging_table
WHERE error_id IN (
    'e4010000-0000-4000-8000-000000000401',
    'e4120000-0000-4000-8000-000000000412',
    'e4130000-0000-4000-8000-000000000413',
    'e4140000-0000-4000-8000-000000000414',
    'e4150000-0000-4000-8000-000000000415',
    'e4160000-0000-4000-8000-000000000416'
);

DELETE FROM meta.etl_execution_log
WHERE pipeline_run_id = 'TEST-FAILED-SILVER-GOLD-RUN-001'
  AND pipeline_name = 'silver_to_gold';
*/
