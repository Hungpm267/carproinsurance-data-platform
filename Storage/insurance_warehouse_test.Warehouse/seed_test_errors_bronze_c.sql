/*
  Seed open errors for recover_bronze_to_silver testing.

  Simulates a failed bronze_to_silver run (policy tables: policy / payment / cancellation).
  Recovery lookup joins:
    error_logging_table.execution_id → etl_execution_log.log_id
    etl_execution_log.controller_id  → etl_transform_config.transform_config_id

  Run in: Fabric Warehouse after ddl_meta_schema1.sql seed.

  Recovery pipeline parameters:
    failed_transform_run_id = TEST-FAILED-BRONZE-SILVER-RUN-001
    pipeline_name           = bronze_to_silver
    error_status            = New
    default_watermark       = 1900-01-01T00:00:00Z
*/

DECLARE @failed_run_id VARCHAR(64) = 'TEST-FAILED-BRONZE-SILVER-RUN-001';
DECLARE @pipeline_name VARCHAR(100) = 'bronze_to_silver';
DECLARE @base_log_id   BIGINT;
DECLARE @wm_from       VARCHAR(200) = '1900-01-01T00:00:00.000';

SELECT @base_log_id = COALESCE(MAX(log_id), 0) FROM meta.etl_execution_log;

-- ---------------------------------------------------------------------------
-- 1) Entity execution log rows (one per failed silver transform)
--    controller_id = transform_config_id from meta.etl_transform_config
-- ---------------------------------------------------------------------------
INSERT INTO meta.etl_execution_log (
    log_id, pipeline_run_id, controller_id, pipeline_name,
    start_time, end_time, status,
    rows_read, rows_inserted, rows_updated, rows_rejected,
    watermark_from, watermark_to, dynamic_source_file, error_message, logged_at
)
VALUES
    (@base_log_id + 1, @failed_run_id, 307, @pipeline_name,
     SYSUTCDATETIME(), SYSUTCDATETIME(), 'Failed',
     0, 0, 0, 0,
     @wm_from, NULL, NULL,
     'Simulated TRANSFORM_FAILED: bronze.policy_cancellation → silver.policy_cancellation', SYSUTCDATETIME()),
    (@base_log_id + 2, @failed_run_id, 308, @pipeline_name,
     SYSUTCDATETIME(), SYSUTCDATETIME(), 'Failed',
     0, 0, 0, 0,
     @wm_from, NULL, NULL,
     'Simulated TRANSFORM_FAILED: bronze.policy_payment → silver.policy_payment', SYSUTCDATETIME()),
    (@base_log_id + 3, @failed_run_id, 309, @pipeline_name,
     SYSUTCDATETIME(), SYSUTCDATETIME(), 'Failed',
     0, 0, 0, 0,
     @wm_from, NULL, NULL,
     'Simulated TRANSFORM_FAILED: bronze.policy_policy → silver.policy_policy', SYSUTCDATETIME());

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
    ('e3070000-0000-4000-8000-000000000307', @base_log_id + 1, @pipeline_name,
     SYSUTCDATETIME(), 'TRANSFORM_FAILED', 'silver', 'policy_cancellation',
     'Test error: Notebook failed bronze.policy_cancellation → silver.policy_cancellation',
     'ERROR', NULL, 'New', SYSUTCDATETIME()),
    ('e3080000-0000-4000-8000-000000000308', @base_log_id + 2, @pipeline_name,
     SYSUTCDATETIME(), 'TRANSFORM_FAILED', 'silver', 'policy_payment',
     'Test error: Notebook failed bronze.policy_payment → silver.policy_payment',
     'ERROR', NULL, 'New', SYSUTCDATETIME()),
    ('e3090000-0000-4000-8000-000000000309', @base_log_id + 3, @pipeline_name,
     SYSUTCDATETIME(), 'TRANSFORM_FAILED', 'silver', 'policy_policy',
     'Test error: Notebook failed bronze.policy_policy → silver.policy_policy',
     'ERROR', NULL, 'New', SYSUTCDATETIME());

-- ---------------------------------------------------------------------------
-- 3) Verify — same shape as recover_bronze_to_silver Lookup_Recovery_Errors
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
-- Expect 3 rows (307–309).



/*
-- Cleanup (run when finished testing)
DELETE FROM meta.error_logging_table
WHERE error_id IN (
    'e3070000-0000-4000-8000-000000000307',
    'e3080000-0000-4000-8000-000000000308',
    'e3090000-0000-4000-8000-000000000309'
);

DELETE FROM meta.etl_execution_log
WHERE pipeline_run_id = 'TEST-FAILED-BRONZE-SILVER-RUN-001'
  AND pipeline_name = 'bronze_to_silver';
*/
