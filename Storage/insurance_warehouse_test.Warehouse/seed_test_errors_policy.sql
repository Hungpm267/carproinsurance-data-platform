/*
  Seed 3 open errors for recover_ingest_policy_to_bronze testing.

  Simulates a failed ingest_policy_to_bronze run (policy / payment / cancellation).
  Recovery lookup joins:
    error_logging_table.execution_id → etl_execution_log.log_id
    etl_execution_log.controller_id  → etl_ingestion_config.ingestion_config_id

  Run in: Fabric Warehouse.

  Recovery pipeline parameters:
    failed_ingest_run_id = TEST-FAILED-INGEST-POLICY-RUN-001
    pipeline_name        = ingest_policy_to_bronze
    error_status         = New
    default_watermark    = 1900-01-01   (or your ingest default)
*/

DECLARE @failed_run_id VARCHAR(64) = 'TEST-FAILED-INGEST-POLICY-RUN-001';
DECLARE @pipeline_name VARCHAR(100) = 'ingest_policy_to_bronze';
DECLARE @base_log_id   BIGINT;
DECLARE @wm_from       VARCHAR(200) = '1900-01-01T00:00:00.000';

SELECT @base_log_id = COALESCE(MAX(log_id), 0) FROM meta.etl_execution_log;

-- ---------------------------------------------------------------------------
-- 1) Entity execution log rows (one per failed policy entity)
-- ---------------------------------------------------------------------------
INSERT INTO meta.etl_execution_log (
    log_id, pipeline_run_id, controller_id, pipeline_name,
    start_time, end_time, status,
    rows_read, rows_inserted, rows_updated, rows_rejected,
    watermark_from, watermark_to, dynamic_source_file, error_message, logged_at
)
VALUES
    (@base_log_id + 1, @failed_run_id, 301, @pipeline_name,
     SYSUTCDATETIME(), SYSUTCDATETIME(), 'Failed',
     0, 0, 0, 0,
     @wm_from, NULL, 'policy_*.json',
     'Simulated COPY_FAILED: landing.policy → bronze.policy_policy', SYSUTCDATETIME()),
    (@base_log_id + 2, @failed_run_id, 302, @pipeline_name,
     SYSUTCDATETIME(), SYSUTCDATETIME(), 'Failed',
     0, 0, 0, 0,
     @wm_from, NULL, 'payment_*.json',
     'Simulated COPY_FAILED: landing.payment → bronze.policy_payment', SYSUTCDATETIME()),
    (@base_log_id + 3, @failed_run_id, 303, @pipeline_name,
     SYSUTCDATETIME(), SYSUTCDATETIME(), 'Failed',
     0, 0, 0, 0,
     @wm_from, NULL, 'cancellation_*.json',
     'Simulated COPY_FAILED: landing.cancellation → bronze.policy_cancellation', SYSUTCDATETIME());

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
    ('e3010000-0000-4000-8000-000000000301', @base_log_id + 1, @pipeline_name,
     SYSUTCDATETIME(), 'COPY_FAILED', 'bronze', 'policy_policy',
     'Test error: Copy failed loading Files/landing/policy → bronze.policy_policy',
     'ERROR', NULL, 'New', SYSUTCDATETIME()),
    ('e3020000-0000-4000-8000-000000000302', @base_log_id + 2, @pipeline_name,
     SYSUTCDATETIME(), 'COPY_FAILED', 'bronze', 'policy_payment',
     'Test error: Copy failed loading Files/landing/payment → bronze.policy_payment',
     'ERROR', NULL, 'New', SYSUTCDATETIME()),
    ('e3030000-0000-4000-8000-000000000303', @base_log_id + 3, @pipeline_name,
     SYSUTCDATETIME(), 'COPY_FAILED', 'bronze', 'policy_cancellation',
     'Test error: Copy failed loading Files/landing/cancellation → bronze.policy_cancellation',
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
    ic.source_path,
    ic.file_pattern,
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
-- Expect 3 rows (301–303).


/*
-- Cleanup (run when finished testing)
DELETE FROM meta.error_logging_table
WHERE error_id IN (
    'e3010000-0000-4000-8000-000000000301',
    'e3020000-0000-4000-8000-000000000302',
    'e3030000-0000-4000-8000-000000000303'
);

DELETE FROM meta.etl_execution_log
WHERE pipeline_run_id = 'TEST-FAILED-INGEST-POLICY-RUN-001'
  AND pipeline_name = 'ingest_policy_to_bronze';
*/
