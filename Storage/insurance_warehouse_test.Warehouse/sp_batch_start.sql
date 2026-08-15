/*
  Recovery batch START — one Running row per failed gold transform (from error lookup).

  Pair with: usp_etl_end_batch_recovery.sql

  Pipeline call (SqlServerStoredProcedure), after Lookup_Recovery_Errors / before Recovery_Orchestrator:

    @pipeline_run_id         = @pipeline().RunId
    @pipeline_name           = @pipeline().parameters.recover_pipeline_name   -- recover_silver_to_gold
    @failed_transform_run_id = @pipeline().parameters.failed_transform_run_id
    @source_pipeline_name    = @pipeline().parameters.pipeline_name          -- silver_to_gold
    @error_status            = @pipeline().parameters.error_status           -- New

  Stores original error_id in etl_execution_log.dynamic_source_file for traceability on end batch.
*/

CREATE OR ALTER PROCEDURE meta.usp_etl_start_batch_recovery_gold
    @pipeline_run_id          NVARCHAR(100),
    @pipeline_name            NVARCHAR(200),
    @failed_transform_run_id  NVARCHAR(100),
    @source_pipeline_name     NVARCHAR(200) = N'silver_to_gold',
    @error_status             NVARCHAR(64)  = N'New'
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @base BIGINT = (SELECT COALESCE(MAX(log_id), 0) FROM meta.etl_execution_log);

    ;WITH errors AS (
        SELECT
            e.error_id,
            l.controller_id AS transform_config_id,
            ROW_NUMBER() OVER (
                PARTITION BY l.controller_id
                ORDER BY e.error_timestamp DESC
            ) AS rn
        FROM meta.error_logging_table e
        INNER JOIN meta.etl_execution_log l ON e.execution_id = l.log_id
        WHERE l.pipeline_run_id = @failed_transform_run_id
          AND l.pipeline_name = @source_pipeline_name
          AND e.status = @error_status
          AND e.layer_name = N'gold'
    ),
    recovery_targets AS (
        SELECT
            err.error_id,
            tc.transform_config_id,
            NULLIF(LTRIM(RTRIM(tc.last_watermark)), N'') AS watermark_from
        FROM errors err
        INNER JOIN meta.etl_transform_config tc
            ON tc.transform_config_id = err.transform_config_id
        WHERE err.rn = 1
    ),
    numbered AS (
        SELECT
            error_id,
            transform_config_id,
            watermark_from,
            ROW_NUMBER() OVER (ORDER BY transform_config_id) AS rn
        FROM recovery_targets
    )
    INSERT INTO meta.etl_execution_log (
        log_id,
        pipeline_run_id,
        controller_id,
        pipeline_name,
        start_time,
        status,
        rows_read,
        rows_inserted,
        rows_updated,
        rows_rejected,
        watermark_from,
        dynamic_source_file,
        logged_at
    )
    SELECT
        @base + rn,
        @pipeline_run_id,
        transform_config_id,
        @pipeline_name,
        SYSDATETIME(),
        N'Running',
        0,
        0,
        0,
        0,
        watermark_from,
        error_id,
        SYSDATETIME()
    FROM numbered;

    SELECT @@ROWCOUNT AS entities_started;
END;
