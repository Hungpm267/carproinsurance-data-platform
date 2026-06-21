CREATE   PROCEDURE meta.usp_etl_log
    @action              NVARCHAR(50),   
    @pipeline_run_id     NVARCHAR(100),
    @pipeline_name       NVARCHAR(200),
    @controller_id       BIGINT      = 0,
    @log_id              BIGINT      = NULL,
    @load_type           NVARCHAR(20)= NULL,   
    @rows_read           BIGINT      = 0,
    @rows_inserted       BIGINT      = 0,
    @watermark_from      NVARCHAR(50)= NULL,
    @watermark_to        NVARCHAR(50)= NULL,
    @error_message       NVARCHAR(2000) = NULL,
    @target_table        NVARCHAR(200)= NULL,
    @ingestion_config_id BIGINT      = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Synchronize using Fabric Warehouse's time standard system.
    DECLARE @now DATETIME2(6) = SYSDATETIME();
    DECLARE @current_log_id BIGINT = @log_id;

    -- ------------------------------------------------------------------------------------------
    -- ACTION: PIPELINE_START / ENTITY_START
    -- ------------------------------------------------------------------------------------------
    IF @action IN ('pipeline_start', 'entity_start')
    BEGIN
        DECLARE @new_id BIGINT;
        SELECT @new_id = COALESCE(MAX(log_id), 0) + 1 FROM meta.etl_execution_log;

        INSERT INTO meta.etl_execution_log
            (log_id, pipeline_run_id, controller_id, pipeline_name,
             start_time, status, rows_read, rows_inserted, rows_updated,
             rows_rejected, watermark_from, logged_at)
        VALUES
            (@new_id, @pipeline_run_id, @controller_id, @pipeline_name,
             @now, 'Running', 0, 0, 0, 0, @watermark_from, @now);

        SET @current_log_id = @new_id;
    END

    -- ------------------------------------------------------------------------------------------
    -- ACTION: ENTITY_SUCCESS
    -- ------------------------------------------------------------------------------------------
    ELSE IF @action = 'entity_success'
    BEGIN
        UPDATE meta.etl_execution_log
        SET status         = 'Success',
            end_time      = @now,
            rows_read     = @rows_read,
            rows_inserted = @rows_inserted,
            watermark_from= @watermark_from,
            watermark_to  = @watermark_to
        WHERE log_id = @log_id;

        -- Update the watermark progress for incremental growth tables.
        IF @load_type != 'FULL' AND @watermark_to IS NOT NULL AND @ingestion_config_id > 0
        BEGIN
            UPDATE meta.etl_ingestion_config
            SET last_watermark = @watermark_to
            WHERE ingestion_config_id = @ingestion_config_id;
        END
    END

    -- ------------------------------------------------------------------------------------------
    -- ACTION: ENTITY_FAILED
    -- ------------------------------------------------------------------------------------------
    ELSE IF @action = 'entity_failed'
    BEGIN
        UPDATE meta.etl_execution_log
        SET status = 'Failed', 
            end_time = @now, 
            error_message = LEFT(@error_message, 2000)
        WHERE log_id = @log_id;

        INSERT INTO meta.error_logging_table
            (error_id, execution_id, error_pipelinename, error_timestamp,
             error_code, layer_name, target_table, error_message,
             error_severity_level, status, updated_at)
        VALUES
            (CAST(NEWID() AS VARCHAR(50)), @log_id, @pipeline_name, @now,
             'COPY_FAILED', 'bronze', ISNULL(@target_table, 'unknown'),
             LEFT(@error_message, 2000), 'ERROR', 'New', @now);
    END

    -- ------------------------------------------------------------------------------------------
    -- ACTION: PIPELINE_END
    -- ------------------------------------------------------------------------------------------
    ELSE IF @action = 'pipeline_end'
    BEGIN
        DECLARE @agg_read BIGINT = 0;
        DECLARE @agg_inserted BIGINT = 0;

        SELECT 
            @agg_read = COALESCE(SUM(rows_read), 0),
            @agg_inserted = COALESCE(SUM(rows_inserted), 0)
        FROM meta.etl_execution_log
        WHERE pipeline_run_id = @pipeline_run_id
          AND controller_id > 0;

        UPDATE meta.etl_execution_log
        SET status        = 'Success',
            end_time      = @now,
            rows_read     = @agg_read,
            rows_inserted = @agg_inserted
        WHERE log_id = @log_id;
    END

    -- ------------------------------------------------------------------------------------------
    -- PROVIDE 100% READABLE RECORDSET OUTPUT FOR FABRIC PIPELINE
    -- ------------------------------------------------------------------------------------------
    SELECT @current_log_id AS log_id;
END;