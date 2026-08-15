CREATE PROCEDURE silver.usp_orchestrate_bronze_to_silver
    @pipeline_run_id VARCHAR(100) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    IF @pipeline_run_id IS NULL
        SET @pipeline_run_id = CAST(NEWID() AS VARCHAR(100));

    DECLARE @pipeline_name VARCHAR(100) = 'bronze_to_silver';
    DECLARE @master_log_id BIGINT;

    EXEC meta.usp_etl_log
        @action          = 'pipeline_start',
        @pipeline_run_id = @pipeline_run_id,
        @pipeline_name   = @pipeline_name;

    SELECT TOP 1 @master_log_id = log_id
    FROM meta.etl_execution_log
    WHERE pipeline_run_id = @pipeline_run_id
      AND pipeline_name   = @pipeline_name
      AND controller_id   = 0
      AND status          = 'Running'
    ORDER BY log_id DESC;

    DECLARE @i_init  BIGINT;
    DECLARE @tt_init VARCHAR(255);

    SET @i_init = 311;
    WHILE @i_init <= 319
    BEGIN
        SELECT @tt_init = target_table
        FROM meta.etl_transform_config
        WHERE transform_config_id = @i_init;

        EXEC meta.usp_etl_log
            @action          = 'entity_start',
            @pipeline_run_id = @pipeline_run_id,
            @pipeline_name   = @pipeline_name,
            @controller_id   = @i_init,
            @target_table    = @tt_init;

        SET @i_init = @i_init + 1;
    END

    DECLARE @i                BIGINT;
    DECLARE @entity_id        BIGINT;
    DECLARE @entity_proc      VARCHAR(200);
    DECLARE @target_table     VARCHAR(255);
    DECLARE @watermark_column VARCHAR(100);
    DECLARE @last_watermark   VARCHAR(255);
    DECLARE @new_watermark    VARCHAR(255);
    DECLARE @rows_read        BIGINT;
    DECLARE @rows_inserted    BIGINT;
    DECLARE @entity_log_id    BIGINT;
    DECLARE @error_msg        VARCHAR(MAX);
    DECLARE @failed_count     INT = 0;
    DECLARE @sql              NVARCHAR(MAX);
    DECLARE @err_final        VARCHAR(255);

    SET @i = 311;
    WHILE @i <= 319
    BEGIN
        SET @entity_id = @i;

        SET @entity_proc = CASE @entity_id
            WHEN 311 THEN 'silver.usp_load_crm_agent'
            WHEN 312 THEN 'silver.usp_load_crm_customer'
            WHEN 313 THEN 'silver.usp_load_crm_insurance_provider'
            WHEN 314 THEN 'silver.usp_load_crm_quotation'
            WHEN 315 THEN 'silver.usp_load_crm_quotation_item'
            WHEN 316 THEN 'silver.usp_load_crm_vehicle'
            WHEN 317 THEN 'silver.usp_load_policy_cancellation'
            WHEN 318 THEN 'silver.usp_load_policy_payment'
            WHEN 319 THEN 'silver.usp_load_policy_policy'
        END;

        SELECT
            @target_table     = target_table,
            @watermark_column = watermark_column,
            @last_watermark   = last_watermark
        FROM meta.etl_transform_config
        WHERE transform_config_id = @entity_id;

        SET @entity_log_id = NULL;
        SELECT TOP 1 @entity_log_id = log_id
        FROM meta.etl_execution_log
        WHERE pipeline_run_id = @pipeline_run_id
          AND controller_id   = @entity_id
          AND status          = 'Running';

        BEGIN TRY
            SET @rows_read     = 0;
            SET @rows_inserted = 0;
            SET @new_watermark = NULL;

            SET @sql = N'EXEC ' + @entity_proc + N'
                @pipeline_run_id  = @run_id,
                @last_watermark   = @last_wm,
                @watermark_column = @wm_col,
                @rows_read        = @r_read  OUTPUT,
                @rows_inserted    = @r_ins   OUTPUT,
                @new_watermark    = @n_wm    OUTPUT;';

            EXEC sp_executesql @sql,
                N'@run_id VARCHAR(100), @last_wm VARCHAR(255), @wm_col VARCHAR(100),
                  @r_read BIGINT OUTPUT, @r_ins BIGINT OUTPUT,
                  @n_wm VARCHAR(255) OUTPUT',
                @run_id  = @pipeline_run_id,
                @last_wm = @last_watermark,
                @wm_col  = @watermark_column,
                @r_read  = @rows_read     OUTPUT,
                @r_ins   = @rows_inserted OUTPUT,
                @n_wm    = @new_watermark OUTPUT;

            IF @new_watermark IS NOT NULL
                UPDATE meta.etl_transform_config
                SET last_watermark = @new_watermark
                WHERE transform_config_id = @entity_id;

            IF @entity_log_id IS NOT NULL
                EXEC meta.usp_etl_log
                    @action          = 'entity_success',
                    @log_id          = @entity_log_id,
                    @pipeline_run_id = @pipeline_run_id,
                    @pipeline_name   = @pipeline_name,
                    @controller_id   = @entity_id,
                    @rows_read       = @rows_read,
                    @rows_inserted   = @rows_inserted,
                    @watermark_from  = @last_watermark,
                    @watermark_to    = @new_watermark;

        END TRY
        BEGIN CATCH
            SET @error_msg    = ERROR_MESSAGE();
            SET @failed_count = @failed_count + 1;

            IF @entity_log_id IS NOT NULL
                EXEC meta.usp_etl_log
                    @action          = 'entity_failed',
                    @log_id          = @entity_log_id,
                    @pipeline_run_id = @pipeline_run_id,
                    @pipeline_name   = @pipeline_name,
                    @controller_id   = @entity_id,
                    @target_table    = @target_table,
                    @error_message   = @error_msg;
        END CATCH;

        SET @i = @i + 1;
    END

    IF @failed_count = 0
    BEGIN
        EXEC meta.usp_etl_log
            @action          = 'pipeline_end',
            @pipeline_run_id = @pipeline_run_id,
            @pipeline_name   = @pipeline_name,
            @log_id          = @master_log_id;
    END
    ELSE
    BEGIN
        SET @err_final = 'ETL failed: ' + CAST(@failed_count AS VARCHAR(10)) + ' task(s) failed in silver layer.';

        UPDATE meta.etl_execution_log
        SET status        = 'Failed',
            end_time      = SYSDATETIME(),
            error_message = CAST(@failed_count AS VARCHAR(10)) + ' child task(s) failed.'
        WHERE log_id = @master_log_id;

        THROW 50000, @err_final, 1;
    END
END;