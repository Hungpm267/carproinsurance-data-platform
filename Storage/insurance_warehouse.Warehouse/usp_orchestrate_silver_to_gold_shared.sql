CREATE   PROCEDURE gold.usp_orchestrate_silver_to_gold
        @pipeline_run_id VARCHAR(100) = NULL
    AS
    BEGIN
        SET NOCOUNT ON;

        -- Generate a run ID if not provided
        IF @pipeline_run_id IS NULL
            SET @pipeline_run_id = CAST(NEWID() AS VARCHAR(100));

        DECLARE @pipeline_name VARCHAR(100) = 'silver_to_gold';
        DECLARE @master_log_id BIGINT;

        -- 1. Log Start of Master Pipeline (Avoid INSERT EXEC as it is not supported in Fabric Warehouse)
        EXEC meta.usp_etl_log
            @action = 'pipeline_start',
            @pipeline_run_id = @pipeline_run_id,
            @pipeline_name = @pipeline_name;

        -- Query the log_id that was just inserted
        SELECT TOP 1 @master_log_id = log_id
        FROM meta.etl_execution_log
        WHERE pipeline_run_id = @pipeline_run_id
          AND pipeline_name = @pipeline_name
          AND controller_id = 0
          AND status = 'Running'
        ORDER BY log_id DESC;

        -- Log start batch for all config items (Running status in etl_execution_log)
        EXEC meta.usp_etl_start_batch_gold
            @pipeline_run_id = @pipeline_run_id,
            @pipeline_name = @pipeline_name;

        -- Declared variables for loops/execution
        DECLARE @transform_config_id BIGINT;
        DECLARE @target_table VARCHAR(255);
        DECLARE @watermark_column VARCHAR(100);
        DECLARE @last_watermark VARCHAR(255);

        DECLARE @rows_read BIGINT;
        DECLARE @rows_inserted BIGINT;
        DECLARE @new_watermark VARCHAR(255);

        DECLARE @entity_log_id BIGINT;
        DECLARE @error_msg VARCHAR(MAX);
        DECLARE @failed_count INT = 0;

        -- ==========================================================================
        -- STEP 1: LOAD SEEDS / LOOKUPS (Config IDs: 405, 406, 407, 408, 409, 410, 411)
        -- ==========================================================================
        BEGIN TRY
            SET @rows_read = 0;
            SET @rows_inserted = 0;
            SET @new_watermark = NULL;

            -- We call the unified lookup seeds loader
            EXEC gold.usp_load_lookup_seeds
                @pipeline_run_id = @pipeline_run_id,
                @rows_read = @rows_read OUTPUT,
                @rows_inserted = @rows_inserted OUTPUT;

            -- Update execution logs for each of the seed configs using a WHILE loop
            DECLARE @i BIGINT = 405;
            DECLARE @seed_id BIGINT;
 
            WHILE @i <= 411
            BEGIN
                SET @seed_id = @i;
                SELECT @target_table = target_table
                FROM meta.etl_transform_config
                WHERE transform_config_id = @seed_id;
 
                -- Get log_id for this entity
                SET @entity_log_id = NULL;
                SELECT TOP 1 @entity_log_id = log_id
                FROM meta.etl_execution_log
                WHERE pipeline_run_id = @pipeline_run_id AND controller_id = @seed_id AND status = 'Running';
 
                -- Log success for seed
                IF @entity_log_id IS NOT NULL
                BEGIN
                    EXEC meta.usp_etl_log
                        @action = 'entity_success',
                        @log_id = @entity_log_id,
                        @pipeline_run_id = @pipeline_run_id,
                        @pipeline_name = @pipeline_name,
                        @controller_id = @seed_id,
                        @rows_read = 0,
                        @rows_inserted = @rows_inserted, -- Approximate
                        @watermark_from = NULL,
                        @watermark_to = NULL;
                END
 
                SET @i += 1;
            END

        END TRY
        BEGIN CATCH
            SET @error_msg = ERROR_MESSAGE();
            SET @failed_count += 7;

            -- Log failure for all seeds using a WHILE loop
            DECLARE @i_fail BIGINT = 405;
 
            WHILE @i_fail <= 411
            BEGIN
                SET @seed_id = @i_fail;
                SELECT @target_table = target_table
                FROM meta.etl_transform_config
                WHERE transform_config_id = @seed_id;
 
                SET @entity_log_id = NULL;
                SELECT TOP 1 @entity_log_id = log_id
                FROM meta.etl_execution_log
                WHERE pipeline_run_id = @pipeline_run_id AND controller_id = @seed_id AND status = 'Running';
 
                IF @entity_log_id IS NOT NULL
                BEGIN
                    EXEC meta.usp_etl_log
                        @action = 'entity_failed',
                        @log_id = @entity_log_id,
                        @pipeline_run_id = @pipeline_run_id,
                        @pipeline_name = @pipeline_name,
                        @controller_id = @seed_id,
                        @target_table = @target_table,
                        @error_message = @error_msg;
                END
 
                SET @i_fail += 1;
            END
        END CATCH;

        -- ==========================================================================
        -- STEP 2: LOAD MAIN DIMENSIONS SEQUENTIALLY USING WHILE LOOP
        -- ==========================================================================
        DECLARE @i_dim BIGINT = 401;
        DECLARE @dim_id BIGINT;
        DECLARE @dim_proc VARCHAR(100);
 
        WHILE @i_dim <= 404
        BEGIN
            SET @dim_id = @i_dim;
            SET @dim_proc = CASE @dim_id
                WHEN 401 THEN 'gold.usp_load_dim_customer'
                WHEN 402 THEN 'gold.usp_load_dim_agent'
                WHEN 403 THEN 'gold.usp_load_dim_insurance_provider'
                WHEN 404 THEN 'gold.usp_load_dim_vehicle'
            END;
 
            -- Get config
            SELECT
                @target_table = target_table,
                @watermark_column = watermark_column,
                @last_watermark = last_watermark
            FROM meta.etl_transform_config
            WHERE transform_config_id = @dim_id;
 
            -- Get log_id
            SET @entity_log_id = NULL;
            SELECT TOP 1 @entity_log_id = log_id
            FROM meta.etl_execution_log
            WHERE pipeline_run_id = @pipeline_run_id AND controller_id = @dim_id AND status = 'Running';
 
            BEGIN TRY
                SET @rows_read = 0;
                SET @rows_inserted = 0;
                SET @new_watermark = NULL;
 
                -- Run the specific loader
                DECLARE @sql NVARCHAR(MAX) = '
                    EXEC ' + @dim_proc + '
                        @pipeline_run_id = @run_id,
                        @last_watermark = @last_wm,
                        @watermark_column = @wm_col,
                        @rows_read = @r_read OUTPUT,
                        @rows_inserted = @r_ins OUTPUT,
                        @new_watermark = @n_wm OUTPUT;
                ';
                EXEC sp_executesql @sql,
                    N'@run_id VARCHAR(100), @last_wm VARCHAR(100), @wm_col VARCHAR(100), @r_read BIGINT OUTPUT, @r_ins BIGINT OUTPUT, @n_wm VARCHAR(100) OUTPUT',
                    @run_id = @pipeline_run_id, @last_wm = @last_watermark, @wm_col = @watermark_column,
                    @r_read = @rows_read OUTPUT, @r_ins = @rows_inserted OUTPUT, @n_wm = @new_watermark OUTPUT;
 
                -- Update config watermark
                IF @new_watermark IS NOT NULL
                BEGIN
                    UPDATE meta.etl_transform_config
                    SET last_watermark = @new_watermark
                    WHERE transform_config_id = @dim_id;
                END
 
                -- Log success
                IF @entity_log_id IS NOT NULL
                BEGIN
                    EXEC meta.usp_etl_log
                        @action = 'entity_success',
                        @log_id = @entity_log_id,
                        @pipeline_run_id = @pipeline_run_id,
                        @pipeline_name = @pipeline_name,
                        @controller_id = @dim_id,
                        @rows_read = @rows_read,
                        @rows_inserted = @rows_inserted,
                        @watermark_from = @last_watermark,
                        @watermark_to = @new_watermark;
                END
            END TRY
            BEGIN CATCH
                SET @error_msg = ERROR_MESSAGE();
                SET @failed_count += 1;
 
                -- Log failure
                IF @entity_log_id IS NOT NULL
                BEGIN
                    EXEC meta.usp_etl_log
                        @action = 'entity_failed',
                        @log_id = @entity_log_id,
                        @pipeline_run_id = @pipeline_run_id,
                        @pipeline_name = @pipeline_name,
                        @controller_id = @dim_id,
                        @target_table = @target_table,
                        @error_message = @error_msg;
                END
            END CATCH;
 
            SET @i_dim += 1;
        END

        -- ==========================================================================
        -- STEP 3: LOAD FACTS SEQUENTIALLY USING WHILE LOOP (ONLY IF NO DIMENSION ERRORS)
        -- ==========================================================================
        IF @failed_count = 0
        BEGIN
            DECLARE @i_fact BIGINT = 412;
            DECLARE @fact_id BIGINT;
            DECLARE @fact_proc VARCHAR(100);
 
            WHILE @i_fact <= 416
            BEGIN
                SET @fact_id = @i_fact;
                SET @fact_proc = CASE @fact_id
                    WHEN 412 THEN 'gold.usp_load_fact_quotation'
                    WHEN 413 THEN 'gold.usp_load_fact_quotation_item'
                    WHEN 414 THEN 'gold.usp_load_fact_policy'
                    WHEN 415 THEN 'gold.usp_load_fact_payment'
                    WHEN 416 THEN 'gold.usp_load_fact_cancellation'
                END;
 
                -- Get config
                SELECT
                    @target_table = target_table,
                    @watermark_column = watermark_column,
                    @last_watermark = last_watermark
                FROM meta.etl_transform_config
                WHERE transform_config_id = @fact_id;
 
                -- Get log_id
                SET @entity_log_id = NULL;
                SELECT TOP 1 @entity_log_id = log_id
                FROM meta.etl_execution_log
                WHERE pipeline_run_id = @pipeline_run_id AND controller_id = @fact_id AND status = 'Running';
 
                BEGIN TRY
                    SET @rows_read = 0;
                    SET @rows_inserted = 0;
                    SET @new_watermark = NULL;
 
                    -- Run the specific loader
                    DECLARE @sql_f NVARCHAR(MAX) = '
                        EXEC ' + @fact_proc + '
                            @pipeline_run_id = @run_id,
                            @last_watermark = @last_wm,
                            @watermark_column = @wm_col,
                            @rows_read = @r_read OUTPUT,
                            @rows_inserted = @r_ins OUTPUT,
                            @new_watermark = @n_wm OUTPUT;
                    ';
                    EXEC sp_executesql @sql_f,
                        N'@run_id VARCHAR(100), @last_wm VARCHAR(100), @wm_col VARCHAR(100), @r_read BIGINT OUTPUT, @r_ins BIGINT OUTPUT, @n_wm VARCHAR(100) OUTPUT',
                        @run_id = @pipeline_run_id, @last_wm = @last_watermark, @wm_col = @watermark_column,
                        @r_read = @rows_read OUTPUT, @r_ins = @rows_inserted OUTPUT, @n_wm = @new_watermark OUTPUT;
 
                    -- Update config watermark
                    IF @new_watermark IS NOT NULL
                    BEGIN
                        UPDATE meta.etl_transform_config
                        SET last_watermark = @new_watermark
                        WHERE transform_config_id = @fact_id;
                    END
 
                    -- Log success
                    IF @entity_log_id IS NOT NULL
                    BEGIN
                        EXEC meta.usp_etl_log
                            @action = 'entity_success',
                            @log_id = @entity_log_id,
                            @pipeline_run_id = @pipeline_run_id,
                            @pipeline_name = @pipeline_name,
                            @controller_id = @fact_id,
                            @rows_read = @rows_read,
                            @rows_inserted = @rows_inserted,
                            @watermark_from = @last_watermark,
                            @watermark_to = @new_watermark;
                    END
                END TRY
                BEGIN CATCH
                    SET @error_msg = ERROR_MESSAGE();
                    SET @failed_count += 1;
 
                    -- Log failure
                    IF @entity_log_id IS NOT NULL
                    BEGIN
                        EXEC meta.usp_etl_log
                            @action = 'entity_failed',
                            @log_id = @entity_log_id,
                            @pipeline_run_id = @pipeline_run_id,
                            @pipeline_name = @pipeline_name,
                            @controller_id = @fact_id,
                            @target_table = @target_table,
                            @error_message = @error_msg;
                    END
                END CATCH;
 
                SET @i_fact += 1;
            END
        END
        ELSE
        BEGIN
            -- If dimensions failed, log skipping for all facts using WHILE loop
            DECLARE @i_skip BIGINT = 412;
            DECLARE @skipped_id BIGINT;
            DECLARE @skipped_table VARCHAR(255);
 
            WHILE @i_skip <= 416
            BEGIN
                SET @skipped_id = @i_skip;
                SELECT @skipped_table = target_table
                FROM meta.etl_transform_config
                WHERE transform_config_id = @skipped_id;
 
                SET @entity_log_id = NULL;
                SELECT TOP 1 @entity_log_id = log_id
                FROM meta.etl_execution_log
                WHERE pipeline_run_id = @pipeline_run_id AND controller_id = @skipped_id AND status = 'Running';
 
                IF @entity_log_id IS NOT NULL
                BEGIN
                    EXEC meta.usp_etl_log
                        @action = 'entity_failed',
                        @log_id = @entity_log_id,
                        @pipeline_run_id = @pipeline_run_id,
                        @pipeline_name = @pipeline_name,
                        @controller_id = @skipped_id,
                        @target_table = @skipped_table,
                        @error_message = 'Skipped because dimension loading failed';
                END
 
                SET @i_skip += 1;
            END
        END

        -- ==========================================================================
        -- STEP 4: UPDATE MASTER PIPELINE STATUS
        -- ==========================================================================
        IF @failed_count = 0
        BEGIN
            EXEC meta.usp_etl_log
                @action = 'pipeline_end',
                @pipeline_run_id = @pipeline_run_id,
                @pipeline_name = @pipeline_name,
                @log_id = @master_log_id;
        END
        ELSE
        BEGIN
            -- Mark master as failed
            UPDATE meta.etl_execution_log
            SET status = 'Failed',
                end_time = SYSDATETIME(),
                error_message = CAST(@failed_count AS VARCHAR(10)) + ' child task(s) failed during execution.'
            WHERE log_id = @master_log_id;

            -- Raise error to fail pipeline run in Fabric
            DECLARE @err_msg_master VARCHAR(255) = 'ETL Execution failed: ' + CAST(@failed_count AS VARCHAR(10)) + ' task(s) failed in gold layer.';
            THROW 50000, @err_msg_master, 1;
        END
    END;