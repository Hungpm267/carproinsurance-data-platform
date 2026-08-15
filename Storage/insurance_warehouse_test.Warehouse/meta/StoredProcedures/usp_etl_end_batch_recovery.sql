/*
  Recovery batch END — closes Running rows created by usp_etl_start_batch_recovery_gold.

  Fabric Warehouse: no table variables — uses WHILE + OPENJSON index (same pattern as usp_etl_end_batch).

  Expects JSON from recovery_orchestrator.ipynb exitValue, e.g.:

  {
    "failed": 0,
    "resolved": 1,
    "manual_check": 0,
    "rows": [
      {
        "transform_config_id": 401,
        "target_table": "dim_customer",
        "run_status": "Succeeded",
        "error_id": "e401...",
        "rows_read": 10,
        "rows_inserted": 10,
        "watermark_from": "...",
        "watermark_to": "...",
        "error": null
      }
    ]
  }

  Pipeline call (after Recovery_Orchestrator):

    @pipeline_run_id = @pipeline().RunId
    @pipeline_name   = @pipeline().parameters.recover_pipeline_name
    @results         = @activity('Recovery_Orchestrator').output.result.exitValue
*/

CREATE   PROCEDURE meta.usp_etl_end_batch_recovery
    @pipeline_run_id NVARCHAR(100),
    @results         NVARCHAR(MAX),
    @pipeline_name   NVARCHAR(200)
AS
BEGIN
    SET NOCOUNT ON;

    IF @results IS NULL OR LTRIM(RTRIM(@results)) = N''
        RETURN;

    DECLARE @n INT;
    SELECT @n = COUNT(*) FROM OPENJSON(@results, '$.rows');

    DECLARE @i INT = 0;
    DECLARE @cid BIGINT, @status NVARCHAR(20), @tt NVARCHAR(200),
            @error_id NVARCHAR(36), @rr BIGINT, @ri BIGINT,
            @wf NVARCHAR(50), @wt NVARCHAR(50), @err NVARCHAR(2000),
            @lid BIGINT, @wt2 NVARCHAR(50), @errmsg NVARCHAR(2000),
            @error_status NVARCHAR(32), @sql NVARCHAR(MAX);

    WHILE @i < @n
    BEGIN
        SET @sql = N'
            SELECT
                @cid_out = TRY_CAST(JSON_VALUE(@json, ''$.rows[' + CAST(@i AS NVARCHAR(10)) + N'].transform_config_id'') AS BIGINT),
                @tt_out = JSON_VALUE(@json, ''$.rows[' + CAST(@i AS NVARCHAR(10)) + N'].target_table''),
                @status_out = JSON_VALUE(@json, ''$.rows[' + CAST(@i AS NVARCHAR(10)) + N'].run_status''),
                @error_id_out = COALESCE(
                    JSON_VALUE(@json, ''$.rows[' + CAST(@i AS NVARCHAR(10)) + N'].error_id''),
                    JSON_VALUE(@json, ''$.rows[' + CAST(@i AS NVARCHAR(10)) + N'].original_error_id'')
                ),
                @rr_out = TRY_CAST(JSON_VALUE(@json, ''$.rows[' + CAST(@i AS NVARCHAR(10)) + N'].rows_read'') AS BIGINT),
                @ri_out = TRY_CAST(JSON_VALUE(@json, ''$.rows[' + CAST(@i AS NVARCHAR(10)) + N'].rows_inserted'') AS BIGINT),
                @wf_out = JSON_VALUE(@json, ''$.rows[' + CAST(@i AS NVARCHAR(10)) + N'].watermark_from''),
                @wt_out = JSON_VALUE(@json, ''$.rows[' + CAST(@i AS NVARCHAR(10)) + N'].watermark_to''),
                @err_out = JSON_VALUE(@json, ''$.rows[' + CAST(@i AS NVARCHAR(10)) + N'].error'')
        ';

        EXEC sp_executesql @sql,
            N'@json NVARCHAR(MAX),
              @cid_out BIGINT OUTPUT,
              @tt_out NVARCHAR(200) OUTPUT,
              @status_out NVARCHAR(20) OUTPUT,
              @error_id_out NVARCHAR(36) OUTPUT,
              @rr_out BIGINT OUTPUT,
              @ri_out BIGINT OUTPUT,
              @wf_out NVARCHAR(50) OUTPUT,
              @wt_out NVARCHAR(50) OUTPUT,
              @err_out NVARCHAR(2000) OUTPUT',
            @json = @results,
            @cid_out = @cid OUTPUT,
            @tt_out = @tt OUTPUT,
            @status_out = @status OUTPUT,
            @error_id_out = @error_id OUTPUT,
            @rr_out = @rr OUTPUT,
            @ri_out = @ri OUTPUT,
            @wf_out = @wf OUTPUT,
            @wt_out = @wt OUTPUT,
            @err_out = @err OUTPUT;

        SET @rr = COALESCE(@rr, 0);
        SET @ri = COALESCE(@ri, 0);

        SELECT @lid = log_id
        FROM meta.etl_execution_log
        WHERE pipeline_run_id = @pipeline_run_id
          AND controller_id = @cid
          AND status = N'Running';

        SET @wt2 = NULLIF(LTRIM(RTRIM(@wt)), N'');
        SET @errmsg = COALESCE(
            NULLIF(LTRIM(RTRIM(@err)), N''),
            CASE
                WHEN @status = N'Skipped' THEN N'Skipped'
                WHEN @status = N'Succeeded' THEN NULL
                ELSE N'Unknown'
            END
        );

        IF @status = N'Succeeded' AND @wt2 IS NOT NULL
            UPDATE meta.etl_transform_config
            SET last_watermark = @wt2
            WHERE transform_config_id = @cid;

        IF @lid IS NOT NULL
        BEGIN
            IF @status = N'Succeeded'
                EXEC meta.usp_etl_log
                    @action = N'entity_success',
                    @log_id = @lid,
                    @pipeline_run_id = @pipeline_run_id,
                    @pipeline_name = @pipeline_name,
                    @controller_id = @cid,
                    @rows_read = @rr,
                    @rows_inserted = @ri,
                    @watermark_from = @wf,
                    @watermark_to = @wt2;
            ELSE
                EXEC meta.usp_etl_log
                    @action = N'entity_failed',
                    @log_id = @lid,
                    @pipeline_run_id = @pipeline_run_id,
                    @pipeline_name = @pipeline_name,
                    @controller_id = @cid,
                    @target_table = @tt,
                    @error_message = @errmsg;
        END;

        IF @error_id IS NULL OR LTRIM(RTRIM(@error_id)) = N''
            SELECT @error_id = dynamic_source_file
            FROM meta.etl_execution_log
            WHERE log_id = @lid;

        SET @error_status = CASE
            WHEN @status = N'Succeeded' THEN N'Resolved'
            ELSE N'Need Manual Check'
        END;

        IF @error_id IS NOT NULL AND LTRIM(RTRIM(@error_id)) <> N''
        BEGIN
            UPDATE meta.error_logging_table
            SET
                status = @error_status,
                updated_at = SYSDATETIME()
            WHERE error_id = @error_id
              AND status IN (N'New', N'In-Progress');
        END;

        SET @i = @i + 1;
    END;
END;