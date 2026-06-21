-- end log pipeline
CREATE OR ALTER PROCEDURE meta.usp_etl_end_batch
    @pipeline_run_id VARCHAR(100), @results VARCHAR(MAX), @pipeline_name VARCHAR(200)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @n INT;
    SELECT @n = COUNT(*) FROM OPENJSON(@results, '$.rows');

    DECLARE @i INT = 0;
    DECLARE @cid BIGINT, @status VARCHAR(20), @tt VARCHAR(200),
            @rr BIGINT, @ri BIGINT, @wf VARCHAR(50), @wt VARCHAR(50), @err VARCHAR(2000),
            @lid BIGINT,
            @wt2 VARCHAR(50), @errmsg VARCHAR(2000);

    WHILE @i < @n
    BEGIN
        -- Dynamically build query to read from the JSON array at index @i
        DECLARE @sql NVARCHAR(MAX) = N'
            SELECT 
                @cid_out = JSON_VALUE(@json, ''$.rows[' + CAST(@i AS VARCHAR(10)) + '].controller_id''),
                @tt_out = JSON_VALUE(@json, ''$.rows[' + CAST(@i AS VARCHAR(10)) + '].target_table''),
                @status_out = JSON_VALUE(@json, ''$.rows[' + CAST(@i AS VARCHAR(10)) + '].status''),
                @rr_out = JSON_VALUE(@json, ''$.rows[' + CAST(@i AS VARCHAR(10)) + '].rows_read''),
                @ri_out = JSON_VALUE(@json, ''$.rows[' + CAST(@i AS VARCHAR(10)) + '].rows_inserted''),
                @wf_out = JSON_VALUE(@json, ''$.rows[' + CAST(@i AS VARCHAR(10)) + '].watermark_from''),
                @wt_out = JSON_VALUE(@json, ''$.rows[' + CAST(@i AS VARCHAR(10)) + '].watermark_to''),
                @err_out = JSON_VALUE(@json, ''$.rows[' + CAST(@i AS VARCHAR(10)) + '].error'')
        ';

        EXEC sp_executesql @sql,
            N'@json VARCHAR(MAX), @cid_out BIGINT OUTPUT, @tt_out VARCHAR(200) OUTPUT, @status_out VARCHAR(20) OUTPUT, @rr_out BIGINT OUTPUT, @ri_out BIGINT OUTPUT, @wf_out VARCHAR(50) OUTPUT, @wt_out VARCHAR(50) OUTPUT, @err_out VARCHAR(2000) OUTPUT',
            @json = @results,
            @cid_out = @cid OUTPUT,
            @tt_out = @tt OUTPUT,
            @status_out = @status OUTPUT,
            @rr_out = @rr OUTPUT,
            @ri_out = @ri OUTPUT,
            @wf_out = @wf OUTPUT,
            @wt_out = @wt OUTPUT,
            @err_out = @err OUTPUT;

        -- Process retrieved values
        SELECT @lid = log_id FROM meta.etl_execution_log
        WHERE pipeline_run_id=@pipeline_run_id AND controller_id=@cid AND status='Running';

        SET @wt2 = NULLIF(@wt, '');
        SET @errmsg = COALESCE(
                        NULLIF(@err, ''),
                        CASE WHEN @status = 'Skipped' THEN 'Skipped'
                        ELSE 'Unknow' END);
        
        IF @status = 'Succeeded' AND @wt2 IS NOT NULL
            UPDATE meta.etl_transform_config
            SET last_watermark = @wt2
            WHERE transform_config_id = @cid;

        IF @status = 'Succeeded'
            EXEC meta.usp_etl_log
                 @action='entity_success', @log_id=@lid,
                 @pipeline_run_id=@pipeline_run_id, @pipeline_name=@pipeline_name,
                 @controller_id=@cid,
                 @rows_read=@rr, @rows_inserted=@ri,
                 @watermark_from=@wf, @watermark_to=@wt2;
        ELSE
            EXEC meta.usp_etl_log
                 @action='entity_failed', @log_id=@lid,
                 @pipeline_run_id=@pipeline_run_id, @pipeline_name=@pipeline_name,
                 @controller_id=@cid, @target_table=@tt,
                 @error_message=@errmsg;

        SET @i += 1;
    END
END