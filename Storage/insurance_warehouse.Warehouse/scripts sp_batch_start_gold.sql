-- CREATE OR ALTER PROCEDURE meta.usp_etl_start_batch_gold
--     @pipeline_run_id NVARCHAR(100), @pipeline_name NVARCHAR(200)
-- AS
-- BEGIN
--     DECLARE @base BIGINT = (SELECT COALESCE(MAX(log_id),0) FROM meta.etl_execution_log);

--     INSERT INTO meta.etl_execution_log
--         (log_id, pipeline_run_id, controller_id, pipeline_name, start_time, status,
--          rows_read, rows_inserted, rows_updated, rows_rejected, watermark_from, logged_at)

--     SELECT @base + ROW_NUMBER() OVER (ORDER BY transform_config_id),
--            @pipeline_run_id, transform_config_id, @pipeline_name, SYSDATETIME(), 'Running',
--            0,0,0,0, last_watermark, SYSDATETIME()
--     FROM   meta.etl_transform_config
--     WHERE  pipeline_name = 'silver_to_gold';
-- END


-- end log pipeline
CREATE OR ALTER PROCEDURE meta.usp_etl_end_batch
    @pipeline_run_id VARCHAR(100), @results VARCHAR(MAX), @pipeline_name VARCHAR(200)
AS
BEGIN
    SET NOCOUNT ON;

    SELECT ROW_NUMBER() OVER (ORDER BY controller_id) AS rn, *
    INTO #r
    FROM OPENJSON(@results, '$.rows')
    WITH (
        controller_id  BIGINT       '$.controller_id',
        target_table   VARCHAR(200) '$.target_table',
        status         VARCHAR(20)  '$.status',
        rows_read      BIGINT       '$.rows_read',
        rows_inserted  BIGINT       '$.rows_inserted',
        watermark_from VARCHAR(50)  '$.watermark_from',
        watermark_to   VARCHAR(50)  '$.watermark_to',
        error          VARCHAR(2000)'$.error'
    );

    DECLARE @i INT = 1, @n INT = (SELECT COUNT(*) FROM #r);
    DECLARE @cid BIGINT, @status VARCHAR(20), @tt VARCHAR(200),
            @rr BIGINT, @ri BIGINT, @wf VARCHAR(50), @wt VARCHAR(50), @err VARCHAR(2000),
            @lid BIGINT,
            @wt2 VARCHAR(50), @errmsg VARCHAR(2000);

    WHILE @i <= @n
    BEGIN
        SELECT @cid=controller_id, @status=status, @tt=target_table,
               @rr=rows_read, @ri=rows_inserted,
               @wf=watermark_from, @wt=watermark_to, @err=error
        FROM #r WHERE rn=@i;

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

    DROP TABLE #r;
END