CREATE   PROCEDURE meta.usp_etl_start_batch_gold
    @pipeline_run_id NVARCHAR(100), @pipeline_name NVARCHAR(200)
AS
BEGIN
    DECLARE @base BIGINT = (SELECT COALESCE(MAX(log_id),0) FROM meta.etl_execution_log);

    INSERT INTO meta.etl_execution_log
        (log_id, pipeline_run_id, controller_id, pipeline_name, start_time, status,
         rows_read, rows_inserted, rows_updated, rows_rejected, watermark_from, logged_at)
    SELECT @base + ROW_NUMBER() OVER (ORDER BY transform_config_id),
           @pipeline_run_id, transform_config_id, @pipeline_name, SYSDATETIME(), 'Running',
           0,0,0,0, last_watermark, SYSDATETIME()
    FROM   meta.etl_transform_config
    WHERE  pipeline_name = 'silver_to_gold';
END