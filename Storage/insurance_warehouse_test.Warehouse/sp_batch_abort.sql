/*
  Recovery batch ABORT — when Recovery_Orchestrator fails before exitValue is produced.

  Use on pipeline failure branch (Recovery_Orchestrator → Failed):

    EXEC meta.usp_etl_abort_batch_recovery
        @pipeline_run_id = '<recovery RunId>',
        @pipeline_name   = 'recover_silver_to_gold',
        @error_message   = @activity('Recovery_Orchestrator').error.message;

  Closes any entity rows still Running for this recovery run and sets linked
  error_logging_table rows to Need Manual Check (from dynamic_source_file).
*/

CREATE OR ALTER PROCEDURE meta.usp_etl_abort_batch_recovery
    @pipeline_run_id NVARCHAR(100),
    @pipeline_name   NVARCHAR(200),
    @error_message   NVARCHAR(2000) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    SET @error_message = LEFT(
        COALESCE(NULLIF(LTRIM(RTRIM(@error_message)), N''), N'Recovery orchestrator failed'),
        2000
    );

    DECLARE @lid BIGINT, @cid BIGINT, @error_id NVARCHAR(36), @tt NVARCHAR(200);

    WHILE EXISTS (
        SELECT 1
        FROM meta.etl_execution_log
        WHERE pipeline_run_id = @pipeline_run_id
          AND pipeline_name = @pipeline_name
          AND status = N'Running'
    )
    BEGIN
        SELECT TOP 1
            @lid = log_id,
            @cid = controller_id,
            @error_id = dynamic_source_file
        FROM meta.etl_execution_log
        WHERE pipeline_run_id = @pipeline_run_id
          AND pipeline_name = @pipeline_name
          AND status = N'Running'
        ORDER BY log_id;

        SELECT @tt = target_table
        FROM meta.etl_transform_config
        WHERE transform_config_id = @cid;

        EXEC meta.usp_etl_log
            @action = N'entity_failed',
            @log_id = @lid,
            @pipeline_run_id = @pipeline_run_id,
            @pipeline_name = @pipeline_name,
            @controller_id = @cid,
            @target_table = @tt,
            @error_message = @error_message;

        IF @error_id IS NOT NULL AND LTRIM(RTRIM(@error_id)) <> N''
        BEGIN
            UPDATE meta.error_logging_table
            SET
                status = N'Need Manual Check',
                updated_at = SYSDATETIME()
            WHERE error_id = @error_id
              AND status IN (N'New', N'In-Progress');
        END;
    END;
END;
