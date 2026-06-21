/*
  Seed meta.etl_pipeline_config for recovery_master_pipeline.
  Run once in insurance_warehouse SQL editor (adjust columns if your table differs).
*/

IF NOT EXISTS (
    SELECT 1
    FROM meta.etl_pipeline_config
    WHERE pipeline_name = 'recovery_master_pipeline'
)
BEGIN
    INSERT INTO meta.etl_pipeline_config (
        pipeline_name,
        pipeline_stage,
        is_active,
        retry_count,
        retry_interval_minutes,
        timeout_minutes,
        created_at,
        created_by
    )
    VALUES (
        'recovery_master_pipeline',
        'orchestrator',
        1,
        0,
        0,
        180,
        SYSUTCDATETIME(),
        'seed'
    );
END;

SELECT pipeline_name, pipeline_stage, is_active
FROM meta.etl_pipeline_config
WHERE pipeline_name = 'recovery_master_pipeline';
