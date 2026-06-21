CREATE   PROCEDURE silver.usp_load_crm_agent
    @pipeline_run_id   VARCHAR(100) = NULL,
    @last_watermark    VARCHAR(100) = NULL,
    @watermark_column  VARCHAR(100) = NULL,
    @controller_id     VARCHAR(100) = NULL,
    @rows_read         BIGINT       = 0 OUTPUT,
    @rows_inserted     BIGINT       = 0 OUTPUT,
    @new_watermark     VARCHAR(100) = NULL OUTPUT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @last_watermark_dt DATETIME2(6) = TRY_CAST(@last_watermark AS DATETIME2(6));
    DECLARE @incremental BIT = CASE
        WHEN @watermark_column IS NOT NULL AND @last_watermark_dt IS NOT NULL THEN 1
        ELSE 0
    END;

    -- Build #cleansed
    ;WITH src AS (
        SELECT *
        FROM insurance_lakehouse.bronze.crm_agent
        WHERE @incremental = 0 OR updated_at > @last_watermark_dt
    ),
    deduped AS (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY agent_id ORDER BY updated_at DESC) AS _rn
        FROM src
    ),
    cleansed AS (
        SELECT
            UPPER(TRIM(CAST(agent_id AS VARCHAR(100)))) AS agent_id,

            (SELECT STRING_AGG(UPPER(LEFT(w.value,1)) + LOWER(SUBSTRING(w.value,2,4000)), ' ')
                    WITHIN GROUP (ORDER BY w.ordinal)
             FROM STRING_SPLIT(LOWER(LTRIM(RTRIM(
                 REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                     ISNULL(CAST(agent_name AS VARCHAR(4000)), ''),
                 '  ',' '),'  ',' '),'  ',' '),'  ',' '),'  ',' ')
             ))), ' ', 1) w WHERE w.value <> '') AS agent_name,

            (SELECT STRING_AGG(UPPER(LEFT(w.value,1)) + LOWER(SUBSTRING(w.value,2,4000)), ' ')
                    WITHIN GROUP (ORDER BY w.ordinal)
             FROM STRING_SPLIT(LOWER(LTRIM(RTRIM(ISNULL(CAST(region AS VARCHAR(4000)), '')))), ' ', 1) w
             WHERE w.value <> '') AS region,

            (SELECT STRING_AGG(UPPER(LEFT(w.value,1)) + LOWER(SUBSTRING(w.value,2,4000)), ' ')
                    WITHIN GROUP (ORDER BY w.ordinal)
             FROM STRING_SPLIT(LOWER(LTRIM(RTRIM(ISNULL(CAST(branch AS VARCHAR(4000)), '')))), ' ', 1) w
             WHERE w.value <> '') AS branch,

            (SELECT STRING_AGG(UPPER(LEFT(w.value,1)) + LOWER(SUBSTRING(w.value,2,4000)), ' ')
                    WITHIN GROUP (ORDER BY w.ordinal)
             FROM STRING_SPLIT(LOWER(LTRIM(RTRIM(
                 REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                     ISNULL(CAST(manager_name AS VARCHAR(4000)), ''),
                 '  ',' '),'  ',' '),'  ',' '),'  ',' '),'  ',' ')
             ))), ' ', 1) w WHERE w.value <> '') AS manager_name,

            TRY_CAST(created_date AS DATE) AS created_date,
            updated_at
        FROM deduped
        WHERE _rn = 1
          AND agent_id IS NOT NULL
          AND TRIM(CAST(agent_id AS VARCHAR(100))) <> ''
          AND UPPER(TRIM(CAST(agent_id AS VARCHAR(100)))) <> 'UNKNOWN'
    )
    SELECT
        agent_id, agent_name, region, branch, manager_name, created_date, updated_at,
        CONVERT(VARCHAR(64), HASHBYTES('SHA2_256',
            CONCAT(
                COALESCE(agent_name,    ''), '||',
                COALESCE(region,        ''), '||',
                COALESCE(branch,        ''), '||',
                COALESCE(manager_name,  '')
            )
        ), 2) AS row_hash
    INTO #cleansed
    FROM cleansed;

    --  Metadata 
    SET @rows_read = (
        SELECT COUNT(*) FROM insurance_lakehouse.bronze.crm_agent
        WHERE @incremental = 0 OR updated_at > @last_watermark_dt
    );

    SET @new_watermark = NULL;
    IF @watermark_column IS NOT NULL
        SET @new_watermark = (SELECT CONVERT(VARCHAR(33), MAX(updated_at), 126) FROM #cleansed);

    SET @rows_inserted = 0;

    -- Load silver
    IF EXISTS (SELECT 1 FROM #cleansed)
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = 'silver' AND t.name = 'crm_agent'
        )
        BEGIN
            SELECT * INTO silver.crm_agent FROM #cleansed;
            SET @rows_inserted = @@ROWCOUNT;   
        END
        ELSE
        BEGIN
            MERGE INTO silver.crm_agent AS tgt
            USING #cleansed AS src
            ON tgt.agent_id = src.agent_id
            WHEN MATCHED AND src.row_hash <> tgt.row_hash THEN UPDATE SET
                tgt.agent_name   = src.agent_name,
                tgt.region       = src.region,
                tgt.branch       = src.branch,
                tgt.manager_name = src.manager_name,
                tgt.created_date = src.created_date,
                tgt.updated_at   = CASE WHEN @watermark_column IS NOT NULL THEN src.updated_at ELSE tgt.updated_at END,
                tgt.row_hash     = src.row_hash
            WHEN NOT MATCHED THEN INSERT (
                agent_id, agent_name, region, branch, manager_name, created_date, updated_at, row_hash
            )
            VALUES (
                src.agent_id, src.agent_name, src.region, src.branch, src.manager_name,
                src.created_date, src.updated_at, src.row_hash
            );
            SET @rows_inserted = @@ROWCOUNT;   
        END

        -- Update watermark (độc lập với @rows_inserted) 
        IF @new_watermark IS NOT NULL AND @controller_id IS NOT NULL
        BEGIN
            UPDATE insurance_warehouse.meta.etl_transform_config
            SET    last_watermark = @new_watermark
            WHERE  transform_config_id = TRY_CAST(@controller_id AS INT);
        END
    END

    DROP TABLE IF EXISTS #cleansed;

    SELECT
        @rows_read     AS rows_read,
        @rows_inserted AS rows_inserted,
        @new_watermark AS watermark_to;
END;