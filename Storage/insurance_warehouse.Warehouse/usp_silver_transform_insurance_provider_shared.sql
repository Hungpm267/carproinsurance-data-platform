CREATE OR ALTER PROCEDURE silver.usp_load_crm_insurance_provider
    @pipeline_run_id   VARCHAR(100) = NULL,
    @last_watermark    VARCHAR(100) = NULL,
    @watermark_column  VARCHAR(100) = NULL,
    @rows_read         BIGINT = 0 OUTPUT,
    @rows_inserted     BIGINT = 0 OUTPUT,
    @new_watermark     VARCHAR(100) = NULL OUTPUT
AS
BEGIN
    SET NOCOUNT ON;

    -- @last_watermark NULL => full load; otherwise incremental by updated_at
    DECLARE @last_watermark_dt DATETIME2(6) = TRY_CAST(@last_watermark AS DATETIME2(6));

    -- 1. Incremental row count read from insurance_lakehouse.bronze
    SET @rows_read = (
        SELECT COUNT(*)
        FROM insurance_lakehouse.bronze.crm_insurance_provider
        WHERE @last_watermark_dt IS NULL OR updated_at > @last_watermark_dt
    );

    -- 2. Cleanse + dedup by BK, keep latest record by updated_at
    ;WITH cleansed AS (
        SELECT
            UPPER(TRIM(provider_code)) AS provider_code,
            TRIM(provider_name)        AS provider_name,
            TRIM(provider_group)       AS provider_group,
            TRY_CAST(active_flag AS BIT) AS active_flag,
            TRY_CAST(created_date AS DATE) AS created_date,
            updated_at,
            ROW_NUMBER() OVER (
                PARTITION BY UPPER(TRIM(provider_code))
                ORDER BY updated_at DESC
            ) AS rn
        FROM insurance_lakehouse.bronze.crm_insurance_provider
        WHERE @last_watermark_dt IS NULL OR updated_at > @last_watermark_dt
    )
    SELECT *
    INTO #cleansed
    FROM cleansed
    WHERE rn = 1;

    -- 3. Count new keys before merge (Fabric DW has no MERGE OUTPUT clause)
    SET @rows_inserted = (
        SELECT COUNT(*)
        FROM #cleansed s
        LEFT JOIN silver.crm_insurance_provider t ON t.provider_code = s.provider_code
        WHERE t.provider_code IS NULL
    );

    -- 4. Upsert into silver
    MERGE INTO silver.crm_insurance_provider AS tgt
    USING #cleansed AS src
    ON tgt.provider_code = src.provider_code

    WHEN MATCHED THEN UPDATE SET
        tgt.provider_name  = src.provider_name,
        tgt.provider_group = src.provider_group,
        tgt.active_flag    = src.active_flag,
        tgt.created_date   = src.created_date,
        tgt.updated_at      = src.updated_at

    WHEN NOT MATCHED THEN INSERT (
        provider_code, provider_name, provider_group, active_flag, created_date, updated_at
    )
    VALUES (
        src.provider_code, src.provider_name, src.provider_group, src.active_flag, src.created_date, src.updated_at
    );

    -- 5. New watermark = max updated_at seen this run; if no rows, carry old value forward
    SET @new_watermark = (
        SELECT CONVERT(VARCHAR(33), MAX(updated_at), 126)
        FROM #cleansed
    );
    IF @new_watermark IS NULL
        SET @new_watermark = @last_watermark;

    DROP TABLE IF EXISTS #cleansed;

    -- Select result set for Fabric pipeline
    SELECT
        @rows_read AS rows_read,
        @rows_inserted AS rows_inserted,
        @new_watermark AS watermark_to;
END;