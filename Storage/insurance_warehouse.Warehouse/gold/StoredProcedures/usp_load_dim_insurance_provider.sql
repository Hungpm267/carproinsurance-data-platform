CREATE   PROCEDURE gold.usp_load_dim_insurance_provider
    @pipeline_run_id VARCHAR(100) = NULL,
    @last_watermark VARCHAR(100) = NULL,
    @watermark_column VARCHAR(100) = NULL,
    @rows_read BIGINT = 0 OUTPUT,
    @rows_inserted BIGINT = 0 OUTPUT,
    @new_watermark VARCHAR(100) = NULL OUTPUT
AS
BEGIN
    SET NOCOUNT ON;

    -- 1. Insert Unknown if not exists
    IF NOT EXISTS (SELECT 1 FROM gold.dim_insurance_provider WHERE provider_key = -1)
        INSERT INTO gold.dim_insurance_provider (provider_key, provider_code, provider_name, provider_group, is_active)
        VALUES (-1, 'UNKNOWN', 'Unknown', 'Unknown', 0);

    -- Calculate rows_read
    SET @rows_read = (SELECT COUNT(*) FROM silver.crm_insurance_provider);

    -- Calculate max key
    DECLARE @max_key INT = COALESCE((SELECT MAX(provider_key) FROM gold.dim_insurance_provider), 0);
    IF @max_key < 0 SET @max_key = 0;

    -- Deduplicate source in CTE using row number to avoid merge duplicate errors
    WITH src_dedup AS (
        SELECT 
            provider_code,
            provider_name,
            provider_group,
            active_flag,
            ROW_NUMBER() OVER (PARTITION BY provider_code ORDER BY updated_at DESC) as rn
        FROM silver.crm_insurance_provider
    )
    SELECT * INTO #src_provider
    FROM src_dedup
    WHERE rn = 1;

    -- Calculate how many rows will be inserted
    SET @rows_inserted = (
        SELECT COUNT(*)
        FROM #src_provider s
        LEFT JOIN gold.dim_insurance_provider t ON t.provider_code = s.provider_code 
        WHERE t.provider_code IS NULL
    );

    -- Perform UPDATE & INSERT
    BEGIN TRANSACTION;

    -- Update existing records
    UPDATE tgt
    SET 
        tgt.provider_name = src.provider_name,
        tgt.provider_group = src.provider_group,
        tgt.is_active = CAST(src.active_flag AS BIT)
    FROM gold.dim_insurance_provider tgt
    INNER JOIN #src_provider src ON tgt.provider_code = src.provider_code;

    -- Insert new records
    INSERT INTO gold.dim_insurance_provider (
        provider_key,
        provider_code,
        provider_name,
        provider_group,
        is_active
    )
    SELECT 
        @max_key + ROW_NUMBER() OVER (ORDER BY src.provider_code) AS provider_key,
        src.provider_code,
        src.provider_name,
        src.provider_group,
        CAST(src.active_flag AS BIT) AS is_active
    FROM #src_provider src
    LEFT JOIN gold.dim_insurance_provider t ON t.provider_code = src.provider_code
    WHERE t.provider_code IS NULL;

    COMMIT TRANSACTION;

    DROP TABLE #src_provider;

    SET @new_watermark = NULL;

    -- Select result set for Fabric pipeline
    SELECT 
        @rows_read AS rows_read, 
        @rows_inserted AS rows_inserted, 
        @new_watermark AS watermark_to;
END;