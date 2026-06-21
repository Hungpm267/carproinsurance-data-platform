CREATE   PROCEDURE gold.usp_load_dim_agent
    @pipeline_run_id VARCHAR(100) = NULL,
    @last_watermark VARCHAR(100) = NULL,
    @watermark_column VARCHAR(100) = NULL,
    @rows_read BIGINT = 0 OUTPUT,
    @rows_inserted BIGINT = 0 OUTPUT,
    @new_watermark VARCHAR(100) = NULL OUTPUT
AS
BEGIN
    SET NOCOUNT ON;

    -- Calculate rows_read from source
    SET @rows_read = (SELECT COUNT(*) FROM silver.crm_agent);

    -- Auto-generate Surrogate Keys logic
    DECLARE @max_key INT = COALESCE((SELECT MAX(agent_key) FROM gold.dim_agent), 0);
    IF @max_key < 0 SET @max_key = 0;

    -- Begin Transaction
    BEGIN TRANSACTION;

    -- 1. Expire changed records in Gold
    UPDATE gold.dim_agent
    SET is_current = 0,
        expiry_date = CAST(GETDATE() AS DATE)
    WHERE agent_id IN (
        SELECT t.agent_id
        FROM gold.dim_agent t
        INNER JOIN silver.crm_agent s ON t.agent_id = s.agent_id 
        WHERE t.is_current = 1 AND t.row_hash != s.row_hash
    )
      AND is_current = 1;

    -- 2. Insert new/updated versions into Gold
    ;WITH new_inserts AS (
        SELECT 
            s.agent_id,
            s.agent_name,
            s.region,
            s.branch,
            s.manager_name,
            s.row_hash,
            CAST(CASE WHEN t.agent_id IS NULL THEN s.created_date ELSE s.updated_at END AS DATE) AS effective_date
        FROM silver.crm_agent s
        LEFT JOIN gold.dim_agent t ON t.agent_id = s.agent_id  AND t.is_current = 1
        WHERE t.agent_id IS NULL OR t.row_hash != s.row_hash
    ),
    new_inserts_with_keys AS (
        SELECT 
            @max_key + ROW_NUMBER() OVER (ORDER BY n.agent_id) AS agent_key,
            n.agent_id,
            n.agent_name,
            n.region,
            n.branch,
            n.manager_name,
            n.row_hash,
            n.effective_date
        FROM new_inserts n
    )
    INSERT INTO gold.dim_agent (
        agent_key, agent_id, agent_name, region, branch,
        manager_name, row_hash, effective_date, expiry_date, is_current
    )
    SELECT 
        agent_key, agent_id, agent_name, region, branch,
        manager_name, row_hash, effective_date, CAST('9999-12-31' AS DATE), 1
    FROM new_inserts_with_keys;

    COMMIT TRANSACTION;

    SET @rows_inserted = @@ROWCOUNT;
    SET @new_watermark = NULL;

    -- Select result set for Fabric pipeline
    SELECT 
        @rows_read AS rows_read, 
        @rows_inserted AS rows_inserted, 
        @new_watermark AS watermark_to;
END;