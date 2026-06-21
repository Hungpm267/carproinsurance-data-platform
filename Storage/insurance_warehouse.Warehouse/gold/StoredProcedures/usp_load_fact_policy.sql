CREATE   PROCEDURE gold.usp_load_fact_policy
    @pipeline_run_id VARCHAR(100) = NULL,
    @last_watermark VARCHAR(100) = NULL,
    @watermark_column VARCHAR(100) = NULL,
    @rows_read BIGINT = 0 OUTPUT,
    @rows_inserted BIGINT = 0 OUTPUT,
    @new_watermark VARCHAR(100) = NULL OUTPUT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @lw_norm VARCHAR(100) = NULLIF(LTRIM(RTRIM(@last_watermark)), '');
    DECLARE @wc_norm VARCHAR(100) = NULLIF(LTRIM(RTRIM(@watermark_column)), '');

    -- 1. Calculate rows_read and new_watermark
    DECLARE @sql_stats NVARCHAR(MAX);
    IF @lw_norm IS NOT NULL AND @wc_norm IS NOT NULL
    BEGIN
        SET @sql_stats = N'
            SELECT 
                @r_read = COUNT(*),
                @n_wm = CAST(MAX(' + QUOTENAME(@wc_norm) + N') AS VARCHAR(100))
            FROM silver.policy_policy
            WHERE ' + QUOTENAME(@wc_norm) + N' > CAST(@lw AS DATETIME2(6));
        ';
        EXEC sp_executesql @sql_stats, 
            N'@lw VARCHAR(100), @r_read BIGINT OUTPUT, @n_wm VARCHAR(100) OUTPUT', 
            @lw = @lw_norm, @r_read = @rows_read OUTPUT, @n_wm = @new_watermark OUTPUT;
    END
    ELSE
    BEGIN
        SET @sql_stats = N'
            SELECT 
                @r_read = COUNT(*),
                @n_wm = CAST(MAX(' + QUOTENAME(@wc_norm) + N') AS VARCHAR(100))
            FROM silver.policy_policy;
        ';
        EXEC sp_executesql @sql_stats, 
            N'@r_read BIGINT OUTPUT, @n_wm VARCHAR(100) OUTPUT', 
            @r_read = @rows_read OUTPUT, @n_wm = @new_watermark OUTPUT;
    END

    -- 2. Execute Load if rows exist
    IF @rows_read > 0
    BEGIN
        DECLARE @max_key INT = COALESCE((SELECT MAX(policy_key) FROM gold.fact_policy), 0);
        IF @max_key < 0 SET @max_key = 0;

        DECLARE @sql_load NVARCHAR(MAX) = N'
            DECLARE @base_key INT = @max_k;

            INSERT INTO gold.fact_policy (
                policy_key, policy_id, quotation_id, policy_number,
                policy_start_date_key, policy_end_date_key, issued_date_key,
                customer_key, provider_key, policy_status_key,
                written_premium_amount, quoted_premium_amount, premium_variance_amount,
                is_in_force, policy_term_days
            )
            SELECT 
                @base_key + ROW_NUMBER() OVER (ORDER BY s.policy_id) AS policy_key,
                s.policy_id,
                s.quotation_id,
                s.policy_number,
                CAST(CONVERT(VARCHAR(8), s.policy_start_date, 112) AS INT) AS policy_start_date_key,
                CAST(CONVERT(VARCHAR(8), s.policy_end_date, 112) AS INT) AS policy_end_date_key,
                CAST(CONVERT(VARCHAR(8), s.issued_date, 112) AS INT) AS issued_date_key,
                COALESCE(c.customer_key, -1) AS customer_key,
                COALESCE(p.provider_key, -1) AS provider_key,
                COALESCE(ps.policy_status_key, -1) AS policy_status_key,
                s.premium_amount AS written_premium_amount,
                q.premium_amount AS quoted_premium_amount,
                CAST((s.premium_amount - COALESCE(q.premium_amount, 0)) AS DECIMAL(18,2)) AS premium_variance_amount,
                CASE WHEN s.policy_status = ''ACTIVE'' THEN 1 ELSE 0 END AS is_in_force,
                DATEDIFF(day, s.policy_start_date, s.policy_end_date) AS policy_term_days
            FROM silver.policy_policy s
            LEFT JOIN silver.crm_quotation q ON q.quotation_id = s.quotation_id 
            LEFT JOIN gold.dim_customer c 
              ON c.customer_id = s.customer_id 
             AND c.is_current = 1
            LEFT JOIN gold.dim_insurance_provider p ON p.provider_code = s.provider_code 
            LEFT JOIN gold.dim_policy_status ps ON ps.policy_status_name = UPPER(TRIM(s.policy_status)) 
            WHERE s.policy_id IS NOT NULL
        ';

        -- Append filter for incremental load
        IF @lw_norm IS NOT NULL AND @wc_norm IS NOT NULL
        BEGIN
            SET @sql_load = @sql_load + N' AND s.' + QUOTENAME(@wc_norm) + N' > CAST(@lw AS DATETIME2(6));';
            EXEC sp_executesql @sql_load, 
                N'@max_k INT, @lw VARCHAR(100), @r_ins BIGINT OUTPUT', 
                @max_k = @max_key, @lw = @lw_norm, @r_ins = @rows_inserted OUTPUT;
        END
        ELSE
        BEGIN
            -- Execute without filter
            -- We need to capture @@ROWCOUNT. So we append it to the dynamic SQL
            SET @sql_load = @sql_load + N'; SET @r_ins = @@ROWCOUNT;';
            EXEC sp_executesql @sql_load, 
                N'@max_k INT, @r_ins BIGINT OUTPUT', 
                @max_k = @max_key, @r_ins = @rows_inserted OUTPUT;
        END
    END
    ELSE
    BEGIN
        SET @rows_inserted = 0;
        SET @new_watermark = @last_watermark;
    END

    -- Select result set for Fabric pipeline
    SELECT 
        @rows_read AS rows_read, 
        @rows_inserted AS rows_inserted, 
        @new_watermark AS watermark_to;
END;