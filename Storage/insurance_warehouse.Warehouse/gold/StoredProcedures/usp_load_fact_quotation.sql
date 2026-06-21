CREATE   PROCEDURE gold.usp_load_fact_quotation
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
            FROM silver.crm_quotation
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
            FROM silver.crm_quotation;
        ';
        EXEC sp_executesql @sql_stats, 
            N'@r_read BIGINT OUTPUT, @n_wm VARCHAR(100) OUTPUT', 
            @r_read = @rows_read OUTPUT, @n_wm = @new_watermark OUTPUT;
    END

    -- 2. Execute Load if rows exist
    IF @rows_read > 0
    BEGIN
        DECLARE @max_key INT = COALESCE((SELECT MAX(quotation_key) FROM gold.fact_quotation), 0);
        IF @max_key < 0 SET @max_key = 0;

        DECLARE @sql_load NVARCHAR(MAX) = N'
            DECLARE @base_key INT = @max_k;

            ;WITH veh_map AS (
                SELECT customer_id, MIN(vehicle_id) AS vehicle_id
                FROM silver.crm_vehicle
                WHERE customer_id IS NOT NULL
                GROUP BY customer_id
            )
            INSERT INTO gold.fact_quotation (
                quotation_key, quotation_id, quotation_date_key, quotation_expiry_date_key,
                customer_key, agent_key, provider_key, vehicle_key,
                product_package_key, quotation_status_key, quotation_premium_amount
            )
            SELECT 
                @base_key + ROW_NUMBER() OVER (ORDER BY s.quotation_id) AS quotation_key,
                s.quotation_id,
                CAST(CONVERT(VARCHAR(8), s.quotation_date, 112) AS INT) AS quotation_date_key,
                TRY_CAST(CONVERT(VARCHAR(8), TRY_CAST(s.quotation_expiry_date AS DATE), 112) AS INT) AS quotation_expiry_date_key,
                COALESCE(c.customer_key, -1) AS customer_key,
                COALESCE(a.agent_key, -1) AS agent_key,
                COALESCE(p.provider_key, -1) AS provider_key,
                COALESCE(v.vehicle_key, -1) AS vehicle_key,
                COALESCE(pkg.product_package_key, -1) AS product_package_key,
                COALESCE(qs.quotation_status_key, -1) AS quotation_status_key,
                s.premium_amount AS quotation_premium_amount
            FROM silver.crm_quotation s
            LEFT JOIN veh_map vm ON vm.customer_id = s.customer_id 
            LEFT JOIN gold.dim_customer c 
              ON c.customer_id = s.customer_id 
             AND CAST(s.quotation_date AS DATE) >= c.effective_date 
             AND CAST(s.quotation_date AS DATE) < COALESCE(c.expiry_date, ''9999-12-31'')
            LEFT JOIN gold.dim_agent a ON a.agent_id = s.agent_id  AND a.is_current = 1
            LEFT JOIN gold.dim_insurance_provider p ON p.provider_code = s.provider_code 
            LEFT JOIN gold.dim_vehicle v ON v.vehicle_id = vm.vehicle_id 
            LEFT JOIN gold.dim_product_package pkg ON pkg.product_package_name = UPPER(TRIM(s.package_code)) 
            LEFT JOIN gold.dim_quotation_status qs ON qs.quotation_status_name = UPPER(TRIM(s.quotation_status)) 
            WHERE s.quotation_id IS NOT NULL
        ';

        -- Append filter for incremental load
        IF @lw_norm IS NOT NULL AND @wc_norm IS NOT NULL
        BEGIN
            SET @sql_load = @sql_load + N' AND s.' + QUOTENAME(@wc_norm) + N' > CAST(@lw AS DATETIME2(6));';
            -- Add @@ROWCOUNT capturing
            SET @sql_load = @sql_load + N' SET @r_ins = @@ROWCOUNT;';
            EXEC sp_executesql @sql_load, 
                N'@max_k INT, @lw VARCHAR(100), @r_ins BIGINT OUTPUT', 
                @max_k = @max_key, @lw = @lw_norm, @r_ins = @rows_inserted OUTPUT;
        END
        ELSE
        BEGIN
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