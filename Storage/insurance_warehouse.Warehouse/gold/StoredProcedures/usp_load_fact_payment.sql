CREATE   PROCEDURE gold.usp_load_fact_payment
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
            FROM silver.policy_payment
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
            FROM silver.policy_payment;
        ';
        EXEC sp_executesql @sql_stats, 
            N'@r_read BIGINT OUTPUT, @n_wm VARCHAR(100) OUTPUT', 
            @r_read = @rows_read OUTPUT, @n_wm = @new_watermark OUTPUT;
    END

    -- 2. Execute Load if rows exist
    IF @rows_read > 0
    BEGIN
        DECLARE @max_key INT = COALESCE((SELECT MAX(payment_key) FROM gold.fact_payment), 0);
        IF @max_key < 0 SET @max_key = 0;

        DECLARE @sql_load NVARCHAR(MAX) = N'
            DECLARE @base_key INT = @max_k;

            INSERT INTO gold.fact_payment (
                payment_key, payment_id, policy_id, payment_date_key,
                payment_method_key, payment_status_key, payment_amount, is_successful_payment
            )
            SELECT 
                @base_key + ROW_NUMBER() OVER (ORDER BY s.payment_id) AS payment_key,
                s.payment_id,
                s.policy_id,
                CAST(CONVERT(VARCHAR(8), s.payment_date, 112) AS INT) AS payment_date_key,
                COALESCE(pm.payment_method_key, -1) AS payment_method_key,
                COALESCE(ps.payment_status_key, -1) AS payment_status_key,
                s.payment_amount,
                CASE WHEN UPPER(TRIM(s.payment_status)) = ''PAID'' THEN 1 ELSE 0 END AS is_successful_payment
            FROM silver.policy_payment s
            LEFT JOIN gold.dim_payment_method pm ON pm.payment_method_name = UPPER(TRIM(s.payment_method)) 
            LEFT JOIN gold.dim_payment_status ps ON ps.payment_status_name = UPPER(TRIM(s.payment_status)) 
            WHERE s.payment_id IS NOT NULL
        ';

        -- Append filter for incremental load
        IF @lw_norm IS NOT NULL AND @wc_norm IS NOT NULL
        BEGIN
            SET @sql_load = @sql_load + N' AND s.' + QUOTENAME(@wc_norm) + N' > CAST(@lw AS DATETIME2(6));';
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