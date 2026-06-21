CREATE   PROCEDURE gold.usp_load_lookup_seeds
    @pipeline_run_id VARCHAR(100) = NULL,
    @last_watermark VARCHAR(100) = NULL,
    @watermark_column VARCHAR(100) = NULL,
    @rows_read BIGINT = 0 OUTPUT,
    @rows_inserted BIGINT = 0 OUTPUT,
    @new_watermark VARCHAR(100) = NULL OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @total_inserted BIGINT = 0;
    DECLARE @inserted BIGINT = 0;

    -- ==========================================
    -- 1. dim_product_package
    -- ==========================================
    IF NOT EXISTS (SELECT 1 FROM gold.dim_product_package WHERE product_package_key = -1)
        INSERT INTO gold.dim_product_package (product_package_key, product_package_name) VALUES (-1, 'UNKNOWN');

    DECLARE @max_pkg INT = COALESCE((SELECT MAX(product_package_key) FROM gold.dim_product_package), 0);
    IF @max_pkg < 0 SET @max_pkg = 0;

    INSERT INTO gold.dim_product_package (product_package_key, product_package_name)
    SELECT 
        @max_pkg + ROW_NUMBER() OVER (ORDER BY s.package_code) AS product_package_key,
        s.package_code
    FROM (
        SELECT DISTINCT UPPER(TRIM(package_code)) AS package_code
        FROM silver.crm_quotation
        WHERE package_code IS NOT NULL AND TRIM(package_code) != ''
    ) s
    LEFT JOIN gold.dim_product_package t ON t.product_package_name = s.package_code 
    WHERE t.product_package_name IS NULL;
    
    SET @inserted = @@ROWCOUNT;
    SET @total_inserted += @inserted;

    -- ==========================================
    -- 2. dim_quotation_status
    -- ==========================================
    IF NOT EXISTS (SELECT 1 FROM gold.dim_quotation_status WHERE quotation_status_key = -1)
        INSERT INTO gold.dim_quotation_status (quotation_status_key, quotation_status_name) VALUES (-1, 'UNKNOWN');

    DECLARE @max_qstat INT = COALESCE((SELECT MAX(quotation_status_key) FROM gold.dim_quotation_status), 0);
    IF @max_qstat < 0 SET @max_qstat = 0;

    INSERT INTO gold.dim_quotation_status (quotation_status_key, quotation_status_name)
    SELECT 
        @max_qstat + ROW_NUMBER() OVER (ORDER BY s.quotation_status) AS quotation_status_key,
        s.quotation_status
    FROM (
        SELECT DISTINCT UPPER(TRIM(quotation_status)) AS quotation_status
        FROM silver.crm_quotation
        WHERE quotation_status IS NOT NULL AND TRIM(quotation_status) != ''
    ) s
    LEFT JOIN gold.dim_quotation_status t ON t.quotation_status_name = s.quotation_status 
    WHERE t.quotation_status_name IS NULL;

    SET @inserted = @@ROWCOUNT;
    SET @total_inserted += @inserted;

    -- ==========================================
    -- 3. dim_policy_status
    -- ==========================================
    IF NOT EXISTS (SELECT 1 FROM gold.dim_policy_status WHERE policy_status_key = -1)
        INSERT INTO gold.dim_policy_status (policy_status_key, policy_status_name) VALUES (-1, 'UNKNOWN');

    DECLARE @max_pstat INT = COALESCE((SELECT MAX(policy_status_key) FROM gold.dim_policy_status), 0);
    IF @max_pstat < 0 SET @max_pstat = 0;

    INSERT INTO gold.dim_policy_status (policy_status_key, policy_status_name)
    SELECT 
        @max_pstat + ROW_NUMBER() OVER (ORDER BY s.policy_status) AS policy_status_key,
        s.policy_status
    FROM (
        SELECT DISTINCT UPPER(TRIM(policy_status)) AS policy_status
        FROM silver.policy_policy
        WHERE policy_status IS NOT NULL AND TRIM(policy_status) != ''
    ) s
    LEFT JOIN gold.dim_policy_status t ON t.policy_status_name = s.policy_status 
    WHERE t.policy_status_name IS NULL;

    SET @inserted = @@ROWCOUNT;
    SET @total_inserted += @inserted;

    -- ==========================================
    -- 4. dim_payment_method
    -- ==========================================
    IF NOT EXISTS (SELECT 1 FROM gold.dim_payment_method WHERE payment_method_key = -1)
        INSERT INTO gold.dim_payment_method (payment_method_key, payment_method_name) VALUES (-1, 'UNKNOWN');

    DECLARE @max_pm INT = COALESCE((SELECT MAX(payment_method_key) FROM gold.dim_payment_method), 0);
    IF @max_pm < 0 SET @max_pm = 0;

    INSERT INTO gold.dim_payment_method (payment_method_key, payment_method_name)
    SELECT 
        @max_pm + ROW_NUMBER() OVER (ORDER BY s.payment_method) AS payment_method_key,
        s.payment_method
    FROM (
        SELECT DISTINCT UPPER(TRIM(payment_method)) AS payment_method
        FROM silver.policy_payment
        WHERE payment_method IS NOT NULL AND TRIM(payment_method) != ''
    ) s
    LEFT JOIN gold.dim_payment_method t ON t.payment_method_name = s.payment_method 
    WHERE t.payment_method_name IS NULL;

    SET @inserted = @@ROWCOUNT;
    SET @total_inserted += @inserted;

    -- ==========================================
    -- 5. dim_payment_status
    -- ==========================================
    IF NOT EXISTS (SELECT 1 FROM gold.dim_payment_status WHERE payment_status_key = -1)
        INSERT INTO gold.dim_payment_status (payment_status_key, payment_status_name) VALUES (-1, 'UNKNOWN');

    DECLARE @max_ps INT = COALESCE((SELECT MAX(payment_status_key) FROM gold.dim_payment_status), 0);
    IF @max_ps < 0 SET @max_ps = 0;

    INSERT INTO gold.dim_payment_status (payment_status_key, payment_status_name)
    SELECT 
        @max_ps + ROW_NUMBER() OVER (ORDER BY s.payment_status) AS payment_status_key,
        s.payment_status
    FROM (
        SELECT DISTINCT UPPER(TRIM(payment_status)) AS payment_status
        FROM silver.policy_payment
        WHERE payment_status IS NOT NULL AND TRIM(payment_status) != ''
    ) s
    LEFT JOIN gold.dim_payment_status t ON t.payment_status_name = s.payment_status 
    WHERE t.payment_status_name IS NULL;

    SET @inserted = @@ROWCOUNT;
    SET @total_inserted += @inserted;

    -- ==========================================
    -- 6. dim_coverage_type
    -- ==========================================
    IF NOT EXISTS (SELECT 1 FROM gold.dim_coverage_type WHERE coverage_type_key = -1)
        INSERT INTO gold.dim_coverage_type (coverage_type_key, coverage_type_name) VALUES (-1, 'UNKNOWN');

    DECLARE @max_cov INT = COALESCE((SELECT MAX(coverage_type_key) FROM gold.dim_coverage_type), 0);
    IF @max_cov < 0 SET @max_cov = 0;

    INSERT INTO gold.dim_coverage_type (coverage_type_key, coverage_type_name)
    SELECT 
        @max_cov + ROW_NUMBER() OVER (ORDER BY s.coverage_type) AS coverage_type_key,
        s.coverage_type
    FROM (
        SELECT DISTINCT UPPER(TRIM(coverage_type)) AS coverage_type
        FROM silver.crm_quotation_item
        WHERE coverage_type IS NOT NULL AND TRIM(coverage_type) != ''
    ) s
    LEFT JOIN gold.dim_coverage_type t ON t.coverage_type_name = s.coverage_type 
    WHERE t.coverage_type_name IS NULL;

    SET @inserted = @@ROWCOUNT;
    SET @total_inserted += @inserted;

    -- ==========================================
    -- 7. dim_cancellation_reason
    -- ==========================================
    IF NOT EXISTS (SELECT 1 FROM gold.dim_cancellation_reason WHERE cancellation_reason_key = -1)
        INSERT INTO gold.dim_cancellation_reason (cancellation_reason_key, cancellation_reason_name) VALUES (-1, 'UNKNOWN');

    DECLARE @max_cancel INT = COALESCE((SELECT MAX(cancellation_reason_key) FROM gold.dim_cancellation_reason), 0);
    IF @max_cancel < 0 SET @max_cancel = 0;

    INSERT INTO gold.dim_cancellation_reason (cancellation_reason_key, cancellation_reason_name)
    SELECT 
        @max_cancel + ROW_NUMBER() OVER (ORDER BY s.cancellation_reason) AS cancellation_reason_key,
        s.cancellation_reason
    FROM (
        SELECT DISTINCT UPPER(TRIM(cancellation_reason)) AS cancellation_reason
        FROM silver.policy_cancellation
        WHERE cancellation_reason IS NOT NULL AND TRIM(cancellation_reason) != ''
    ) s
    LEFT JOIN gold.dim_cancellation_reason t ON t.cancellation_reason_name = s.cancellation_reason 
    WHERE t.cancellation_reason_name IS NULL;

    SET @inserted = @@ROWCOUNT;
    SET @total_inserted += @inserted;

    -- Set output parameters
    SET @rows_read = 0;
    SET @rows_inserted = @total_inserted;
    SET @new_watermark = NULL;

    -- Select result set for Fabric pipeline
    SELECT 
        @rows_read AS rows_read, 
        @rows_inserted AS rows_inserted, 
        @new_watermark AS watermark_to;
END;