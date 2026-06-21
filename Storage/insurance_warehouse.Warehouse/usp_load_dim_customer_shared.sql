CREATE OR ALTER PROCEDURE gold.usp_load_dim_customer
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
    SET @rows_read = (SELECT COUNT(*) FROM silver.crm_customer);

    -- Auto-generate Surrogate Keys logic
    DECLARE @max_key INT = COALESCE((SELECT MAX(customer_key) FROM gold.dim_customer), 0);
    IF @max_key < 0 SET @max_key = 0;

    -- Begin Transaction
    BEGIN TRANSACTION;

    -- 1. Expire changed records in Gold
    UPDATE gold.dim_customer
    SET is_current = 0,
        expiry_date = CAST(GETDATE() AS DATE)
    WHERE customer_id IN (
        SELECT t.customer_id
        FROM gold.dim_customer t
        INNER JOIN silver.crm_customer s ON t.customer_id = s.customer_id 
        WHERE t.is_current = 1 AND t.row_hash != s.row_hash
    )
      AND is_current = 1;

    -- 2. Insert new/updated versions into Gold
    ;WITH new_inserts AS (
        SELECT 
            s.customer_id,
            s.full_name,
            s.gender,
            s.date_of_birth,
            s.age,
            s.phone_number,
            s.email,
            s.city,
            s.district,
            s.customer_since_date,
            s.row_hash,
            CAST(CASE WHEN t.customer_id IS NULL THEN s.created_date ELSE s.updated_at END AS DATE) AS effective_date
        FROM silver.crm_customer s
        LEFT JOIN gold.dim_customer t ON t.customer_id = s.customer_id  AND t.is_current = 1
        WHERE t.customer_id IS NULL OR t.row_hash != s.row_hash
    ),
    new_inserts_with_keys AS (
        SELECT 
            @max_key + ROW_NUMBER() OVER (ORDER BY n.customer_id) AS customer_key,
            n.customer_id,
            n.full_name,
            n.gender,
            n.date_of_birth,
            n.age,
            n.phone_number,
            n.email,
            n.city,
            n.district,
            n.customer_since_date,
            n.effective_date,
            n.row_hash
        FROM new_inserts n
    )
    INSERT INTO gold.dim_customer (
        customer_key, customer_id, full_name, gender, date_of_birth, age,
        phone_number, email, city, district, customer_since_date,
        effective_date, expiry_date, is_current, row_hash
    )
    SELECT 
        customer_key, customer_id, full_name, gender, date_of_birth, age,
        phone_number, email, city, district, customer_since_date,
        effective_date, CAST('9999-12-31' AS DATE), 1, row_hash
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

