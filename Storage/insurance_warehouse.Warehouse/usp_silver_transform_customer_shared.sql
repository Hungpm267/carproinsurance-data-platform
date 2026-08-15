CREATE OR ALTER PROCEDURE silver.usp_load_crm_customer
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

    ;WITH src AS (
        SELECT *
        FROM insurance_lakehouse.bronze.crm_customer
        WHERE @incremental = 0 OR updated_at > @last_watermark_dt
    ),
    deduped AS (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY updated_at DESC) AS _rn
        FROM src
    ),
    cleansed AS (
        SELECT
            UPPER(TRIM(CAST(customer_id AS VARCHAR(100)))) AS customer_id,

            (SELECT STRING_AGG(UPPER(LEFT(w.value,1)) + LOWER(SUBSTRING(w.value,2,4000)), ' ')
                    WITHIN GROUP (ORDER BY w.ordinal)
             FROM STRING_SPLIT(LOWER(LTRIM(RTRIM(
                 REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                     ISNULL(CAST(full_name AS VARCHAR(4000)), ''),
                 '  ',' '),'  ',' '),'  ',' '),'  ',' '),'  ',' ')
             ))), ' ', 1) w WHERE w.value <> '') AS full_name,

            CASE
                WHEN UPPER(TRIM(CAST(gender AS VARCHAR(100)))) IN ('M', 'MALE', 'NAM')         THEN 'Male'
                WHEN UPPER(TRIM(CAST(gender AS VARCHAR(100)))) IN ('F', 'FEMALE', N'NỮ', 'NU') THEN 'Female'
                ELSE 'Unknown'
            END AS gender,

            CASE
                WHEN TRY_CAST(dob AS DATE) IS NULL                   THEN NULL
                WHEN TRY_CAST(dob AS DATE) < '1900-01-01'            THEN NULL
                WHEN TRY_CAST(dob AS DATE) > CAST(GETDATE() AS DATE) THEN NULL
                ELSE TRY_CAST(dob AS DATE)
            END AS date_of_birth,

            CASE
                WHEN TRY_CAST(dob AS DATE) IS NULL                   THEN NULL
                WHEN TRY_CAST(dob AS DATE) < '1900-01-01'            THEN NULL
                WHEN TRY_CAST(dob AS DATE) > CAST(GETDATE() AS DATE) THEN NULL
                ELSE FLOOR(DATEDIFF(MONTH, TRY_CAST(dob AS DATE), GETDATE()) / 12.0)
            END AS age,

            TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                ISNULL(CAST(phone_number AS VARCHAR(50)), ''),
            ' ',''), '-',''), '(',''), ')',''), '.','')) AS phone_number,

            CASE
                WHEN LOWER(TRIM(CAST(email AS VARCHAR(4000)))) LIKE '%_@_%_.__%'
                 AND LOWER(TRIM(CAST(email AS VARCHAR(4000)))) NOT LIKE '%@%@%'
                    THEN LOWER(TRIM(CAST(email AS VARCHAR(4000))))
                ELSE NULL
            END AS email,

            (SELECT STRING_AGG(UPPER(LEFT(w.value,1)) + LOWER(SUBSTRING(w.value,2,4000)), ' ')
                    WITHIN GROUP (ORDER BY w.ordinal)
             FROM STRING_SPLIT(LOWER(LTRIM(RTRIM(ISNULL(CAST(city AS VARCHAR(4000)), '')))), ' ', 1) w
             WHERE w.value <> '') AS city,

            (SELECT STRING_AGG(UPPER(LEFT(w.value,1)) + LOWER(SUBSTRING(w.value,2,4000)), ' ')
                    WITHIN GROUP (ORDER BY w.ordinal)
             FROM STRING_SPLIT(LOWER(LTRIM(RTRIM(ISNULL(CAST(district AS VARCHAR(4000)), '')))), ' ', 1) w
             WHERE w.value <> '') AS district,

            TRY_CAST(created_date AS DATE) AS created_date,
            TRY_CAST(created_date AS DATE) AS customer_since_date,
            updated_at
        FROM deduped
        WHERE _rn = 1
          AND customer_id IS NOT NULL
          AND TRIM(CAST(customer_id AS VARCHAR(100))) <> ''
          AND UPPER(TRIM(CAST(customer_id AS VARCHAR(100)))) <> 'UNKNOWN'
    )
    SELECT
        customer_id, full_name, gender, date_of_birth, age,
        phone_number, email, city, district, created_date, customer_since_date, updated_at,
        CONVERT(VARCHAR(64), HASHBYTES('SHA2_256',
            CONCAT(
                COALESCE(customer_id,                        ''), '||',
                COALESCE(full_name,                          ''), '||',
                COALESCE(gender,                             ''), '||',
                COALESCE(CAST(date_of_birth AS VARCHAR(10)), ''), '||',
                COALESCE(phone_number,                       ''), '||',
                COALESCE(email,                              ''), '||',
                COALESCE(city,                               ''), '||',
                COALESCE(district,                           '')
            )
        ), 2) AS row_hash
    INTO #cleansed
    FROM cleansed;

    SET @rows_read = (
        SELECT COUNT(*) FROM insurance_lakehouse.bronze.crm_customer
        WHERE @incremental = 0 OR updated_at > @last_watermark_dt
    );

    SET @new_watermark = NULL;
    IF @watermark_column IS NOT NULL
        SET @new_watermark = (SELECT CONVERT(VARCHAR(33), MAX(updated_at), 126) FROM #cleansed);

    SET @rows_inserted = 0;

    IF EXISTS (SELECT 1 FROM #cleansed)
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = 'silver' AND t.name = 'crm_customer'
        )
        BEGIN
            SELECT * INTO silver.crm_customer FROM #cleansed;
            SET @rows_inserted = @@ROWCOUNT;
        END
        ELSE
        BEGIN
            MERGE INTO silver.crm_customer AS tgt
            USING #cleansed AS src
            ON tgt.customer_id = src.customer_id
            WHEN MATCHED AND src.row_hash <> tgt.row_hash THEN UPDATE SET
                tgt.full_name           = src.full_name,
                tgt.gender              = src.gender,
                tgt.date_of_birth       = src.date_of_birth,
                tgt.age                 = src.age,
                tgt.phone_number        = src.phone_number,
                tgt.email               = src.email,
                tgt.city                = src.city,
                tgt.district            = src.district,
                tgt.created_date        = src.created_date,
                tgt.customer_since_date = src.customer_since_date,
                tgt.updated_at          = CASE WHEN @watermark_column IS NOT NULL THEN src.updated_at ELSE tgt.updated_at END,
                tgt.row_hash            = src.row_hash
            WHEN NOT MATCHED THEN INSERT (
                customer_id, full_name, gender, date_of_birth, age,
                phone_number, email, city, district, created_date, customer_since_date, updated_at, row_hash
            )
            VALUES (
                src.customer_id, src.full_name, src.gender, src.date_of_birth, src.age,
                src.phone_number, src.email, src.city, src.district, src.created_date, src.customer_since_date, src.updated_at, src.row_hash
            );
            SET @rows_inserted = @@ROWCOUNT;
        END

        IF @new_watermark IS NOT NULL AND @controller_id IS NOT NULL
        BEGIN
            UPDATE meta.etl_transform_config
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
GO