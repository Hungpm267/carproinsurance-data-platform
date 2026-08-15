CREATE   PROCEDURE silver.usp_load_policy_policy
    @pipeline_run_id   VARCHAR(100) = NULL,
    @last_watermark    VARCHAR(100) = NULL,
    @watermark_column  VARCHAR(100) = NULL,
    @controller_id     VARCHAR(100) = NULL,
    @rows_read         BIGINT = 0 OUTPUT,
    @rows_inserted     BIGINT = 0 OUTPUT,
    @new_watermark     VARCHAR(100) = NULL OUTPUT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @last_watermark_dt DATETIME2(6) = TRY_CAST(@last_watermark AS DATETIME2(6));
    DECLARE @incremental BIT = CASE WHEN @watermark_column IS NOT NULL AND @last_watermark_dt IS NOT NULL THEN 1 ELSE 0 END;

    ;WITH src AS (
        SELECT *
        FROM insurance_lakehouse.bronze.policy_policy
        WHERE @incremental = 0 OR last_updated > @last_watermark_dt
    ),
    deduped AS (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY policy_id ORDER BY last_updated DESC) AS _rn
        FROM src
    ),
    cleansed AS (
        SELECT
            UPPER(TRIM(CAST(policy_id        AS VARCHAR(100))))  AS policy_id,
            UPPER(TRIM(CAST(quotation_id     AS VARCHAR(100))))  AS quotation_id,
            UPPER(TRIM(CAST(customer_id      AS VARCHAR(100))))  AS customer_id,
            UPPER(TRIM(CAST(provider_code    AS VARCHAR(100))))  AS provider_code,
            TRIM(CAST(policy_number          AS VARCHAR(100)))   AS policy_number,
            TRY_CAST(policy_start_date       AS DATE)            AS policy_start_date,
            TRY_CAST(policy_end_date         AS DATE)            AS policy_end_date,
            TRY_CAST(
                REPLACE(CAST(issued_date AS VARCHAR(50)), 'T', ' ')
            AS DATETIME2(6))                                     AS issued_date,
            TRY_CAST(premium_amount          AS DECIMAL(18,2))   AS premium_amount,
            UPPER(TRIM(CAST(policy_status    AS VARCHAR(100))))  AS policy_status,
            TRY_CAST(
                REPLACE(CAST(last_updated AS VARCHAR(50)), 'T', ' ')
            AS DATETIME2(6))                                     AS last_updated,
            TRIM(CAST(operation_type         AS VARCHAR(50)))    AS operation_type,
            TRY_CAST(batch_date              AS DATE)            AS batch_date,
            TRIM(CAST(source_system          AS VARCHAR(100)))   AS source_system
        FROM deduped
        WHERE _rn = 1
          AND policy_id IS NOT NULL
          AND TRIM(CAST(policy_id AS VARCHAR(100))) <> ''
          AND TRY_CAST(premium_amount AS DECIMAL(18,2)) > 0
    )
    SELECT *
    INTO #cleansed
    FROM cleansed;

    SET @rows_read = (
        SELECT COUNT(*) FROM insurance_lakehouse.bronze.policy_policy
        WHERE @incremental = 0 OR last_updated > @last_watermark_dt
    );

    SET @new_watermark = NULL;
    IF @watermark_column IS NOT NULL
        SET @new_watermark = (SELECT CONVERT(VARCHAR(33), MAX(last_updated), 126) FROM #cleansed);

    SET @rows_inserted = 0;

    IF EXISTS (SELECT 1 FROM #cleansed)
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = 'silver' AND t.name = 'policy_policy'
        )
        BEGIN
            SELECT * INTO silver.policy_policy FROM #cleansed;
        END
        ELSE
        BEGIN
            MERGE INTO silver.policy_policy AS tgt
            USING #cleansed AS src
            ON tgt.policy_id = src.policy_id
            WHEN MATCHED THEN UPDATE SET
                tgt.quotation_id      = src.quotation_id,
                tgt.customer_id       = src.customer_id,
                tgt.provider_code     = src.provider_code,
                tgt.policy_number     = src.policy_number,
                tgt.policy_start_date = src.policy_start_date,
                tgt.policy_end_date   = src.policy_end_date,
                tgt.issued_date       = src.issued_date,
                tgt.premium_amount    = src.premium_amount,
                tgt.policy_status     = src.policy_status,
                tgt.last_updated      = CASE WHEN @watermark_column IS NOT NULL THEN src.last_updated ELSE tgt.last_updated END,
                tgt.operation_type    = src.operation_type,
                tgt.batch_date        = src.batch_date,
                tgt.source_system     = src.source_system
            WHEN NOT MATCHED THEN INSERT (
                policy_id, quotation_id, customer_id, provider_code, policy_number,
                policy_start_date, policy_end_date, issued_date, premium_amount,
                policy_status, last_updated, operation_type, batch_date, source_system
            )
            VALUES (
                src.policy_id, src.quotation_id, src.customer_id, src.provider_code, src.policy_number,
                src.policy_start_date, src.policy_end_date, src.issued_date, src.premium_amount,
                src.policy_status, src.last_updated, src.operation_type, src.batch_date, src.source_system
            );
        END

        IF @new_watermark IS NOT NULL AND @controller_id IS NOT NULL
        BEGIN
            UPDATE insurance_warehouse.meta.etl_transform_config
            SET    last_watermark = @new_watermark
            WHERE  transform_config_id = TRY_CAST(@controller_id AS INT);

            SET @rows_inserted = (SELECT COUNT(*) FROM #cleansed);
        END
    END

    DROP TABLE IF EXISTS #cleansed;

    SELECT
        @rows_read     AS rows_read,
        @rows_inserted AS rows_inserted,
        @new_watermark AS watermark_to;
END;