CREATE   PROCEDURE silver.usp_load_crm_quotation_item
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
        FROM insurance_lakehouse.bronze.crm_quotation_item
        WHERE @incremental = 0 OR updated_at > @last_watermark_dt
    ),
    deduped AS (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY quotation_item_id ORDER BY updated_at DESC) AS _rn
        FROM src
    ),
    cleansed AS (
        SELECT
            UPPER(TRIM(CAST(quotation_item_id  AS VARCHAR(100))))  AS quotation_item_id,
            UPPER(TRIM(CAST(quotation_id       AS VARCHAR(100))))  AS quotation_id,
            TRIM(CAST(coverage_type            AS VARCHAR(4000)))  AS coverage_type,
            TRY_CAST(coverage_amount           AS DECIMAL(18,2))   AS coverage_amount,
            TRY_CAST(deductible_amount         AS DECIMAL(18,2))   AS deductible_amount,
            TRY_CAST(created_date              AS DATE)            AS created_date,
            updated_at
        FROM deduped
        WHERE _rn = 1
          AND quotation_item_id IS NOT NULL
          AND TRIM(CAST(quotation_item_id AS VARCHAR(100))) <> ''
          AND TRY_CAST(coverage_amount    AS DECIMAL(18,2)) > 0
          AND TRY_CAST(deductible_amount  AS DECIMAL(18,2)) >= 0
    )
    SELECT *
    INTO #cleansed
    FROM cleansed;

    SET @rows_read = (
        SELECT COUNT(*) FROM insurance_lakehouse.bronze.crm_quotation_item
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
            WHERE s.name = 'silver' AND t.name = 'crm_quotation_item'
        )
        BEGIN
            SELECT * INTO silver.crm_quotation_item FROM #cleansed;
        END
        ELSE
        BEGIN
            MERGE INTO silver.crm_quotation_item AS tgt
            USING #cleansed AS src
            ON tgt.quotation_item_id = src.quotation_item_id
            WHEN MATCHED THEN UPDATE SET
                tgt.quotation_id      = src.quotation_id,
                tgt.coverage_type     = src.coverage_type,
                tgt.coverage_amount   = src.coverage_amount,
                tgt.deductible_amount = src.deductible_amount,
                tgt.created_date      = src.created_date,
                tgt.updated_at        = CASE WHEN @watermark_column IS NOT NULL THEN src.updated_at ELSE tgt.updated_at END
            WHEN NOT MATCHED THEN INSERT (
                quotation_item_id, quotation_id, coverage_type,
                coverage_amount, deductible_amount, created_date, updated_at
            )
            VALUES (
                src.quotation_item_id, src.quotation_id, src.coverage_type,
                src.coverage_amount, src.deductible_amount, src.created_date, src.updated_at
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