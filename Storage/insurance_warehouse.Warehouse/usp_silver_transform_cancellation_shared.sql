CREATE OR ALTER PROCEDURE silver.usp_load_policy_cancellation
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
        FROM insurance_lakehouse.bronze.policy_cancellation
        WHERE @incremental = 0 OR last_updated > @last_watermark_dt
    ),
    deduped AS (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY cancellation_id ORDER BY last_updated DESC) AS _rn
        FROM src
    ),
    cleansed AS (
        SELECT
            UPPER(TRIM(CAST(cancellation_id   AS VARCHAR(100))))  AS cancellation_id,
            UPPER(TRIM(CAST(policy_id         AS VARCHAR(100))))  AS policy_id,
            TRY_CAST(
                REPLACE(CAST(cancellation_date AS VARCHAR(50)), 'T', ' ')
            AS DATETIME2(6))                                      AS cancellation_date,
            TRIM(CAST(cancellation_reason     AS VARCHAR(4000)))  AS cancellation_reason,
            TRY_CAST(refund_amount            AS DECIMAL(18,2))   AS refund_amount,
            TRY_CAST(
                REPLACE(CAST(last_updated AS VARCHAR(50)), 'T', ' ')
            AS DATETIME2(6))                                      AS last_updated,
            TRIM(CAST(operation_type          AS VARCHAR(50)))    AS operation_type,
            TRY_CAST(batch_date               AS DATE)            AS batch_date,
            TRIM(CAST(source_system           AS VARCHAR(100)))   AS source_system
        FROM deduped
        WHERE _rn = 1
          AND cancellation_id IS NOT NULL
          AND TRIM(CAST(cancellation_id AS VARCHAR(100))) <> ''
          AND TRY_CAST(refund_amount AS DECIMAL(18,2)) >= 0
    )
    SELECT *
    INTO #cleansed
    FROM cleansed;

    SET @rows_read = (
        SELECT COUNT(*) FROM insurance_lakehouse.bronze.policy_cancellation
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
            WHERE s.name = 'silver' AND t.name = 'policy_cancellation'
        )
        BEGIN
            SELECT * INTO silver.policy_cancellation FROM #cleansed;
        END
        ELSE
        BEGIN
            MERGE INTO silver.policy_cancellation AS tgt
            USING #cleansed AS src
            ON tgt.cancellation_id = src.cancellation_id
            WHEN MATCHED THEN UPDATE SET
                tgt.policy_id           = src.policy_id,
                tgt.cancellation_date   = src.cancellation_date,
                tgt.cancellation_reason = src.cancellation_reason,
                tgt.refund_amount       = src.refund_amount,
                tgt.last_updated        = CASE WHEN @watermark_column IS NOT NULL THEN src.last_updated ELSE tgt.last_updated END,
                tgt.operation_type      = src.operation_type,
                tgt.batch_date          = src.batch_date,
                tgt.source_system       = src.source_system
            WHEN NOT MATCHED THEN INSERT (
                cancellation_id, policy_id, cancellation_date, cancellation_reason,
                refund_amount, last_updated, operation_type, batch_date, source_system
            )
            VALUES (
                src.cancellation_id, src.policy_id, src.cancellation_date, src.cancellation_reason,
                src.refund_amount, src.last_updated, src.operation_type, src.batch_date, src.source_system
            );
        END

        IF @new_watermark IS NOT NULL AND @controller_id IS NOT NULL
        BEGIN
            UPDATE insurance_lakehouse.meta.etl_transform_config
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
GO