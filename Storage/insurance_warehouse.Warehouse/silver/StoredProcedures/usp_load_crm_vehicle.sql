CREATE   PROCEDURE silver.usp_load_crm_vehicle
    @pipeline_run_id   VARCHAR(100) = NULL,
    @last_watermark    VARCHAR(100) = NULL,
    @watermark_column  VARCHAR(100) = NULL,
    @rows_read         BIGINT = 0 OUTPUT,
    @rows_inserted     BIGINT = 0 OUTPUT,
    @new_watermark     VARCHAR(100) = NULL OUTPUT
AS
BEGIN
    SET NOCOUNT ON;

    -- @last_watermark NULL => full load; otherwise incremental by updated_at
    DECLARE @last_watermark_dt DATETIME2(6) = TRY_CAST(@last_watermark AS DATETIME2(6));

    -- 1. Incremental row count read from insurance_lakehouse.bronze
    SET @rows_read = (
        SELECT COUNT(*)
        FROM insurance_lakehouse.bronze.crm_vehicle
        WHERE @last_watermark_dt IS NULL OR updated_at > @last_watermark_dt
    );

    -- 2. Cleanse + validate + dedup by BK, keep latest record by updated_at
    ;WITH cleansed AS (
        SELECT
            UPPER(TRIM(vehicle_id))   AS vehicle_id,
            UPPER(TRIM(customer_id))  AS customer_id,
            UPPER(TRIM(plate_number)) AS plate_number,
            TRIM(vehicle_brand)       AS vehicle_brand,
            TRIM(vehicle_model)       AS vehicle_model,
            CASE
                WHEN TRY_CAST(manufacture_year AS INT) BETWEEN 1900 AND YEAR(GETDATE())
                    THEN TRY_CAST(manufacture_year AS INT)
                ELSE NULL
            END AS manufacture_year,
            CASE
                WHEN TRY_CAST(vehicle_value AS DECIMAL(18,2)) >= 0
                    THEN TRY_CAST(vehicle_value AS DECIMAL(18,2))
                ELSE NULL
            END AS vehicle_value,
            TRY_CAST(created_date AS DATE) AS created_date,
            updated_at,
            ROW_NUMBER() OVER (
                PARTITION BY UPPER(TRIM(vehicle_id))
                ORDER BY updated_at DESC
            ) AS rn
        FROM insurance_lakehouse.bronze.crm_vehicle
        WHERE @last_watermark_dt IS NULL OR updated_at > @last_watermark_dt
    )
    SELECT *
    INTO #cleansed
    FROM cleansed
    WHERE rn = 1;

    -- 3. Count new keys before merge (Fabric DW has no MERGE OUTPUT clause)
    SET @rows_inserted = (
        SELECT COUNT(*)
        FROM #cleansed s
        LEFT JOIN silver.crm_vehicle t ON t.vehicle_id = s.vehicle_id
        WHERE t.vehicle_id IS NULL
    );

    -- 4. Upsert into silver
    MERGE INTO silver.crm_vehicle AS tgt
    USING #cleansed AS src
    ON tgt.vehicle_id = src.vehicle_id

    WHEN MATCHED THEN UPDATE SET
        tgt.customer_id      = src.customer_id,
        tgt.plate_number     = src.plate_number,
        tgt.vehicle_brand    = src.vehicle_brand,
        tgt.vehicle_model    = src.vehicle_model,
        tgt.manufacture_year = src.manufacture_year,
        tgt.vehicle_value    = src.vehicle_value,
        tgt.created_date     = src.created_date,
        tgt.updated_at       = src.updated_at

    WHEN NOT MATCHED THEN INSERT (
        vehicle_id, customer_id, plate_number, vehicle_brand, vehicle_model,
        manufacture_year, vehicle_value, created_date, updated_at
    )
    VALUES (
        src.vehicle_id, src.customer_id, src.plate_number, src.vehicle_brand, src.vehicle_model,
        src.manufacture_year, src.vehicle_value, src.created_date, src.updated_at
    );

    -- 5. New watermark = max updated_at seen this run; if no rows, carry old value forward
    SET @new_watermark = (
        SELECT CONVERT(VARCHAR(33), MAX(updated_at), 126)
        FROM #cleansed
    );
    IF @new_watermark IS NULL
        SET @new_watermark = @last_watermark;

    DROP TABLE IF EXISTS #cleansed;

    -- Select result set for Fabric pipeline
    SELECT
        @rows_read AS rows_read,
        @rows_inserted AS rows_inserted,
        @new_watermark AS watermark_to;
END;