CREATE   PROCEDURE gold.usp_load_dim_vehicle
    @pipeline_run_id VARCHAR(100) = NULL,
    @last_watermark VARCHAR(100) = NULL,
    @watermark_column VARCHAR(100) = NULL,
    @rows_read BIGINT = 0 OUTPUT,
    @rows_inserted BIGINT = 0 OUTPUT,
    @new_watermark VARCHAR(100) = NULL OUTPUT
AS
BEGIN
    SET NOCOUNT ON;

    -- 1. Insert Unknown if not exists
    IF NOT EXISTS (SELECT 1 FROM gold.dim_vehicle WHERE vehicle_key = -1)
        INSERT INTO gold.dim_vehicle (vehicle_key, vehicle_id, customer_id, plate_number, vehicle_brand, vehicle_model, manufacture_year, vehicle_age_years, vehicle_value, vehicle_value_band)
        VALUES (-1, 'UNKNOWN', 'UNKNOWN', 'Unknown', 'Unknown', 'Unknown', NULL, NULL, NULL, 'Unknown');

    -- Calculate rows_read
    SET @rows_read = (SELECT COUNT(*) FROM silver.crm_vehicle);

    -- Calculate max key
    DECLARE @max_key INT = COALESCE((SELECT MAX(vehicle_key) FROM gold.dim_vehicle), 0);
    IF @max_key < 0 SET @max_key = 0;

    DECLARE @current_year INT = YEAR(GETDATE());

    -- Deduplicate source in CTE using row number to avoid merge duplicate errors
    WITH src_dedup AS (
        SELECT 
            vehicle_id,
            customer_id,
            plate_number,
            vehicle_brand,
            vehicle_model,
            manufacture_year,
            vehicle_value,
            ROW_NUMBER() OVER (PARTITION BY vehicle_id ORDER BY updated_at DESC) as rn
        FROM silver.crm_vehicle
    )
    SELECT * INTO #src_vehicle
    FROM src_dedup
    WHERE rn = 1;

    -- Calculate how many rows will be inserted
    SET @rows_inserted = (
        SELECT COUNT(*)
        FROM #src_vehicle s
        LEFT JOIN gold.dim_vehicle t ON t.vehicle_id = s.vehicle_id 
        WHERE t.vehicle_id IS NULL
    );

    -- Perform UPDATE & INSERT
    BEGIN TRANSACTION;

    -- Update existing records
    UPDATE tgt
    SET 
        tgt.customer_id = src.customer_id,
        tgt.plate_number = src.plate_number,
        tgt.vehicle_brand = src.vehicle_brand,
        tgt.vehicle_model = src.vehicle_model,
        tgt.manufacture_year = src.manufacture_year,
        tgt.vehicle_value = src.vehicle_value,
        tgt.vehicle_age_years = CAST((@current_year - src.manufacture_year) AS INT),
        tgt.vehicle_value_band = CASE 
            WHEN src.vehicle_value < 500000000 THEN '<500M'
            WHEN src.vehicle_value <= 1000000000 THEN '500M-1B'
            ELSE '>1B'
        END
    FROM gold.dim_vehicle tgt
    INNER JOIN #src_vehicle src ON tgt.vehicle_id = src.vehicle_id;

    -- Insert new records
    INSERT INTO gold.dim_vehicle (
        vehicle_key,
        vehicle_id,
        customer_id,
        plate_number,
        vehicle_brand,
        vehicle_model,
        manufacture_year,
        vehicle_age_years,
        vehicle_value,
        vehicle_value_band
    )
    SELECT 
        @max_key + ROW_NUMBER() OVER (ORDER BY src.vehicle_id) AS vehicle_key,
        src.vehicle_id,
        src.customer_id,
        src.plate_number,
        src.vehicle_brand,
        src.vehicle_model,
        src.manufacture_year,
        CAST((@current_year - src.manufacture_year) AS INT) AS vehicle_age_years,
        src.vehicle_value,
        CASE 
            WHEN src.vehicle_value < 500000000 THEN '<500M'
            WHEN src.vehicle_value <= 1000000000 THEN '500M-1B'
            ELSE '>1B'
        END AS vehicle_value_band
    FROM #src_vehicle src
    LEFT JOIN gold.dim_vehicle t ON t.vehicle_id = src.vehicle_id
    WHERE t.vehicle_id IS NULL;

    COMMIT TRANSACTION;

    DROP TABLE #src_vehicle;

    SET @new_watermark = NULL;

    -- Select result set for Fabric pipeline
    SELECT 
        @rows_read AS rows_read, 
        @rows_inserted AS rows_inserted, 
        @new_watermark AS watermark_to;
END;