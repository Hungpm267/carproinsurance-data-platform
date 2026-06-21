CREATE   PROCEDURE dbo.usp_test_sync_lakehouse_to_warehouse
AS
BEGIN
    SET NOCOUNT ON;

    PRINT '--- BẮT ĐẦU ĐỒNG BỘ DỮ LIỆU TỪ LAKEHOUSE SANG WAREHOUSE ---';

    -- 1. Xóa sạch bảng đích ở Warehouse để kiểm tra kết quả nạp mới
    TRUNCATE TABLE bronze.crm_customer;

    -- 2. Đọc chéo (Cross-database query) từ Lakehouse ghi thẳng vào Warehouse
    INSERT INTO bronze.crm_customer (
        customer_id,
        full_name,
        gender,
        dob,
        phone_number,
        email,
        city,
        district,
        created_date,
        updated_at
    )
    SELECT 
        customer_id,
        full_name,
        gender,
        dob,
        phone_number,
        email,
        city,
        district,
        created_date,
        updated_at
    FROM [insurance_lakehouse].[bronze].[crm_customer]; -- Đọc trực tiếp từ SQL Endpoint của Lakehouse

    -- 3. Lấy số lượng dòng đã đồng bộ thành công
    DECLARE @inserted_rows INT = @@ROWCOUNT;

    PRINT '--- ĐỒNG BỘ THÀNH CÔNG ---';
    PRINT 'Số lượng bản ghi đã nạp vào Warehouse: ' + CAST(@inserted_rows AS VARCHAR(10));
    
    -- 4. Trả về kết quả để hiển thị trên lưới Grid của Fabric
    SELECT 
        @inserted_rows AS rows_synchronized,
        GETDATE() AS sync_timestamp;
END;