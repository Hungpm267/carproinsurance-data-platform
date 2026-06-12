CREATE PROCEDURE sp_init_warehouse_schemas
AS
BEGIN
    -- ==========================================================================================
    -- A. CREATE SCHEMA BRONZE / SILVER / GOLD
    -- ==========================================================================================
    IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'bronze')
        EXEC('CREATE SCHEMA bronze');

    IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'silver')
        EXEC('CREATE SCHEMA silver');

    IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'gold')
        EXEC('CREATE SCHEMA gold');

    -- ==========================================================================================
    -- B. BRONZE LAYER TABLES
    -- ==========================================================================================
    
    -- 1. Table bronze.crm_agent
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'bronze.crm_agent') AND type in (N'U'))
    CREATE TABLE bronze.crm_agent (
        agent_id VARCHAR(50) NOT NULL, -- PK | Source: agent_id VARCHAR(20)
        agent_name VARCHAR(255),       -- Source: agent_name NVARCHAR(200)
        region VARCHAR(100),           -- Source: region NVARCHAR(100)
        branch VARCHAR(100),           -- Source: branch NVARCHAR(100)
        manager_name VARCHAR(255),     -- Source: manager_name NVARCHAR(200)
        created_date DATETIME2,        -- Source: created_date DATETIME
        updated_at DATETIME2           -- Source: updated_at DATETIME
    );

    -- 2. Table bronze.crm_customer
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'bronze.crm_customer') AND type in (N'U'))
    CREATE TABLE bronze.crm_customer (
        customer_id VARCHAR(50) NOT NULL, -- PK | Source: customer_id VARCHAR(20)
        full_name VARCHAR(255),           -- Source: full_name NVARCHAR(200)
        gender VARCHAR(20),               -- Source: gender VARCHAR(10)
        dob DATE,                         -- Source: dob DATE — raw value; non-standard M/d/yyyy
        phone_number VARCHAR(50),         -- Source: phone_number VARCHAR(20)
        email VARCHAR(255),               -- Source: email VARCHAR(200)
        city VARCHAR(150),                -- Source: city VARCHAR(100)
        district VARCHAR(150),            -- Source: district NVARCHAR(100)
        created_date DATETIME2,           -- Source: created_date DATETIME
        updated_at DATETIME2              -- Source: updated_at DATETIME
    );

    -- 3. Table bronze.crm_insurance_provider
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'bronze.crm_insurance_provider') AND type in (N'U'))
    CREATE TABLE bronze.crm_insurance_provider (
        provider_code VARCHAR(50) NOT NULL, -- PK | Source: provider_code VARCHAR(20)
        provider_name VARCHAR(255),         -- Source: provider_name NVARCHAR(200)
        provider_group VARCHAR(150),        -- Source: provider_group NVARCHAR(100)
        active_flag INT,                    -- Source: active_flag INT
        created_date DATETIME2,             -- Source: created_date DATETIME
        updated_at DATETIME2                -- Source: updated_at DATETIME
    );

    -- 4. Table bronze.crm_quotation
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'bronze.crm_quotation') AND type in (N'U'))
    CREATE TABLE bronze.crm_quotation (
        quotation_id VARCHAR(50) NOT NULL,    -- PK | Source: quotation_id VARCHAR(20)
        customer_id VARCHAR(50) NOT NULL,     -- FK -> crm_customer | Source: customer_id VARCHAR(20)
        agent_id VARCHAR(50) NOT NULL,        -- FK -> crm_agent | Source: agent_id VARCHAR(20)
        provider_code VARCHAR(50),            -- FK -> crm_insurance_provider | Source: provider_code VARCHAR(20)
        quotation_date DATETIME2,             -- Source: quotation_date DATETIME
        quotation_status VARCHAR(100),        -- Source: quotation_status VARCHAR(50)
        package_code VARCHAR(100),            -- Source: package_code VARCHAR(50)
        premium_amount DECIMAL(18, 2),        -- Source: premium_amount DECIMAL(18,2)
        quotation_expiry_date VARCHAR(100),   -- Source: quotation_expiry_date DATETIME — stored as STRING; format varies
        created_date DATETIME2,               -- Source: created_date DATETIME
        updated_at DATETIME2                  -- Source: updated_at DATETIME
    );

    -- 5. Table bronze.crm_quotation_item
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'bronze.crm_quotation_item') AND type in (N'U'))
    CREATE TABLE bronze.crm_quotation_item (
        quotation_item_id VARCHAR(50) NOT NULL, -- PK | Source: quotation_item_id VARCHAR(20)
        quotation_id VARCHAR(50) NOT NULL,      -- FK -> crm_quotation | Source: quotation_id VARCHAR(20)
        coverage_type VARCHAR(150),             -- Source: coverage_type NVARCHAR(100)
        coverage_amount DECIMAL(18, 2),         -- Source: coverage_amount DECIMAL(18,2)
        deductible_amount DECIMAL(18, 2),       -- Source: deductible_amount DECIMAL(18,2)
        created_date DATETIME2,                 -- Source: created_date DATETIME
        updated_at DATETIME2                    -- Source: updated_at DATETIME
    );

    -- 6. Table bronze.crm_vehicle
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'bronze.crm_vehicle') AND type in (N'U'))
    CREATE TABLE bronze.crm_vehicle (
        vehicle_id VARCHAR(50) NOT NULL,    -- PK | Source: vehicle_id varchar(20)
        customer_id VARCHAR(50) NOT NULL,   -- FK -> crm_customer | Source: customer_id varchar(20)
        plate_number VARCHAR(50),           -- Source: plate_number varchar(20)
        vehicle_brand VARCHAR(150),         -- Source: vehicle_brand nvarchar(100)
        vehicle_model VARCHAR(150),         -- Source: vehicle_model nvarchar(100)
        manufacture_year INT,               -- Source: manufacture_year int
        vehicle_value DECIMAL(18, 2),       -- Source: vehicle_value decimal(18,2)
        created_date DATETIME2,             -- Source: created_date DATETIME
        updated_at DATETIME2                -- Source: updated_at DATETIME
    );

    -- 7. Table bronze.policy_policy
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'bronze.policy_policy') AND type in (N'U'))
    CREATE TABLE bronze.policy_policy (
        policy_id VARCHAR(50) NOT NULL,      -- PK | Source JSON field: policy_id
        quotation_id VARCHAR(50) NOT NULL,   -- FK -> crm_quotation | Source JSON field: quotation_id
        customer_id VARCHAR(50) NOT NULL,    -- FK -> crm_customer | Source JSON field: customer_id
        provider_code VARCHAR(50) NOT NULL,  -- FK -> crm_insurance_provider | Source JSON field: provider_code
        policy_number VARCHAR(100),          -- Source JSON field: policy_number
        policy_start_date DATE,              -- Source JSON field: policy_start_date
        policy_end_date DATE,                -- Source JSON field: policy_end_date
        policy_status VARCHAR(100),          -- Source JSON field: policy_status
        premium_amount DECIMAL(18, 2),       -- Source JSON field: premium_amount
        issued_date DATETIME2,               -- Source JSON field: issued_date (ISO 8601 — T separator)
        last_updated DATETIME2,              -- Source JSON field: last_updated (ISO 8601 — T separator)
        operation_type VARCHAR(50),          -- Source JSON field: operation_type (CDC marker)
        batch_date DATE,                     -- Source JSON field: batch_date (file partition date)
        source_system VARCHAR(100)           -- Source JSON field: source_system
    );

    -- 8. Table bronze.policy_payment
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'bronze.policy_payment') AND type in (N'U'))
    CREATE TABLE bronze.policy_payment (
        payment_id VARCHAR(50) NOT NULL,       -- PK | Source JSON field: payment_id
        policy_id VARCHAR(50) NOT NULL,        -- FK -> policy_policy | Source JSON field: policy_id
        payment_date DATETIME2,                -- Source JSON field: payment_date
        payment_method VARCHAR(100),           -- Source JSON field: payment_method
        payment_status VARCHAR(100),           -- Source JSON field: payment_status
        payment_amount DECIMAL(18, 2),         -- Source JSON field: payment_amount
        transaction_reference VARCHAR(255),    -- Source JSON field: transaction_reference
        last_updated DATETIME2,                -- Source JSON field: last_updated
        operation_type VARCHAR(50),            -- Source JSON field: operation_type (CDC marker)
        batch_date DATE,                       -- Source JSON field: batch_date
        source_system VARCHAR(100)             -- Source JSON field: source_system
    );

    -- 9. Table bronze.policy_cancellation
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'bronze.policy_cancellation') AND type in (N'U'))
    CREATE TABLE bronze.policy_cancellation (
        cancellation_id VARCHAR(50) NOT NULL,  -- PK | Source JSON field: cancellation_id
        policy_id VARCHAR(50) NOT NULL,        -- FK -> policy_policy | Source JSON field: policy_id
        cancellation_date DATETIME2,           -- Source JSON field: cancellation_date
        cancellation_reason VARCHAR(MAX),      -- Source JSON field: cancellation_reason
        refund_amount DECIMAL(18, 2),          -- Source JSON field: refund_amount
        last_updated DATETIME2,                -- Source JSON field: last_updated
        operation_type VARCHAR(50),            -- Source JSON field: operation_type (CDC marker)
        batch_date DATE,                       -- Source JSON field: batch_date
        source_system VARCHAR(100)             -- Source JSON field: source_system
    );


    -- ==========================================================================================
    -- C. SILVER LAYER TABLES
    -- ==========================================================================================
    
    -- 1. Table silver.crm_agent
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'silver.crm_agent') AND type in (N'U'))
    CREATE TABLE silver.crm_agent (
        agent_id VARCHAR(50) NOT NULL,
        agent_name VARCHAR(255),
        region VARCHAR(100),
        branch VARCHAR(100),
        manager_name VARCHAR(255),
        created_date DATETIME2,
        updated_at DATETIME2
    );

    -- 2. Table silver.crm_customer
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'silver.crm_customer') AND type in (N'U'))
    CREATE TABLE silver.crm_customer (
        customer_id VARCHAR(50) NOT NULL,
        full_name VARCHAR(255),
        gender VARCHAR(20),
        dob DATE,
        phone_number VARCHAR(50),
        email VARCHAR(255),
        city VARCHAR(150),
        district VARCHAR(150),
        created_date DATETIME2,
        updated_at DATETIME2
    );

    -- 3. Table silver.crm_insurance_provider
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'silver.crm_insurance_provider') AND type in (N'U'))
    CREATE TABLE silver.crm_insurance_provider (
        provider_code VARCHAR(50) NOT NULL,
        provider_name VARCHAR(255),
        provider_group VARCHAR(150),
        active_flag INT,
        created_date DATETIME2,
        updated_at DATETIME2
    );

    -- 4. Table silver.crm_quotation
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'silver.crm_quotation') AND type in (N'U'))
    CREATE TABLE silver.crm_quotation (
        quotation_id VARCHAR(50) NOT NULL,
        customer_id VARCHAR(50) NOT NULL,
        agent_id VARCHAR(50) NOT NULL,
        provider_code VARCHAR(50),
        quotation_date DATETIME2,
        quotation_status VARCHAR(100),
        package_code VARCHAR(100),
        premium_amount DECIMAL(18, 2),
        quotation_expiry_date VARCHAR(100),
        created_date DATETIME2,
        updated_at DATETIME2
    );

    -- 5. Table silver.crm_quotation_item
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'silver.crm_quotation_item') AND type in (N'U'))
    CREATE TABLE silver.crm_quotation_item (
        quotation_item_id VARCHAR(50) NOT NULL,
        quotation_id VARCHAR(50) NOT NULL,
        coverage_type VARCHAR(150),
        coverage_amount DECIMAL(18, 2),
        deductible_amount DECIMAL(18, 2),
        created_date DATETIME2,
        updated_at DATETIME2
    );

    -- 6. Table silver.crm_vehicle
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'silver.crm_vehicle') AND type in (N'U'))
    CREATE TABLE silver.crm_vehicle (
        vehicle_id VARCHAR(50) NOT NULL,
        customer_id VARCHAR(50) NOT NULL,
        plate_number VARCHAR(50),
        vehicle_brand VARCHAR(150),
        vehicle_model VARCHAR(150),
        manufacture_year INT,
        vehicle_value DECIMAL(18, 2),
        created_date DATETIME2,
        updated_at DATETIME2
    );

    -- 7. Table silver.policy_policy
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'silver.policy_policy') AND type in (N'U'))
    CREATE TABLE silver.policy_policy (
        policy_id VARCHAR(50) NOT NULL,
        quotation_id VARCHAR(50) NOT NULL,
        customer_id VARCHAR(50) NOT NULL,
        provider_code VARCHAR(50) NOT NULL,
        policy_number VARCHAR(100),
        policy_start_date DATE,
        policy_end_date DATE,
        policy_status VARCHAR(100),
        premium_amount DECIMAL(18, 2),
        issued_date DATETIME2,
        last_updated DATETIME2,
        operation_type VARCHAR(50),
        batch_date DATE,
        source_system VARCHAR(100)
    );

    -- 8. Table silver.policy_payment
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'silver.policy_payment') AND type in (N'U'))
    CREATE TABLE silver.policy_payment (
        payment_id VARCHAR(50) NOT NULL,
        policy_id VARCHAR(50) NOT NULL,
        payment_date DATETIME2,
        payment_method VARCHAR(100),
        payment_status VARCHAR(100),
        payment_amount DECIMAL(18, 2),
        transaction_reference VARCHAR(255),
        last_updated DATETIME2,
        operation_type VARCHAR(50),
        batch_date DATE,
        source_system VARCHAR(100)
    );

    -- 9. Table silver.policy_cancellation
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'silver.policy_cancellation') AND type in (N'U'))
    CREATE TABLE silver.policy_cancellation (
        cancellation_id VARCHAR(50) NOT NULL,
        policy_id VARCHAR(50) NOT NULL,
        cancellation_date DATETIME2,
        cancellation_reason VARCHAR(MAX),
        refund_amount DECIMAL(18, 2),
        last_updated DATETIME2,
        operation_type VARCHAR(50),
        batch_date DATE,
        source_system VARCHAR(100)
    );


    -- ==========================================================================================
    -- D. GOLD LAYER TABLES — DIMENSIONS
    -- ==========================================================================================
    
    -- 1. Table gold.dim_date (Re-create logic)
    IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'gold.dim_date') AND type in (N'U'))
        DROP TABLE gold.dim_date;

    CREATE TABLE gold.dim_date (
        date_key INT NOT NULL,
        full_date DATE NOT NULL,
        day_of_month INT NOT NULL,
        day_of_year INT NOT NULL,
        day_of_week INT NOT NULL,
        day_name VARCHAR(50) NOT NULL,
        day_name_short VARCHAR(20) NOT NULL,
        is_weekend BIT NOT NULL,
        week_of_year INT NOT NULL,
        week_start_date DATE NOT NULL,
        week_end_date DATE NOT NULL,
        month_number INT NOT NULL,
        month_name VARCHAR(50) NOT NULL,
        month_name_short VARCHAR(20) NOT NULL,
        days_in_month INT NOT NULL,
        month_start_date DATE NOT NULL,
        month_end_date DATE NOT NULL,
        is_month_start BIT NOT NULL,
        is_month_end BIT NOT NULL,
        year_number INT NOT NULL,
        year_month VARCHAR(20) NOT NULL,
        year_month_sort_key INT NOT NULL,
        year_month_label VARCHAR(50) NOT NULL,
        year_month_short VARCHAR(20) NOT NULL,
        quarter_number INT NOT NULL,
        quarter_name VARCHAR(10) NOT NULL,
        quarter_year_label VARCHAR(50) NOT NULL,
        quarter_year_sort_key INT NOT NULL,
        fiscal_year INT NOT NULL,
        fiscal_quarter INT NOT NULL
    );

    -- Giải pháp thay thế: Tạo chuỗi ngày bằng CROSS JOIN (Không dùng MAXRECURSION)
    WITH 
    L0 AS (SELECT 1 AS c UNION ALL SELECT 1),
    L1 AS (SELECT 1 AS c FROM L0 AS a CROSS JOIN L0 AS b),
    L2 AS (SELECT 1 AS c FROM L1 AS a CROSS JOIN L1 AS b),
    L3 AS (SELECT 1 AS c FROM L2 AS a CROSS JOIN L2 AS b),
    L4 AS (SELECT 1 AS c FROM L3 AS a CROSS JOIN L3 AS b),
    Nums AS (SELECT ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS n FROM L4),
    DateSpine AS (
        SELECT DATEADD(DAY, n, CAST('2015-01-01' AS DATE)) AS d
        FROM Nums
        WHERE n <= DATEDIFF(DAY, '2015-01-01', '2035-12-31')
    )
    INSERT INTO gold.dim_date
    SELECT
        CAST(FORMAT(d, 'yyyyMMdd') AS INT) AS date_key,
        d AS full_date,
        DAY(d) AS day_of_month,
        DATEPART(DAYOFYEAR, d) AS day_of_year,
        DATEPART(WEEKDAY, d) AS day_of_week,
        DATENAME(WEEKDAY, d) AS day_name,
        SUBSTRING(DATENAME(WEEKDAY, d), 1, 3) AS day_name_short,
        CASE WHEN DATEPART(WEEKDAY, d) IN (1, 7) THEN 1 ELSE 0 END AS is_weekend,
        DATEPART(ISO_WEEK, d) AS week_of_year,
        DATEADD(DAY, 1 - DATEPART(WEEKDAY, DATEADD(DAY, -1, d)), d) AS week_start_date,
        DATEADD(DAY, 7 - DATEPART(WEEKDAY, DATEADD(DAY, -1, d)), d) AS week_end_date,
        MONTH(d) AS month_number,
        DATENAME(MONTH, d) AS month_name,
        SUBSTRING(DATENAME(MONTH, d), 1, 3) AS month_name_short,
        DAY(EOMONTH(d)) AS days_in_month,
        DATEFROMPARTS(YEAR(d), MONTH(d), 1) AS month_start_date,
        EOMONTH(d) AS month_end_date,
        CASE WHEN DAY(d) = 1 THEN 1 ELSE 0 END AS is_month_start,
        CASE WHEN d = EOMONTH(d) THEN 1 ELSE 0 END AS is_month_end,
        YEAR(d) AS year_number,
        FORMAT(d, 'yyyy-MM') AS year_month,
        CAST(FORMAT(d, 'yyyyMM') AS INT) AS year_month_sort_key,
        FORMAT(d, 'MMM yyyy') AS year_month_label,
        FORMAT(d, 'MMM yy') AS year_month_short,
        DATEPART(QUARTER, d) AS quarter_number,
        'Q' + CAST(DATEPART(QUARTER, d) AS VARCHAR(1)) AS quarter_name,
        'Q' + CAST(DATEPART(QUARTER, d) AS VARCHAR(1)) + ' ' + CAST(YEAR(d) AS VARCHAR(4)) AS quarter_year_label,
        CAST(CAST(YEAR(d) AS VARCHAR(4)) + CAST(DATEPART(QUARTER, d) AS VARCHAR(1)) AS INT) AS quarter_year_sort_key,
        YEAR(d) AS fiscal_year,
        DATEPART(QUARTER, d) AS fiscal_quarter
    FROM DateSpine;

    -- 2. Table gold.dim_customer
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'gold.dim_customer') AND type in (N'U'))
    CREATE TABLE gold.dim_customer (
        customer_key INT NOT NULL,
        customer_id VARCHAR(50) NOT NULL,
        full_name VARCHAR(255) NOT NULL,
        gender VARCHAR(20),
        date_of_birth DATE,
        age INT,
        phone_number VARCHAR(50),
        email VARCHAR(255),
        city VARCHAR(150),
        district VARCHAR(150),
        customer_since_date DATE NOT NULL,
        effective_date DATE NOT NULL,
        expiry_date DATE,
        is_current BIT NOT NULL,
        row_hash VARCHAR(64) NOT NULL
    );

    -- 3. Table gold.dim_agent
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'gold.dim_agent') AND type in (N'U'))
    CREATE TABLE gold.dim_agent (
        agent_key INT NOT NULL,
        agent_id VARCHAR(50) NOT NULL,
        agent_name VARCHAR(255) NOT NULL,
        region VARCHAR(100),
        branch VARCHAR(100),
        manager_name VARCHAR(255),
        effective_date DATE NOT NULL,
        expiry_date DATE,
        is_current BIT NOT NULL,
        row_hash VARCHAR(64) NOT NULL
    );

    -- 4. Table gold.dim_insurance_provider
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'gold.dim_insurance_provider') AND type in (N'U'))
    CREATE TABLE gold.dim_insurance_provider (
        provider_key INT NOT NULL,
        provider_code VARCHAR(50) NOT NULL,
        provider_name VARCHAR(255) NOT NULL,
        provider_group VARCHAR(150),
        is_active BIT NOT NULL
    );

    IF NOT EXISTS (SELECT 1 FROM gold.dim_insurance_provider WHERE provider_key = -1)
        INSERT INTO gold.dim_insurance_provider (provider_key, provider_code, provider_name, provider_group, is_active)
        VALUES (-1, 'UNKNOWN', 'Unknown', 'Unknown', 0);

    -- 5. Table gold.dim_vehicle
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'gold.dim_vehicle') AND type in (N'U'))
    CREATE TABLE gold.dim_vehicle (
        vehicle_key INT NOT NULL,
        vehicle_id VARCHAR(50) NOT NULL,
        customer_id VARCHAR(50) NOT NULL,
        plate_number VARCHAR(50),
        vehicle_brand VARCHAR(150),
        vehicle_model VARCHAR(150),
        manufacture_year INT,
        vehicle_age_years INT,
        vehicle_value DECIMAL(18, 2),
        vehicle_value_band VARCHAR(50)
    );

    IF NOT EXISTS (SELECT 1 FROM gold.dim_vehicle WHERE vehicle_key = -1)
        INSERT INTO gold.dim_vehicle (vehicle_key, vehicle_id, customer_id, plate_number, vehicle_brand, vehicle_model, manufacture_year, vehicle_value, vehicle_age_years, vehicle_value_band)
        VALUES (-1, 'UNKNOWN', 'UNKNOWN', 'Unknown', 'Unknown', 'Unknown', NULL, NULL, NULL, 'Unknown');

    -- 6. Table gold.dim_product_package
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'gold.dim_product_package') AND type in (N'U'))
    CREATE TABLE gold.dim_product_package (
        product_package_key INT NOT NULL,
        product_package_name VARCHAR(255) NOT NULL
    );

    IF NOT EXISTS (SELECT 1 FROM gold.dim_product_package WHERE product_package_key = -1)
        INSERT INTO gold.dim_product_package (product_package_key, product_package_name) VALUES (-1, 'UNKNOWN');

    -- 7. Table gold.dim_quotation_status
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'gold.dim_quotation_status') AND type in (N'U'))
    CREATE TABLE gold.dim_quotation_status (
        quotation_status_key INT NOT NULL,
        quotation_status_name VARCHAR(255) NOT NULL
    );

    IF NOT EXISTS (SELECT 1 FROM gold.dim_quotation_status WHERE quotation_status_key = -1)
        INSERT INTO gold.dim_quotation_status (quotation_status_key, quotation_status_name) VALUES (-1, 'UNKNOWN');

    -- 8. Table gold.dim_policy_status
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'gold.dim_policy_status') AND type in (N'U'))
    CREATE TABLE gold.dim_policy_status (
        policy_status_key INT NOT NULL,
        policy_status_name VARCHAR(255) NOT NULL
    );

    IF NOT EXISTS (SELECT 1 FROM gold.dim_policy_status WHERE policy_status_key = -1)
        INSERT INTO gold.dim_policy_status (policy_status_key, policy_status_name) VALUES (-1, 'UNKNOWN');

    -- 9. Table gold.dim_payment_status
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'gold.dim_payment_status') AND type in (N'U'))
    CREATE TABLE gold.dim_payment_status (
        payment_status_key INT NOT NULL,
        payment_status_name VARCHAR(255) NOT NULL
    );

    IF NOT EXISTS (SELECT 1 FROM gold.dim_payment_status WHERE payment_status_key = -1)
        INSERT INTO gold.dim_payment_status (payment_status_key, payment_status_name) VALUES (-1, 'UNKNOWN');

    -- 10. Table gold.dim_payment_method
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'gold.dim_payment_method') AND type in (N'U'))
    CREATE TABLE gold.dim_payment_method (
        payment_method_key INT NOT NULL,
        payment_method_name VARCHAR(255) NOT NULL
    );

    IF NOT EXISTS (SELECT 1 FROM gold.dim_payment_method WHERE payment_method_key = -1)
        INSERT INTO gold.dim_payment_method (payment_method_key, payment_method_name) VALUES (-1, 'UNKNOWN');

    -- 11. Table gold.dim_coverage_type
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'gold.dim_coverage_type') AND type in (N'U'))
    CREATE TABLE gold.dim_coverage_type (
        coverage_type_key INT NOT NULL,
        coverage_type_name VARCHAR(255) NOT NULL
    );

    IF NOT EXISTS (SELECT 1 FROM gold.dim_coverage_type WHERE coverage_type_key = -1)
        INSERT INTO gold.dim_coverage_type (coverage_type_key, coverage_type_name) VALUES (-1, 'UNKNOWN');

    -- 12. Table gold.dim_cancellation_reason
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'gold.dim_cancellation_reason') AND type in (N'U'))
    CREATE TABLE gold.dim_cancellation_reason (
        cancellation_reason_key INT NOT NULL,
        cancellation_reason_name VARCHAR(255) NOT NULL
    );

    IF NOT EXISTS (SELECT 1 FROM gold.dim_cancellation_reason WHERE cancellation_reason_key = -1)
        INSERT INTO gold.dim_cancellation_reason (cancellation_reason_key, cancellation_reason_name) VALUES (-1, 'UNKNOWN');


    -- ==========================================================================================
    -- E. GOLD LAYER TABLES — FACTS
    -- ==========================================================================================
    
    -- 1. Table gold.fact_quotation
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'gold.fact_quotation') AND type in (N'U'))
    CREATE TABLE gold.fact_quotation (
        quotation_key INT NOT NULL,
        quotation_id VARCHAR(50) NOT NULL,
        quotation_date_key INT NOT NULL,
        quotation_expiry_date_key INT,
        customer_key INT NOT NULL,
        agent_key INT,
        provider_key INT NOT NULL,
        vehicle_key INT,
        product_package_key INT NOT NULL,
        quotation_status_key INT NOT NULL,
        quotation_premium_amount DECIMAL(18, 2) NOT NULL
    );

    -- 2. Table gold.fact_quotation_item
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'gold.fact_quotation_item') AND type in (N'U'))
    CREATE TABLE gold.fact_quotation_item (
        quotation_item_key INT NOT NULL,
        quotation_item_id VARCHAR(50) NOT NULL,
        coverage_type_key INT NOT NULL,
        coverage_amount DECIMAL(18, 2) NOT NULL,
        deductible_amount DECIMAL(18, 2)
    );

    -- 3. Table gold.fact_policy
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'gold.fact_policy') AND type in (N'U'))
    CREATE TABLE gold.fact_policy (
        policy_key INT NOT NULL,
        policy_id VARCHAR(50) NOT NULL,
        quotation_id VARCHAR(50),
        policy_number VARCHAR(100) NOT NULL,
        policy_start_date_key INT NOT NULL,
        policy_end_date_key INT,
        issued_date_key INT NOT NULL,
        customer_key INT NOT NULL,
        provider_key INT NOT NULL,
        policy_status_key INT NOT NULL,
        written_premium_amount DECIMAL(18, 2) NOT NULL,
        quoted_premium_amount DECIMAL(18, 2),
        premium_variance_amount DECIMAL(18, 2),
        is_in_force BIT NOT NULL,
        policy_term_days INT
    );

    -- 4. Table gold.fact_payment
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'gold.fact_payment') AND type in (N'U'))
    CREATE TABLE gold.fact_payment (
        payment_key INT NOT NULL,
        payment_id VARCHAR(50) NOT NULL,
        policy_id VARCHAR(50) NOT NULL,
        payment_date_key INT NOT NULL,
        payment_method_key INT NOT NULL,
        payment_status_key INT NOT NULL,
        payment_amount DECIMAL(18, 2) NOT NULL,
        is_successful_payment BIT NOT NULL
    );

    -- 5. Table gold.fact_cancellation
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'gold.fact_cancellation') AND type in (N'U'))
    CREATE TABLE gold.fact_cancellation (
        cancellation_key INT NOT NULL,
        cancellation_id VARCHAR(50) NOT NULL,
        policy_id VARCHAR(50) NOT NULL,
        cancellation_date_key INT NOT NULL,
        customer_key INT NOT NULL,
        provider_key INT NOT NULL,
        cancellation_reason_key INT,
        refund_amount DECIMAL(18, 2)
    );
END;