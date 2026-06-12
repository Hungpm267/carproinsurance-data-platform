CREATE TABLE [bronze].[policy_policy] (

	[policy_id] varchar(50) NOT NULL, 
	[quotation_id] varchar(50) NOT NULL, 
	[customer_id] varchar(50) NOT NULL, 
	[provider_code] varchar(50) NOT NULL, 
	[policy_number] varchar(100) NULL, 
	[policy_start_date] date NULL, 
	[policy_end_date] date NULL, 
	[policy_status] varchar(100) NULL, 
	[premium_amount] decimal(18,2) NULL, 
	[issued_date] datetime2(6) NULL, 
	[last_updated] datetime2(6) NULL, 
	[operation_type] varchar(50) NULL, 
	[batch_date] date NULL, 
	[source_system] varchar(100) NULL
);