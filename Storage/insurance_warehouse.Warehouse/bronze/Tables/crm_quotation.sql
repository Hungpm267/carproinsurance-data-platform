CREATE TABLE [bronze].[crm_quotation] (

	[quotation_id] varchar(50) NOT NULL, 
	[customer_id] varchar(50) NOT NULL, 
	[agent_id] varchar(50) NOT NULL, 
	[provider_code] varchar(50) NULL, 
	[quotation_date] datetime2(6) NULL, 
	[quotation_status] varchar(100) NULL, 
	[package_code] varchar(100) NULL, 
	[premium_amount] decimal(18,2) NULL, 
	[quotation_expiry_date] varchar(100) NULL, 
	[created_date] datetime2(6) NULL, 
	[updated_at] datetime2(6) NULL
);