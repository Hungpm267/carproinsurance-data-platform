CREATE TABLE [silver].[crm_insurance_provider] (

	[provider_code] varchar(50) NOT NULL, 
	[provider_name] varchar(255) NULL, 
	[provider_group] varchar(150) NULL, 
	[active_flag] int NULL, 
	[created_date] datetime2(6) NULL, 
	[updated_at] datetime2(6) NULL
);