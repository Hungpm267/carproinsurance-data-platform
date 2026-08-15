CREATE TABLE [gold].[dim_insurance_provider] (

	[provider_key] int NOT NULL, 
	[provider_code] varchar(50) NOT NULL, 
	[provider_name] varchar(255) NOT NULL, 
	[provider_group] varchar(150) NULL, 
	[is_active] bit NOT NULL
);