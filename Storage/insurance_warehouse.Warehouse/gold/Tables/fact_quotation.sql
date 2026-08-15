CREATE TABLE [gold].[fact_quotation] (

	[quotation_key] int NOT NULL, 
	[quotation_id] varchar(50) NOT NULL, 
	[quotation_date_key] int NOT NULL, 
	[quotation_expiry_date_key] int NULL, 
	[customer_key] int NOT NULL, 
	[agent_key] int NULL, 
	[provider_key] int NOT NULL, 
	[vehicle_key] int NULL, 
	[product_package_key] int NOT NULL, 
	[quotation_status_key] int NOT NULL, 
	[quotation_premium_amount] decimal(18,2) NOT NULL
);