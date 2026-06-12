CREATE TABLE [gold].[fact_payment] (

	[payment_key] int NOT NULL, 
	[payment_id] varchar(50) NOT NULL, 
	[policy_id] varchar(50) NOT NULL, 
	[payment_date_key] int NOT NULL, 
	[payment_method_key] int NOT NULL, 
	[payment_status_key] int NOT NULL, 
	[payment_amount] decimal(18,2) NOT NULL, 
	[is_successful_payment] bit NOT NULL
);