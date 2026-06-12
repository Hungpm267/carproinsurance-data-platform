CREATE TABLE [gold].[fact_cancellation] (

	[cancellation_key] int NOT NULL, 
	[cancellation_id] varchar(50) NOT NULL, 
	[policy_id] varchar(50) NOT NULL, 
	[cancellation_date_key] int NOT NULL, 
	[customer_key] int NOT NULL, 
	[provider_key] int NOT NULL, 
	[cancellation_reason_key] int NULL, 
	[refund_amount] decimal(18,2) NULL
);