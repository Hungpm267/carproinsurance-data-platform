CREATE TABLE [silver].[policy_payment] (

	[payment_id] varchar(50) NOT NULL, 
	[policy_id] varchar(50) NOT NULL, 
	[payment_date] datetime2(6) NULL, 
	[payment_method] varchar(100) NULL, 
	[payment_status] varchar(100) NULL, 
	[payment_amount] decimal(18,2) NULL, 
	[transaction_reference] varchar(255) NULL, 
	[last_updated] datetime2(6) NULL, 
	[operation_type] varchar(50) NULL, 
	[batch_date] date NULL, 
	[source_system] varchar(100) NULL
);