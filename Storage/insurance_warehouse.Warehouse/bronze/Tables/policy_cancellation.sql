CREATE TABLE [bronze].[policy_cancellation] (

	[cancellation_id] varchar(50) NOT NULL, 
	[policy_id] varchar(50) NOT NULL, 
	[cancellation_date] datetime2(6) NULL, 
	[cancellation_reason] varchar(max) NULL, 
	[refund_amount] decimal(18,2) NULL, 
	[last_updated] datetime2(6) NULL, 
	[operation_type] varchar(50) NULL, 
	[batch_date] date NULL, 
	[source_system] varchar(100) NULL
);