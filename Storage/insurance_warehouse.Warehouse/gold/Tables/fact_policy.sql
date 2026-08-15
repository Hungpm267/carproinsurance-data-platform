CREATE TABLE [gold].[fact_policy] (

	[policy_key] int NOT NULL, 
	[policy_id] varchar(50) NOT NULL, 
	[quotation_id] varchar(50) NULL, 
	[policy_number] varchar(100) NOT NULL, 
	[policy_start_date_key] int NOT NULL, 
	[policy_end_date_key] int NULL, 
	[issued_date_key] int NOT NULL, 
	[customer_key] int NOT NULL, 
	[provider_key] int NOT NULL, 
	[policy_status_key] int NOT NULL, 
	[written_premium_amount] decimal(18,2) NOT NULL, 
	[quoted_premium_amount] decimal(18,2) NULL, 
	[premium_variance_amount] decimal(18,2) NULL, 
	[is_in_force] bit NOT NULL, 
	[policy_term_days] int NULL
);