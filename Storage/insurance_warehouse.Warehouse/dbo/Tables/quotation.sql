CREATE TABLE [dbo].[quotation] (

	[quotation_id] varchar(20) NOT NULL, 
	[customer_id] varchar(20) NULL, 
	[agent_id] varchar(20) NULL, 
	[provider_code] varchar(20) NULL, 
	[quotation_date] datetime2(3) NULL, 
	[quotation_status] varchar(50) NULL, 
	[package_code] varchar(50) NULL, 
	[premium_amount] decimal(18,2) NULL, 
	[quotation_expiry_date] datetime2(3) NULL, 
	[created_date] datetime2(3) NULL, 
	[updated_at] datetime2(3) NULL
);


GO
ALTER TABLE [dbo].[quotation] ADD CONSTRAINT PK_dbo_quotation primary key NONCLUSTERED ([quotation_id]);