CREATE TABLE [dbo].[quotation_item] (

	[quotation_item_id] varchar(20) NOT NULL, 
	[quotation_id] varchar(20) NULL, 
	[coverage_type] varchar(100) NULL, 
	[coverage_amount] decimal(18,2) NULL, 
	[deductible_amount] decimal(18,2) NULL, 
	[created_date] datetime2(3) NULL, 
	[updated_at] datetime2(3) NULL
);


GO
ALTER TABLE [dbo].[quotation_item] ADD CONSTRAINT PK_dbo_quotation_item primary key NONCLUSTERED ([quotation_item_id]);