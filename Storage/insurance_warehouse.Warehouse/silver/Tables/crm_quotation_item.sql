CREATE TABLE [silver].[crm_quotation_item] (

	[quotation_item_id] varchar(50) NOT NULL, 
	[quotation_id] varchar(50) NOT NULL, 
	[coverage_type] varchar(150) NULL, 
	[coverage_amount] decimal(18,2) NULL, 
	[deductible_amount] decimal(18,2) NULL, 
	[created_date] datetime2(6) NULL, 
	[updated_at] datetime2(6) NULL
);