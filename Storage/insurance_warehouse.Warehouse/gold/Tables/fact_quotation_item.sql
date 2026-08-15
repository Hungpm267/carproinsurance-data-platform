CREATE TABLE [gold].[fact_quotation_item] (

	[quotation_item_key] int NOT NULL, 
	[quotation_item_id] varchar(50) NOT NULL, 
	[coverage_type_key] int NOT NULL, 
	[coverage_amount] decimal(18,2) NOT NULL, 
	[deductible_amount] decimal(18,2) NULL
);