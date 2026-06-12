CREATE TABLE [bronze].[crm_vehicle] (

	[vehicle_id] varchar(50) NOT NULL, 
	[customer_id] varchar(50) NOT NULL, 
	[plate_number] varchar(50) NULL, 
	[vehicle_brand] varchar(150) NULL, 
	[vehicle_model] varchar(150) NULL, 
	[manufacture_year] int NULL, 
	[vehicle_value] decimal(18,2) NULL, 
	[created_date] datetime2(6) NULL, 
	[updated_at] datetime2(6) NULL
);