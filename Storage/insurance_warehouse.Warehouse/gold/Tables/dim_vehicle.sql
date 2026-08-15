CREATE TABLE [gold].[dim_vehicle] (

	[vehicle_key] int NOT NULL, 
	[vehicle_id] varchar(50) NOT NULL, 
	[customer_id] varchar(50) NOT NULL, 
	[plate_number] varchar(50) NULL, 
	[vehicle_brand] varchar(150) NULL, 
	[vehicle_model] varchar(150) NULL, 
	[manufacture_year] int NULL, 
	[vehicle_age_years] int NULL, 
	[vehicle_value] decimal(18,2) NULL, 
	[vehicle_value_band] varchar(50) NULL
);