CREATE TABLE [dbo].[vehicle] (

	[vehicle_id] varchar(20) NOT NULL, 
	[customer_id] varchar(20) NULL, 
	[plate_number] varchar(20) NULL, 
	[vehicle_brand] varchar(100) NULL, 
	[vehicle_model] varchar(100) NULL, 
	[manufacture_year] int NULL, 
	[vehicle_value] decimal(18,2) NULL, 
	[created_date] datetime2(3) NULL, 
	[updated_at] datetime2(3) NULL
);


GO
ALTER TABLE [dbo].[vehicle] ADD CONSTRAINT PK_dbo_vehicle primary key NONCLUSTERED ([vehicle_id]);