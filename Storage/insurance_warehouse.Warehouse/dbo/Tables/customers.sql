CREATE TABLE [dbo].[customers] (

	[customer_id] varchar(20) NOT NULL, 
	[full_name] varchar(200) NULL, 
	[gender] varchar(10) NULL, 
	[dob] date NULL, 
	[phone_number] varchar(20) NULL, 
	[email] varchar(200) NULL, 
	[city] varchar(100) NULL, 
	[district] varchar(100) NULL, 
	[created_date] datetime2(3) NULL, 
	[updated_at] datetime2(3) NULL
);


GO
ALTER TABLE [dbo].[customers] ADD CONSTRAINT PK_dbo_customers primary key NONCLUSTERED ([customer_id]);