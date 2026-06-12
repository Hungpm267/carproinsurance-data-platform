CREATE TABLE [silver].[crm_customer] (

	[customer_id] varchar(50) NOT NULL, 
	[full_name] varchar(255) NULL, 
	[gender] varchar(20) NULL, 
	[dob] date NULL, 
	[phone_number] varchar(50) NULL, 
	[email] varchar(255) NULL, 
	[city] varchar(150) NULL, 
	[district] varchar(150) NULL, 
	[created_date] datetime2(6) NULL, 
	[updated_at] datetime2(6) NULL
);