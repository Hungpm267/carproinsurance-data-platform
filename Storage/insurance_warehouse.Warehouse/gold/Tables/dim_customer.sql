CREATE TABLE [gold].[dim_customer] (

	[customer_key] int NOT NULL, 
	[customer_id] varchar(50) NOT NULL, 
	[full_name] varchar(255) NOT NULL, 
	[gender] varchar(20) NULL, 
	[date_of_birth] date NULL, 
	[age] int NULL, 
	[phone_number] varchar(50) NULL, 
	[email] varchar(255) NULL, 
	[city] varchar(150) NULL, 
	[district] varchar(150) NULL, 
	[customer_since_date] date NOT NULL, 
	[effective_date] date NOT NULL, 
	[expiry_date] date NULL, 
	[is_current] bit NOT NULL, 
	[row_hash] varchar(64) NOT NULL
);