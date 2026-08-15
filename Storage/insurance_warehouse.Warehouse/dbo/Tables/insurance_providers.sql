CREATE TABLE [dbo].[insurance_providers] (

	[provider_code] varchar(20) NOT NULL, 
	[provider_name] varchar(200) NULL, 
	[provider_group] varchar(100) NULL, 
	[active_flag] int NULL, 
	[created_date] datetime2(3) NULL, 
	[updated_at] datetime2(3) NULL
);


GO
ALTER TABLE [dbo].[insurance_providers] ADD CONSTRAINT PK_dbo_insurance_providers primary key NONCLUSTERED ([provider_code]);