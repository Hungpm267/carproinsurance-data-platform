CREATE TABLE [meta].[etl_pipeline_config] (

	[pipeline_name] varchar(100) NOT NULL, 
	[pipeline_stage] varchar(50) NOT NULL, 
	[is_active] bit NOT NULL, 
	[retry_count] int NOT NULL, 
	[retry_interval_minutes] int NOT NULL, 
	[timeout_minutes] int NOT NULL, 
	[created_at] datetime2(3) NOT NULL, 
	[updated_at] datetime2(3) NULL, 
	[created_by] varchar(100) NULL, 
	[updated_by] varchar(100) NULL
);


GO
ALTER TABLE [meta].[etl_pipeline_config] ADD CONSTRAINT PK_meta_etl_pipeline_config primary key NONCLUSTERED ([pipeline_name]);