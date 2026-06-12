CREATE TABLE [meta].[etl_pipeline_config] (

	[pipeline_name] varchar(255) NOT NULL, 
	[pipeline_stage] varchar(50) NOT NULL, 
	[is_active] bit NOT NULL, 
	[retry_count] int NOT NULL, 
	[retry_interval_minutes] int NOT NULL, 
	[timeout_minutes] int NOT NULL, 
	[created_at] datetime2(6) NOT NULL, 
	[updated_at] datetime2(6) NULL, 
	[created_by] varchar(100) NULL, 
	[updated_by] varchar(100) NULL
);