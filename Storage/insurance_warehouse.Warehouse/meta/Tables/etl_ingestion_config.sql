CREATE TABLE [meta].[etl_ingestion_config] (

	[ingestion_config_id] bigint NOT NULL, 
	[pipeline_name] varchar(255) NOT NULL, 
	[source_system] varchar(100) NULL, 
	[source_schema] varchar(100) NULL, 
	[source_table] varchar(255) NULL, 
	[source_path] varchar(500) NULL, 
	[source_format] varchar(50) NOT NULL, 
	[file_pattern] varchar(255) NULL, 
	[target_layer] varchar(50) NOT NULL, 
	[target_schema] varchar(100) NOT NULL, 
	[target_table] varchar(255) NOT NULL, 
	[load_type] varchar(50) NOT NULL, 
	[watermark_column] varchar(100) NULL, 
	[last_watermark] varchar(255) NULL
);