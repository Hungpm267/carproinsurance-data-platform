CREATE TABLE [meta].[etl_transform_config] (

	[transform_config_id] bigint NOT NULL, 
	[pipeline_name] varchar(255) NOT NULL, 
	[source_layer] varchar(50) NOT NULL, 
	[source_schema] varchar(100) NOT NULL, 
	[source_table] varchar(255) NOT NULL, 
	[target_layer] varchar(50) NOT NULL, 
	[target_schema] varchar(100) NOT NULL, 
	[target_table] varchar(255) NOT NULL, 
	[transform_type] varchar(50) NOT NULL, 
	[primary_key_columns] varchar(255) NULL, 
	[partition_column] varchar(100) NULL, 
	[dependency_pipeline] varchar(255) NULL, 
	[notebook_id] varchar(255) NULL, 
	[watermark_column] varchar(100) NULL, 
	[last_watermark] varchar(255) NULL
);