CREATE TABLE [meta].[etl_transform_config] (

	[transform_config_id] bigint NOT NULL, 
	[pipeline_name] varchar(100) NOT NULL, 
	[source_layer] varchar(50) NOT NULL, 
	[source_schema] varchar(100) NOT NULL, 
	[source_table] varchar(200) NOT NULL, 
	[target_layer] varchar(50) NOT NULL, 
	[target_schema] varchar(100) NOT NULL, 
	[target_table] varchar(200) NOT NULL, 
	[transform_type] varchar(20) NOT NULL, 
	[primary_key_columns] varchar(500) NULL, 
	[partition_column] varchar(200) NULL, 
	[dependency_pipeline] varchar(100) NULL, 
	[notebook_id] varchar(36) NULL, 
	[watermark_column] varchar(200) NULL, 
	[last_watermark] varchar(200) NULL
);


GO
ALTER TABLE [meta].[etl_transform_config] ADD CONSTRAINT PK_meta_etl_transform_config primary key NONCLUSTERED ([transform_config_id]);