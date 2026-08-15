CREATE TABLE [meta].[etl_ingestion_config] (

	[ingestion_config_id] bigint NOT NULL, 
	[pipeline_name] varchar(100) NOT NULL, 
	[source_system] varchar(50) NULL, 
	[source_schema] varchar(100) NULL, 
	[source_table] varchar(200) NULL, 
	[source_path] varchar(500) NULL, 
	[source_format] varchar(20) NOT NULL, 
	[file_pattern] varchar(200) NULL, 
	[target_layer] varchar(50) NOT NULL, 
	[target_schema] varchar(100) NOT NULL, 
	[target_table] varchar(200) NOT NULL, 
	[load_type] varchar(20) NOT NULL, 
	[watermark_column] varchar(200) NULL, 
	[last_watermark] varchar(200) NULL
);


GO
ALTER TABLE [meta].[etl_ingestion_config] ADD CONSTRAINT PK_meta_etl_ingestion_config primary key NONCLUSTERED ([ingestion_config_id]);