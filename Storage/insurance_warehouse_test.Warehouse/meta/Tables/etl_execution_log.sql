CREATE TABLE [meta].[etl_execution_log] (

	[log_id] bigint NOT NULL, 
	[pipeline_run_id] varchar(64) NOT NULL, 
	[controller_id] bigint NOT NULL, 
	[pipeline_name] varchar(100) NOT NULL, 
	[start_time] datetime2(3) NOT NULL, 
	[end_time] datetime2(3) NULL, 
	[status] varchar(20) NOT NULL, 
	[rows_read] bigint NULL, 
	[rows_inserted] bigint NULL, 
	[rows_updated] bigint NULL, 
	[rows_rejected] bigint NULL, 
	[watermark_from] varchar(200) NULL, 
	[watermark_to] varchar(200) NULL, 
	[dynamic_source_file] varchar(500) NULL, 
	[error_message] varchar(max) NULL, 
	[logged_at] datetime2(3) NOT NULL
);


GO
ALTER TABLE [meta].[etl_execution_log] ADD CONSTRAINT PK_meta_etl_execution_log primary key NONCLUSTERED ([log_id]);