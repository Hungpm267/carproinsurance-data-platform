CREATE TABLE [meta].[etl_execution_log] (

	[log_id] bigint NOT NULL, 
	[pipeline_run_id] varchar(255) NOT NULL, 
	[controller_id] bigint NOT NULL, 
	[pipeline_name] varchar(255) NOT NULL, 
	[start_time] datetime2(6) NOT NULL, 
	[end_time] datetime2(6) NULL, 
	[status] varchar(50) NOT NULL, 
	[rows_read] bigint NULL, 
	[rows_inserted] bigint NULL, 
	[rows_updated] bigint NULL, 
	[rows_rejected] bigint NULL, 
	[watermark_from] varchar(255) NULL, 
	[watermark_to] varchar(255) NULL, 
	[dynamic_source_file] varchar(500) NULL, 
	[error_message] varchar(max) NULL, 
	[logged_at] datetime2(6) NOT NULL
);