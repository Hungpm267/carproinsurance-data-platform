CREATE TABLE [meta].[error_logging_table] (

	[error_id] varchar(50) NOT NULL, 
	[execution_id] bigint NULL, 
	[error_pipelinename] varchar(255) NOT NULL, 
	[error_timestamp] datetime2(6) NOT NULL, 
	[error_code] varchar(100) NOT NULL, 
	[layer_name] varchar(50) NOT NULL, 
	[target_table] varchar(255) NOT NULL, 
	[error_message] varchar(max) NOT NULL, 
	[error_severity_level] varchar(50) NOT NULL, 
	[bad_record_content] varchar(max) NULL, 
	[status] varchar(50) NOT NULL, 
	[updated_at] datetime2(6) NOT NULL
);