CREATE TABLE [meta].[error_logging_table] (

	[error_id] varchar(36) NOT NULL, 
	[execution_id] bigint NULL, 
	[error_pipelinename] varchar(100) NOT NULL, 
	[error_timestamp] datetime2(3) NOT NULL, 
	[error_code] varchar(50) NOT NULL, 
	[layer_name] varchar(50) NOT NULL, 
	[target_table] varchar(200) NOT NULL, 
	[error_message] varchar(max) NOT NULL, 
	[error_severity_level] varchar(20) NOT NULL, 
	[bad_record_content] varchar(max) NULL, 
	[status] varchar(20) NOT NULL, 
	[updated_at] datetime2(3) NOT NULL
);


GO
ALTER TABLE [meta].[error_logging_table] ADD CONSTRAINT PK_meta_error_logging_table primary key NONCLUSTERED ([error_id]);