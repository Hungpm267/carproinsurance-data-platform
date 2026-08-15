CREATE TABLE [silver].[crm_agent] (

	[agent_id] varchar(50) NOT NULL, 
	[agent_name] varchar(255) NULL, 
	[region] varchar(100) NULL, 
	[branch] varchar(100) NULL, 
	[manager_name] varchar(255) NULL, 
	[created_date] datetime2(6) NULL, 
	[updated_at] datetime2(6) NULL, 
	[row_hash] varchar(64) NULL
);