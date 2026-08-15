CREATE TABLE [gold].[dim_agent] (

	[agent_key] int NOT NULL, 
	[agent_id] varchar(50) NOT NULL, 
	[agent_name] varchar(255) NOT NULL, 
	[region] varchar(100) NULL, 
	[branch] varchar(100) NULL, 
	[manager_name] varchar(255) NULL, 
	[effective_date] date NOT NULL, 
	[expiry_date] date NULL, 
	[is_current] bit NOT NULL, 
	[row_hash] varchar(64) NOT NULL
);