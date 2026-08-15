CREATE TABLE [dbo].[agents] (

	[agent_id] varchar(20) NOT NULL, 
	[agent_name] varchar(200) NULL, 
	[region] varchar(100) NULL, 
	[branch] varchar(100) NULL, 
	[manager_name] varchar(200) NULL, 
	[created_date] datetime2(3) NULL, 
	[updated_at] datetime2(3) NULL
);


GO
ALTER TABLE [dbo].[agents] ADD CONSTRAINT PK_dbo_agents primary key NONCLUSTERED ([agent_id]);