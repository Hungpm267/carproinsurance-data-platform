# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "0aa0a14b-3288-4f9f-93d1-d888edaf7070",
# META       "default_lakehouse_name": "insurance_lakehouse",
# META       "default_lakehouse_workspace_id": "e13dac5b-f5b1-4169-bb58-0f6d0bfea366",
# META       "known_lakehouses": [
# META         {
# META           "id": "0aa0a14b-3288-4f9f-93d1-d888edaf7070"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%s
# MAGIC 
# MAGIC INSERT INTO dbo.security_user_scope
# MAGIC VALUES
# MAGIC (
# MAGIC     'rookie07@ntdatateam.onmicrosoft.com',
# MAGIC     'RegionalManager',
# MAGIC     'North',
# MAGIC     NULL,0@ntdatateam.onmicrosoft.com',
# MAGIC     NULL
# MAGIC ),
# MAGIC (
# MAGIC     'rookie08@ntdatateam.onmicrosoft.com',
# MAGIC     'BranchManager',
# MAGIC     'South',
# MAGIC     'Hcm',
# MAGIC     NULL
# MAGIC ),
# MAGIC (
# MAGIC     'rookie1ql
# MAGIC 
# MAGIC 
# MAGIC     'Agent',
# MAGIC     'North',
# MAGIC     'Ha Noi',
# MAGIC     'AG001'
# MAGIC );

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC INSERT INTO dbo.security_user_scope
# MAGIC VALUES
# MAGIC (
# MAGIC     'rookie11@ntdatateam.onmicrosoft.com',
# MAGIC     'tét tơ lỏ',
# MAGIC     'South',
# MAGIC     'Hcm',
# MAGIC     'ai biet'
# MAGIC )

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
