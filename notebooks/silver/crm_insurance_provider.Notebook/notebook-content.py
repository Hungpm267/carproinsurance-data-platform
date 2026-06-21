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

# Silver: crm_insurance_provider
# BK: provider_code
# Responsibility: cleanse, dedup → silver.crm_insurance_provider

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import json

bronze_schema = "bronze"
silver_schema = "silver"
SOURCE_TABLE  = f"{bronze_schema}.crm_insurance_provider"
TARGET_SILVER = f"{silver_schema}.crm_insurance_provider"
BK_COL        = "provider_code"



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_provider_src = spark.read.table(SOURCE_TABLE)
rows_read = df_provider_src.count()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_provider_silver = (
    df_provider_src
    .select(
        F.upper(F.trim(F.col("provider_code"))).alias("provider_code"),
        F.trim(F.col("provider_name")).alias("provider_name"),
        F.trim(F.col("provider_group")).alias("provider_group"),
        F.col("active_flag").cast("boolean"),
        F.to_date(F.col("created_date")).alias("created_date"),
        F.col("updated_at")
    )
    .dropDuplicates([BK_COL])
)

df_provider_silver.cache()

rows_inserted = df_provider_silver.count()

(
    df_provider_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_SILVER)
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebookutils.notebook.exit(json.dumps({
        "rows_read": rows_read,
        "rows_inserted": rows_inserted
    }))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
