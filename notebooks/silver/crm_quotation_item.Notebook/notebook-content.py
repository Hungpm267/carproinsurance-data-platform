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

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import json

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Silver: crm_quotation_item
# BK: quotation_item_id
# Responsibility: cleanse, validate → silver.crm_quotation_item

bronze_schema = "bronze"
silver_schema = "silver"
SOURCE_TABLE  = f"{bronze_schema}.crm_quotation_item"
TARGET_SILVER = f"{silver_schema}.crm_quotation_item"
BK_COL        = "quotation_item_id"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_quo_item_src = spark.read.table(SOURCE_TABLE)
rows_read = df_quo_item_src.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_quo_item_silver = (
    df_quo_item_src
    .select(
        F.upper(F.trim(F.col("quotation_item_id"))).alias("quotation_item_id"),
        F.upper(F.trim(F.col("quotation_id"))).alias("quotation_id"),
        F.trim(F.col("coverage_type")).alias("coverage_type"),
        F.col("coverage_amount").cast("decimal(18,2)").alias("coverage_amount"),
        F.col("deductible_amount").cast("decimal(18,2)").alias("deductible_amount"),
        F.to_date(F.col("created_date")).alias("created_date"),  
        F.col("updated_at"),                                      
    )
    .filter(F.col("coverage_amount") > 0)
    .filter(F.col("deductible_amount") >= 0)
    .filter(F.col("quotation_item_id").isNotNull() & (F.trim(F.col("quotation_item_id")) != ""))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_quo_item_silver.cache()

rows_inserted = df_quo_item_silver.count()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(
    df_quo_item_silver.write
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
