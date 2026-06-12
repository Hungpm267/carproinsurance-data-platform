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
# META     },
# META     "warehouse": {
# META       "known_warehouses": []
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

# Silver: crm_vehicle
# BK: vehicle_id
# Responsibility: cleanse, validate, dedup → silver.crm_vehicle


bronze_schema = "bronze"
silver_schema = "silver"
SOURCE_TABLE  = f"{bronze_schema}.crm_vehicle"
TARGET_SILVER = f"{silver_schema}.crm_vehicle"
BK_COL        = "vehicle_id"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_vehicle_src = spark.read.table(SOURCE_TABLE)
rows_read = df_vehicle_src.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************





df_vehicle_silver = (
    df_vehicle_src
    .select(
        F.upper(F.trim(F.col("vehicle_id"))).alias("vehicle_id"),
        F.upper(F.trim(F.col("customer_id"))).alias("customer_id"),
        F.upper(F.trim(F.col("plate_number"))).alias("plate_number"),
        F.trim(F.col("vehicle_brand")).alias("vehicle_brand"),
        F.trim(F.col("vehicle_model")).alias("vehicle_model"),
        F.when(
            (F.col("manufacture_year").cast("int") >= 1900) &
            (F.col("manufacture_year").cast("int") <= F.year(F.current_date())),
            F.col("manufacture_year").cast("int")
        ).otherwise(F.lit(None).cast("int")).alias("manufacture_year"),
        F.when(
            F.col("vehicle_value").cast("decimal(18,2)") >= 0,
            F.col("vehicle_value").cast("decimal(18,2)")
        ).otherwise(F.lit(None).cast("decimal(18,2)")).alias("vehicle_value"),
    )
    .dropDuplicates([BK_COL])
)




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_vehicle_silver.cache()

rows_inserted = df_vehicle_silver.count()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(
    df_vehicle_silver.write
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
