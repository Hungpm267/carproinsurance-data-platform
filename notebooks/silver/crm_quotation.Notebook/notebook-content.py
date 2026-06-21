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
from delta.tables import DeltaTable
import json 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Silver: crm_quotation
# BK: quotation_id
# Responsibility: cleanse, validate → silver.crm_quotation

bronze_schema = "bronze"
silver_schema = "silver"
SOURCE_TABLE  = f"{bronze_schema}.crm_quotation"
TARGET_SILVER = f"{silver_schema}.crm_quotation"
BK_COL        = "quotation_id"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Parameter cell
watermark_column = ""   
last_watermark   = ""  
controller_id    = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Pipeline injects parameters as strings; normalize empty/null values to None
_wc = watermark_column.strip() if watermark_column and watermark_column.strip() not in ("", "null", "None") else None
_lw = last_watermark.strip()   if last_watermark   and last_watermark.strip()   not in ("", "null", "None") else None
_ci = controller_id.strip()    if controller_id    and controller_id.strip()    not in ("", "null", "None") else None

print(f"watermark_column = {_wc!r}")
print(f"last_watermark   = {_lw!r}  # None → full load")
print(f"controller_id    = {_ci!r}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# If both watermark column and last watermark are present, apply incremental filter
# Otherwise, perform a full load (e.g. first run where last_watermark is NULL)
df_quotation_src = spark.read.table(SOURCE_TABLE)

if _wc and _lw:
    df_quotation_src = df_quotation_src.filter(
        F.col(_wc) > F.to_timestamp(F.lit(_lw))
    )
    pri
    print(f"[INFO] Full load — no last_watermark found")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

rows_read = df_quotation_src.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_quo_silver = (
    df_quotation_src
    .select(
        F.upper(F.trim(F.col("quotation_id"))).alias("quotation_id"),
        F.upper(F.trim(F.col("customer_id"))).alias("customer_id"),
        F.upper(F.trim(F.col("agent_id"))).alias("agent_id"),
        F.upper(F.trim(F.col("provider_code"))).alias("provider_code"),
        F.col("quotation_date").cast("timestamp").alias("quotation_date"),
        F.upper(F.trim(F.col("quotation_status"))).alias("quotation_status"),
        F.upper(F.trim(F.col("package_code"))).alias("package_code"),
        F.col("premium_amount").cast("decimal(18,2)").alias("premium_amount"),
        F.col("quotation_expiry_date").cast("timestamp").alias("quotation_expiry_date"),
        F.to_date(F.col("created_date")).alias("created_date"),  
        F.col("updated_at")
    )
    .filter(F.col("premium_amount") > 0)
    .filter(F.col("quotation_id").isNotNull() & (F.trim(F.col("quotation_id")) != ""))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_quo_silver.cache()
rows_inserted = df_quo_silver.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(
    df_quo_silver.write
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
