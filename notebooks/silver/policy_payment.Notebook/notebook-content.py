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

# BK: payment_id
# Responsibility: cleanse, validate → silver.policy_payment

bronze_schema = "bronze"
silver_schema = "silver"
SOURCE_TABLE  = f"{bronze_schema}.policy_payment"
TARGET_SILVER = f"{silver_schema}.policy_payment"
BK_COL        = "payment_id"

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
df_pay_src = spark.read.table(SOURCE_TABLE)

if _wc and _lw:
    df_pay_src = df_pay_src.filter(
        F.col(_wc) > F.to_timestamp(F.lit(_lw))
    )
    print(f"[INFO] Incremental load: {_wc} > {_lw}")
else:
    print(f"[INFO] Full load — no last_watermark found")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

rows_read = df_pay_src.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# Silver: policy_payment

df_pay_silver = (
    df_pay_src
    .select(
        F.upper(F.trim(F.col("payment_id"))).alias("payment_id"),
        F.upper(F.trim(F.col("policy_id"))).alias("policy_id"),
        F.to_timestamp(
            F.regexp_replace(F.col("payment_date"), "T", " "),
            "yyyy-MM-dd HH:mm:ss.SSS"
        ).alias("payment_date"),
        F.trim(F.col("payment_method")).alias("payment_method"),
        F.upper(F.trim(F.col("payment_status"))).alias("payment_status"),
        F.col("payment_amount").cast("decimal(18,2)").alias("payment_amount"),
        F.trim(F.col("transaction_reference")).alias("transaction_reference"),
        F.to_timestamp(
            F.regexp_replace(F.col("last_updated"), "T", " "),
            "yyyy-MM-dd HH:mm:ss.SSS"
        ).alias("last_updated"),
        F.trim(F.col("operation_type")).alias("operation_type"),
        F.col("batch_date").cast("date").alias("batch_date"),
        F.trim(F.col("source_system")).alias("source_system"),
    )
    .filter(F.col("payment_amount") > 0)
    .filter(F.col("payment_id").isNotNull() & (F.trim(F.col("payment_id")) != ""))
)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_pay_silver.cache()
rows_inserted = df_pay_silver.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(
    df_pay_silver.write
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
