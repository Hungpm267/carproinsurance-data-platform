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
# META       "default_warehouse": "10f24a18-40e7-90f4-438b-b0056974ed4f",
# META       "known_warehouses": [
# META         {
# META           "id": "10f24a18-40e7-90f4-438b-b0056974ed4f",
# META           "type": "Datawarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Lakehouse constants
BRONZE_LH = "bronze"
SILVER_LH = "silver"
GOLD_LH = "gold"

# Table constants
POLICY_PAYMENT = "policy_payment"

DIM_PAYMENT_METHOD = "dim_payment_method"
DIM_PAYMENT_STATUS = "dim_payment_status"

FACT_PAYMENT = "fact_payment"

CFG = "insurance_warehouse.meta.etl_transform_config"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

#parameter cell
watermark_column = ""
last_watermark = ""
controller_id = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pay = spark.read.table(f"{SILVER_LH}.{POLICY_PAYMENT}")
if last_watermark:
    pay = pay.filter(col(watermark_column) > to_timestamp(lit(last_watermark)))
row_read = pay.count()

pm = (
    spark.read.table(f"{GOLD_LH}.{DIM_PAYMENT_METHOD}")
    .select(
        col("payment_method_name").alias("payment_method"),
        "payment_method_key"
    )
)

ps = (
    spark.read.table(f"{GOLD_LH}.{DIM_PAYMENT_STATUS}")
    .select(
        col("payment_status_name").alias("payment_status"),
        "payment_status_key"
    )
)

def date_key(col_name):
    return date_format(col(col_name), "yyyyMMdd").cast("int")

new_watermark = pay.agg(max(watermark_column)).collect()[0][0]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact = (
    pay.select(
        "payment_id",
        "policy_id",
        date_key("payment_date").alias("payment_date_key"),
        "payment_method",
        "payment_status",
        col("payment_amount"),
    )
    .join(pm, "payment_method", "left")
    .join(ps, "payment_status", "left")
    .withColumn(
        "is_successful_payment",
        when(col("payment_status") == "PAID", lit(True))
        .otherwise(lit(False))
    )
    .withColumn(
        "fact_payment_key",
        row_number().over(Window.orderBy("payment_id"))
    )
    .select(
        "fact_payment_key",
        "payment_id",
        "policy_id",
        "payment_date_key",
        "payment_method_key",
        "payment_status_key",
        "payment_amount",
        "is_successful_payment",
    )
    .dropDuplicates(["payment_id"])
)
row_inserted = fact.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# fact.write.format("delta") \
#     .mode("overwrite") \
#     .option("overwriteSchema", "true") \
#     .saveAsTable(f"{GOLD_LH}.{FACT_PAYMENT}")

fact.write.format("delta") \
    .mode("append") \
    .saveAsTable(f"{GOLD_LH}.{FACT_PAYMENT}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# if new_watermark is not None:
#     spark.sql(f"""
#         UPDATE {CFG}
#         SET last_watermark = '{new_watermark}'
#         WHERE transform_config_id = {controller_id}
#     """)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
notebookutils.notebook.exit(json.dumps({
    "watermark_from": str(last_watermark) if last_watermark else "",
    "watermark_to": str(new_watermark) if new_watermark else "",
    "rows_read": int(row_read),
    "rows_inserted": int(row_inserted)
}))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
