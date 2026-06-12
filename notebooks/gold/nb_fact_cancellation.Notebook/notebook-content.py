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
POLICY_CANCELLATION = "policy_cancellation"
POLICY_POLICY = "policy_policy"

DIM_CUSTOMER = "dim_customer"
DIM_INSURANCE_PROVIDER = "dim_insurance_provider"
DIM_CANCELLATION_REASON = "dim_cancellation_reason"

FACT_CANCELLATION = "fact_cancellation"

CFG = "insurance_warehouse.etl_transform_config"


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


can = spark.read.table(f"{SILVER_LH}.{POLICY_CANCELLATION}")
if last_watermark:
    can = can.filter(col(watermark_column) > to_timestamp(lit(last_watermark)))
row_read = can.count()

pol = (
    spark.read.table(f"{SILVER_LH}.{POLICY_POLICY}")
    .select(
        "policy_id",
        "customer_id",
        "provider_code"
    )
)

cust = (
    spark.read.table(f"{GOLD_LH}.{DIM_CUSTOMER}")
    .select(
        "customer_id",
        "customer_key",
        "effective_date",
        coalesce(
            col("expiry_date"),
            lit("9999-12-31").cast("date")
        ).alias("expiry_date")
    )
)

prov = (
    spark.read.table(f"{GOLD_LH}.{DIM_INSURANCE_PROVIDER}")
    .select("provider_code", "provider_key")
)

reason = (
    spark.read.table(f"{GOLD_LH}.{DIM_CANCELLATION_REASON}")
    .select(
        col("cancellation_reason_name").alias("cancellation_reason"),
        "cancellation_reason_key"
    )
)
base = (
    can.select(
        "cancellation_id",
        "policy_id",
        col("cancellation_date").cast("date").alias("cancellation_dt"),
        date_format(col("cancellation_date"), "yyyyMMdd").cast("int").alias("cancellation_date_key"),
        "cancellation_reason",
        col("refund_amount"),
    )
    .join(pol, "policy_id", "left")
)

new_watermark = can.agg(max(watermark_column)).collect()[0][0]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact = (
    base.join(
        cust,
        (
            (base["customer_id"] == cust["customer_id"]) &
            (base["cancellation_dt"] >= cust["effective_date"]) &
            (base["cancellation_dt"] < cust["expiry_date"])
        ),
        "left"
    )
    .join(prov, "provider_code", "left")
    .join(reason, "cancellation_reason", "left")
    .dropDuplicates(["cancellation_id"])
    .withColumn(
        "fact_cancellation_key",
        row_number().over(Window.orderBy("cancellation_id"))
    )
    .select(
        "fact_cancellation_key",
        "cancellation_id",
        "policy_id",
        "cancellation_date_key",
        "customer_key",
        "provider_key",
        "cancellation_reason_key",
        "refund_amount",
    )
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
#     .saveAsTable(f"{GOLD_LH}.{FACT_CANCELLATION}")
if row_read > 0:
    fact.write.format("delta") \
        .mode("append") \
        .saveAsTable(f"{GOLD_LH}.{FACT_CANCELLATION}")

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
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }
