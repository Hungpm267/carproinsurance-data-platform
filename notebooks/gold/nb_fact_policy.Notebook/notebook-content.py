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
POLICY_POLICY = "policy_policy"
CRM_QUOTATION = "crm_quotation"

DIM_CUSTOMER = "dim_customer"
DIM_INSURANCE_PROVIDER = "dim_insurance_provider"
DIM_POLICY_STATUS = "dim_policy_status"

FACT_POLICY = "fact_policy"

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

pol = spark.read.table(f"{SILVER_LH}.{POLICY_POLICY}")
if last_watermark:
    pol = pol.filter(col(watermark_column) > to_timestamp(lit(last_watermark)))
row_read = pol.count()

quo = (
    spark.read.table(f"{SILVER_LH}.{CRM_QUOTATION}")
    .select(
        "quotation_id",
        col("premium_amount").alias("quoted_premium_amount")
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

pstat = (
    spark.read.table(f"{GOLD_LH}.{DIM_POLICY_STATUS}")
    .select(
        col("policy_status_name").alias("policy_status"),
        "policy_status_key"
    )
)

base = (
    pol.select(
        "policy_id",
        "quotation_id",
        "policy_number",
        "customer_id",
        "provider_code",
        "policy_start_date",
        "policy_end_date",
        "issued_date",
        "policy_status",
        col("premium_amount").alias("written_premium_amount"),
    )
)

def date_key(col_name):
    return date_format(col(col_name), "yyyyMMdd").cast("int")

base_quo = (
    base
    .join(quo, "quotation_id", "left")
    .withColumn("issued_dt", col("issued_date").cast("date"))
)

new_watermark = pol.agg(max(watermark_column)).collect()[0][0]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact = (
    base_quo
    # .join(
    #     cust,
    #     (
    #         (base_quo["customer_id"] == cust["customer_id"]) &
    #         (base_quo["issued_dt"] >= cust["effective_date"]) &
    #         (base_quo["issued_dt"] <  cust["expiry_date"])
    #     ),
    #     "left",
    # )
    .join(cust, "customer_id", "left")
    .join(prov, "provider_code", "left")
    .join(pstat, "policy_status", "left")
    .withColumn("policy_start_date_key", date_key("policy_start_date"))
    .withColumn("policy_end_date_key", date_key("policy_end_date"))
    .withColumn("issued_date_key", date_key("issued_date"))
    .withColumn(
        "premium_variance_amount",
        (col("written_premium_amount") - col("quoted_premium_amount")).cast("decimal(18,2)")
    )
    .withColumn(
        "is_in_force",
        when(col("policy_status") == "ACTIVE", lit(True))
        .otherwise(lit(False))
    )
    .withColumn(
        "policy_term_days",
        datediff(col("policy_end_date"), col("policy_start_date"))
    )
    .dropDuplicates(["policy_id"])
    .withColumn(
        "fact_policy_key",
        row_number().over(Window.orderBy("policy_id"))
    )
    .select(
        "fact_policy_key",
        "policy_id",
        "quotation_id",
        "policy_number",
        "policy_start_date_key",
        "policy_end_date_key",
        "issued_date_key",
        "customer_key",
        "provider_key",
        "policy_status_key",
        "written_premium_amount",
        "quoted_premium_amount",
        "premium_variance_amount",
        "is_in_force",
        "policy_term_days",
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
#     .saveAsTable(f"{GOLD_LH}.{FACT_POLICY}")

fact.write.format("delta") \
    .mode("append") \
    .saveAsTable(f"{GOLD_LH}.{FACT_POLICY}")

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
