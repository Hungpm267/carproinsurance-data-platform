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
CRM_QUOTATION = "crm_quotation"
CRM_VEHICLE   = "crm_vehicle"

DIM_CUSTOMER = "dim_customer"
DIM_AGENT = "dim_agent"
DIM_INSURANCE_PROVIDER = "dim_insurance_provider"
DIM_VEHICLE = "dim_vehicle"
DIM_PRODUCT_PACKAGE = "dim_product_package"
DIM_QUOTATION_STATUS = "dim_quotation_status"

FACT_QUOTATION = "fact_quotation"

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

src = spark.read.table(f"{SILVER_LH}.{CRM_QUOTATION}")
if last_watermark:
    src = src.filter(col(watermark_column) > to_timestamp(lit(last_watermark)))
row_read = src.count()

cust = (
    spark.read.table(f"{GOLD_LH}.{DIM_CUSTOMER}")
    .select(
        "customer_id",
        "customer_key",
        "effective_date",
        coalesce(col("expiry_date"), lit("9999-12-31").cast("date")).alias("expiry_date")
    )
)

agent = (
    spark.read.table(f"{GOLD_LH}.{DIM_AGENT}")
    .select(
        "agent_id",
        "agent_key",
        "effective_date",
        coalesce(col("expiry_date"), lit("9999-12-31").cast("date")).alias("expiry_date")
    )
)

prov = (
    spark.read.table(f"{GOLD_LH}.{DIM_INSURANCE_PROVIDER}")
    .select("provider_code", "provider_key")
)

pkg = (
    spark.read.table(f"{GOLD_LH}.{DIM_PRODUCT_PACKAGE}")
    .select(col("product_package_name").alias("package_code"), "product_package_key")
)

qstat = (
    spark.read.table(f"{GOLD_LH}.{DIM_QUOTATION_STATUS}")
    .select(col("quotation_status_name").alias("quotation_status"), "quotation_status_key")
)

veh_map = (
    spark.read.table(f"{SILVER_LH}.{CRM_VEHICLE}")
    .select("customer_id", "vehicle_id")
    .dropDuplicates(["customer_id"])
)

veh = (
    spark.read.table(f"{GOLD_LH}.{DIM_VEHICLE}")
    .select("vehicle_id", "vehicle_key")
)

def date_key(col_name):
    return date_format(col(col_name), "yyyyMMdd").cast("int")

base = (
    src.select(
        "quotation_id",
        "quotation_date",
        date_key("quotation_date").alias("quotation_date_key"),
        date_key("quotation_expiry_date").alias("quotation_expiry_date_key"),
        "customer_id",
        "agent_id",
        "provider_code",
        "package_code",
        "quotation_status",
        col("premium_amount").alias("quotation_premium_amount"),
    )
    .join(veh_map, "customer_id", "left")  
)
new_watermark = src.agg(max(watermark_column)).collect()[0][0]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact = (
    base
    .join(
        cust,
        (
            (base["customer_id"] == cust["customer_id"]) &
            (base["quotation_date"] >= cust["effective_date"]) &
            (base["quotation_date"] <  cust["expiry_date"])
        ),
        "left"
    )
    # .join(
    #     agent,
    #     (
    #         (base["agent_id"] == agent["agent_id"]) &
    #         (base["quotation_date"] >= agent["effective_date"]) &
    #         (base["quotation_date"] <  agent["expiry_date"])
    #     ),
    #     "left"
    # )
    .join(agent, "agent_id", "left")
    .join(prov, "provider_code", "left")
    .join(veh, "vehicle_id", "left")       
    .join(pkg, "package_code", "left")
    .join(qstat, "quotation_status", "left")
    .dropDuplicates(["quotation_id"])
    .withColumn(
        "fact_quotation_key",
        row_number().over(Window.orderBy("quotation_id"))
    )
    .select(
        "fact_quotation_key",
        "quotation_id",
        "quotation_date_key",
        "quotation_expiry_date_key",
        "customer_key",
        "agent_key",
        "provider_key",
        "vehicle_key",
        "product_package_key",
        "quotation_status_key",
        "quotation_premium_amount",
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
#     .saveAsTable(f"{GOLD_LH}.{FACT_QUOTATION}")

fact.write.format("delta") \
    .mode("append") \
    .saveAsTable(f"{GOLD_LH}.{FACT_QUOTATION}")

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
