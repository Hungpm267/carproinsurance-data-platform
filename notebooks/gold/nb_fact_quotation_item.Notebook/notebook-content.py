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
CRM_QUOTATION_ITEM  = "crm_quotation_item"
CRM_QUOTATION       = "crm_quotation"
DIM_PRODUCT_PACKAGE = "dim_product_package"
DIM_COVERAGE_TYPE   = "dim_coverage_type"

FACT_QUOTATION_ITEM = "fact_quotation_item"

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

src = spark.read.table(f"{SILVER_LH}.{CRM_QUOTATION_ITEM}")
if last_watermark:
    src = src.filter(col(watermark_column) > to_timestamp(lit(last_watermark)))
row_read = src.count()

quo = (
    spark.read.table(f"{SILVER_LH}.{CRM_QUOTATION}")
    .select("quotation_id", "package_code")
)

cov = (
    spark.read.table(f"{GOLD_LH}.{DIM_COVERAGE_TYPE}")
    .select(
        col("coverage_type_name").alias("coverage_type"),
        "coverage_type_key"
    )
)

new_watermark = src.agg(max(watermark_column)).collect()[0][0]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact = (
    src.select(
        "quotation_item_id",
        "quotation_id",
        "coverage_type",
        col("coverage_amount"),
        col("deductible_amount"),
    )
    .join(quo, "quotation_id", "left")  
    .join(cov, "coverage_type", "left")
    .dropDuplicates(["quotation_item_id"])
    .withColumn(
        "quotation_item_key",
        row_number().over(Window.orderBy("quotation_item_id"))
    )
    .select(
        "quotation_item_key",
        "quotation_item_id",
        "coverage_type_key",
        "coverage_amount",
        "deductible_amount",
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
#     .saveAsTable(f"{GOLD_LH}.{FACT_QUOTATION_ITEM}")

fact.write.format("delta") \
    .mode("append") \
    .saveAsTable(f"{GOLD_LH}.{FACT_QUOTATION_ITEM}")

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
