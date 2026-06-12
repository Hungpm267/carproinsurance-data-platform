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

# MARKDOWN ********************

# ##### **Imports & Helpers**

# CELL ********************

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import json

silver_schema = "silver"
gold_schema = "gold"

def generate_sk(new_rows_df: DataFrame, target_table: str, sk_col: str) -> DataFrame:
    try:
        max_sk = spark.read.table(target_table).agg(F.max(sk_col)).collect()[0][0]
        max_sk = max_sk if max_sk is not None else 0
    except Exception:
        max_sk = 0
    window = Window.orderBy(F.monotonically_increasing_id())
    return new_rows_df.withColumn(sk_col, (F.row_number().over(window) + F.lit(max_sk)).cast("int"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### **Load Data**

# CELL ********************

df_vehicle_src = spark.read.table(f"{silver_schema}.crm_vehicle")

rows_read = df_vehicle_src.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### **Transformation & SK Generation**

# CELL ********************

# Gold: dim_vehicle
# SK: vehicle_key IDENTITY(1,1) ETL generated
# BK: vehicle_id

SOURCE_SILVER = f"{silver_schema}.crm_vehicle"
TARGET_GOLD   = f"{gold_schema}.dim_vehicle"
SK_COL        = "vehicle_key"
BK_COL        = "vehicle_id"

df_vehicle_silver = spark.read.table(SOURCE_SILVER)

# Derived / business columns
df_vehicle = (
    df_vehicle_silver
    .withColumn(
        "vehicle_age_years",
        (F.year(F.current_date()) - F.col("manufacture_year")).cast("int")
    )
    .withColumn(
        "vehicle_value_band",
        F.when(F.col("vehicle_value") < 500_000_000,   F.lit("<500M"))
         .when(F.col("vehicle_value") <= 1_000_000_000, F.lit("500M-1B"))
         .otherwise(F.lit(">1B"))
    )
)

VEHICLE_COLS = [
    SK_COL, "vehicle_id", "customer_id", "plate_number",
    "vehicle_brand", "vehicle_model", "manufacture_year",
    "vehicle_value", "vehicle_age_years", "vehicle_value_band",
]

# SK generation
existing_vehicle = spark.read.table(TARGET_GOLD).select(BK_COL, SK_COL)

new_vehicles     = df_vehicle.join(existing_vehicle, on=BK_COL, how="left_anti")
new_vehicles_sk  = generate_sk(new_vehicles, TARGET_GOLD, SK_COL)

existing_vehicles_full = (
    df_vehicle
    .join(existing_vehicle, on=BK_COL, how="inner")
    .select(*VEHICLE_COLS)
)

merge_vehicle = existing_vehicles_full.unionByName(
    new_vehicles_sk.select(*VEHICLE_COLS)
)
merge_vehicle.createOrReplaceTempView("src_dim_vehicle")

rows_inserted = new_vehicles.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### **Executing MERGE**

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC MERGE INTO gold.dim_vehicle AS tgt
# MAGIC USING src_dim_vehicle AS src
# MAGIC ON tgt.vehicle_id = src.vehicle_id
# MAGIC  
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC     tgt.customer_id = src.customer_id,
# MAGIC     tgt.plate_number = src.plate_number,
# MAGIC     tgt.vehicle_brand = src.vehicle_brand,
# MAGIC     tgt.vehicle_model = src.vehicle_model,
# MAGIC     tgt.manufacture_year = src.manufacture_year,
# MAGIC     tgt.vehicle_value = src.vehicle_value,
# MAGIC     tgt.vehicle_age_years = src.vehicle_age_years,
# MAGIC     tgt.vehicle_value_band = src.vehicle_value_band
# MAGIC  
# MAGIC WHEN NOT MATCHED THEN INSERT (
# MAGIC     vehicle_key,
# MAGIC     vehicle_id,
# MAGIC     customer_id,
# MAGIC     plate_number,
# MAGIC     vehicle_brand,
# MAGIC     vehicle_model,
# MAGIC     manufacture_year,
# MAGIC     vehicle_value,
# MAGIC     vehicle_age_years,
# MAGIC     vehicle_value_band
# MAGIC )
# MAGIC VALUES (
# MAGIC     src.vehicle_key,
# MAGIC     src.vehicle_id,
# MAGIC     src.customer_id,
# MAGIC     src.plate_number,
# MAGIC     src.vehicle_brand,
# MAGIC     src.vehicle_model,
# MAGIC     src.manufacture_year,
# MAGIC     src.vehicle_value,
# MAGIC     src.vehicle_age_years,
# MAGIC     src.vehicle_value_band
# MAGIC );

# METADATA ********************

# META {
# META   "language": "sparksql",
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
