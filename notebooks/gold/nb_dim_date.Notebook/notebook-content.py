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

from pyspark.sql import functions as F

gold_schema = "gold"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### **Load Data**

# CELL ********************

USE_MOCK = False 

if USE_MOCK:
    date_start, date_end = "2024-01-01", "2025-12-31"
else:
    date_start, date_end = "2024-01-01", "2027-12-31"

df_dates = spark.sql(f"""
    SELECT explode(sequence(
        to_date('{date_start}'),
        to_date('{date_end}'),
        interval 1 day
    )) AS full_date
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### **Transformation**

# CELL ********************

# SK : date_key = YYYYMMDD (natural key — integer)
# SCD Type 0 : static dimension, full overwrite once

TARGET = f"{gold_schema}.dim_date"

dim_date = (
    df_dates
    .select(
        (
            F.year("full_date") * 10000
            + F.month("full_date") * 100
            + F.dayofmonth("full_date")
        ).cast("int").alias("date_key"),

        F.col("full_date"),
        F.dayofweek("full_date").cast("int").alias("day_of_week"),
        F.date_format("full_date", "EEEE").alias("day_name"),
        F.weekofyear("full_date").cast("int").alias("week_of_year"),
        F.month("full_date").cast("int").alias("month_number"),
        F.date_format("full_date", "MMMM").alias("month_name"),
        F.quarter("full_date").cast("int").alias("quarter_number"),
        F.concat(F.lit("Q"), F.quarter("full_date")).alias("quarter_name"),
        F.year("full_date").cast("int").alias("year_number"),
        F.when(F.dayofweek("full_date").isin(1, 7), F.lit(True))
         .otherwise(F.lit(False))
         .alias("is_weekend"),
        F.year("full_date").cast("int").alias("fiscal_year"),
        F.quarter("full_date").cast("int").alias("fiscal_quarter"),
    )
)
#  SCD0
dim_date.write.format("delta").mode("overwrite").saveAsTable(TARGET)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
