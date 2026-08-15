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

fd          = F.col("full_date")
week_start  = F.date_trunc("week", fd).cast("date") 
month_start = F.trunc(fd, "MM")                       
month_end   = F.last_day(fd)   

dim_date = (
    df_dates
    .select(
        (F.year(fd) * 10000 + F.month(fd) * 100 + F.dayofmonth(fd))
            .cast("int").alias("date_key"),
        fd.cast("date").alias("full_date"),

        F.dayofmonth(fd).cast("int").alias("day_of_month"),
        F.dayofyear(fd).cast("int").alias("day_of_year"),
        F.dayofweek(fd).cast("int").alias("day_of_week"),               # 1=Sun … 7=Sat
        F.date_format(fd, "EEEE").alias("day_name"),                    # Monday
        F.date_format(fd, "EEE").alias("day_name_short"),              # Mon
        F.when(F.dayofweek(fd).isin(1, 7), F.lit(True))
         .otherwise(F.lit(False)).alias("is_weekend"),

        F.weekofyear(fd).cast("int").alias("week_of_year"),
        week_start.alias("week_start_date"),
        F.date_add(week_start, 6).alias("week_end_date"),

        F.month(fd).cast("int").alias("month_number"),
        F.date_format(fd, "MMMM").alias("month_name"),                  # January
        F.date_format(fd, "MMM").alias("month_name_short"),            # Jan
        F.dayofmonth(month_end).cast("int").alias("days_in_month"),
        month_start.alias("month_start_date"),
        month_end.alias("month_end_date"),
        (F.dayofmonth(fd) == 1).alias("is_month_start"),
        (fd == month_end).alias("is_month_end"),

        F.year(fd).cast("int").alias("year_number"),

        # Year-month
        F.date_format(fd, "yyyy-MM").alias("year_month"),              # 2026-01
        (F.year(fd) * 100 + F.month(fd)).cast("int").alias("year_month_sort_key"),  # 202601
        F.date_format(fd, "MMM yyyy").alias("year_month_label"),       # Jan 2026
        F.date_format(fd, "MMM yy").alias("year_month_short"),         # Jan 26

        # Quarter
        F.quarter(fd).cast("int").alias("quarter_number"),
        F.concat(F.lit("Q"), F.quarter(fd).cast("string")).alias("quarter_name"),          # Q1
        F.concat(F.lit("Q"), F.quarter(fd).cast("string"),
                 F.lit(" "), F.year(fd).cast("string")).alias("quarter_year_label"),        # Q2 2026
        (F.year(fd) * 10 + F.quarter(fd)).cast("int").alias("quarter_year_sort_key"),       # 20262

        F.year(fd).cast("int").alias("fiscal_year"),
        F.quarter(fd).cast("int").alias("fiscal_quarter"),
    )
)

# spark.sql(f"DROP TABLE IF EXISTS {TARGET}")
dim_date.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(TARGET)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
