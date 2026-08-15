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

# Silver: crm_customer
# BK: customer_id
# Responsibility: dedup, cleanse, validate, row_hash → silver.crm_customer

bronze_schema = "bronze"
silver_schema = "silver"
SOURCE_TABLE  = f"{bronze_schema}.crm_customer"
TARGET_SILVER = f"{silver_schema}.crm_customer"
BK_COL        = "customer_id"



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
df_customer_src = spark.read.table(SOURCE_TABLE)

if _wc and _lw:
    df_customer_src = df_customer_src.filter(
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

rows_read = df_customer_src.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# DEDUP 
w_dedup = Window.partitionBy(BK_COL).orderBy(F.col("updated_at").desc())

df_deduped = (
    df_customer_src
    .withColumn("_rn", F.row_number().over(w_dedup))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#  CLEAN EXPRESSIONS
_clean_full_name = F.initcap(
    F.trim(F.regexp_replace(F.col("full_name").cast("string"), r"\s+", " "))
)
_clean_gender = (
    F.when(
        F.upper(F.trim(F.col("gender").cast("string"))).isin("M", "MALE", "NAM"),
        F.lit("Male")
    )
    .when(
        F.upper(F.trim(F.col("gender").cast("string"))).isin("F", "FEMALE", "NỮ", "NU"),
        F.lit("Female")
    )
    .otherwise(F.lit("Unknown"))
)
_raw_dob   = F.to_date(F.col("dob").cast("string"))
_min_dob   = F.to_date(F.lit("1900-01-01"))
_clean_dob = (
    F.when(_raw_dob.isNull(), F.lit(None).cast("date"))
     .when((_raw_dob < _min_dob) | (_raw_dob > F.current_date()), F.lit(None).cast("date"))
     .otherwise(_raw_dob)
)
_clean_age = (
    F.when(_clean_dob.isNull(), F.lit(None).cast("int"))
     .otherwise(F.floor(F.months_between(F.current_date(), _clean_dob) / 12).cast("int"))
)
_clean_phone = F.trim(
    F.regexp_replace(F.col("phone_number").cast("string"), r"[^\d+]", "")
)
_clean_email = (
    F.when(
        F.lower(F.trim(F.col("email").cast("string"))).rlike(r"^[^@]+@[^@]+\.[^@]+$"),
        F.lower(F.trim(F.col("email").cast("string")))
    )
    .otherwise(F.lit(None).cast("string"))
)
_clean_city     = F.initcap(F.trim(F.col("city").cast("string")))
_clean_district = F.initcap(F.trim(F.col("district").cast("string")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# TRANSFORM 
HASH_COLS = [
    "customer_id", "full_name", "gender", "date_of_birth",
    "phone_number", "email", "city", "district",
]

df_customer_silver = (
    df_deduped
    .filter(
        F.col("customer_id").isNotNull() &
        (F.trim(F.col("customer_id").cast("string")) != "") &
        (F.upper(F.trim(F.col("customer_id").cast("string"))) != "UNKNOWN")
    )
    .select(
        F.upper(F.trim(F.col("customer_id").cast("string"))).alias("customer_id"),
        _clean_full_name.alias("full_name"),
        _clean_gender.alias("gender"),
        _clean_dob.alias("date_of_birth"),
        _clean_age.alias("age"),
        _clean_phone.alias("phone_number"),
        _clean_email.alias("email"),
        _clean_city.alias("city"),
        _clean_district.alias("district"),
        F.to_date(F.col("created_date")).alias("customer_since_date"),
        F.to_date(F.col("created_date")).alias("created_date"),
        F.col("updated_at"),
    )
    .withColumn(
        "row_hash",
        F.sha2(
            F.concat_ws("||", *[F.coalesce(F.col(c), F.lit("")) for c in HASH_COLS]),
            256
        )
    )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

new_watermark = None
if _wc:
    new_watermark = df_customer_silver.agg(F.max(F.col(_wc))).collect()[0][0]
print(f"[INFO] new_watermark = {new_watermark}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if df_customer_silver.limit(1).count() > 0:
    df_customer_silver.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(TARGET_SILVER)
    rows_inserted = df_customer_silver.count()
else:
    rows_inserted = 0
    

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if new_watermark is not None and _ci:
    spark.sql(f"""
        UPDATE insurance_lakehouse.meta.etl_transform_config
        SET    last_watermark = '{new_watermark}'
        WHERE  transform_config_id = {_ci}
    """)

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
