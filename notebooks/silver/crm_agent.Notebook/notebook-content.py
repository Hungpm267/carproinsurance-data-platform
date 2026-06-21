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

bronze_schema = "bronze"
silver_schema = "silver"
SOURCE_TABLE  = f"{bronze_schema}.crm_agent"
TARGET_SILVER = f"{silver_schema}.crm_agent"
BK_COL        = "agent_id"

HASH_COLS = ["agent_name", "region", "branch", "manager_name"]

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


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_agent_src = spark.read.table(SOURCE_TABLE)

if _wc and _lw:
    df_agent_src = df_agent_src.filter(
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

rows_read = df_agent_src.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#  DEDUP
w_dedup = Window.partitionBy(BK_COL).orderBy(F.col("updated_at").desc())

df_deduped = (
    df_agent_src
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

# CLEAN EXPRESSIONS 
_clean_agent_name   = F.initcap(F.trim(F.regexp_replace(F.col("agent_name").cast("string"),   r"\s+", " ")))
_clean_region       = F.initcap(F.trim(F.col("region").cast("string")))
_clean_branch       = F.initcap(F.trim(F.col("branch").cast("string")))
_clean_manager_name = F.initcap(F.trim(F.regexp_replace(F.col("manager_name").cast("string"), r"\s+", " ")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# TRANSFORM + ROW HASH 
df_agent_silver = (
    df_deduped
    .filter(
        F.col("agent_id").isNotNull() &
        (F.trim(F.col("agent_id").cast("string")) != "") &
        (F.upper(F.trim(F.col("agent_id").cast("string"))) != "UNKNOWN")
    )
    .select(
        F.upper(F.trim(F.col("agent_id").cast("string"))).alias("agent_id"),
        _clean_agent_name.alias("agent_name"),
        _clean_region.alias("region"),
        _clean_branch.alias("branch"),
        _clean_manager_name.alias("manager_name"),
        F.to_date(F.col("created_date")).alias("created_date"),
        F.col("updated_at"),
    )
    .withColumn(
        "row_hash",
        F.sha2(
            F.concat_ws(
                "||",
                *[F.coalesce(F.col(c), F.lit("")) for c in HASH_COLS]
            ),
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
    new_watermark = df_agent_silver.agg(F.max(F.col(_wc))).collect()[0][0]
print(f"[INFO] new_watermark = {new_watermark}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if df_agent_silver.limit(1).count() > 0:

    if DeltaTable.isDeltaTable(spark, TARGET_SILVER):
        # Table exists → MERGE (upsert)
        delta_target = DeltaTable.forName(spark, TARGET_SILVER)

        delta_target.alias("tgt").merge(
            df_agent_silver.alias("src"),
            f"tgt.{BK_COL} = src.{BK_COL}"              
        ).whenMatchedUpdate(
            condition="src.row_hash != tgt.row_hash",    
            set={
                "agent_name"  : "src.agent_name",
                "region"      : "src.region",
                "branch"      : "src.branch",
                "manager_name": "src.manager_name",
                "is_active"   : "src.is_active",
                "created_date": "src.created_date",
                "updated_at"  : f"src.{_wc}" if _wc else "tgt.updated_at",
                "row_hash"    : "src.row_hash",
            }
        ).whenNotMatchedInsertAll(                        
        ).execute()

        print("[INFO] MERGE completed successfully")

    else:
        # Table does not exist yet → initial full load
        df_agent_silver.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(TARGET_SILVER)
        print("[INFO] Initial full load completed")


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
    # rows_inserted = df_customer_silver.count() old: wrong df
    rows_inserted = df_agent_silver.count()
    
else:
    rows_inserted = 0

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
