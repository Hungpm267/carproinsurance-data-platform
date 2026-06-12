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

# ##### **Imports & SK Generate**

# CELL ********************

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import json

silver_schema = "silver"
gold_schema = "gold"

def generate_sk(new_rows_df: DataFrame, target_table: str, sk_col: str) -> DataFrame:
    try:
        max_sk = spark.table(target_table).agg(F.max(sk_col)).collect()[0][0]
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

df_agent = spark.table(f"{silver_schema}.crm_agent")

rows_read = df_agent.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### **Transformation & SK Generation**

# CELL ********************

# SK: agent_key IDENTITY(1,1) ETL generated
# BK: agent_id
# SCD Type 2 tracked cols: agent_name, region, branch, manager_name

silver_schema = "silver"
gold_schema   = "gold"
SOURCE_SILVER = f"{silver_schema}.crm_agent"
TARGET        = f"{gold_schema}.dim_agent"
SK_COL        = "agent_key"
BK_COL        = "agent_id"

AGENT_COLS = [
    SK_COL, "agent_id", "agent_name", "region", "branch",
    "manager_name", "row_hash", "effective_date", "expiry_date", "is_current",
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#  FIRST RUN CHECK
is_first_run = not spark.catalog.tableExists(TARGET)

if is_first_run:
    spark.createDataFrame([], StructType([
        StructField("agent_key",      IntegerType(), True),
        StructField("agent_id",       StringType(),  True),
        StructField("agent_name",     StringType(),  True),
        StructField("region",         StringType(),  True),
        StructField("branch",         StringType(),  True),
        StructField("manager_name",   StringType(),  True),
        StructField("row_hash",       StringType(),  True),
        StructField("effective_date", DateType(),    True),
        StructField("expiry_date",    DateType(),    True),
        StructField("is_current",     StringType(),  True),
    ])) \
    .write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true").saveAsTable(TARGET)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#  LOAD CURRENT DIM 
df_dim_current_full = (
    spark.read.table(TARGET)
    .filter(F.col("is_current") == True)
    .cache()
)

df_dim_current = df_dim_current_full.select(
    BK_COL,
    F.col("row_hash").alias("dim_hash"),
    SK_COL,
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#  CLASSIFY 
df_joined = (
    df_agent
    .join(df_dim_current, on=BK_COL, how="left")
    .cache()
)

df_new     = df_joined.filter(F.col("dim_hash").isNull())
df_changed = df_joined.filter(
    F.col("dim_hash").isNotNull() & (F.col("row_hash") != F.col("dim_hash"))
)

v_new, v_chg = df_new.count(), df_changed.count()
print(f"New: {v_new} | Changed: {v_chg}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# BUILD INSERT 
src_cols = [
    "agent_id", "agent_name", "region", "branch",
    "manager_name", "row_hash", "created_date", "updated_at",
]

df_insert_raw = (
    df_new.select(*src_cols)
          .withColumn("effective_date", F.to_date(F.col("created_date")))
    .unionByName(
        df_changed.select(*src_cols)
                  .withColumn("effective_date", F.to_date(F.col("updated_at")))
    )
    .withColumn("expiry_date", F.lit("9999-12-31").cast("date"))
    .withColumn("is_current",  F.lit(True))
    .drop("created_date", "updated_at")
)

df_insert = generate_sk(df_insert_raw, TARGET, SK_COL).select(*AGENT_COLS)

rows_inserted = df_insert.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#  EXPIRE CHANGED RECORDS
df_expire_updates = (
    df_dim_current_full                      # reuse cache, không đọc lại TARGET
    .join(df_changed.select(BK_COL), on=BK_COL, how="inner")
    .withColumn("is_current",  F.lit(False))
    .withColumn("expiry_date", F.current_date())
    .select(*AGENT_COLS)
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#  MERGE SOURCE 
merge_agent = df_expire_updates.unionByName(df_insert)
merge_agent.createOrReplaceTempView("src_dim_agent")

df_joined.unpersist()
df_dim_current_full.unpersist()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### **Executing MERGE**

# CELL ********************

# MAGIC %%sql
# MAGIC -- 9. MERGE
# MAGIC MERGE INTO gold.dim_agent AS tgt
# MAGIC USING src_dim_agent       AS src
# MAGIC ON tgt.agent_key = src.agent_key
# MAGIC 
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC     tgt.is_current  = src.is_current,
# MAGIC     tgt.expiry_date = src.expiry_date
# MAGIC 
# MAGIC WHEN NOT MATCHED THEN INSERT (
# MAGIC     agent_key, agent_id, agent_name, region, branch,
# MAGIC     manager_name, row_hash, effective_date, expiry_date, is_current
# MAGIC )
# MAGIC VALUES (
# MAGIC     src.agent_key, src.agent_id, src.agent_name, src.region, src.branch,
# MAGIC     src.manager_name, src.row_hash, src.effective_date, src.expiry_date, src.is_current
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
