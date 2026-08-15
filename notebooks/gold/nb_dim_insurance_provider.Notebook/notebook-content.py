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

df_provider_src = spark.read.table(f"{silver_schema}.crm_insurance_provider")

rows_read = df_provider_src.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### **Transformation & SK Generation**

# CELL ********************

# SK: provider_key IDENTITY(1,1) ETL generated
# BK: provider_code

silver_schema = "silver"
gold_schema   = "gold"
SOURCE_SILVER = f"{silver_schema}.crm_insurance_provider"
TARGET        = f"{gold_schema}.dim_insurance_provider"
SK_COL        = "provider_key"
BK_COL        = "provider_code"

df_provider = spark.read.table(SOURCE_SILVER)

PROVIDER_COLS = [
    SK_COL, "provider_code", "provider_name", "provider_group", "is_active"
]

# Identify new rows
existing_provider = spark.read.table(TARGET).select(BK_COL, SK_COL)
new_providers     = df_provider.join(existing_provider, on=BK_COL, how="left_anti")

# Generate SK for new rows
new_providers_sk = generate_sk(new_providers, TARGET, SK_COL)

# Build merge source
existing_providers_full = (
    df_provider
    .join(existing_provider, on=BK_COL, how="inner")
    .select(*PROVIDER_COLS)
)
merge_provider = existing_providers_full.unionByName(
    new_providers_sk.select(*PROVIDER_COLS)
)
merge_provider.createOrReplaceTempView("src_dim_provider")

rows_inserted = new_providers.count()

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
# MAGIC MERGE INTO gold.dim_insurance_provider AS tgt
# MAGIC USING src_dim_provider AS src
# MAGIC ON tgt.provider_code = src.provider_code
# MAGIC  
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC     tgt.provider_name = src.provider_name,
# MAGIC     tgt.provider_group = src.provider_group,
# MAGIC     tgt.is_active = src.is_active
# MAGIC  
# MAGIC WHEN NOT MATCHED THEN INSERT (
# MAGIC     provider_key,
# MAGIC     provider_code,
# MAGIC     provider_name,
# MAGIC     provider_group,
# MAGIC     is_active
# MAGIC )
# MAGIC VALUES (
# MAGIC     src.provider_key,
# MAGIC     src.provider_code,
# MAGIC     src.provider_name,
# MAGIC     src.provider_group,
# MAGIC     src.is_active
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
