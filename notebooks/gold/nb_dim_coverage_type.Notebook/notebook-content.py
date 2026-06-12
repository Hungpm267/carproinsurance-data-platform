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

USE_MOCK = False

if USE_MOCK:
    df_quot_item_src = spark.createDataFrame([
        ("QI001", "Q001", "Physical Damage", 100_000_000.0, 5_000_000.0),
        ("QI002", "Q002", "Physical Damage", 200_000_000.0, 10_000_000.0),
    ], schema="quotation_item_id STRING, quotation_id STRING, coverage_type STRING, coverage_amount DOUBLE, deductible_amount DOUBLE")
else:
    df_quot_item_src = spark.read.table(f"{silver_schema}.crm_quotation_item")

rows_read = df_quot_item_src.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### **Transformation & SK Generation**

# CELL ********************

TARGET_TABLE = f"{gold_schema}.dim_coverage_type"
SK_COL = "coverage_type_key"
NAME_COL = "coverage_type_name"
SOURCE_COL = "coverage_type"

src_df = (
    df_quot_item_src
    .select(F.initcap(F.trim(F.col(SOURCE_COL))).alias(NAME_COL))
    .filter(F.col(NAME_COL).isNotNull() & (F.col(NAME_COL) != ""))
    .distinct()
)

existing = spark.read.table(TARGET_TABLE).select(NAME_COL, SK_COL)
new_rows = src_df.join(existing, on=NAME_COL, how="left_anti")
new_rows_sk = generate_sk(new_rows, TARGET_TABLE, SK_COL)
new_rows_sk.select(SK_COL, NAME_COL).createOrReplaceTempView("src_dim_coverage_type")

rows_inserted = new_rows.count()

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
# MAGIC MERGE INTO gold.dim_coverage_type AS tgt
# MAGIC USING src_dim_coverage_type AS src
# MAGIC ON tgt.coverage_type_name = src.coverage_type_name
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC     tgt.coverage_type_name = src.coverage_type_name
# MAGIC WHEN NOT MATCHED THEN INSERT (coverage_type_key, coverage_type_name)
# MAGIC VALUES (src.coverage_type_key, src.coverage_type_name);

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
