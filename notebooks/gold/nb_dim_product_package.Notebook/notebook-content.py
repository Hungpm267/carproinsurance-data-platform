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
    df_quotation_src = spark.createDataFrame([
        ("Q001", "C001", "A001", "P001", "Pending",  "PKG-A", 5_000_000.0, None),
        ("Q002", "C002", "A002", "P002", "Approved", "PKG-B", 8_000_000.0, None),
    ], schema="quotation_id STRING, customer_id STRING, agent_id STRING, provider_code STRING, quotation_status STRING, package_code STRING, premium_amount DOUBLE, quotation_expiry_date STRING")
else:
    df_quotation_src = spark.read.table(f"{silver_schema}.crm_quotation")

rows_read = df_quotation_src.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### **Transformation & SK Generation**

# CELL ********************

TARGET_TABLE = f"{gold_schema}.dim_product_package"
SK_COL = "product_package_key"
NAME_COL = "product_package_name"
SOURCE_COL = "package_code"

src_df = (
    df_quotation_src
    .select(F.upper(F.trim(F.col(SOURCE_COL))).alias(NAME_COL))
    .filter(F.col(NAME_COL).isNotNull() & (F.col(NAME_COL) != ""))
    .distinct()
)

existing = spark.read.table(TARGET_TABLE).select(NAME_COL, SK_COL)
new_rows = src_df.join(existing, on=NAME_COL, how="left_anti")
new_rows_sk = generate_sk(new_rows, TARGET_TABLE, SK_COL)
new_rows_sk.select(SK_COL, NAME_COL).createOrReplaceTempView("src_dim_product_package")

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
# MAGIC MERGE INTO gold.dim_product_package AS tgt
# MAGIC USING src_dim_product_package AS src
# MAGIC ON tgt.product_package_name = src.product_package_name
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC     tgt.product_package_name = src.product_package_name
# MAGIC WHEN NOT MATCHED THEN INSERT (product_package_key, product_package_name)
# MAGIC VALUES (src.product_package_key, src.product_package_name);

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
