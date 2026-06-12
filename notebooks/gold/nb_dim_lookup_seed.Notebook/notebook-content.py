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
from pyspark.sql import Row

bronze_schema = "bronze"
gold_schema = "gold"

def generate_sk(new_rows_df: DataFrame, target_table: str, sk_col: str) -> DataFrame:
    try:
        max_sk = spark.table(target_table).agg(F.max(sk_col)).collect()[0][0]
        max_sk = max_sk if max_sk is not None else 0
    except Exception:
        max_sk = 0

    window = Window.orderBy(F.monotonically_increasing_id())

    return new_rows_df.withColumn(
        sk_col,
        (F.row_number().over(window) + F.lit(max_sk)).cast("int")
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_quotation_src = spark.table(f"{bronze_schema}.crm_quotation")
df_policy_src = spark.table(f"{bronze_schema}.policy_policy")
df_payment_src = spark.table(f"{bronze_schema}.policy_payment")
df_quot_item_src = spark.table(f"{bronze_schema}.crm_quotation_item")
df_cancel_src = spark.table(f"{bronze_schema}.policy_cancellation")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def seed_lookup_dim(
    source_df: DataFrame,
    source_col: str,
    target_table: str,
    sk_col: str,
    name_col: str
):
    src = (
        source_df
        .select(F.col(source_col).alias(name_col))
        .distinct()
        .filter(F.col(name_col).isNotNull())
        .filter(F.col(name_col) != "")
    )

    existing = spark.table(target_table).select(name_col, sk_col)

    new_rows = src.join(existing, on=name_col, how="left_anti")

    new_rows_sk = generate_sk(new_rows, target_table, sk_col)

    rows_to_insert = new_rows_sk.count()

    if rows_to_insert > 0:
        (
            new_rows_sk
            .select(sk_col, name_col)
            .write
            .mode("append")
            .saveAsTable(target_table)
        )

    print(f"{target_table:<40} inserted rows = {rows_to_insert}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

seed_lookup_dim(
    df_quotation_src,
    "product_package_name",
    f"{gold_schema}.dim_product_package",
    "product_package_key",
    "product_package_name"
)

seed_lookup_dim(
    df_quotation_src,
    "quotation_status_name",
    f"{gold_schema}.dim_quotation_status",
    "quotation_status_key",
    "quotation_status_name"
)

seed_lookup_dim(
    df_policy_src,
    "policy_status_name",
    f"{gold_schema}.dim_policy_status",
    "policy_status_key",
    "policy_status_name"
)

seed_lookup_dim(
    df_payment_src,
    "payment_method_name",
    f"{gold_schema}.dim_payment_method",
    "payment_method_key",
    "payment_method_name"
)

seed_lookup_dim(
    df_payment_src,
    "payment_status_name",
    f"{gold_schema}.dim_payment_status",
    "payment_status_key",
    "payment_status_name"
)

seed_lookup_dim(
    df_quot_item_src,
    "coverage_type_name",
    f"{gold_schema}.dim_coverage_type",
    "coverage_type_key",
    "coverage_type_name"
)

seed_lookup_dim(
    df_cancel_src,
    "cancellation_reason_name",
    f"{gold_schema}.dim_cancellation_reason",
    "cancellation_reason_key",
    "cancellation_reason_name"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def seed_unknown_member(table_name: str, key_col: str, name_col: str):
    full_table_name = f"{gold_schema}.{table_name}"

    existing_count = (
        spark.table(full_table_name)
        .filter(F.col(key_col) == -1)
        .count()
    )

    if existing_count == 0:
        df_unknown = spark.createDataFrame([
            Row(**{
                key_col: -1,
                name_col: "UNKNOWN"
            })
        ])

        df_unknown.write.mode("append").saveAsTable(full_table_name)

        print(f"{full_table_name:<40} inserted UNKNOWN member")
    else:
        print(f"{full_table_name:<40} UNKNOWN already exists")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

lookup_dims = [
    ("dim_product_package", "product_package_key", "product_package_name"),
    ("dim_quotation_status", "quotation_status_key", "quotation_status_name"),
    ("dim_policy_status", "policy_status_key", "policy_status_name"),
    ("dim_payment_method", "payment_method_key", "payment_method_name"),
    ("dim_payment_status", "payment_status_key", "payment_status_name"),
    ("dim_coverage_type", "coverage_type_key", "coverage_type_name"),
    ("dim_cancellation_reason", "cancellation_reason_key", "cancellation_reason_name"),
]

for table_name, key_col, name_col in lookup_dims:
    seed_unknown_member(table_name, key_col, name_col)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
