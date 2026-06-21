# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "44e67c59-51d0-48bf-9ab0-8941a5d7c912",
# META       "default_lakehouse_name": "insurance_lakehouse_test",
# META       "default_lakehouse_workspace_id": "e13dac5b-f5b1-4169-bb58-0f6d0bfea366",
# META       "known_lakehouses": [
# META         {
# META           "id": "44e67c59-51d0-48bf-9ab0-8941a5d7c912"
# META         },
# META         {
# META           "id": "0aa0a14b-3288-4f9f-93d1-d888edaf7070"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Default lakehouse = insurance_lakehouse_test (writes land here)
SOURCE = "insurance_lakehouse"

TABLES = [
    # bronze
    ("bronze", "crm_customer"),
    ("bronze", "crm_agent"),
    ("bronze", "crm_insurance_provider"),
    ("bronze", "crm_vehicle"),
    ("bronze", "crm_quotation"),
    ("bronze", "crm_quotation_item"),
    ("bronze", "policy_policy"),
    ("bronze", "policy_payment"),
    ("bronze", "policy_cancellation"),
    # silver
    ("silver", "crm_customer"),
    ("silver", "crm_agent"),
    ("silver", "crm_insurance_provider"),
    ("silver", "crm_vehicle"),
    ("silver", "crm_quotation"),
    ("silver", "crm_quotation_item"),
    ("silver", "policy_policy"),
    ("silver", "policy_payment"),
    ("silver", "policy_cancellation"),
    # gold dims
    ("gold", "dim_customer"),
    ("gold", "dim_agent"),
    ("gold", "dim_insurance_provider"),
    ("gold", "dim_vehicle"),
    ("gold", "dim_product_package"),
    ("gold", "dim_quotation_status"),
    ("gold", "dim_coverage_type"),
    ("gold", "dim_cancellation_reason"),
    ("gold", "dim_policy_status"),
    ("gold", "dim_payment_method"),
    ("gold", "dim_payment_status"),
    # gold facts
    ("gold", "fact_quotation"),
    ("gold", "fact_quotation_item"),
    ("gold", "fact_policy"),
    ("gold", "fact_payment"),
    ("gold", "fact_cancellation"),
]

for schema, table in TABLES:
    src = f"{SOURCE}.{schema}.{table}"
    tgt = f"{schema}.{table}"
    print(f"Copying {src} -> {tgt}")
    (
        spark.table(src)
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(tgt)
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -- %%sql
# -- TRUNCATE TABLE gold.fact_cancellation;
# -- TRUNCATE TABLE gold.fact_payment;
# -- TRUNCATE TABLE gold.fact_policy;
# -- TRUNCATE TABLE gold.fact_quotation_item;
# -- TRUNCATE TABLE gold.fact_quotation;
# -- TRUNCATE TABLE gold.dim_customer;

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

POLICY_SILVER = [
    "silver.policy_cancellation",
    "silver.policy_payment",
    "silver.policy_policy",
]

GOLD_ALL = [
    # facts
    "gold.fact_cancellation",
    "gold.fact_payment",
    "gold.fact_policy",
    "gold.fact_quotation",
    "gold.fact_quotation_item",
    # dims
    "gold.dim_customer",
    "gold.dim_agent",
    "gold.dim_insurance_provider",
    "gold.dim_vehicle",
    "gold.dim_product_package",
    "gold.dim_quotation_status",
    "gold.dim_coverage_type",
    "gold.dim_cancellation_reason",
    "gold.dim_policy_status",
    "gold.dim_payment_method",
    "gold.dim_payment_status",
]

def clear_table(fqn: str):
    try:
        spark.sql(f"TRUNCATE TABLE {fqn}")
    except Exception:
        spark.sql(f"DELETE FROM {fqn}")
    print(f"Cleared {fqn} -> {spark.table(fqn).count()} rows")

for t in POLICY_SILVER + GOLD_ALL:
    clear_table(t)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
