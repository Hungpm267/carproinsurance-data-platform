# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# PARAMETERS CELL ********************

# Parameters: target_schema, target_table, watermark_column, load_type

wm_to = None
if load_type and load_type.upper() != "FULL" and watermark_column:
    fq = f"{target_schema}.{target_table}"
    row = spark.sql(
        f"SELECT CAST(MAX(`{watermark_column}`) AS STRING) AS wm FROM {fq}"
    ).collect()
    if row and row[0]["wm"] is not None:
        wm_to = row[0]["wm"]

# Fabric pipeline reads this as notebook exit value → Set Variable watermark_to
notebookutils.notebook.exit(wm_to or "")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
