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
# META       "default_warehouse": "10f24a18-40e7-90f4-438b-b0056974ed4f",
# META       "known_warehouses": [
# META         {
# META           "id": "10f24a18-40e7-90f4-438b-b0056974ed4f",
# META           "type": "Datawarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import notebookutils, json

cfg = spark.sql("""
    SELECT target_table, transform_config_id,
           watermark_column, last_watermark
    FROM   insurance_warehouse.meta.etl_transform_config
""").collect()
display(cfg)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def tbl(r):
    return r.target_table.split(".")[-1].lower()

def nb_name(r):
    return "nb_" + tbl(r)

cfg_dim  = [r for r in cfg if tbl(r).startswith("dim_")]
cfg_fact = [r for r in cfg if tbl(r).startswith("fact_")]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

meta = {nb_name(r): r for r in cfg_dim + cfg_fact}

def make_activity(r, deps):
    return {
        "name": nb_name(r),
        "path": nb_name(r),
        "timeoutPerCellInSeconds": 600,
        "dependencies": deps,
        "args": {
            "watermark_column": r.watermark_column or "",
            "last_watermark":   str(r.last_watermark) if r.last_watermark is not None else "",
            "controller_id":    str(r.transform_config_id),
        },
    }

activities = []

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_names = [nb_name(r) for r in cfg_dim]
for r in cfg_dim:
    activities.append(make_activity(r, []))

for r in cfg_fact:
    activities.append(make_activity(r, dim_names))

DAG = {
    "activities": activities,
    "timeoutInSeconds": 14400,
    "concurrency": len(cfg_dim) or 10,
}

try:
    results = notebookutils.notebook.runMultiple(DAG, {"displayDAGViaGraphviz": True})
except Exception as e:
    results = getattr(e, "result", None) or {}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

payload = []
for name, r in results.items():
    exc     = r.get("exception")
    exitVal = r.get("exitVal")
    info    = json.loads(exitVal) if exitVal else {}
    c       = meta.get(name)            

    payload.append({
        "notebook_id":   name,
        "controller_id": (c.transform_config_id if c else None),
        "target_table":  (c.target_table if c else None),
        "status":        "Failed"  if exc
                         else "Skipped" if r.get("status") == "Skipped"
                         else "Succeeded",
        "rows_read":      info.get("rows_read"),
        "rows_inserted":  info.get("rows_inserted"),
        "watermark_from": info.get("watermark_from"),
        "watermark_to":   info.get("watermark_to"),
        "error":          (str(exc)[:4000] if exc else None),
    })

failed = sum(1 for p in payload if p["status"] == "Failed")
notebookutils.notebook.exit(json.dumps({"failed": failed, "rows": payload}))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
