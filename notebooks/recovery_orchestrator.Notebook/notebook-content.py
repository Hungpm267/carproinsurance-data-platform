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
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "default_warehouse": "db297df8-2356-b5dd-4b73-02bfbec559f0",
# META       "known_warehouses": [
# META         {
# META           "id": "db297df8-2356-b5dd-4b73-02bfbec559f0",
# META           "type": "Datawarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Recovery orchestrator — gold dim/fact notebooks
# 
# Re-runs only the transforms returned by **`Lookup_Recovery_Errors`**.
# 
# **Pipeline parameter:** `recovery_rows` — JSON array from lookup output:
# 
# ```text
# @if(empty(activity('Lookup_Recovery_Errors').output.value), '[]', string(activity('Lookup_Recovery_Errors').output.value))
# ```

# CELL ********************

import json

import notebookutils

STATUS_RESOLVED = "Resolved"
STATUS_MANUAL = "Need Manual Check"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Populated by pipeline Notebook activity base parameter "recovery_rows"
try:
    _raw = recovery_rows  # noqa: F821 — injected by pipeline
except NameError:
    _raw = "[]"

if _raw is None or (isinstance(_raw, str) and not _raw.strip()):
    _raw = "[]"

def normalize_row(row) -> dict:
    """Lookup / pipeline rows may be dict, Row, or use non-lowercase keys."""
    if hasattr(row, "asDict"):
        row = row.asDict()
    if not isinstance(row, dict):
        row = dict(row)
    return {str(k).lower(): v for k, v in row.items()}


if isinstance(_raw, str):
    _parsed = json.loads(_raw)
elif isinstance(_raw, list):
    _parsed = _raw
else:
    _parsed = list(_raw)

rows = [normalize_row(r) for r in _parsed]

print(f"Recovery rows: {len(rows)}")
if rows:
    print(f"Sample keys: {list(rows[0].keys())}")
    print(f"Sample target_table: {rows[0].get('target_table')}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def table_short(target_table: str) -> str:
    if not target_table:
        return ""
    return target_table.split(".")[-1].lower()


def notebook_path(row: dict) -> str:
    if row.get("notebook_path"):
        return row["notebook_path"]
    return "nb_" + table_short(row.get("target_table", ""))


def is_dim(row: dict) -> bool:
    return table_short(row.get("target_table", "")).startswith("dim_")


def is_fact(row: dict) -> bool:
    return table_short(row.get("target_table", "")).startswith("fact_")


def make_activity(row: dict, deps: list[str]) -> dict:
    path = notebook_path(row)
    wm_col = row.get("watermark_column") or ""
    wm_val = row.get("last_watermark")
    return {
        "name": path,
        "path": path,
        "timeoutPerCellInSeconds": 600,
        "dependencies": deps,
        "args": {
            "watermark_column": wm_col,
            "last_watermark": str(wm_val) if wm_val is not None else "",
            "controller_id": str(row.get("transform_config_id", "")),
            "error_id": str(row.get("error_id", "")),
        },
    }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

meta_by_path = {notebook_path(r): r for r in rows}
dim_rows = [r for r in rows if is_dim(r)]
fact_rows = [r for r in rows if is_fact(r)]
other_rows = [r for r in rows if not is_dim(r) and not is_fact(r)]

activities = []
dim_paths = [notebook_path(r) for r in dim_rows]

for r in dim_rows:
    activities.append(make_activity(r, []))

for r in fact_rows:
    # If recovering dims too, facts wait for those dims; else run facts immediately
    activities.append(make_activity(r, dim_paths))

for r in other_rows:
    activities.append(make_activity(r, []))

print(f"DAG plan: dims={len(dim_rows)}, facts={len(fact_rows)}, other={len(other_rows)}, activities={len(activities)}")
print(f"Activity paths: {[a['path'] for a in activities]}")

if not activities:
    notebookutils.notebook.exit(json.dumps({"failed": 0, "resolved": 0, "manual_check": 0, "rows": []}))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dag = {
    "activities": activities,
    "timeoutInSeconds": 14400,
    "concurrency": max(len(dim_rows), 1) if dim_rows else 1,
    "useRootDefaultLakehouse": True,
}

try:
    results = notebookutils.notebook.runMultiple(dag, {"displayDAGViaGraphviz": True})
except Exception as e:
    print(f"runMultiple error: {e}")
    results = getattr(e, "result", None) or {}

print(f"runMultiple finished: {len(results)} activities")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

payload = []
resolved = 0
manual_check = 0

for name, result in results.items():
    exc = result.get("exception")
    exit_val = result.get("exitVal")
    info = json.loads(exit_val) if exit_val else {}
    cfg = meta_by_path.get(name)
    error_id = (cfg or {}).get("error_id")

    if exc:
        run_status = "Failed"
    elif result.get("status") == "Skipped":
        run_status = "Skipped"
    else:
        run_status = "Succeeded"

    if run_status == "Succeeded":
        resolved += 1
        error_status = STATUS_RESOLVED
    else:
        manual_check += 1
        error_status = STATUS_MANUAL

    payload.append({
        "notebook_path": name,
        "error_id": error_id,
        "transform_config_id": (cfg or {}).get("transform_config_id"),
        "target_table": (cfg or {}).get("target_table"),
        "run_status": run_status,
        "error_status": error_status,
        "rows_read": info.get("rows_read"),
        "rows_inserted": info.get("rows_inserted"),
        "watermark_from": info.get("watermark_from"),
        "watermark_to": info.get("watermark_to"),
        "error": (str(exc)[:4000] if exc else None),
    })

failed = sum(1 for p in payload if p["run_status"] == "Failed")
exit_body = {
    "failed": failed,
    "resolved": resolved,
    "manual_check": manual_check,
    "rows": payload,
}
notebookutils.notebook.exit(json.dumps(exit_body))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
