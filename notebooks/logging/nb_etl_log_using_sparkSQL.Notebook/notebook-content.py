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

# PARAMETERS CELL ********************

action            = ""   
pipeline_run_id   = ""    
controller_id     = 0    
pipeline_name     = ""    
status            = ""    
rows_read         = 0
rows_inserted     = 0
rows_updated      = 0
rows_rejected     = 0
watermark_from    = ""
watermark_to      = ""
error_message     = ""  
error_code        = ""     
layer_name        = ""     
target_table      = ""    
severity          = "ERROR" 
bad_record_content = ""    

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import time

LOG = "insurance_lakehouse.meta.etl_execution_log"
ERR = "insurance_lakehouse.meta.error_logging_table"

emsg = error_message.replace("'", "''")
bad  = bad_record_content.replace("'", "''")


if action in ("start_pipeline", "start_table"):
    spark.sql(f"""
        INSERT INTO {LOG} (
            log_id, pipeline_run_id, controller_id, pipeline_name,
            start_time, end_time, status,
            rows_read, rows_inserted, rows_updated, rows_rejected,
            watermark_from, watermark_to,
            error_message, logged_at
        )
        VALUES (
            {int(time.time_ns()//1000)}, '{pipeline_run_id}', {controller_id}, '{pipeline_name}',
            current_timestamp(), NULL, 'Running',
            0, 0, 0, 0,
            NULL, NULL, NULL, current_timestamp()
        )
    """)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if action in ("end_pipeline", "end_table"):
    if target_table:
            h = (spark.sql(f"DESCRIBE HISTORY {target_table} LIMIT 1")
                      .collect()[0]["operationMetrics"]) or {}
            rows_inserted = int(h.get("numTargetRowsInserted", h.get("numOutputRows", 0)))
            rows_updated  = int(h.get("numTargetRowsUpdated", 0))
            rows_read     = int(h.get("numSourceRows", h.get("numOutputRows", 0)))

    spark.sql(f"""
        UPDATE {LOG} SET
            status         = '{status}',
            end_time       = current_timestamp(),
            rows_read      = {rows_read},
            rows_inserted  = {rows_inserted},
            rows_updated   = {rows_updated},
            rows_rejected  = {rows_rejected},
            watermark_from = NULLIF('{watermark_from}', ''),
            watermark_to   = NULLIF('{watermark_to}', ''),
            error_message  = NULLIF('{emsg}', '')
        WHERE pipeline_run_id = '{pipeline_run_id}'
          AND controller_id   = '{controller_id}'
    """)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if action == "log_error":
    spark.sql(f"""
        INSERT INTO {ERR} (
            error_id, execution_id, error_pipelinename, error_timestamp,
            error_code, layer_name, target_table, error_message,
            error_severity_level, bad_record_content, status, updated_at
        )
        SELECT
            uuid(),
            COALESCE((SELECT MAX(log_id) FROM {LOG}
                      WHERE pipeline_run_id = '{pipeline_run_id}'
                        AND pipeline_name   = '{pipeline_name}'), 0),
            '{pipeline_name}', current_timestamp(),
            '{error_code}', '{layer_name}', '{target_table}', '{emsg}',
            '{severity}', NULLIF('{bad}', ''), 'New', current_timestamp()
    """)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
