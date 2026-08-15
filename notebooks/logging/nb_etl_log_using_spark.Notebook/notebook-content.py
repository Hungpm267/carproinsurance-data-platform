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

# # `nb_etl_log` — single logging notebook for pipelines
# 
# Attach the **lakehouse** on every pipeline Notebook activity. Pass parameter **`action`**:
# 
# | `action` | When | Returns |
# |----------|------|--------|
# | `pipeline_start` | After pipeline gate | `log_id` |
# | `entity_start` | ForEach, before Copy | `log_id` |
# | `entity_success` | After Copy success | `watermark_to` (incremental) |
# | `entity_failed` | Copy failure branch | raises (fails activity) |
# | `pipeline_end` | After ForEach | `status` |
# 
# Unused parameters can be left at defaults. See [`docs/INGEST_CRM_PIPELINE_LOGGING.md`](../../docs/INGEST_CRM_PIPELINE_LOGGING.md).

# PARAMETERS CELL ********************

# All parameters must be strings when passed from Fabric pipelines.
action = "pipeline_start"

pipeline_run_id = "manual_run"
pipeline_name = "ingest_crm_to_bronze"
controller_id = "0"
log_id = ""

watermark_from = ""
ingestion_config_id = ""
target_schema = "bronze"
target_table = ""
watermark_column = ""
load_type = "INCREMENTAL"

rows_read = "0"
rows_inserted = "0"
auto_row_counts = "true"

error_message = ""
pipeline_status = "Success"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import traceback
import uuid
from datetime import datetime

import notebookutils
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, to_timestamp
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

EXEC_LOG = "meta.etl_execution_log"
ERROR_LOG = "meta.error_logging_table"
INGESTION_CONFIG = "meta.etl_ingestion_config"

EXEC_SCHEMA = StructType(
    [
        StructField("log_id", LongType(), False),
        StructField("pipeline_run_id", StringType(), False),
        StructField("controller_id", LongType(), False),
        StructField("pipeline_name", StringType(), False),
        StructField("start_time", TimestampType(), False),
        StructField("end_time", TimestampType(), True),
        StructField("status", StringType(), False),
        StructField("rows_read", LongType(), True),
        StructField("rows_inserted", LongType(), True),
        StructField("rows_updated", LongType(), True),
        StructField("rows_rejected", LongType(), True),
        StructField("watermark_from", StringType(), True),
        StructField("watermark_to", StringType(), True),
        StructField("dynamic_source_file", StringType(), True),
        StructField("error_message", StringType(), True),
        StructField("logged_at", TimestampType(), False),
    ]
)

ERROR_SCHEMA = StructType(
    [
        StructField("error_id", StringType(), False),
        StructField("execution_id", LongType(), False),
        StructField("error_pipelinename", StringType(), False),
        StructField("error_timestamp", TimestampType(), False),
        StructField("error_code", StringType(), False),
        StructField("layer_name", StringType(), False),
        StructField("target_table", StringType(), False),
        StructField("error_message", StringType(), False),
        StructField("error_severity_level", StringType(), False),
        StructField("bad_record_content", StringType(), True),
        StructField("status", StringType(), False),
        StructField("updated_at", TimestampType(), False),
    ]
)

VALID_ACTIONS = (
    "pipeline_start",
    "entity_start",
    "entity_success",
    "entity_failed",
    "pipeline_end",
)


def _opt_str(value):
    if value is None:
        return None
    s = str(value).strip()
    return s if s else None


def _to_int(value, default=0, name="value"):
    if value is None:
        return default
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    s = str(value).strip()
    if not s or s.lower() in ("none", "null", ""):
        return default
    try:
        return int(float(s))
    except ValueError as exc:
        raise ValueError(f"Invalid {name}: {value!r}") from exc


def _parse_bool(value, default=True):
    if value is None:
        return default
    s = str(value).strip().lower()
    if s in ("false", "0", "no", "n"):
        return False
    if s in ("true", "1", "yes", "y", ""):
        return True
    return default


def _next_log_id(spark):
    if spark.catalog.tableExists(EXEC_LOG):
        return int(
            spark.sql(f"SELECT COALESCE(MAX(log_id), 0) + 1 AS n FROM {EXEC_LOG}")
            .collect()[0]["n"]
        )
    return 1


def _insert_running(spark, pipeline_run_id, controller_id, pipeline_name, watermark_from=None):
    new_id = _next_log_id(spark)
    now = datetime.now()
    row = [
        (
            new_id,
            pipeline_run_id,
            _to_int(controller_id, name="controller_id"),
            pipeline_name,
            now,
            None,
            "Running",
            0,
            0,
            0,
            0,
            watermark_from,
            None,
            None,
            None,
            now,
        )
    ]
    spark.createDataFrame(row, EXEC_SCHEMA).write.format("delta").mode("append").saveAsTable(
        EXEC_LOG
    )
    return new_id


def _write_error_row(
    spark,
    log_id,
    pipeline_name,
    target_table,
    error_code,
    message,
    bad_record=None,
):
    now = datetime.now()
    spark.createDataFrame(
        [
            (
                str(uuid.uuid4()),
                _to_int(log_id, name="log_id"),
                pipeline_name,
                now,
                error_code,
                "bronze",
                target_table or "unknown",
                str(message)[:2000],
                "ERROR",
                bad_record,
                "New",
                now,
            )
        ],
        ERROR_SCHEMA,
    ).write.format("delta").mode("append").saveAsTable(ERROR_LOG)


def _compute_watermark(spark, schema, table, column):
    col = str(column).replace("`", "")
    fq = f"{schema}.{table}"
    row = spark.sql(f"SELECT CAST(MAX(`{col}`) AS STRING) AS hw FROM {fq}").collect()[0]
    return _opt_str(row["hw"])


def _normalize_watermark(value):
    """Spark-friendly timestamp literal (Copy often passes ISO with T)."""
    s = str(value).strip()
    if s.endswith("Z"):
        s = s[:-1]
    return s.replace("T", " ")


def _operation_metrics_dict(metrics):
    if metrics is None:
        return {}
    if hasattr(metrics, "asDict"):
        return metrics.asDict()
    if isinstance(metrics, dict):
        return metrics
    return {}


def _delta_last_write_counts(spark, schema, table):
    """Rows written by the latest Delta operation on the bronze table (post-Copy)."""
    fq = f"{schema}.{table}"
    if not spark.catalog.tableExists(fq):
        return None
    try:
        # Spark SQL: DESCRIBE HISTORY does not support ORDER BY (ParseException on Fabric).
        hist = DeltaTable.forName(spark, fq).history(1).collect()
    except Exception:
        try:
            hist = spark.sql(f"DESCRIBE HISTORY {fq} LIMIT 1").collect()
        except Exception:
            return None
    if not hist:
        return None
    om = _operation_metrics_dict(hist[0]["operationMetrics"])
    inserted = _to_int(
        om.get("numTargetRowsInserted") or om.get("numInsertedRows") or 0,
        name="numTargetRowsInserted",
    )
    if inserted <= 0:
        inserted = _to_int(
            om.get("numOutputRows") or om.get("numTargetRowsInserted") or 0,
            name="numOutputRows",
        )
    return inserted


def _count_rows_after_watermark(spark, schema, table, column, wm_from):
    """Match Copy filter: watermark_column > var_watermark (no upper bound)."""
    col_name = str(column).replace("`", "")
    wm = _normalize_watermark(wm_from)
    df = spark.table(f"{schema}.{table}")
    return int(
        df.filter(col(col_name).cast("timestamp") > to_timestamp(lit(wm))).count()
    )


def _resolve_row_counts(
    spark,
    load_type,
    target_schema,
    target_table,
    watermark_column,
    wm_from,
    rows_read,
    rows_inserted,
    auto_row_counts,
):
    """rows_read from Copy; rows_inserted from Delta history, else watermark count, else rows_read."""
    lt = str(load_type or "").upper()
    ts, tt, wc = _opt_str(target_schema), _opt_str(target_table), _opt_str(watermark_column)
    r_read = _to_int(rows_read, name="rows_read")
    r_ins = _to_int(rows_inserted, name="rows_inserted")
    auto = _parse_bool(auto_row_counts, default=True)

    if not auto:
        return r_read, r_ins

    delta_ins = _delta_last_write_counts(spark, ts, tt) if ts and tt else None

    if lt == "FULL":
        if delta_ins is not None and delta_ins > 0:
            return r_read, delta_ins
        if r_read > 0:
            return r_read, r_read
        if ts and tt:
            total = int(spark.sql(f"SELECT COUNT(*) AS c FROM {ts}.{tt}").collect()[0]["c"])
            return r_read, total
        return r_read, r_ins

    # INCREMENTAL (and other non-FULL)
    if delta_ins is not None and delta_ins > 0:
        return r_read, delta_ins

    if ts and tt and wc and wm_from:
        r_ins = _count_rows_after_watermark(spark, ts, tt, wc, wm_from)
        if r_ins > 0:
            return r_read, r_ins

    if r_read > 0:
        return r_read, r_read

    return r_read, r_ins


def _update_execution_log(spark, log_id, status, rows_read=0, rows_inserted=0, watermark_from=None, watermark_to=None, error_message=None):
    clean_error = str(error_message)[:2000] if error_message else None
    DeltaTable.forName(spark, EXEC_LOG).update(
        condition=f"log_id = {_to_int(log_id, name='log_id')}",
        set={
            "status": lit(status),
            "end_time": current_timestamp(),
            "rows_read": lit(int(rows_read)),
            "rows_inserted": lit(int(rows_inserted)),
            "rows_updated": lit(0),
            "rows_rejected": lit(0),
            "watermark_from": lit(watermark_from),
            "watermark_to": lit(watermark_to),
            "error_message": lit(clean_error),
        },
    )


def _update_ingestion_watermark(spark, ingestion_config_id, watermark_to):
    if not watermark_to:
        return
    icid = _to_int(ingestion_config_id, name="ingestion_config_id")
    if icid <= 0:
        return
    safe = str(watermark_to).replace("'", "''")
    spark.sql(
        f"""
        UPDATE {INGESTION_CONFIG}
        SET last_watermark = '{safe}'
        WHERE ingestion_config_id = {icid}
        """
    )


def _require_log_id(log_id, action_name):
    lid = _to_int(log_id, name="log_id")
    if lid <= 0:
        raise ValueError(
            f"log_id is required for {action_name}. Set notebook parameter log_id to "
            "@activity('<Entity_Start_Activity>').output.result.exitValue"
        )
    return lid


def _resolve_copy_error_message(log_id, error_message):
    """Use Copy failure text; ignore pipeline mistakes that pass log_id as error_message."""
    lid = _to_int(log_id, name="log_id")
    msg = _opt_str(error_message)
    if msg and msg.isdigit() and int(msg) == lid:
        return (
            "Copy activity failed. Pipeline bound error_message to log_id; "
            "use @activity('Copy_<name>').error.message instead."
        )
    return msg or "Copy activity failed"


spark = SparkSession.builder.getOrCreate()
act = _opt_str(action)
if not act:
    raise ValueError("Parameter 'action' is required.")
act = act.lower()
if act not in VALID_ACTIONS:
    raise ValueError(f"Unknown action '{action}'. Use one of: {', '.join(VALID_ACTIONS)}")

try:
    if act in ("pipeline_start", "entity_start"):
        wf = _opt_str(watermark_from) if act == "entity_start" else None
        cid = 0 if act == "pipeline_start" else _to_int(controller_id, name="controller_id")
        new_log_id = _insert_running(spark, pipeline_run_id, cid, pipeline_name, wf)
        notebookutils.notebook.exit(str(new_log_id))

    elif act == "entity_success":
        lid = _require_log_id(log_id, "entity_success")
        wm_from = _opt_str(watermark_from)
        lt = str(load_type or "").upper()
        ts, tt, wc = _opt_str(target_schema), _opt_str(target_table), _opt_str(watermark_column)

        wm_to = None
        if lt != "FULL" and ts and tt and wc:
            wm_to = _compute_watermark(spark, ts, tt, wc)

        r_read, r_ins = _resolve_row_counts(
            spark,
            lt,
            ts,
            tt,
            wc,
            wm_from,
            rows_read,
            rows_inserted,
            auto_row_counts,
        )

        _update_execution_log(
            spark,
            lid,
            "Success",
            rows_read=r_read,
            rows_inserted=r_ins,
            watermark_from=wm_from,
            watermark_to=wm_to,
        )

        icid = _to_int(ingestion_config_id or controller_id, name="ingestion_config_id")
        if lt != "FULL" and wm_to:
            _update_ingestion_watermark(spark, icid, wm_to)

        notebookutils.notebook.exit(str(lid))

    elif act == "entity_failed":
        lid = _require_log_id(log_id, "entity_failed")
        msg = _resolve_copy_error_message(log_id, error_message)[:2000]
        tt = _opt_str(target_table) or "unknown"
        _write_error_row(spark, lid, pipeline_name, tt, "COPY_FAILED", msg)
        DeltaTable.forName(spark, EXEC_LOG).update(
            condition=f"log_id = {lid}",
            set={
                "status": lit("Failed"),
                "end_time": current_timestamp(),
                "error_message": lit(msg),
            },
        )
        raise Exception(msg)

    elif act == "pipeline_end":
        lid = _require_log_id(log_id, "pipeline_end")
        safe_run_id = str(pipeline_run_id).replace("'", "''")
        agg = spark.sql(
            f"""
            SELECT
                COALESCE(SUM(rows_read), 0) AS rows_read,
                COALESCE(SUM(rows_inserted), 0) AS rows_inserted,
                COALESCE(SUM(rows_updated), 0) AS rows_updated,
                COALESCE(SUM(rows_rejected), 0) AS rows_rejected
            FROM {EXEC_LOG}
            WHERE pipeline_run_id = '{safe_run_id}'
              AND controller_id > 0
            """
        ).collect()[0]
        ps = _opt_str(pipeline_status) or "Success"
        DeltaTable.forName(spark, EXEC_LOG).update(
            condition=f"log_id = {lid}",
            set={
                "status": lit(ps),
                "end_time": current_timestamp(),
                "rows_read": lit(int(agg["rows_read"])),
                "rows_inserted": lit(int(agg["rows_inserted"])),
                "rows_updated": lit(int(agg["rows_updated"])),
                "rows_rejected": lit(int(agg["rows_rejected"])),
            },
        )
        notebookutils.notebook.exit(ps)

except Exception as exc:
    lid = _to_int(log_id, default=0, name="log_id")
    if lid > 0 and act == "entity_success":
        tt = _opt_str(target_table) or "unknown"
        _write_error_row(
            spark,
            lid,
            pipeline_name,
            tt,
            "LOG_END_FAILED",
            str(exc),
            traceback.format_exc()[:4000],
        )
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
