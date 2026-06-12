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
# META       "known_warehouses": []
# META     }
# META   }
# META }

# CELL ********************

# Cell 1: Parameters
pipeline_run_id = "manual_run_001" 
batch_id = "batch_default_001"
load_type = "incremental"          

source_entity = "default_source"   
target_table = "default_target"    
watermark_column = "updated_at"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************


import uuid
import traceback
from pyspark.sql.functions import current_timestamp, lit
from delta.tables import DeltaTable
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType

def start_pipeline(spark, batch_id, pipeline_run_id, notebook_name, load_type):
    """Khởi tạo log tổng thể khi một Notebook bắt đầu chạy"""
    status = "running"
    
    batch_schema = StructType([
        StructField("batch_id", StringType(), True),
        StructField("pipeline_run_id", StringType(), True),
        StructField("notebook_name", StringType(), True),
        StructField("load_type", StringType(), True),
        StructField("status", StringType(), True),
        StructField("end_time", TimestampType(), True),
        StructField("rows_read", LongType(), True),
        StructField("rows_inserted", LongType(), True),
        StructField("rows_updated", LongType(), True),
        StructField("rows_deleted", LongType(), True),
        StructField("error_message", StringType(), True)
    ])
    
    # Tạo row dữ liệu ban đầu
    raw_data = [(batch_id, pipeline_run_id, notebook_name, load_type, status, None, 0, 0, 0, 0, None)]
    
    # Tạo DataFrame dựa trên Schema đã ép kiểu
    run_df = spark.createDataFrame(data=raw_data, schema=batch_schema)
    
    # Ghi vào Delta Table và log bước khởi tạo
    run_df.withColumn("start_time", current_timestamp()) \
          .write.format("delta").mode("append").saveAsTable("audit.etl_batch_run")
          
    log_audit(spark, batch_id, pipeline_run_id, "INFO", "Initialization", f"Bắt đầu chạy notebook {notebook_name}", None)


def log_audit(spark, batch_id, pipeline_run_id, event_type, step_name, message, error_stack_trace=None):
    """Ghi chi tiết sự kiện hoặc lỗi vào bảng audit.etl_audit_log"""
    log_id = str(uuid.uuid4())
    
    audit_schema = StructType([
        StructField("log_id", StringType(), True),
        StructField("batch_id", StringType(), True),
        StructField("pipeline_run_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("step_name", StringType(), True),
        StructField("message", StringType(), True),
        StructField("error_stack_trace", StringType(), True)
    ])
    
    log_df = spark.createDataFrame(
        data=[(log_id, batch_id, pipeline_run_id, event_type, step_name, message, error_stack_trace)], 
        schema=audit_schema
    )
    
    log_df.withColumn("event_time", current_timestamp()) \
          .write.format("delta").mode("append").saveAsTable("audit.etl_audit_log")

# def log_audit(spark, batch_id, pipeline_run_id, event_type, step_name, message, error_stack_trace=None):
#     log_id = str(uuid.uuid4())
#     log_df = spark.createDataFrame([(
#         log_id, batch_id, pipeline_run_id, event_type, step_name, message, error_stack_trace
#     )], ["log_id", "batch_id", "pipeline_run_id", "event_type", "step_name", "message", "error_stack_trace"])
    
#     log_df.withColumn("event_time", current_timestamp()) \
#           .write.format("delta").mode("append").saveAsTable("audit.etl_audit_log")

# def start_pipeline(spark, batch_id, pipeline_run_id, notebook_name, load_type):
#     status = "running"
#     run_df = spark.createDataFrame([(
#         batch_id, pipeline_run_id, notebook_name, load_type, status, 
#         None, 0, 0, 0, 0, None  
#     )], ["batch_id", "pipeline_run_id", "notebook_name", "load_type", "status", 
#          "end_time", "rows_read", "rows_inserted", "rows_updated", "rows_deleted", "error_message"])
    
#     run_df.withColumn("start_time", current_timestamp()) \
#           .write.format("delta").mode("append").saveAsTable("audit.etl_batch_run")
#     log_audit(spark, batch_id, pipeline_run_id, "INFO", "Initialization", f"Bắt đầu chạy notebook {notebook_name}", None)

def end_pipeline(spark, batch_id, status, rows_read=0, rows_inserted=0, rows_updated=0, rows_deleted=0, error_message=None):
    delta_table = DeltaTable.forName(spark, "audit.etl_batch_run")
    delta_table.update(
        condition=f"batch_id = '{batch_id}'",
        set={
            "status": lit(status),
            "end_time": current_timestamp(),
            "rows_read": lit(rows_read),
            "rows_inserted": lit(rows_inserted),
            "rows_updated": lit(rows_updated),
            "rows_deleted": lit(rows_deleted),
            "error_message": lit(error_message)
        }
    )

def upsert_delta_table(spark, source_df, target_table, merge_keys, load_type, batch_id, pipeline_run_id):
    try:
        if load_type == "initial":
            log_audit(spark, batch_id, pipeline_run_id, "INFO", "Delta Load", f"Thực hiện Initial Load vào {target_table}")
            source_df.write.format("delta").mode("overwrite").saveAsTable(target_table)
            return source_df.count(), 0 

        elif load_type == "incremental":
            log_audit(spark, batch_id, pipeline_run_id, "INFO", "Delta Load", f"Thực hiện Incremental Load vào {target_table}")
            
            # 1. Lấy danh sách tất cả các cột nội dung (không bao gồm các cột khóa chính dùng để MERGE)
            all_columns = source_df.columns
            non_merge_cols = [col for col in all_columns if col not in merge_keys]
            
            # 2. Lấy bảng đích hiện tại
            target_df = spark.table(target_table)
            
            # 3. Xây dựng điều kiện JOIN dựa trên khóa chính
            join_condition = [source_df[key] == target_df[key] for key in merge_keys]
            
            # Kết nối Nguồn và Đích để phân loại dòng chính xác
            compared_df = source_df.alias("src").join(target_df.alias("tgt"), join_condition, "left_outer")
            
            # Đại diện khóa của bảng đích để check dòng mới hay dòng cũ
            target_key_col = target_df[merge_keys[0]]
            
            # ĐẾM DÒNG INSERT: Khóa bảng đích bị NULL (dòng chưa từng tồn tại)
            inserted = compared_df.filter(target_key_col.isNull()).count()
            
            # Xây dựng điều kiện kiểm tra xem có bất kỳ cột nội dung nào bị khác nhau không
            # Ví dụ: (src.customer_name != tgt.customer_name) OR (src.gender != tgt.gender)
            data_changed_condition = " OR ".join([f"(src.{col} <=> tgt.{col}) =  false" for col in non_merge_cols])
            
            # ĐẾM DÒNG UPDATE THỰC TẾ: Khóa bảng đích NOT NULL VÀ nội dung có sự thay đổi
            updated = compared_df.filter(target_key_col.isNotNull()).filter(data_changed_condition).count()
            
            # 4. THỰC THI MERGE VẬT LÝ VỚI ĐIỀU KIỆN TỐI ƯU HIỆU NĂNG
            target_delta = DeltaTable.forName(spark, target_table)
            merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in merge_keys])
            
            # Thêm điều kiện bên trong whenMatchedUpdate để Spark bỏ qua các dòng không đổi
            target_delta.alias("target") \
                .merge(source_df.alias("source"), merge_condition) \
                .whenMatchedUpdateAll(condition=data_changed_condition.replace("src.", "source.").replace("tgt.", "target.")) \
                .whenNotMatchedInsertAll() \
                .execute()
            
            return inserted, updated
        else:
            raise ValueError("load_type phải là 'initial' hoặc 'incremental'")
    except Exception as e:
        error_trace = traceback.format_exc()
        log_audit(spark, batch_id, pipeline_run_id, "ERROR", "Delta Load", f"Lỗi tại {target_table}: {str(e)}", error_trace)
        raise e

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

import uuid
import traceback
from datetime import datetime
from pyspark.sql.functions import current_timestamp, lit, col, expr
from delta.tables import DeltaTable
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType, IntegerType

# 1. KHỞI TẠO TIẾN TRÌNH (BƯỚC BẮT ĐẦU NOTEBOOK)
def start_pipeline(spark, log_id, pipeline_run_id, controller_id, pipeline_name):
    """
    Hàm này dùng để ghi nhận một phiên chạy mới vào bảng quản trị hệ thống.
    Trạng thái ban đầu luôn là 'Running'.
    """
    # Bước 1: Khai báo cấu trúc bảng (Schema) tường minh từng cột một cho Spark
    execution_schema = StructType([
        StructField("log_id", LongType(), False),
        StructField("pipeline_run_id", StringType(), False),
        StructField("controller_id", IntegerType(), False),
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
        StructField("logged_at", TimestampType(), False)
    ])
    
    # Bước 2: Tạo một dòng dữ liệu thô ban đầu (mặc định các cột đếm dòng = 0)
    current_now = datetime.now()
    raw_data_row = [(
        int(log_id),          # Khóa chính phiên chạy
        pipeline_run_id,      # ID phiên chạy từ Fabric
        int(controller_id),   # ID điều phối
        pipeline_name,        # Tên pipeline
        current_now,          # start_time (Thời gian bắt đầu)
        None,                 # end_time (Chưa kết thúc nên để trống)
        "Running",            # status (Trạng thái đang chạy)
        0, 0, 0, 0,           # rows_read, rows_inserted, rows_updated, rows_rejected
        None, None, None,     # watermark_from, watermark_to, dynamic_source_file
        None,                 # error_message (Chưa có lỗi)
        current_now           # logged_at (Thời gian ghi log)
    )]
    
    # Bước 3: Chuyển data thô thành DataFrame của Spark
    run_df = spark.createDataFrame(data=raw_data_row, schema=execution_schema)
    
    # Bước 4: Lưu (Append) dòng log này xuống bảng Delta vật lý
    run_df.write.format("delta").mode("append").saveAsTable("metadata.etl_execution_log")


# 2. KẾT THÚC TIẾN TRÌNH (BƯỚC CUỐI CÙNG CỦA NOTEBOOK)
def end_pipeline(spark, log_id, status, rows_read=0, rows_inserted=0, rows_updated=0, rows_rejected=0, watermark_from=None, watermark_to=None, error_message=None):
    """
    Hàm này dùng để cập nhật trạng thái cuối cùng (Success/Failed) 
    và các số liệu đếm dòng khi Notebook chạy xong.
    """
    # Bước 1: Gọi bảng Delta Log bằng tên bảng
    delta_table = DeltaTable.forName(spark, "metadata.etl_execution_log")
    
    # Bước 2: Kiểm tra nếu có tin nhắn lỗi, cắt bớt chuỗi ký tự để không làm tràn bộ nhớ bảng
    clean_error_msg = None
    if error_message is not None:
        clean_error_msg = error_message[:2000]
        
    # Bước 3: Thực hiện câu lệnh UPDATE tìm đúng log_id để cập nhật dữ liệu
    delta_table.update(
        condition=f"log_id = {int(log_id)}",
        set={
            "status": lit(status),
            "end_time": current_timestamp(),
            "rows_read": lit(rows_read),
            "rows_inserted": lit(rows_inserted),
            "rows_updated": lit(rows_updated),
            "rows_rejected": lit(rows_rejected),
            "watermark_from": lit(watermark_from),
            "watermark_to": lit(watermark_to),
            "error_message": lit(clean_error_msg)
        }
    )


# 3. GHI NHẬT KÝ LỖI CHI TIẾT (BẢNG error_logging_table)
def log_pipeline_error(spark, log_id, pipeline_name, layer_name, target_table, error_code, error_message, error_stack_trace, bad_record_content_json=None):
    """
    Hàm này dùng để bắt lỗi chi tiết (Stack Trace) và lưu payload dòng data rác (nếu có).
    """
    # Bước 1: Sinh ra một chuỗi ID ngẫu nhiên không trùng lặp cho mã lỗi
    error_id = str(uuid.uuid4())
    
    # Bước 2: Khai báo cấu trúc Schema của bảng bắt lỗi
    error_schema = StructType([
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
        StructField("updated_at", TimestampType(), False)
    ])
    
    # Bước 3: Phân loại mức độ nghiêm trọng bằng câu lệnh if/else cơ bản
    severity = "ERROR"
    if error_code == "CONNECTION_TIMEOUT" or error_code == "CATALOG_ERROR":
        severity = "CRITICAL"
        
    # Bước 4: Đóng gói dữ liệu lỗi
    current_now = datetime.now()
    error_data = [(
        error_id, int(log_id), pipeline_name, current_now, 
        error_code, layer_name, target_table, error_message, 
        severity, bad_record_content_json, "New", current_now
    )]
    
    # Bước 5: Chuyển dữ liệu lỗi thành DataFrame và lưu xuống bảng
    error_df = spark.createDataFrame(data=error_data, schema=error_schema)
    error_df.write.format("delta").mode("append").saveAsTable("metadata.error_logging_table")

def get_pipeline_config(spark_session, pipeline_name: str, config_type: str) -> dict:
    """
    Hàm tổng hợp linh động đọc cấu hình từ Lakehouse định danh: insurance_lakehouse.metadata
    """
    # 1. Ánh xạ chính xác kèm theo tên Lakehouse và Schema của bạn
    table_mapping = {
        "pipeline": "insurance_lakehouse.metadata.etl_pipeline_config",
        "ingestion": "insurance_lakehouse.metadata.etl_ingestion_config",
        "transform": "insurance_lakehouse.metadata.etl_transform_config"
    }
    
    config_type_clean = config_type.strip().lower()
    
    if config_type_clean not in table_mapping:
        raise ValueError(
            f"❌ LỖI TRUYỀN THAM SỐ: config_type phải là 'pipeline', 'ingestion', hoặc 'transform'. "
            f"Tham số bạn vừa truyền là: '{config_type}'"
        )
        
    target_table = table_mapping[config_type_clean]
    
    query = f"SELECT * FROM {target_table} WHERE pipeline_name = '{pipeline_name}'"
    
    try:
        config_df = spark_session.sql(query)
    except Exception as sql_err:
        raise Exception(
            f"❌ LỖI TRUY VẤN: Không thể truy cập bảng `{target_table}`. "
            f"Vui lòng kiểm tra xem bảng này đã được chạy lệnh CREATE TABLE chưa!\n"
            f"Chi tiết lỗi hệ thống: {str(sql_err)}"
        )
        
    # 3. Kiểm tra dữ liệu rỗng (Bản ghi cấu hình của pipeline này đã tồn tại chưa)
    if config_df.count() == 0:
        raise Exception(
            f"❌ LỖI THIẾU CONFIG: Pipeline '{pipeline_name}' chưa được khai báo dữ liệu mẫu "
            f"trong bảng `{target_table}`. Vui lòng INSERT dữ liệu cấu hình trước!"
        )
        
    # 4. Trả về kết quả dạng Dictionary
    first_row = config_df.collect()[0]
    return first_row.asDict()

# 4. TRÁI TIM TỰ ĐỘNG: ĐỌC CẤU HÌNH VÀ XỬ LÝ BIẾN ĐỔI (BRONZE -> GOLD)
def upsert_to_gold(spark, source_df, pipeline_name, log_id):
    """
    Hàm trung tâm: Tra cứu bảng cấu hình để tự động chạy chiến lược dữ liệu phù hợp.
    """
    try:
        # BƯỚC A: TỰ ĐỘNG ĐỌC CONFIG (LOOK-UP)
        # Đọc bảng cấu hình và lọc đúng tên pipeline truyền vào
        config_df = spark.table("metadata.etl_transform_config").filter(col("pipeline_name") == pipeline_name)
        config_records = config_df.collect()
        
        # Nếu không tìm thấy dòng cấu hình nào, thông báo lỗi ngay
        if len(config_records) == 0:
            raise ValueError(f"Mã cấu hình không tồn tại cho tên pipeline: {pipeline_name}")
            
        # Lấy dòng cấu hình đầu tiên tìm được
        config = config_records[0]
        
        # Bóc tách từng giá trị cấu hình ra các biến đơn giản
        target_table = config['target_schema'] + "." + config['target_table']
        transform_type = config['transform_type']      # Lấy loại chiến lược (SCD1, SCD2, APPEND...)
        partition_col = config['partition_column']      # Lấy cột phân sàn (nếu có)
        pk_string = config['primary_key_columns']       # Lấy chuỗi khóa chính
        
        # Chuyển chuỗi khóa chính cách nhau bằng dấu phẩy thành một danh sách (List)
        merge_keys = []
        if pk_string is not None and pk_string.strip() != "":
            raw_keys = pk_string.split(",")
            for key in raw_keys:
                merge_keys.append(key.strip()) # Thêm khóa sạch sau khi xóa khoảng trắng

        # Đếm số dòng dữ liệu mới đọc được từ nguồn
        rows_read = source_df.count()
        if rows_read == 0:
            return 0, 0, 0 # Nếu không có data, thoát hàm và trả về toàn bộ bằng 0

        # BƯỚC B: RẼ NHÁNH ĐIỀU HƯỚNG CHIẾN LƯỢC XỬ LÝ
        
        # KỊCH BẢN 1: BẢNG ĐÍCH CHƯA TỒN TẠI (TẠO MỚI LẦN ĐẦU)
        if spark.catalog.tableExists(target_table) == False:
            print(f"-> Bảng đích {target_table} chưa tồn tại. Tiến hành tạo bảng vật lý lần đầu...")
            
            # Nếu là lưu lịch sử SCD2, bổ sung 3 cột kỹ thuật thời gian vào bảng
            if transform_type == "MERGE_SCD2":
                source_df = source_df.withColumn("effective_date", current_timestamp()) \
                                     .withColumn("expiry_date", lit(None).cast(TimestampType())) \
                                     .withColumn("is_current", lit(True))
            
            # Cấu hình lệnh ghi đè (tự động tạo bảng)
            writer = source_df.write.format("delta").mode("overwrite")
            
            # Kiểm tra xem cấu hình có yêu cầu phân sàn bảng không
            if partition_col is not None and partition_col.strip() != "":
                writer.partitionBy(partition_col).saveAsTable(target_table)
            else:
                writer.saveAsTable(target_table)
                
            return rows_read, rows_read, 0 # Trả về: đọc vào = chèn mới, cập nhật = 0


        # KỊCH BẢN 2: CHẾ ĐỘ APPEND (CHÈN THUẦN TÚY)
        if transform_type == "APPEND":
            source_df.write.format("delta").mode("append").saveAsTable(target_table)
            return rows_read, rows_read, 0


        # KỊCH BẢN 3: CHẾ ĐỘ OVERWRITE (XÓA ĐI GHI LẠI TOÀN BỘ)
        if transform_type == "OVERWRITE":
            source_df.write.format("delta").mode("overwrite").saveAsTable(target_table)
            return rows_read, rows_read, 0


        # KỊCH BẢN 4: CHẾ ĐỘ CẬP NHẬT NÂNG CAO (MERGE SCD1 HOẶC SCD2)
        if len(merge_keys) == 0:
            raise ValueError(f"Chiến lược MERGE yêu cầu phải cấu hình cột primary_key_columns!")

        # Gọi bảng Delta vật lý hiện tại lên để chuẩn bị so sánh dữ liệu
        target_table_obj = DeltaTable.forName(spark, target_table)
        target_df = target_table_obj.toDF()

        # Tìm danh sách các cột nội dung cần so sánh (Loại bỏ khóa chính và cột kỹ thuật)
        cols_to_compare = []
        for c in source_df.columns:
            if c not in merge_keys and c not in ["effective_date", "expiry_date", "is_current"]:
                cols_to_compare.append(c)

        # Xây dựng chuỗi kiểm tra thay đổi nội dung (Dùng toán tử <=> để an toàn với dữ liệu Null)
        changed_conditions_list = []
        for c in cols_to_compare:
            changed_conditions_list.append(f"NOT (src.{c} <=> tgt.{c})")
        data_changed_cond = " OR ".join(changed_conditions_list)


        # ----- NHÁNH CON THỰC THI SCD TYPE 1 -----
        if transform_type == "MERGE_SCD1":
            # Kết nối Left Join giữa Nguồn (src) và Đích (tgt) để đối soát dòng dữ liệu
            compared_df = source_df.alias("src").join(target_df.alias("tgt"), merge_keys, "left")
            first_pk_col = merge_keys[0]

            # Đếm dòng mới: Nếu khóa của bảng đích bị rỗng (Null) nghĩa là dòng mới hoàn toàn
            inserted = compared_df.filter(target_df[first_pk_col].isNull()).count()
            
            # Đếm dòng cập nhật: Khóa bảng đích tồn tại VÀ có ít nhất một cột nội dung bị thay đổi giá trị
            updated = compared_df.filter(target_df[first_pk_col].isNotNull() & expr(data_changed_cond)).count()

            # Xây dựng chuỗi điều kiện so sánh khóa phục vụ lệnh MERGE vật lý
            merge_keys_pairs = []
            for k in merge_keys:
                merge_keys_pairs.append(f"target.{k} = source.{k}")
            merge_condition = " AND ".join(merge_keys_pairs)

            # Thực thi lệnh MERGE SCD1 vật lý xuống ổ đĩa OneLake
            target_table_obj.alias("target") \
                .merge(source_df.alias("source"), merge_condition) \
                .whenMatchedUpdateAll(condition=data_changed_cond.replace("src.", "source.").replace("tgt.", "target.")) \
                .whenNotMatchedInsertAll() \
                .execute()

            return rows_read, inserted, updated


        # ----- NHÁNH CON THỰC THI SCD TYPE 2 -----
        elif transform_type == "MERGE_SCD2":
            # Lọc lấy các bản ghi đang còn hiệu lực hiện tại ở bảng đích tầng Gold
            current_target_df = target_df.filter(col("is_current") == True)
            
            # Kết nối đối soát dữ liệu
            compared_df = source_df.alias("src").join(current_target_df.alias("tgt"), merge_keys, "left")
            first_pk_col = merge_keys[0]

            # Đếm số lượng dòng biến động tương tự SCD1
            inserted = compared_df.filter(current_target_df[first_pk_col].isNull()).count()
            updated = compared_df.filter(current_target_df[first_pk_col].isNotNull() & expr(data_changed_cond)).count()

            # Bước B1: Tìm các bản ghi cũ ở bảng đích cần đóng dấu hết hiệu lực (expire)
            records_to_expire = source_df.alias("src") \
                                         .join(current_target_df.alias("tgt"), merge_keys, "inner") \
                                         .filter(expr(data_changed_cond)) \
                                         .select(f"src.{first_pk_col}")

            # Thực thi câu lệnh MERGE đóng dấu lịch sử (Cập nhật cột is_current = false)
            merge_keys_pairs = []
            for k in merge_keys:
                merge_keys_pairs.append(f"target.{k} = expire_src.{k}")
            expire_condition = " AND ".join(merge_keys_pairs) + " AND target.is_current = true"
            
            target_table_obj.alias("target") \
                .merge(records_to_expire.alias("expire_src"), expire_condition) \
                .whenMatchedUpdate(set={"expiry_date": current_timestamp(), "is_current": "false"}) \
                .execute()

            # Bước B2: Lọc ra tập hợp dòng mới tinh + dòng phiên bản mới để chèn (Append) vào bảng
            new_version_df = source_df.alias("src") \
                                   .join(current_target_df.alias("tgt"), merge_keys, "left") \
                                   .filter(current_target_df[first_pk_col].isNull() | expr(data_changed_cond)) \
                                   .select("src.*") \
                                   .withColumn("effective_date", current_timestamp()) \
                                   .withColumn("expiry_date", lit(None).cast(TimestampType())) \
                                   .withColumn("is_current", lit(True))
                                   
            new_version_df.write.format("delta").mode("append").saveAsTable(target_table)

            return rows_read, inserted, updated
            
        else:
            raise ValueError(f"transform_type '{transform_type}' không hợp lệ trên hệ thống.")

    except Exception as e:
        # Nếu có lỗi xảy ra ở bất kỳ công đoạn nào, quăng log lỗi sâu và ném lỗi lên màn hình
        error_trace = traceback.format_exc()
        err_target = "UNKNOWN_GOLD_TABLE"
        if 'target_table' in locals():
            err_target = target_table
            
        log_pipeline_error(spark, log_id, pipeline_name, "gold", err_target, "TRANSFORM_ERROR", str(e), error_trace)
        raise e



print("chạy thành công thư viện rồi nehs em ")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # create schema

# CELL ********************

# MAGIC %%sql
# MAGIC DROP SCHEMA IF EXISTS audit CASCADE;
# MAGIC DROP TABLE IF EXISTS gold.test_customer_target;
# MAGIC create schema if not exists table4test;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- ======================================================================================
# MAGIC -- BƯỚC 1: XÓA SẠCH TOÀN BỘ CẤU TRÚC CŨ ĐỂ LÀM SẠCH HỆ THỐNG
# MAGIC -- ======================================================================================
# MAGIC DROP TABLE IF EXISTS metadata.error_logging_table;
# MAGIC DROP TABLE IF EXISTS metadata.etl_execution_log;
# MAGIC DROP TABLE IF EXISTS metadata.etl_transform_config;
# MAGIC DROP TABLE IF EXISTS metadata.etl_ingestion_config;
# MAGIC DROP TABLE IF EXISTS metadata.etl_pipeline_config;
# MAGIC 
# MAGIC DROP SCHEMA IF EXISTS metadata CASCADE;
# MAGIC 
# MAGIC -- ======================================================================================
# MAGIC -- BƯỚC 2: KHỞI TẠO LẠI SCHEMA VÀ 5 BẢNG METADATA CHUẨN 100% THEO FILE TXT
# MAGIC -- ======================================================================================
# MAGIC CREATE SCHEMA IF NOT EXISTS metadata;
# MAGIC 
# MAGIC -- --------------------------------------------------------------------------------------
# MAGIC -- BẢNG 1: etl_pipeline_config (Cấu hình quản trị chung)
# MAGIC -- --------------------------------------------------------------------------------------
# MAGIC CREATE TABLE IF NOT EXISTS metadata.etl_pipeline_config (
# MAGIC     pipeline_name STRING NOT NULL,          -- PK: Tên định danh duy nhất của pipeline [cite: 2]
# MAGIC     pipeline_stage STRING NOT NULL,         -- Giai đoạn của pipeline [cite: 3]
# MAGIC     is_active BOOLEAN NOT NULL,             -- Trạng thái kích hoạt chạy pipeline [cite: 4]
# MAGIC     retry_count INT NOT NULL,               -- Số lần tối đa chạy lại [cite: 5]
# MAGIC     retry_interval_minutes INT NOT NULL,    -- Thời gian giãn cách giữa các lần thử lại [cite: 6]
# MAGIC     timeout_minutes INT NOT NULL,           -- Thời gian tối đa trước khi ngắt tiến trình [cite: 7]
# MAGIC     created_at TIMESTAMP NOT NULL,          -- Thời gian khởi tạo cấu hình [cite: 8]
# MAGIC     updated_at TIMESTAMP,                   -- Thời gian cập nhật cấu hình lần cuối [cite: 9]
# MAGIC     created_by STRING,                      -- Người tạo cấu hình [cite: 10]
# MAGIC     updated_by STRING                       -- Người cập nhật cấu hình [cite: 11]
# MAGIC ) USING DELTA;
# MAGIC 
# MAGIC -- --------------------------------------------------------------------------------------
# MAGIC -- BẢNG 2: etl_ingestion_config (Cấu hình chặng: Landing -> Bronze)
# MAGIC -- --------------------------------------------------------------------------------------
# MAGIC CREATE TABLE IF NOT EXISTS metadata.etl_ingestion_config (
# MAGIC     ingestion_config_id BIGINT,             -- PK: Khóa chính định danh cấu hình ingestion [cite: 13]
# MAGIC     pipeline_name STRING NOT NULL,          -- FK: Liên kết chặng bay với pipeline master [cite: 14]
# MAGIC     source_system STRING,                   -- Tên hệ thống nguồn cấp dữ liệu [cite: 15]
# MAGIC     source_schema STRING,                   -- Schema của cơ sở dữ liệu nguồn [cite: 16]
# MAGIC     source_table STRING,                    -- Tên bảng thô tại nguồn [cite: 17]
# MAGIC     source_path STRING,                     -- Đường dẫn thư mục tại tầng Landing [cite: 18]
# MAGIC     source_format STRING NOT NULL,          -- Định dạng dữ liệu đầu vào [cite: 19]
# MAGIC     file_pattern STRING,                    -- Biểu thức chính quy lọc tên file [cite: 20]
# MAGIC     target_layer STRING NOT NULL,           -- Destination layer (luôn là 'bronze') [cite: 21]
# MAGIC     target_schema STRING NOT NULL,          -- Target schema name (luôn là 'bronze') [cite: 22]
# MAGIC     target_table STRING NOT NULL,           -- Tên bảng Delta đích lưu dữ liệu thô [cite: 23]
# MAGIC     load_type STRING NOT NULL,              -- Chiến lược nạp dữ liệu [cite: 24]
# MAGIC     max_watermark STRING                    -- Column used for incremental loading [cite: 25]
# MAGIC ) USING DELTA;
# MAGIC 
# MAGIC -- --------------------------------------------------------------------------------------
# MAGIC -- BẢNG 3: etl_transform_config (Cấu hình chặng: Bronze -> Gold)
# MAGIC -- --------------------------------------------------------------------------------------
# MAGIC CREATE TABLE IF NOT EXISTS metadata.etl_transform_config (
# MAGIC     transform_config_id BIGINT,             -- PK: Khóa chính định danh cấu hình transform [cite: 27]
# MAGIC     pipeline_name STRING NOT NULL,          -- FK: Name of the related pipeline [cite: 28]
# MAGIC     source_layer STRING NOT NULL,           -- Source layer (luôn là 'bronze') [cite: 29]
# MAGIC     source_schema STRING NOT NULL,          -- Source schema name (luôn là 'bronze') [cite: 30]
# MAGIC     source_table STRING NOT NULL,           -- Tên bảng Delta nguồn tại tầng Bronze [cite: 31]
# MAGIC     target_layer STRING NOT NULL,           -- Target layer (luôn là 'gold') [cite: 32]
# MAGIC     target_schema STRING NOT NULL,          -- Target schema name (luôn là 'gold') [cite: 33]
# MAGIC     target_table STRING NOT NULL,           -- Tên bảng Dim hoặc Fact đích tại tầng Gold [cite: 34]
# MAGIC     transform_type STRING NOT NULL,         -- Transformation/write strategy [cite: 35]
# MAGIC     primary_key_columns STRING,             -- Key columns used for merge/upsert [cite: 36]
# MAGIC     partition_column STRING,                -- Tên cột được chọn để phân sàn bảng Delta [cite: 37]
# MAGIC     dependency_pipeline STRING              -- Tên của pipeline phụ thuộc bắt buộc chạy trước [cite: 38]
# MAGIC ) USING DELTA;
# MAGIC 
# MAGIC -- --------------------------------------------------------------------------------------
# MAGIC -- BẢNG 4: etl_execution_log (Ghi nhận và Giám sát vận hành - ĐÃ BỔ SUNG ĐẦY ĐỦ CỘT)
# MAGIC -- --------------------------------------------------------------------------------------
# MAGIC CREATE TABLE IF NOT EXISTS metadata.etl_execution_log (
# MAGIC     log_id BIGINT NOT NULL,                 -- PK: Khóa chính tự tăng của mỗi dòng log thực thi [cite: 40]
# MAGIC     pipeline_run_id STRING NOT NULL,        -- Fabric PipelineRunId, một row cho mỗi lần chạy [cite: 41]
# MAGIC     controller_id INT NOT NULL,             -- FK to pipeline config table [cite: 42]
# MAGIC     pipeline_name STRING NOT NULL,          -- Name of the pipeline as defined in Fabric [cite: 43]
# MAGIC     start_time TIMESTAMP NOT NULL,          -- Pipeline start time từ Fabric TriggerTime [cite: 44]
# MAGIC     end_time TIMESTAMP,                     -- Pipeline end time. Để NULL nếu job bị crash [cite: 45]
# MAGIC     status STRING NOT NULL,                 -- Final run status: CHECK Running|Success|Failed [cite: 46]
# MAGIC     rows_read BIGINT,                       -- Total rows read, parsed từ Fabric LogData 
# MAGIC     rows_inserted BIGINT,                   -- Số lượng dòng mới tinh thực tế được chèn 
# MAGIC     rows_updated BIGINT,                    -- Số lượng dòng cũ bị ghi đè hoặc đóng lịch sử [cite: 49]
# MAGIC     rows_rejected BIGINT,                   -- Rows rejected hoặc quarantined trong suốt quá trình load 
# MAGIC     watermark_from STRING,                  -- Lower bound watermark value used this run 
# MAGIC     watermark_to STRING,                    -- Upper bound watermark, lưu lại cho phiên kế tiếp 
# MAGIC     dynamic_source_file STRING,             -- Tên tệp dữ liệu cụ thể được phân giải tại runtime 
# MAGIC     error_message STRING,                   -- Error detail captured khi pipeline gặp sự cố [cite: 54]
# MAGIC     logged_at TIMESTAMP NOT NULL            -- Row insert timestamp (UTC) [cite: 55]
# MAGIC ) USING DELTA;
# MAGIC 
# MAGIC -- --------------------------------------------------------------------------------------
# MAGIC -- BẢNG 5: error_logging_table (Nhật ký bắt lỗi sâu và Quản lý dữ liệu rác)
# MAGIC -- --------------------------------------------------------------------------------------
# MAGIC CREATE TABLE IF NOT EXISTS metadata.error_logging_table (
# MAGIC     error_id STRING NOT NULL,               -- PK: Khóa chính cho mỗi sự cố lỗi (UUID) [cite: 57]
# MAGIC     execution_id BIGINT NOT NULL,           -- FK -> etl_execution_log(log_id) [cite: 58]
# MAGIC     error_pipelinename STRING NOT NULL,     -- name of error pipeline/job [cite: 59]
# MAGIC     error_timestamp TIMESTAMP NOT NULL,     -- specific time that error occur [cite: 60]
# MAGIC     error_code STRING NOT NULL,             -- status code for categorized and report easily [cite: 61]
# MAGIC     layer_name STRING NOT NULL,             -- The specific data layer where the error occurred [cite: 62]
# MAGIC     target_table STRING NOT NULL,           -- Tên của bảng đích gặp lỗi khi ghi [cite: 63]
# MAGIC     error_message STRING NOT NULL,          -- description of error [cite: 64]
# MAGIC     error_severity_level STRING NOT NULL,   -- level of severity of the corrupted row data [cite: 65]
# MAGIC     bad_record_content STRING,              -- Cột lưu Payload JSON dữ liệu rác [cite: 66]
# MAGIC     status STRING NOT NULL,                 -- Status of the corrupted row data [cite: 67]
# MAGIC     updated_at TIMESTAMP NOT NULL           -- Save the timestamp of the last status change [cite: 68]
# MAGIC ) USING DELTA;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# MARKDOWN ********************

# # testing lib

# CELL ********************

# MAGIC %%sql
# MAGIC -- Xóa sạch toàn bộ dữ liệu trong bảng test nhưng giữ nguyên khung bảng
# MAGIC TRUNCATE TABLE metadata.etl_transform_config;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

# MAGIC %%pyspark
# MAGIC from pyspark.sql.types import StructType, StructField, StringType, LongType
# MAGIC 
# MAGIC schema_transform = StructType([
# MAGIC     StructField("transform_config_id", LongType(), True),
# MAGIC     StructField("pipeline_name", StringType(), False),
# MAGIC     StructField("source_layer", StringType(), False),
# MAGIC     StructField("source_schema", StringType(), False),
# MAGIC     StructField("source_table", StringType(), False),
# MAGIC     StructField("target_layer", StringType(), False),
# MAGIC     StructField("target_schema", StringType(), False), # Cột lưu schema đích
# MAGIC     StructField("target_table", StringType(), False),  # Cột lưu table đích
# MAGIC     StructField("transform_type", StringType(), False),
# MAGIC     StructField("primary_key_columns", StringType(), True),
# MAGIC     StructField("partition_column", StringType(), True),
# MAGIC     StructField("dependency_pipeline", StringType(), True)
# MAGIC ])
# MAGIC 
# MAGIC # THAY ĐỔI TẠI ĐÂY: target_layer='table4test', target_schema='table4test'
# MAGIC data_transform = [(
# MAGIC     1, 
# MAGIC     'policy_gold_transformation_pipeline', 
# MAGIC     'bronze', 
# MAGIC     'bronze', 
# MAGIC     'bronze_policy', 
# MAGIC     'table4test',             # target_layer
# MAGIC     'table4test',             # target_schema (đưa vào đúng schema test bạn vừa tạo)
# MAGIC     'test_customer_target',    # target_table
# MAGIC     'MERGE_SCD1', 
# MAGIC     'customer_id', 
# MAGIC     None, 
# MAGIC     None
# MAGIC )]
# MAGIC 
# MAGIC # Ghi đè cấu hình mới vào bảng quản trị
# MAGIC spark.createDataFrame(data=data_transform, schema=schema_transform) \
# MAGIC      .write.format("delta").mode("overwrite").saveAsTable("metadata.etl_transform_config")
# MAGIC 
# MAGIC # Reset cache của Spark Session
# MAGIC spark.catalog.refreshTable("metadata.etl_transform_config")
# MAGIC 
# MAGIC print("-> Đã cập nhật cấu hình mồi: Hướng bảng đích sang table4test.test_customer_target thành công!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

import time
import uuid
from datetime import datetime

test_log_id = int(time.time())
test_pipeline_run_id = f"run_fabric_{str(uuid.uuid4())[:8]}"
test_pipeline_name = "policy_gold_transformation_pipeline" 
test_controller_id = 101 

print(f"--- BẮT ĐẦU TEST FRAMEWORK: Log ID (Batch) = {test_log_id} ---")

try:
    # 1. Gọi hàm khởi tạo phiên chạy mới
    start_pipeline(spark=spark, log_id=test_log_id, pipeline_run_id=test_pipeline_run_id, controller_id=test_controller_id, pipeline_name=test_pipeline_name)
    print("-> Bước 1: Đã gọi start_pipeline() thành công.")

    # 2. Tạo dữ liệu Mock mẫu
    mock_data = [
        (1, "Nguyen Van An", "Male"),
        (2, "Tran Thi Hoa Hong pham", "Female"),
        (3, "Pham Minh Duc", "gay"), #update
        (4, "Pham Minh chu", "gbt") # insert 
    ]
    mock_df = spark.createDataFrame(mock_data, ["customer_id", "customer_name", "gender"])
    print(f"-> Bước 2: Tạo dữ liệu mẫu thành công.")

    # 3. Gọi hàm TRÁI TIM tự động: upsert_to_gold
    print("-> Bước 3: Đang gọi upsert_to_gold(). Hệ thống tự động Look-up cấu hình...")
    r_read, r_inserted, r_updated = upsert_to_gold(spark=spark, source_df=mock_df, pipeline_name=test_pipeline_name, log_id=test_log_id)
    
    print(f"-> Bước 4: Thực thi xử lý dữ liệu hoàn tất.")

    # 4. THAY ĐỔI TẠI ĐÂY: Gọi hàm kết thúc truyền tách biệt hai biến đếm dòng mới
    end_pipeline(
        spark=spark,
        log_id=test_log_id,
        status="Success",
        rows_read=r_read,
        rows_inserted=r_inserted,  # THAY ĐỔI: Truyền cột inserted riêng
        rows_updated=r_updated,    # THAY ĐỔI: Truyền cột updated riêng
        rows_rejected=0,
        watermark_from=None,
        watermark_to=None
    )
    print("--- TEST THÀNH CÔNG: Đã đóng phiên với trạng thái 'Success' ---")

except Exception as e:
    # Xử lý kịch bản sập luồng: Tự động ghi nhận log lỗi sâu và đóng tiến trình dưới dạng 'Failed'
    import traceback
    error_trace = traceback.format_exc()
    
    try:
        log_pipeline_error(
            spark=spark,
            log_id=test_log_id,
            pipeline_name=test_pipeline_name,
            layer_name="table4test",                   # THAY ĐỔI: Chuyển layer thành table4test
            target_table="table4test.test_customer_target", # THAY ĐỔI: Chỉ định đích danh bảng test mới
            error_code="TEST_CRASH",
            error_message=str(e),
            error_stack_trace=error_trace,
            bad_record_content_json=None
        )
        print("-> Đã ghi nhận mã lỗi chi tiết vào bảng error_logging_table.")
    except Exception as log_err:
        print(f"Không thể ghi log lỗi phụ trợ: {str(log_err)}")

    # Cập nhật trạng thái 'Failed' cho bảng execution tổng
    end_pipeline(
        spark=spark, 
        log_id=test_log_id, 
        status="Failed", 
        error_message=str(e)
    )
    print(f"--- TEST THẤT BẠI: Tiến trình sập nguồn: {str(e)} ---")
    raise e

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * FROM metadata.etl_transform_config;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

df = spark.sql("SELECT * FROM insurance_lakehouse.audit.etl_audit_log order by event_time desc LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

df = spark.sql("SELECT * FROM insurance_lakehouse.audit.etl_batch_run order by start_time desc LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }
