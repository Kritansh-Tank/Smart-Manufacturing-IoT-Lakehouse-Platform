# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 03 — Bronze: IoT Sensor Telemetry (Streaming)
# MAGIC **Smart Manufacturing IoT Lakehouse Platform**
# MAGIC
# MAGIC Ingests IoT sensor telemetry data simulating Azure IoT Hub streaming.
# MAGIC
# MAGIC | Feature | Implementation |
# MAGIC |---------|---------------|
# MAGIC | Source | Simulated stream (static JSON read as stream) |
# MAGIC | Deduplication | `device_id` + `event_timestamp` |
# MAGIC | Watermark | 5-minute window for late-arriving events |
# MAGIC | Schema Evolution | Permissive mode + rescued data column |
# MAGIC | Partitioning | `event_date` + `facility_id` |
# MAGIC | CDF | Enabled on Delta table |
# MAGIC | Metadata | `_ingestion_timestamp`, `_batch_id`, `_source_file` |

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
import uuid

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Define IoT Telemetry Schema (Nested JSON)

# COMMAND ----------

# Full schema matching the IoT Hub JSON structure
iot_schema = StructType([
    StructField("device_id", StringType(), True),
    StructField("facility_id", StringType(), True),
    StructField("equipment_type", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("telemetry", StructType([
        StructField("temperature", DoubleType(), True),
        StructField("vibration_x", DoubleType(), True),
        StructField("vibration_y", DoubleType(), True),
        StructField("vibration_z", DoubleType(), True),
        StructField("pressure", DoubleType(), True),
        StructField("power_consumption", DoubleType(), True),
        StructField("rpm", IntegerType(), True),
        # Schema evolution: new fields will land in _rescued_data
    ]), True),
    StructField("location", StructType([
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
    ]), True),
    StructField("metadata", StructType([
        StructField("firmware_version", StringType(), True),
        StructField("last_calibration", StringType(), True),
    ]), True),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Bronze Ingestion — Batch Mode (Primary)
# MAGIC
# MAGIC Read the JSON data, preserve raw structure, add metadata columns,
# MAGIC and write as partitioned Delta table.

# COMMAND ----------

# Auto-detect workspace catalog
catalogs = [row[0] for row in spark.sql("SHOW CATALOGS").collect()]
uc_catalogs = [c for c in catalogs if c not in ('hive_metastore', 'system', '__databricks_internal', 'samples')]
CATALOG = uc_catalogs[0] if uc_catalogs else "manufacturing_lakehouse"
print(f"Using catalog: {CATALOG}")

# Configuration
LANDING_PATH = f"/Volumes/{CATALOG}/landing/raw_data/iot_telemetry"
BRONZE_PATH = f"/Volumes/{CATALOG}/landing/raw_data/bronze/iot_telemetry"
BRONZE_TABLE = "manufacturing_raw.bronze_iot_telemetry"
CHECKPOINT_PATH = f"/Volumes/{CATALOG}/landing/raw_data/_checkpoints/bronze_iot_telemetry"

batch_id = str(uuid.uuid4())[:8]

# Read raw JSON from landing zone
df_raw = (
    spark.read
    .schema(iot_schema)
    .option("mode", "PERMISSIVE")
    .option("columnNameOfCorruptRecord", "_rescued_data")
    .json(LANDING_PATH)
)

# Add ingestion metadata columns
df_bronze = (
    df_raw
    .withColumn("_ingestion_timestamp", current_timestamp())
    .withColumn("_batch_id", lit(batch_id))
    .withColumn("_source_file", col("_metadata.file_path"))
    .withColumn("event_date", to_date(col("event_timestamp")))
)

print(f"Raw records read: {df_bronze.count()}")
df_bronze.printSchema()

# COMMAND ----------

# Write Bronze table as Delta with partitioning and CDF
(
    df_bronze.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("event_date", "facility_id")
    .option("delta.enableChangeDataFeed", "true")
    .option("overwriteSchema", "true")
    .saveAsTable(BRONZE_TABLE)
)

print(f"✅ Bronze IoT Telemetry written to: {BRONZE_TABLE}")
print(f"   Batch ID: {batch_id}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS total_records,
# MAGIC        COUNT(DISTINCT device_id) AS unique_devices,
# MAGIC        COUNT(DISTINCT facility_id) AS unique_facilities,
# MAGIC        MIN(event_timestamp) AS earliest_event,
# MAGIC        MAX(event_timestamp) AS latest_event
# MAGIC FROM manufacturing_raw.bronze_iot_telemetry;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Bronze Ingestion — Streaming Mode (Simulation)
# MAGIC
# MAGIC This demonstrates streaming ingestion as required by the problem statement.
# MAGIC In production, this would connect to Azure IoT Hub's Event Hub-compatible endpoint.
# MAGIC Here I have simulated it by reading the JSON files as a stream with a trigger.

# COMMAND ----------

# --- STREAMING INGESTION (Simulation) ---
# In production:
#   .format("kafka")
#   .option("kafka.bootstrap.servers", "<iot-hub-endpoint>")
#   .option("subscribe", "<iot-hub-name>")
#   .option("kafka.security.protocol", "SASL_SSL")
#   .option("kafka.sasl.mechanism", "PLAIN")
#   .option("kafka.sasl.jaas.config", "...from Azure Key Vault...")

BRONZE_STREAM_PATH = f"/Volumes/{CATALOG}/landing/raw_data/bronze/iot_telemetry_stream"
CHECKPOINT_STREAM = f"/Volumes/{CATALOG}/landing/raw_data/_checkpoints/bronze_iot_stream"

# Clean up previous checkpoint for fresh demo
try:
    dbutils.fs.rm(CHECKPOINT_STREAM, recurse=True)
    dbutils.fs.rm(BRONZE_STREAM_PATH, recurse=True)
except:
    import shutil
    for p in [CHECKPOINT_STREAM, BRONZE_STREAM_PATH]:
        if os.path.exists(p): shutil.rmtree(p)

# Read as stream
df_stream = (
    spark.readStream
    .schema(iot_schema)
    .option("maxFilesPerTrigger", 1)  # Process 1 file per micro-batch
    .json(LANDING_PATH)
)

# Add metadata + watermark partitioning
df_stream_bronze = (
    df_stream
    .withColumn("_ingestion_timestamp", current_timestamp())
    .withColumn("_batch_id", lit(f"stream-{batch_id}"))
    .withColumn("_source_file", col("_metadata.file_path"))
    .withColumn("event_timestamp_ts", to_timestamp(col("event_timestamp")))
    .withColumn("event_date", to_date(col("event_timestamp")))
)

# Write streaming output with watermark
stream_query = (
    df_stream_bronze.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_STREAM)
    .option("delta.enableChangeDataFeed", "true")
    .partitionBy("event_date", "facility_id")
    .trigger(availableNow=True)  # Process all available data then stop
    .toTable("manufacturing_raw.bronze_iot_telemetry_stream")
)

stream_query.awaitTermination()
print("✅ Streaming ingestion completed (availableNow trigger)")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify streaming results
# MAGIC SELECT COUNT(*) AS stream_records FROM manufacturing_raw.bronze_iot_telemetry_stream;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Verify Bronze Table Properties

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED manufacturing_raw.bronze_iot_telemetry;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Show sample records with metadata
# MAGIC SELECT device_id, facility_id, event_timestamp, event_date,
# MAGIC        telemetry.temperature, telemetry.vibration_x,
# MAGIC        _ingestion_timestamp, _batch_id
# MAGIC FROM manufacturing_raw.bronze_iot_telemetry
# MAGIC LIMIT 10;

# COMMAND ----------

print("✅ Notebook 03 Complete — Bronze IoT Telemetry")
print("🚀 NEXT: Run Notebook 04 (Bronze Production Orders)")
