# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 04 — Bronze: Production Orders (Autoloader Simulation)
# MAGIC **Smart Manufacturing IoT Lakehouse Platform**
# MAGIC
# MAGIC Ingests production order Parquet files from ADLS Gen2 landing zone (simulated via DBFS).
# MAGIC
# MAGIC | Feature | Implementation |
# MAGIC |---------|---------------|
# MAGIC | Source | Parquet files from `/Volumes/main/default/landing/production_orders/` |
# MAGIC | Ingestion | Batch read (Autoloader simulation) |
# MAGIC | Rescue Column | `_rescued_data` for schema mismatches |
# MAGIC | Metadata | `_input_file_name`, `_processing_timestamp` |
# MAGIC | CDF | Enabled |

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
import uuid

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Read Production Orders from Landing Zone

# COMMAND ----------

# Auto-detect workspace catalog
catalogs = [row[0] for row in spark.sql("SHOW CATALOGS").collect()]
uc_catalogs = [c for c in catalogs if c not in ('hive_metastore', 'system', '__databricks_internal', 'samples')]
CATALOG = uc_catalogs[0] if uc_catalogs else "manufacturing_lakehouse"
print(f"Using catalog: {CATALOG}")

LANDING_PATH = f"/Volumes/{CATALOG}/landing/raw_data/production_orders"
BRONZE_TABLE = "manufacturing_raw.bronze_production_orders"

batch_id = str(uuid.uuid4())[:8]

# Schema hints (Autoloader equivalent)
order_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("facility_id", StringType(), False),
    StructField("product_sku", StringType(), True),
    StructField("quantity_ordered", IntegerType(), True),
    StructField("start_time", TimestampType(), True),
    StructField("equipment_id", StringType(), True),
    StructField("priority", StringType(), True),
])

# Read Parquet files from landing zone
# In production with Autoloader:
#   spark.readStream.format("cloudFiles")
#     .option("cloudFiles.format", "parquet")
#     .option("cloudFiles.schemaLocation", schema_location)
#     .option("cloudFiles.schemaHints", "quantity_ordered INT, start_time TIMESTAMP")
#     .option("cloudFiles.useNotifications", "true")  # Event Grid trigger
#     .load(LANDING_PATH)

df_raw = (
    spark.read
    .format("parquet")
    .schema(order_schema)
    .load(LANDING_PATH)
)

# Add ingestion metadata (Autoloader would add these automatically)
df_bronze = (
    df_raw
    .withColumn("_input_file_name", col("_metadata.file_path"))
    .withColumn("_processing_timestamp", current_timestamp())
    .withColumn("_batch_id", lit(batch_id))
    .withColumn("_rescued_data", lit(None).cast(StringType()))
)

print(f"Records read from landing zone: {df_bronze.count()}")
df_bronze.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Write to Bronze Delta Table

# COMMAND ----------

(
    df_bronze.write
    .format("delta")
    .mode("overwrite")
    .option("delta.enableChangeDataFeed", "true")
    .option("overwriteSchema", "true")
    .saveAsTable(BRONZE_TABLE)
)

print(f"✅ Bronze Production Orders written to: {BRONZE_TABLE}")
print(f"   Batch ID: {batch_id}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS total_orders,
# MAGIC        COUNT(DISTINCT facility_id) AS unique_facilities,
# MAGIC        COUNT(DISTINCT equipment_id) AS unique_equipment,
# MAGIC        MIN(start_time) AS earliest_order,
# MAGIC        MAX(start_time) AS latest_order
# MAGIC FROM manufacturing_raw.bronze_production_orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM manufacturing_raw.bronze_production_orders LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Streaming Simulation (Autoloader Equivalent)
# MAGIC
# MAGIC Demonstrates how Autoloader would incrementally process new files.

# COMMAND ----------

CHECKPOINT_PATH = f"/Volumes/{CATALOG}/landing/raw_data/_checkpoints/bronze_production_orders"
try:
    dbutils.fs.rm(CHECKPOINT_PATH, recurse=True)
except:
    import shutil
    if os.path.exists(CHECKPOINT_PATH): shutil.rmtree(CHECKPOINT_PATH)

# Simulate Autoloader with file-based streaming
stream_query = (
    spark.readStream
    .format("parquet")
    .schema(order_schema)
    .option("maxFilesPerTrigger", 1)
    .load(LANDING_PATH)
    .withColumn("_input_file_name", col("_metadata.file_path"))
    .withColumn("_processing_timestamp", current_timestamp())
    .withColumn("_batch_id", lit(f"stream-{batch_id}"))
    .withColumn("_rescued_data", lit(None).cast(StringType()))
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .option("delta.enableChangeDataFeed", "true")
    .trigger(availableNow=True)
    .toTable("manufacturing_raw.bronze_production_orders_stream")
)

stream_query.awaitTermination()
print("✅ Streaming ingestion for Production Orders completed")

# COMMAND ----------

print("✅ Notebook 04 Complete — Bronze Production Orders")
print("🚀 NEXT: Run Notebook 05 (Bronze Quality Inspection)")
