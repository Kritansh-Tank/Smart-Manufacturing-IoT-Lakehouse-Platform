# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 05 — Bronze: Quality Inspection (CSV Autoloader Simulation)
# MAGIC **Smart Manufacturing IoT Lakehouse Platform**
# MAGIC
# MAGIC Ingests quality inspection CSV files from ADLS Gen2 Volumes (simulated via DBFS).
# MAGIC PII column: `inspector_employee_id` (masked in governance views).

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
import uuid

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Read Quality Inspection from Landing Zone

# COMMAND ----------

# Auto-detect workspace catalog
catalogs = [row[0] for row in spark.sql("SHOW CATALOGS").collect()]
uc_catalogs = [c for c in catalogs if c not in ('hive_metastore', 'system', '__databricks_internal', 'samples')]
CATALOG = uc_catalogs[0] if uc_catalogs else "manufacturing_lakehouse"
print(f"Using catalog: {CATALOG}")

LANDING_PATH = f"/Volumes/{CATALOG}/landing/raw_data/quality_inspection"
BRONZE_TABLE = "manufacturing_raw.bronze_quality_inspection"

batch_id = str(uuid.uuid4())[:8]

# Schema for CSV files
quality_schema = StructType([
    StructField("inspection_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("equipment_id", StringType(), True),
    StructField("facility_id", StringType(), True),
    StructField("inspector_employee_id", StringType(), True),  # PII
    StructField("inspection_timestamp", TimestampType(), True),
    StructField("product_sku", StringType(), True),
    StructField("batch_id", StringType(), True),
    StructField("units_inspected", IntegerType(), True),
    StructField("units_passed", IntegerType(), True),
    StructField("units_rejected", IntegerType(), True),
    StructField("defect_type", StringType(), True),
    StructField("defect_severity", StringType(), True),
    StructField("inspection_method", StringType(), True),
    StructField("result", StringType(), True),
])

# Read CSV from landing zone
# In production with Autoloader:
#   spark.readStream.format("cloudFiles")
#     .option("cloudFiles.format", "csv")
#     .option("header", "true")
#     .option("cloudFiles.schemaHints", "units_inspected INT")
#     .option("cloudFiles.useNotifications", "true")
#     .load(LANDING_PATH)

df_raw = (
    spark.read
    .format("csv")
    .option("header", "true")
    .schema(quality_schema)
    .option("mode", "PERMISSIVE")
    .option("columnNameOfCorruptRecord", "_rescued_data")
    .load(LANDING_PATH)
)

# Add ingestion metadata
df_bronze = (
    df_raw
    .withColumn("_input_file_name", col("_metadata.file_path"))
    .withColumn("_processing_timestamp", current_timestamp())
    .withColumn("_batch_id", lit(batch_id))
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

print(f"✅ Bronze Quality Inspection written to: {BRONZE_TABLE}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS total_inspections,
# MAGIC        COUNT(DISTINCT facility_id) AS unique_facilities,
# MAGIC        SUM(CASE WHEN result = 'PASS' THEN 1 ELSE 0 END) AS passed,
# MAGIC        SUM(CASE WHEN result = 'FAIL' THEN 1 ELSE 0 END) AS failed
# MAGIC FROM manufacturing_raw.bronze_quality_inspection;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM manufacturing_raw.bronze_quality_inspection LIMIT 10;

# COMMAND ----------

print("✅ Notebook 05 Complete — Bronze Quality Inspection")
print("🚀 NEXT: Run Notebook 06 (Bronze Maintenance CDC)")
