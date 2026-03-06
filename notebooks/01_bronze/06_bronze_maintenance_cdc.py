# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 06 — Bronze: Maintenance Records (CDC Ingestion)
# MAGIC **Smart Manufacturing IoT Lakehouse Platform**
# MAGIC
# MAGIC Ingests maintenance records with CDC (Change Data Capture) metadata.
# MAGIC Simulates JDBC incremental read from Azure SQL Database.
# MAGIC
# MAGIC | Feature | Implementation |
# MAGIC |---------|---------------|
# MAGIC | Source | Delta table simulating Azure SQL CDC |
# MAGIC | CDC Columns | `_cdc_operation`, `_cdc_timestamp`, `_lsn` |
# MAGIC | Operations | INSERT, UPDATE, DELETE |
# MAGIC | PII | `maintenance_technician_id` (masked in governance views) |

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
import uuid

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Read CDC Events from Landing Zone

# COMMAND ----------

# Auto-detect workspace catalog
catalogs = [row[0] for row in spark.sql("SHOW CATALOGS").collect()]
uc_catalogs = [c for c in catalogs if c not in ('hive_metastore', 'system', '__databricks_internal', 'samples')]
CATALOG = uc_catalogs[0] if uc_catalogs else "manufacturing_lakehouse"
print(f"Using catalog: {CATALOG}")

LANDING_PATH = f"/Volumes/{CATALOG}/landing/raw_data/maintenance_records"
BRONZE_TABLE = "manufacturing_raw.bronze_maintenance_records"

batch_id = str(uuid.uuid4())[:8]

# In production with JDBC:
#   df = (spark.read
#     .format("jdbc")
#     .option("url", "jdbc:sqlserver://<server>.database.windows.net:1433;database=<db>")
#     .option("dbtable", "dbo.maintenance_records_cdc")
#     .option("user", dbutils.secrets.get("keyvault-scope", "sql-user"))
#     .option("password", dbutils.secrets.get("keyvault-scope", "sql-password"))
#     .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
#     # Incremental read using high watermark
#     .option("query", f"""
#       SELECT * FROM cdc.dbo_maintenance_records_CT
#       WHERE __$start_lsn > '{last_processed_lsn}'
#       ORDER BY __$start_lsn
#     """)
#     .load())

# Read from simulated CDC landing zone (Delta)
df_raw = spark.read.format("delta").load(LANDING_PATH)

# Add ingestion metadata
df_bronze = (
    df_raw
    .withColumn("_ingestion_timestamp", current_timestamp())
    .withColumn("_batch_id", lit(batch_id))
    .withColumn("_source_system", lit("azure_sql_cdc"))
)

print(f"CDC records read: {df_bronze.count()}")
print(f"  INSERTs: {df_bronze.filter(col('_cdc_operation') == 'INSERT').count()}")
print(f"  UPDATEs: {df_bronze.filter(col('_cdc_operation') == 'UPDATE').count()}")
print(f"  DELETEs: {df_bronze.filter(col('_cdc_operation') == 'DELETE').count()}")

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

print(f"✅ Bronze Maintenance Records written to: {BRONZE_TABLE}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT _cdc_operation, COUNT(*) AS record_count
# MAGIC FROM manufacturing_raw.bronze_maintenance_records
# MAGIC GROUP BY _cdc_operation
# MAGIC ORDER BY _cdc_operation;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT maintenance_id, equipment_id, facility_id, maintenance_type,
# MAGIC        _cdc_operation, _cdc_timestamp, _lsn
# MAGIC FROM manufacturing_raw.bronze_maintenance_records
# MAGIC ORDER BY _cdc_timestamp;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Also Ingest Equipment Master (Delta Source)
# MAGIC
# MAGIC The Equipment Master is the source of truth for SCD Type 2 dim_equipment.

# COMMAND ----------

EQUIPMENT_PATH = f"/Volumes/{CATALOG}/landing/raw_data/equipment_master"
BRONZE_EQUIPMENT_TABLE = "manufacturing_raw.bronze_equipment_master"

df_equipment = (
    spark.read
    .format("delta")
    .load(EQUIPMENT_PATH)
    .withColumn("_ingestion_timestamp", current_timestamp())
    .withColumn("_batch_id", lit(batch_id))
    .withColumn("_source_system", lit("adls_gen2_delta"))
)

(
    df_equipment.write
    .format("delta")
    .mode("overwrite")
    .option("delta.enableChangeDataFeed", "true")
    .option("overwriteSchema", "true")
    .saveAsTable(BRONZE_EQUIPMENT_TABLE)
)

print(f"✅ Bronze Equipment Master written to: {BRONZE_EQUIPMENT_TABLE}")
print(f"   Records: {df_equipment.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Ingest Facility Reference Data

# COMMAND ----------

FACILITY_PATH = f"/Volumes/{CATALOG}/landing/raw_data/dim_facility"
BRONZE_FACILITY_TABLE = "manufacturing_raw.bronze_dim_facility"

df_facility = (
    spark.read
    .format("delta")
    .load(FACILITY_PATH)
    .withColumn("_ingestion_timestamp", current_timestamp())
    .withColumn("_batch_id", lit(batch_id))
)

(
    df_facility.write
    .format("delta")
    .mode("overwrite")
    .option("delta.enableChangeDataFeed", "true")
    .saveAsTable(BRONZE_FACILITY_TABLE)
)

print(f"✅ Bronze Facility Reference written to: {BRONZE_FACILITY_TABLE}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM manufacturing_raw.bronze_dim_facility;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Summary of all Bronze tables
# MAGIC SHOW TABLES IN manufacturing_raw;

# COMMAND ----------

print("✅ Notebook 06 Complete — Bronze Maintenance CDC + Equipment Master + Facility")
print("🚀 NEXT: Run Notebook 07 (Silver Transformations)")
