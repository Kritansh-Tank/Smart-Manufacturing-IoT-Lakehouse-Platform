# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 07 — Silver: Transformations + SCD Type 2
# MAGIC **Smart Manufacturing IoT Lakehouse Platform**
# MAGIC
# MAGIC This notebook transforms all Bronze data into Silver (cleaned, validated, deduplicated).
# MAGIC
# MAGIC | Source | Transformations |
# MAGIC |--------|----------------|
# MAGIC | IoT Telemetry | Flatten nested JSON, dedup on `device_id + event_timestamp`, cast types |
# MAGIC | Production Orders | Cast types, validate, clean |
# MAGIC | Quality Inspection | Cast types, validate, clean |
# MAGIC | Maintenance Records | Apply CDC operations (INSERT/UPDATE/DELETE) |
# MAGIC | Equipment Master | **SCD Type 2** with MERGE for historical tracking |

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from delta.tables import DeltaTable
import uuid

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Silver IoT Sensor Telemetry
# MAGIC - Flatten nested JSON (`telemetry.*`, `location.*`, `metadata.*`)
# MAGIC - Deduplicate on `device_id` + `event_timestamp`
# MAGIC - Cast `event_timestamp` to TimestampType

# COMMAND ----------

SILVER_IOT_TABLE = "manufacturing_refined.silver_iot_telemetry"

df_bronze_iot = spark.table("manufacturing_raw.bronze_iot_telemetry")

# Flatten nested JSON structure
df_silver_iot = (
    df_bronze_iot
    # Flatten telemetry
    .withColumn("temperature", col("telemetry.temperature"))
    .withColumn("vibration_x", col("telemetry.vibration_x"))
    .withColumn("vibration_y", col("telemetry.vibration_y"))
    .withColumn("vibration_z", col("telemetry.vibration_z"))
    .withColumn("pressure", col("telemetry.pressure"))
    .withColumn("power_consumption", col("telemetry.power_consumption"))
    .withColumn("rpm", col("telemetry.rpm"))
    # Flatten location
    .withColumn("latitude", col("location.latitude"))
    .withColumn("longitude", col("location.longitude"))
    # Flatten metadata
    .withColumn("firmware_version", col("metadata.firmware_version"))
    .withColumn("last_calibration", to_date(col("metadata.last_calibration")))
    # Cast event_timestamp to proper type
    .withColumn("event_timestamp", to_timestamp(col("event_timestamp")))
    # Drop nested columns (keep flattened)
    .drop("telemetry", "location", "metadata")
)

# --- DEDUPLICATION: Keep first occurrence (by ingestion time) for each device_id + event_timestamp ---
window_spec = Window.partitionBy("device_id", "event_timestamp").orderBy("_ingestion_timestamp")

df_silver_iot_deduped = (
    df_silver_iot
    .withColumn("_row_num", row_number().over(window_spec))
    .filter(col("_row_num") == 1)
    .drop("_row_num")
)

original_count = df_silver_iot.count()
deduped_count = df_silver_iot_deduped.count()
print(f"Original records: {original_count}")
print(f"After dedup:      {deduped_count}")
print(f"Duplicates removed: {original_count - deduped_count}")

# COMMAND ----------

# Write Silver IoT table
(
    df_silver_iot_deduped.write
    .format("delta")
    .mode("overwrite")
    .option("delta.enableChangeDataFeed", "true")
    .option("overwriteSchema", "true")
    .partitionBy("event_date", "facility_id")
    .saveAsTable(SILVER_IOT_TABLE)
)

print(f"✅ Silver IoT Telemetry written to: {SILVER_IOT_TABLE}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT device_id, facility_id, event_timestamp, temperature, vibration_x,
# MAGIC        vibration_y, pressure, power_consumption, rpm, latitude, longitude
# MAGIC FROM manufacturing_refined.silver_iot_telemetry
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Silver Production Orders
# MAGIC - Type casting and validation
# MAGIC - Add derived columns

# COMMAND ----------

SILVER_ORDERS_TABLE = "manufacturing_refined.silver_production_orders"

df_bronze_orders = spark.table("manufacturing_raw.bronze_production_orders")

df_silver_orders = (
    df_bronze_orders
    .withColumn("start_date", to_date(col("start_time")))
    .withColumn("priority_rank", 
        when(col("priority") == "HIGH", 1)
        .when(col("priority") == "MEDIUM", 2)
        .when(col("priority") == "LOW", 3)
        .otherwise(99)
    )
    # Standardize priority to uppercase
    .withColumn("priority", upper(trim(col("priority"))))
    # Add processing metadata
    .withColumn("_silver_processed_at", current_timestamp())
)

(
    df_silver_orders.write
    .format("delta")
    .mode("overwrite")
    .option("delta.enableChangeDataFeed", "true")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_ORDERS_TABLE)
)

print(f"✅ Silver Production Orders: {df_silver_orders.count()} records")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM manufacturing_refined.silver_production_orders LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Silver Quality Inspection
# MAGIC - Type casting and validation
# MAGIC - Calculate defect rate

# COMMAND ----------

SILVER_QUALITY_TABLE = "manufacturing_refined.silver_quality_inspection"

df_bronze_quality = spark.table("manufacturing_raw.bronze_quality_inspection")

df_silver_quality = (
    df_bronze_quality
    # Calculate defect rate
    .withColumn("defect_rate", 
        when(col("units_inspected") > 0, 
             round(col("units_rejected") / col("units_inspected") * 100, 2))
        .otherwise(lit(0.0))
    )
    # Standardize result
    .withColumn("result", upper(trim(col("result"))))
    # Add inspection date for partitioning
    .withColumn("inspection_date", to_date(col("inspection_timestamp")))
    # Add processing metadata
    .withColumn("_silver_processed_at", current_timestamp())
)

(
    df_silver_quality.write
    .format("delta")
    .mode("overwrite")
    .option("delta.enableChangeDataFeed", "true")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_QUALITY_TABLE)
)

print(f"✅ Silver Quality Inspection: {df_silver_quality.count()} records")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT inspection_id, facility_id, units_inspected, units_passed, 
# MAGIC        units_rejected, defect_rate, result
# MAGIC FROM manufacturing_refined.silver_quality_inspection;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Silver Maintenance Records (Apply CDC Operations)
# MAGIC
# MAGIC Process CDC events to build the current state of maintenance records:
# MAGIC - INSERT → Add new record
# MAGIC - UPDATE → Update existing record
# MAGIC - DELETE → Mark as deleted (soft delete)

# COMMAND ----------

SILVER_MAINTENANCE_TABLE = "manufacturing_refined.silver_maintenance"

df_bronze_maint = spark.table("manufacturing_raw.bronze_maintenance_records")

# Sort by _cdc_timestamp to process operations in order
# For each maintenance_id, apply the latest CDC operation
window_cdc = Window.partitionBy("maintenance_id").orderBy(col("_cdc_timestamp").desc())

df_latest_cdc = (
    df_bronze_maint
    .withColumn("_cdc_rank", row_number().over(window_cdc))
    .filter(col("_cdc_rank") == 1)
    .drop("_cdc_rank")
)

# Filter out DELETEs (soft delete — keep them flagged)
df_silver_maint = (
    df_latest_cdc
    .withColumn("is_deleted", when(col("_cdc_operation") == "DELETE", lit(True)).otherwise(lit(False)))
    .withColumn("_silver_processed_at", current_timestamp())
    .withColumn("maintenance_date", to_date(col("start_datetime")))
)

(
    df_silver_maint.write
    .format("delta")
    .mode("overwrite")
    .option("delta.enableChangeDataFeed", "true")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_MAINTENANCE_TABLE)
)

print(f"✅ Silver Maintenance Records: {df_silver_maint.count()} records")
print(f"   Active: {df_silver_maint.filter(~col('is_deleted')).count()}")
print(f"   Deleted: {df_silver_maint.filter(col('is_deleted')).count()}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT maintenance_id, equipment_id, maintenance_type, 
# MAGIC        _cdc_operation, is_deleted, downtime_minutes, maintenance_cost_usd
# MAGIC FROM manufacturing_refined.silver_maintenance
# MAGIC ORDER BY _cdc_timestamp;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Silver Equipment Master + SCD Type 2
# MAGIC
# MAGIC Implement Slowly Changing Dimension Type 2 for the equipment dimension.
# MAGIC This tracks historical changes with:
# MAGIC - `effective_start_date` — when this version became active
# MAGIC - `effective_end_date` — when this version was superseded (NULL if current)
# MAGIC - `is_current` — TRUE for the latest version
# MAGIC - `version_number` — incrementing version counter

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 Initial Load (First Run)

# COMMAND ----------

SILVER_EQUIPMENT_TABLE = "manufacturing_refined.silver_equipment_scd2"

df_bronze_equipment = spark.table("manufacturing_raw.bronze_equipment_master")

# Add SCD Type 2 columns for initial load
df_initial_scd2 = (
    df_bronze_equipment
    .withColumn("equipment_sk", monotonically_increasing_id() + 1)  # Surrogate key
    .withColumn("effective_start_date", to_date(col("installation_date")))
    .withColumn("effective_end_date", lit(None).cast(DateType()))
    .withColumn("is_current", lit(True))
    .withColumn("version_number", lit(1))
    .withColumn("_scd2_processed_at", current_timestamp())
)

# Write initial SCD2 table
(
    df_initial_scd2.write
    .format("delta")
    .mode("overwrite")
    .option("delta.enableChangeDataFeed", "true")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_EQUIPMENT_TABLE)
)

print(f"✅ Silver Equipment SCD2 initial load: {df_initial_scd2.count()} records")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT equipment_id, equipment_serial_number, equipment_status, 
# MAGIC        effective_start_date, effective_end_date, is_current, version_number
# MAGIC FROM manufacturing_refined.silver_equipment_scd2
# MAGIC ORDER BY equipment_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 SCD Type 2 MERGE Logic (For Subsequent Runs)
# MAGIC
# MAGIC This demonstrates how equipment changes are tracked historically.

# COMMAND ----------

def apply_scd2_merge(spark, source_df, target_table, business_key="equipment_id",
                     tracked_columns=None):
    """
    Apply SCD Type 2 MERGE to track historical changes in equipment dimension.
    
    Args:
        source_df: New/updated equipment records
        target_table: Target SCD2 Delta table name
        business_key: Natural key column
        tracked_columns: Columns to track for changes
    """
    if tracked_columns is None:
        tracked_columns = [
            "equipment_status", "rated_capacity", "operating_voltage",
            "max_temperature_c", "max_vibration_hz", "maintenance_interval_days",
            "asset_value_usd", "criticality_level"
        ]
    
    target = DeltaTable.forName(spark, target_table)
    
    # Build change detection condition
    change_conditions = " OR ".join([
        f"target.{col} <> source.{col}" for col in tracked_columns
    ])
    
    # Step 1: Identify changed records
    # Get current records from target
    df_current = target.toDF().filter(col("is_current") == True)
    
    # Join source with current target to find changes
    df_changes = (
        source_df.alias("source")
        .join(df_current.alias("target"), 
              col(f"source.{business_key}") == col(f"target.{business_key}"),
              "inner")
        .filter(expr(change_conditions))
        .select("source.*")
    )
    
    if df_changes.count() == 0:
        print("No changes detected — SCD2 MERGE skipped")
        return
    
    # IMPORTANT: Materialize df_changes NOW by collecting to Python,
    # because target.update() will set is_current=false and lazy eval
    # would then find 0 changes. (.cache() not supported on serverless)
    changes_schema = df_changes.schema
    changes_data = df_changes.collect()
    change_count = len(changes_data)
    print(f"Changes detected: {change_count} records")
    
    # Step 2: Close out old records (set effective_end_date and is_current = False)
    changed_keys = [row[business_key] for row in changes_data]
    
    target.update(
        condition=expr(f"{business_key} IN ({','.join([repr(k) for k in changed_keys])}) AND is_current = true"),
        set={
            "effective_end_date": current_date(),
            "is_current": lit(False),
        }
    )
    
    # Step 3: Get max version for changed keys
    df_max_versions = (
        target.toDF()
        .filter(col(business_key).isin(changed_keys))
        .groupBy(business_key)
        .agg(max("version_number").alias("max_version"))
    )
    
    # Step 4: Insert new current records with incremented version
    # Recreate df_changes from collected data (no longer tied to target table)
    df_changes = spark.createDataFrame(changes_data, schema=changes_schema)
    
    df_new_versions = (
        df_changes
        .join(df_max_versions, business_key, "left")
        .withColumn("equipment_sk", monotonically_increasing_id() + 100000)
        .withColumn("effective_start_date", current_date())
        .withColumn("effective_end_date", lit(None).cast(DateType()))
        .withColumn("is_current", lit(True))
        .withColumn("version_number", col("max_version") + 1)
        .withColumn("_scd2_processed_at", current_timestamp())
        .drop("max_version")
    )
    
    # Align columns with target table schema (add missing columns with correct types)
    target_df = target.toDF()
    target_columns = target_df.columns
    target_schema = {f.name: f.dataType for f in target_df.schema.fields}
    for tc in target_columns:
        if tc not in df_new_versions.columns:
            df_new_versions = df_new_versions.withColumn(tc, lit(None).cast(target_schema[tc]))
    
    # Select columns in the same order as target table
    df_new_versions = df_new_versions.select(target_columns)
    
    # Append new versions
    df_new_versions.write.format("delta").mode("append").saveAsTable(target_table)
    
    print(f"✅ SCD2 MERGE complete: {df_new_versions.count()} new versions created")
    return df_new_versions

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.3 Simulate Equipment Change (Demonstrate SCD Type 2)

# COMMAND ----------

# Simulate a change: Equipment EQ-PUMP-0078 status changes from UNDER_MAINTENANCE to ACTIVE
# and EQ-CNC-0012 gets a capacity upgrade
simulated_changes = [
    ("EQ-PUMP-0078", "SN-PMP-2020-00078", "FAC-APAC-002", "CENTRIFUGAL_PUMP", "Grundfos", "CR-64",
     "2020-11-05", "2021-01-20", 800, 240, 70.0, 60.0, 30, "ACTIVE", 42000.00, "MEDIUM", "APAC"),
    ("EQ-CNC-0012", "SN-CNC-2021-00012", "FAC-NA-001", "CNC_MACHINE", "Haas Automation", "VF-4SS",
     "2021-03-10", "2021-05-01", 600, 480, 85.0, 45.0, 90, "ACTIVE", 185000.00, "HIGH", "NORTH_AMERICA"),
]

# Use original source schema (bronze has extra metadata columns which are not needed)
change_schema = StructType([
    StructField("equipment_id", StringType(), False),
    StructField("equipment_serial_number", StringType(), False),
    StructField("facility_id", StringType(), False),
    StructField("equipment_type", StringType(), False),
    StructField("manufacturer", StringType(), True),
    StructField("model_number", StringType(), True),
    StructField("manufacture_date", StringType(), True),
    StructField("installation_date", StringType(), True),
    StructField("rated_capacity", IntegerType(), True),
    StructField("operating_voltage", IntegerType(), True),
    StructField("max_temperature_c", DoubleType(), True),
    StructField("max_vibration_hz", DoubleType(), True),
    StructField("maintenance_interval_days", IntegerType(), True),
    StructField("equipment_status", StringType(), True),
    StructField("asset_value_usd", DoubleType(), True),
    StructField("criticality_level", StringType(), True),
    StructField("region", StringType(), True),
])

df_changes = spark.createDataFrame(simulated_changes, schema=change_schema) \
    .withColumn("manufacture_date", to_date(col("manufacture_date"))) \
    .withColumn("installation_date", to_date(col("installation_date")))

# Apply SCD Type 2 MERGE
apply_scd2_merge(spark, df_changes, SILVER_EQUIPMENT_TABLE)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Show SCD2 history for changed equipment
# MAGIC SELECT equipment_id, equipment_status, rated_capacity, 
# MAGIC        effective_start_date, effective_end_date, is_current, version_number
# MAGIC FROM manufacturing_refined.silver_equipment_scd2
# MAGIC WHERE equipment_id IN ('EQ-PUMP-0078', 'EQ-CNC-0012')
# MAGIC ORDER BY equipment_id, version_number;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Silver Facility Reference (Passthrough)

# COMMAND ----------

SILVER_FACILITY_TABLE = "manufacturing_refined.silver_dim_facility"

df_facility = (
    spark.table("manufacturing_raw.bronze_dim_facility")
    .withColumn("_silver_processed_at", current_timestamp())
)

(
    df_facility.write
    .format("delta")
    .mode("overwrite")
    .option("delta.enableChangeDataFeed", "true")
    .saveAsTable(SILVER_FACILITY_TABLE)
)

print(f"✅ Silver Facility Reference: {df_facility.count()} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Summary of Silver Layer

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN manufacturing_refined;

# COMMAND ----------

silver_tables = [
    "manufacturing_refined.silver_iot_telemetry",
    "manufacturing_refined.silver_production_orders",
    "manufacturing_refined.silver_quality_inspection",
    "manufacturing_refined.silver_maintenance",
    "manufacturing_refined.silver_equipment_scd2",
    "manufacturing_refined.silver_dim_facility",
]

print("=" * 80)
print("SILVER LAYER COMPLETE")
print("=" * 80)
for table in silver_tables:
    count = spark.table(table).count()
    print(f"  ✅ {table:55s} | {count:6d} records")
print("=" * 80)
print("🚀 NEXT: Run Notebook 08 (Gold Dimensions)")
