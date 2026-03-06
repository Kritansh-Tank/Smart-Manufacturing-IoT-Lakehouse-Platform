# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 01 — Generate Synthetic Data
# MAGIC **Smart Manufacturing IoT Lakehouse Platform**
# MAGIC
# MAGIC This notebook generates synthetic data for all 5 data sources using Unity Catalog Volumes.
# MAGIC Run this **first** before executing any pipeline notebooks.
# MAGIC
# MAGIC | Dataset | Format | Volume Path |
# MAGIC |---------|--------|-------------|
# MAGIC | IoT Sensor Telemetry | JSON | `/Volumes/main/default/landing/iot_telemetry/` |
# MAGIC | Production Orders | Parquet | `/Volumes/main/default/landing/production_orders/` |
# MAGIC | Maintenance Records | Delta | `/Volumes/main/default/landing/maintenance_records/` |
# MAGIC | Quality Inspection | CSV | `/Volumes/main/default/landing/quality_inspection/` |
# MAGIC | Equipment Master | Delta | `/Volumes/main/default/landing/equipment_master/` |
# MAGIC | Facility Reference | Delta | `/Volumes/main/default/landing/dim_facility/` |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Setup & Imports

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import *
# Save Python builtins before PySpark wildcard import overwrites them
import builtins
_python_round = builtins.round
from pyspark.sql.functions import col, to_date, to_timestamp, lit, current_timestamp
import json
import random
from datetime import datetime, timedelta
# Ensure Python's round() is available (not PySpark's)
round = _python_round

spark = SparkSession.builder.getOrCreate()

# Auto-detect the workspace Unity Catalog (varies by workspace)
catalogs = [row[0] for row in spark.sql("SHOW CATALOGS").collect()]
uc_catalogs = [c for c in catalogs if c not in ('hive_metastore', 'system', '__databricks_internal', 'samples')]

if uc_catalogs:
    CATALOG = uc_catalogs[0]
else:
    # Fallback: try to create one
    spark.sql("CREATE CATALOG IF NOT EXISTS manufacturing_lakehouse")
    CATALOG = "manufacturing_lakehouse"

print(f"✅ Using catalog: {CATALOG}")
spark.sql(f"USE CATALOG {CATALOG}")

# Create schema and volume for landing zone
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.landing")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.landing.raw_data")

# Base path using Unity Catalog Volumes
BASE_PATH = f"/Volumes/{CATALOG}/landing/raw_data"

# Clean up previous runs (idempotent) — using Python I/O for serverless compatibility
import os, shutil
for subdir in ["iot_telemetry", "production_orders", "quality_inspection", 
               "maintenance_records", "equipment_master", "dim_facility"]:
    path = f"{BASE_PATH}/{subdir}"
    if os.path.exists(path):
        shutil.rmtree(path)
print(f"✅ Volume ready: {BASE_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Facility Reference Data (dim_facility)
# MAGIC This is the source-of-truth for referential integrity validation.

# COMMAND ----------

facility_data = [
    ("FAC-NA-001", "North America Plant 1", "NORTH_AMERICA", "USA", "New York", "America/New_York", "ACTIVE"),
    ("FAC-NA-002", "North America Plant 2", "NORTH_AMERICA", "USA", "Chicago", "America/Chicago", "ACTIVE"),
    ("FAC-EU-003", "Europe Plant 3", "EUROPE", "Germany", "Stuttgart", "Europe/Berlin", "ACTIVE"),
    ("FAC-APAC-002", "Asia Pacific Plant 2", "APAC", "Japan", "Osaka", "Asia/Tokyo", "ACTIVE"),
    ("FAC-EU-005", "Europe Plant 5", "EUROPE", "UK", "Birmingham", "Europe/London", "INACTIVE"),
]

facility_schema = StructType([
    StructField("facility_id", StringType(), False),
    StructField("facility_name", StringType(), False),
    StructField("region", StringType(), False),
    StructField("country", StringType(), False),
    StructField("city", StringType(), False),
    StructField("timezone", StringType(), False),
    StructField("facility_status", StringType(), False),
])

df_facility = spark.createDataFrame(facility_data, schema=facility_schema)
df_facility.write.format("delta").mode("overwrite").save(f"{BASE_PATH}/dim_facility")
print(f"✅ dim_facility: {df_facility.count()} rows written")
df_facility.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Equipment Master Data
# MAGIC Source of truth for SCD Type 2 `dim_equipment`. Includes PII column `equipment_serial_number`.

# COMMAND ----------

equipment_data = [
    # --- Reference rows from dataset_reference ---
    ("EQ-CNC-0012", "SN-CNC-2021-00012", "FAC-NA-001", "CNC_MACHINE", "Haas Automation", "VF-4SS",
     "2021-03-10", "2021-05-01", 500, 480, 85.0, 45.0, 90, "ACTIVE", 185000.00, "HIGH", "NORTH_AMERICA"),
    ("EQ-PRESS-0045", "SN-PRS-2019-00045", "FAC-EU-003", "HYDRAULIC_PRESS", "Schuler Group", "MSC-250",
     "2019-07-22", "2019-09-15", 250, 400, 75.0, 30.0, 60, "ACTIVE", 320000.00, "CRITICAL", "EUROPE"),
    ("EQ-PUMP-0078", "SN-PMP-2020-00078", "FAC-APAC-002", "CENTRIFUGAL_PUMP", "Grundfos", "CR-64",
     "2020-11-05", "2021-01-20", 800, 240, 70.0, 60.0, 30, "UNDER_MAINTENANCE", 42000.00, "MEDIUM", "APAC"),
    ("EQ-CNC-0031", "SN-CNC-2018-00031", "FAC-NA-001", "CNC_MACHINE", "Mazak", "VARIAXIS-j-500",
     "2018-04-18", "2018-06-30", 450, 480, 90.0, 50.0, 90, "ACTIVE", 210000.00, "HIGH", "NORTH_AMERICA"),
    ("EQ-ROBOT-0005", "SN-ROB-2023-00005", "FAC-EU-003", "WELDING_ROBOT", "KUKA", "KR-210",
     "2023-01-12", "2023-03-01", None, 400, 65.0, 25.0, 180, "ACTIVE", 560000.00, "CRITICAL", "EUROPE"),
    # --- Additional generated rows ---
    ("EQ-LATHE-0019", "SN-LTH-2022-00019", "FAC-NA-002", "LATHE", "DMG Mori", "NLX-2500",
     "2022-06-15", "2022-08-20", 600, 480, 80.0, 40.0, 60, "ACTIVE", 275000.00, "HIGH", "NORTH_AMERICA"),
    ("EQ-WELD-0088", "SN-WLD-2020-00088", "FAC-NA-001", "WELDING_ROBOT", "Fanuc", "ARC-Mate-120",
     "2020-02-10", "2020-04-15", 350, 400, 70.0, 35.0, 120, "ACTIVE", 420000.00, "CRITICAL", "NORTH_AMERICA"),
    ("EQ-CONV-0056", "SN-CNV-2021-00056", "FAC-EU-003", "CONVEYOR", "Siemens", "SIMATIC-CV200",
     "2021-09-01", "2021-11-10", 1000, 240, 55.0, 20.0, 180, "ACTIVE", 95000.00, "LOW", "EUROPE"),
    ("EQ-PRESS-0099", "SN-PRS-2023-00099", "FAC-APAC-002", "HYDRAULIC_PRESS", "Schuler Group", "MSC-500",
     "2023-05-20", "2023-07-01", 500, 400, 80.0, 35.0, 60, "ACTIVE", 480000.00, "CRITICAL", "APAC"),
    ("EQ-PUMP-0101", "SN-PMP-2019-00101", "FAC-EU-005", "CENTRIFUGAL_PUMP", "Grundfos", "CR-96",
     "2019-03-15", "2019-05-25", 1200, 240, 75.0, 55.0, 30, "DECOMMISSIONED", 55000.00, "LOW", "EUROPE"),
    ("EQ-CNC-0042", "SN-CNC-2024-00042", "FAC-NA-002", "CNC_MACHINE", "Haas Automation", "UMC-750",
     "2024-01-10", "2024-03-15", 400, 480, 88.0, 48.0, 90, "ACTIVE", 320000.00, "HIGH", "NORTH_AMERICA"),
    ("EQ-DRILL-0067", "SN-DRL-2022-00067", "FAC-APAC-002", "DRILL_PRESS", "Mazak", "VCN-530C",
     "2022-08-10", "2022-10-20", 300, 480, 78.0, 42.0, 90, "ACTIVE", 195000.00, "MEDIUM", "APAC"),
]

equipment_schema = StructType([
    StructField("equipment_id", StringType(), False),
    StructField("equipment_serial_number", StringType(), False),  # PII — must be masked
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

df_equipment = spark.createDataFrame(equipment_data, schema=equipment_schema)
df_equipment = df_equipment \
    .withColumn("manufacture_date", to_date(col("manufacture_date"))) \
    .withColumn("installation_date", to_date(col("installation_date")))

df_equipment.write.format("delta").mode("overwrite").save(f"{BASE_PATH}/equipment_master")
print(f"✅ equipment_master: {df_equipment.count()} rows written")
df_equipment.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. IoT Sensor Telemetry (JSON)
# MAGIC Simulates Azure IoT Hub streaming data. Includes edge cases: duplicates, late-arriving, out-of-range, nulls.

# COMMAND ----------

# Helper: known device-to-facility mapping
device_facility_map = {
    "PUMP-FAC01-0012":  ("FAC-NA-001",  "CNC_MACHINE"),
    "CNC-FAC01-0031":   ("FAC-NA-001",  "CNC_MACHINE"),
    "PRESS-FAC03-0045": ("FAC-EU-003",  "HYDRAULIC_PRESS"),
    "PUMP-FAC02-0078":  ("FAC-APAC-002","CENTRIFUGAL_PUMP"),
    "LATHE-FAC02-0019": ("FAC-NA-002",  "LATHE"),
    "WELD-FAC01-0088":  ("FAC-NA-001",  "WELDING_ROBOT"),
    "CONV-FAC03-0056":  ("FAC-EU-003",  "CONVEYOR"),
    "PRESS-FAC02-0099": ("FAC-APAC-002","HYDRAULIC_PRESS"),
    "CNC-FAC02-0042":   ("FAC-NA-002",  "CNC_MACHINE"),
    "DRILL-FAC02-0067": ("FAC-APAC-002","DRILL_PRESS"),
}

# Location lookup
facility_coords = {
    "FAC-NA-001":  (40.7128, -74.0060),
    "FAC-NA-002":  (41.8781, -87.6298),
    "FAC-EU-003":  (48.7758, 9.1829),
    "FAC-APAC-002":(34.6937, 135.5023),
    "FAC-EU-005":  (52.4862, -1.8904),
}

def generate_iot_record(device_id, facility_id, eq_type, ts, firmware="2.3.1",
                        temp=None, vib_x=None, vib_y=None, vib_z=None,
                        pressure=None, power=None, rpm=None):
    """Generate a single IoT telemetry record as a dict (for JSON)."""
    lat, lon = facility_coords.get(facility_id, (0.0, 0.0))
    record = {
        "device_id": device_id,
        "facility_id": facility_id,
        "equipment_type": eq_type,
        "event_timestamp": ts,
        "telemetry": {
            "temperature": temp if temp is not None else round(random.uniform(40.0, 95.0), 1),
            "vibration_x": vib_x if vib_x is not None else round(random.uniform(0.5, 8.0), 1),
            "vibration_y": vib_y if vib_y is not None else round(random.uniform(0.3, 6.0), 1),
            "vibration_z": vib_z if vib_z is not None else round(random.uniform(0.3, 6.0), 1),
            "pressure": pressure if pressure is not None else round(random.uniform(100.0, 250.0), 1),
            "power_consumption": power if power is not None else round(random.uniform(5.0, 25.0), 1),
            "rpm": rpm if rpm is not None else random.randint(1000, 4000),
        },
        "location": {
            "latitude": lat,
            "longitude": lon,
        },
        "metadata": {
            "firmware_version": firmware,
            "last_calibration": "2026-02-01",
        },
    }
    return record

# --- Reference rows from dataset_reference ---
iot_records = [
    generate_iot_record("PUMP-FAC01-0012", "FAC-NA-001", "CNC_MACHINE",
                        "2026-02-15T10:23:45.123Z", temp=68.5, vib_x=2.3, vib_y=1.8,
                        pressure=145.2, power=12.8, rpm=3200),
    generate_iot_record("CNC-FAC01-0031", "FAC-NA-001", "CNC_MACHINE",
                        "2026-02-15T10:24:10.456Z", temp=72.1, vib_x=3.1, vib_y=2.5,
                        pressure=148.0, power=13.2, rpm=3150),
    generate_iot_record("PRESS-FAC03-0045", "FAC-EU-003", "HYDRAULIC_PRESS",
                        "2026-02-15T10:25:00.789Z", temp=55.3, vib_x=1.2, vib_y=0.9,
                        pressure=210.5, power=18.4, rpm=1800),
    generate_iot_record("PUMP-FAC02-0078", "FAC-APAC-002", "CENTRIFUGAL_PUMP",
                        "2026-02-15T10:26:33.001Z", temp=61.0, vib_x=4.5, vib_y=4.2,
                        pressure=130.8, power=9.7, rpm=2900),
]

# --- Generate 200+ additional normal records ---
base_time = datetime(2026, 2, 15, 8, 0, 0)
devices = list(device_facility_map.keys())

for i in range(250):
    dev = random.choice(devices)
    fac, eq = device_facility_map[dev]
    ts = (base_time + timedelta(seconds=random.randint(0, 28800))).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    iot_records.append(generate_iot_record(dev, fac, eq, ts))

# --- Edge Case: DUPLICATE records (same device_id + event_timestamp) ---
iot_records.append(generate_iot_record(
    "PUMP-FAC01-0012", "FAC-NA-001", "CNC_MACHINE",
    "2026-02-15T10:23:45.123Z",  # same timestamp as reference row 1
    temp=68.5, vib_x=2.3, vib_y=1.8, pressure=145.2, power=12.8, rpm=3200
))
iot_records.append(generate_iot_record(
    "CNC-FAC01-0031", "FAC-NA-001", "CNC_MACHINE",
    "2026-02-15T10:24:10.456Z",  # same timestamp as reference row 2
    temp=72.1, vib_x=3.1, vib_y=2.5, pressure=148.0, power=13.2, rpm=3150
))

# --- Edge Case: LATE-ARRIVING events (1 hour late) ---
iot_records.append(generate_iot_record(
    "PUMP-FAC01-0012", "FAC-NA-001", "CNC_MACHINE",
    "2026-02-15T07:00:00.000Z",  # event from 7 AM, arriving at ~10:30 AM
    temp=65.0, vib_x=2.0, vib_y=1.5, pressure=140.0, power=11.0, rpm=3100
))

# --- Edge Case: OUT-OF-RANGE temperature (-200°C) ---
iot_records.append(generate_iot_record(
    "PRESS-FAC03-0045", "FAC-EU-003", "HYDRAULIC_PRESS",
    "2026-02-15T11:00:00.000Z",
    temp=-200.0, vib_x=1.0, vib_y=0.8, pressure=200.0, power=17.0, rpm=1750
))

# --- Edge Case: OUT-OF-RANGE vibration (999 Hz) ---
iot_records.append(generate_iot_record(
    "CNC-FAC01-0031", "FAC-NA-001", "CNC_MACHINE",
    "2026-02-15T11:05:00.000Z",
    temp=70.0, vib_x=999.0, vib_y=999.0, pressure=150.0, power=13.0, rpm=3100
))

# --- Edge Case: NULL device_id ---
null_record = generate_iot_record(
    "TEMP-DEVICE", "FAC-NA-001", "CNC_MACHINE",
    "2026-02-15T11:10:00.000Z"
)
null_record["device_id"] = None
iot_records.append(null_record)

# --- Edge Case: NULL event_timestamp ---
null_ts_record = generate_iot_record(
    "PUMP-FAC01-0012", "FAC-NA-001", "CNC_MACHINE",
    "TEMP"
)
null_ts_record["event_timestamp"] = None
iot_records.append(null_ts_record)

# --- Edge Case: Invalid facility_id (DQ referential integrity check) ---
iot_records.append(generate_iot_record(
    "UNKNOWN-DEV-001", "FAC-INVALID-999", "UNKNOWN_TYPE",
    "2026-02-15T11:15:00.000Z",
    temp=50.0, vib_x=1.0, vib_y=1.0, pressure=100.0, power=10.0, rpm=2000
))

# --- Edge Case: Malformed / schema evolution — extra field (will be in JSON) ---
new_field_record = generate_iot_record(
    "PUMP-FAC02-0078", "FAC-APAC-002", "CENTRIFUGAL_PUMP",
    "2026-02-15T11:20:00.000Z"
)
new_field_record["telemetry"]["humidity"] = 45.2  # new field not in original schema
new_field_record["alert_level"] = "WARNING"       # new top-level field
iot_records.append(new_field_record)

# Convert to JSON strings and write to Volume using Python file I/O
# (sparkContext is not available on serverless compute)
iot_json_strings = [json.dumps(r) for r in iot_records]

# Write as newline-delimited JSON (NDJSON) — Volumes support direct file access
import os
iot_path = f"{BASE_PATH}/iot_telemetry"
os.makedirs(iot_path, exist_ok=True)

# Write JSON files
with open(f"{iot_path}/iot_data_001.json", "w") as f:
    for js in iot_json_strings:
        f.write(js + "\n")

# Read back as Spark DataFrame
df_iot_raw = spark.read.json(iot_path)

print(f"✅ iot_telemetry: {len(iot_records)} records written as JSON")
print(f"   Including edge cases: 2 duplicates, 1 late-arriving, 2 out-of-range, 2 nulls, 1 invalid facility, 1 schema evolution")
df_iot_raw.printSchema()
df_iot_raw.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Production Orders (Parquet)
# MAGIC Simulates ADLS Gen2 landing zone files.

# COMMAND ----------

production_orders_data = [
    # --- Reference rows from dataset_reference ---
    ("ORD-2026-00123", "FAC-NA-001", "SKU-XYZ-500", 1000, "2026-02-15 08:00:00", "EQ-CNC-0012", "HIGH"),
    ("ORD-2026-00124", "FAC-EU-003", "SKU-ABC-250", 500,  "2026-02-15 09:30:00", "EQ-PRESS-0045", "MEDIUM"),
    ("ORD-2026-00125", "FAC-APAC-002","SKU-DEF-100", 200,  "2026-02-15 10:00:00", "EQ-PUMP-0078", "LOW"),
    ("ORD-2026-00126", "FAC-NA-001", "SKU-XYZ-500", 750,  "2026-02-15 11:00:00", "EQ-CNC-0031", "HIGH"),
    # --- Additional generated rows ---
    ("ORD-2026-00127", "FAC-NA-002", "SKU-GHI-300", 300,  "2026-02-15 07:00:00", "EQ-LATHE-0019", "MEDIUM"),
    ("ORD-2026-00128", "FAC-NA-001", "SKU-JKL-150", 150,  "2026-02-15 12:00:00", "EQ-WELD-0088", "HIGH"),
    ("ORD-2026-00129", "FAC-EU-003", "SKU-MNO-800", 800,  "2026-02-15 06:00:00", "EQ-CONV-0056", "LOW"),
    ("ORD-2026-00130", "FAC-APAC-002","SKU-PQR-400", 400,  "2026-02-15 13:00:00", "EQ-PRESS-0099", "MEDIUM"),
    ("ORD-2026-00131", "FAC-NA-002", "SKU-STU-600", 600,  "2026-02-15 14:00:00", "EQ-CNC-0042", "HIGH"),
    ("ORD-2026-00132", "FAC-APAC-002","SKU-VWX-250", 250,  "2026-02-15 15:00:00", "EQ-DRILL-0067", "LOW"),
    ("ORD-2026-00133", "FAC-NA-001", "SKU-XYZ-500", 500,  "2026-02-16 08:00:00", "EQ-CNC-0012", "MEDIUM"),
    ("ORD-2026-00134", "FAC-EU-003", "SKU-ABC-250", 350,  "2026-02-16 09:00:00", "EQ-PRESS-0045", "HIGH"),
    ("ORD-2026-00135", "FAC-NA-001", "SKU-JKL-150", 200,  "2026-02-16 10:00:00", "EQ-WELD-0088", "LOW"),
    ("ORD-2026-00136", "FAC-APAC-002","SKU-DEF-100", 100,  "2026-02-16 11:00:00", "EQ-PUMP-0078", "MEDIUM"),
    ("ORD-2026-00137", "FAC-NA-002", "SKU-GHI-300", 450,  "2026-02-16 12:00:00", "EQ-LATHE-0019", "HIGH"),
    # --- Edge Cases ---
    ("ORD-2026-00138", "FAC-NA-001", "SKU-XYZ-500", None,  "2026-02-16 13:00:00", "EQ-CNC-0012", "HIGH"),  # NULL quantity
    ("ORD-2026-00139", "FAC-INVALID-999", "SKU-ZZZ-999", 100, "2026-02-16 14:00:00", "EQ-UNKNOWN-999", "LOW"),  # Invalid facility
]

production_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("facility_id", StringType(), False),
    StructField("product_sku", StringType(), True),
    StructField("quantity_ordered", IntegerType(), True),
    StructField("start_time", StringType(), True),
    StructField("equipment_id", StringType(), True),
    StructField("priority", StringType(), True),
])

df_orders = spark.createDataFrame(production_orders_data, schema=production_schema) \
    .withColumn("start_time", to_timestamp(col("start_time")))

df_orders.write.format("parquet").mode("overwrite").save(f"{BASE_PATH}/production_orders")
print(f"✅ production_orders: {df_orders.count()} rows written as Parquet")
df_orders.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Maintenance Records (Delta with CDC metadata)
# MAGIC Simulates CDC events from Azure SQL Database. PII column: `maintenance_technician_id`.

# COMMAND ----------

maintenance_data = [
    # --- Reference rows from dataset_reference ---
    ("MNT-2026-00451","EQ-CNC-0012","FAC-NA-001","CORRECTIVE","TECH-00089",
     "2026-02-15 06:00:00","2026-02-15 08:30:00",150,"FC-BEARING-002","BEARING-6205,SEAL-TYPE4",
     1250.75,"WO-2026-0891","INSERT","2026-02-15T08:31:00Z","0x00000001"),
    ("MNT-2026-00452","EQ-PRESS-0045","FAC-EU-003","PREVENTIVE","TECH-00034",
     "2026-02-15 07:00:00","2026-02-15 07:45:00",45,None,"LUBRICANT-ISO46",
     320.00,"WO-2026-0892","INSERT","2026-02-15T07:46:00Z","0x00000002"),
    ("MNT-2026-00453","EQ-PUMP-0078","FAC-APAC-002","PREDICTIVE","TECH-00112",
     "2026-02-15 09:00:00",None,None,"FC-VIBRATION-007",None,
     None,"WO-2026-0893","INSERT","2026-02-15T09:01:00Z","0x00000003"),
    ("MNT-2026-00454","EQ-CNC-0031","FAC-NA-001","CORRECTIVE","TECH-00089",
     "2026-02-14 22:00:00","2026-02-15 02:00:00",240,"FC-MOTOR-011","MOTOR-3PH-5HP,COUPLER-A3",
     4800.00,"WO-2026-0880","UPDATE","2026-02-15T02:01:00Z","0x00000004"),
    # --- Additional rows ---
    ("MNT-2026-00455","EQ-LATHE-0019","FAC-NA-002","PREVENTIVE","TECH-00056",
     "2026-02-15 10:00:00","2026-02-15 11:30:00",90,None,"COOLANT-TYPE2,FILTER-HE10",
     450.00,"WO-2026-0894","INSERT","2026-02-15T11:31:00Z","0x00000005"),
    ("MNT-2026-00456","EQ-WELD-0088","FAC-NA-001","CORRECTIVE","TECH-00089",
     "2026-02-15 14:00:00","2026-02-15 16:00:00",120,"FC-TORCH-003","TORCH-TIP-MIG,NOZZLE-CU",
     780.00,"WO-2026-0895","INSERT","2026-02-15T16:01:00Z","0x00000006"),
    ("MNT-2026-00457","EQ-CONV-0056","FAC-EU-003","PREDICTIVE","TECH-00034",
     "2026-02-15 12:00:00","2026-02-15 12:30:00",30,"FC-BELT-001","BELT-CONV-200MM",
     220.00,"WO-2026-0896","INSERT","2026-02-15T12:31:00Z","0x00000007"),
    ("MNT-2026-00458","EQ-PRESS-0099","FAC-APAC-002","CORRECTIVE","TECH-00112",
     "2026-02-15 15:00:00","2026-02-15 18:00:00",180,"FC-HYDRAULIC-005","HYDRAULIC-SEAL-KIT,PUMP-GEAR",
     3200.00,"WO-2026-0897","INSERT","2026-02-15T18:01:00Z","0x00000008"),
    # --- CDC UPDATE: Maintenance 00453 is now completed ---
    ("MNT-2026-00453","EQ-PUMP-0078","FAC-APAC-002","PREDICTIVE","TECH-00112",
     "2026-02-15 09:00:00","2026-02-15 11:00:00",120,"FC-VIBRATION-007","VIBRATION-DAMPER-V2",
     1850.00,"WO-2026-0893","UPDATE","2026-02-15T11:01:00Z","0x00000009"),
    # --- CDC DELETE: Cancelled maintenance ---
    ("MNT-2026-00459","EQ-CNC-0042","FAC-NA-002","PREVENTIVE","TECH-00056",
     "2026-02-16 08:00:00",None,None,None,None,
     None,"WO-2026-0898","DELETE","2026-02-16T07:30:00Z","0x0000000A"),
    # --- Edge case: Multiple changes to same equipment in single batch ---
    ("MNT-2026-00460","EQ-CNC-0012","FAC-NA-001","PREVENTIVE","TECH-00089",
     "2026-02-16 06:00:00","2026-02-16 07:00:00",60,None,"LUBRICANT-ISO46",
     280.00,"WO-2026-0899","INSERT","2026-02-16T07:01:00Z","0x0000000B"),
    ("MNT-2026-00461","EQ-CNC-0012","FAC-NA-001","CORRECTIVE","TECH-00089",
     "2026-02-16 09:00:00","2026-02-16 12:00:00",180,"FC-SPINDLE-001","SPINDLE-MOTOR,ENCODER-ABS",
     5500.00,"WO-2026-0900","INSERT","2026-02-16T12:01:00Z","0x0000000C"),
]

maintenance_schema = StructType([
    StructField("maintenance_id", StringType(), False),
    StructField("equipment_id", StringType(), False),
    StructField("facility_id", StringType(), False),
    StructField("maintenance_type", StringType(), True),
    StructField("maintenance_technician_id", StringType(), True),  # PII
    StructField("start_datetime", StringType(), True),
    StructField("end_datetime", StringType(), True),
    StructField("downtime_minutes", IntegerType(), True),
    StructField("failure_code", StringType(), True),
    StructField("parts_replaced", StringType(), True),
    StructField("maintenance_cost_usd", DoubleType(), True),
    StructField("work_order_id", StringType(), True),
    StructField("_cdc_operation", StringType(), True),
    StructField("_cdc_timestamp", StringType(), True),
    StructField("_lsn", StringType(), True),
])

df_maintenance = spark.createDataFrame(maintenance_data, schema=maintenance_schema) \
    .withColumn("start_datetime", to_timestamp(col("start_datetime"))) \
    .withColumn("end_datetime", to_timestamp(col("end_datetime"))) \
    .withColumn("_cdc_timestamp", to_timestamp(col("_cdc_timestamp")))

df_maintenance.write.format("delta").mode("overwrite").save(f"{BASE_PATH}/maintenance_records")
print(f"✅ maintenance_records: {df_maintenance.count()} rows written as Delta (with CDC metadata)")
df_maintenance.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Quality Inspection (CSV)
# MAGIC Simulates landing zone CSV files. PII column: `inspector_employee_id`.

# COMMAND ----------

quality_data = [
    # --- Reference rows from dataset_reference ---
    ("INSP-2026-10231","ORD-2026-00123","EQ-CNC-0012","FAC-NA-001","EMP-00567",
     "2026-02-15 11:00:00","SKU-XYZ-500","BAT-2026-0441",100,97,3,"DIMENSIONAL_ERROR","MINOR","VISUAL+CMM","PASS"),
    ("INSP-2026-10232","ORD-2026-00124","EQ-PRESS-0045","FAC-EU-003","EMP-00234",
     "2026-02-15 12:30:00","SKU-ABC-250","BAT-2026-0442",50,41,9,"SURFACE_DEFECT","MAJOR","VISUAL","FAIL"),
    ("INSP-2026-10233","ORD-2026-00125","EQ-PUMP-0078","FAC-APAC-002","EMP-00891",
     "2026-02-15 13:15:00","SKU-DEF-100","BAT-2026-0443",200,200,0,None,None,"CMM","PASS"),
    ("INSP-2026-10234","ORD-2026-00126","EQ-CNC-0031","FAC-NA-001","EMP-00567",
     "2026-02-15 14:00:00","SKU-XYZ-500","BAT-2026-0444",75,60,15,"POROSITY","CRITICAL","X-RAY+VISUAL","FAIL"),
    # --- Additional rows ---
    ("INSP-2026-10235","ORD-2026-00127","EQ-LATHE-0019","FAC-NA-002","EMP-00345",
     "2026-02-15 15:00:00","SKU-GHI-300","BAT-2026-0445",120,115,5,"SURFACE_DEFECT","MINOR","VISUAL","PASS"),
    ("INSP-2026-10236","ORD-2026-00128","EQ-WELD-0088","FAC-NA-001","EMP-00567",
     "2026-02-15 16:30:00","SKU-JKL-150","BAT-2026-0446",80,72,8,"WELD_DEFECT","MAJOR","X-RAY","FAIL"),
    ("INSP-2026-10237","ORD-2026-00129","EQ-CONV-0056","FAC-EU-003","EMP-00234",
     "2026-02-15 17:00:00","SKU-MNO-800","BAT-2026-0447",300,298,2,"DIMENSIONAL_ERROR","MINOR","CMM","PASS"),
    ("INSP-2026-10238","ORD-2026-00130","EQ-PRESS-0099","FAC-APAC-002","EMP-00891",
     "2026-02-15 18:00:00","SKU-PQR-400","BAT-2026-0448",150,130,20,"POROSITY","CRITICAL","X-RAY+VISUAL","FAIL"),
    ("INSP-2026-10239","ORD-2026-00131","EQ-CNC-0042","FAC-NA-002","EMP-00345",
     "2026-02-16 09:00:00","SKU-STU-600","BAT-2026-0449",250,248,2,"SURFACE_DEFECT","MINOR","VISUAL","PASS"),
    ("INSP-2026-10240","ORD-2026-00132","EQ-DRILL-0067","FAC-APAC-002","EMP-00891",
     "2026-02-16 10:00:00","SKU-VWX-250","BAT-2026-0450",100,95,5,"DIMENSIONAL_ERROR","MINOR","CMM","PASS"),
    # --- Edge cases ---
    ("INSP-2026-10241","ORD-2026-00133","EQ-CNC-0012","FAC-NA-001","EMP-00567",
     "2026-02-16 11:00:00","SKU-XYZ-500","BAT-2026-0451",50,50,0,None,None,"VISUAL+CMM","PASS"),
    (None,"ORD-2026-00134","EQ-PRESS-0045","FAC-EU-003","EMP-00234",                                       # NULL inspection_id
     "2026-02-16 12:00:00","SKU-ABC-250","BAT-2026-0452",100,88,12,"SURFACE_DEFECT","MAJOR","VISUAL","FAIL"),
    ("INSP-2026-10243","ORD-2026-00135","EQ-WELD-0088","FAC-INVALID-999","EMP-00567",                      # Invalid facility
     "2026-02-16 13:00:00","SKU-JKL-150","BAT-2026-0453",60,55,5,"WELD_DEFECT","MINOR","VISUAL","PASS"),
]

quality_schema = StructType([
    StructField("inspection_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("equipment_id", StringType(), True),
    StructField("facility_id", StringType(), True),
    StructField("inspector_employee_id", StringType(), True),  # PII
    StructField("inspection_timestamp", StringType(), True),
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

df_quality = spark.createDataFrame(quality_data, schema=quality_schema) \
    .withColumn("inspection_timestamp", to_timestamp(col("inspection_timestamp")))

# Write as CSV (simulating ADLS Gen2 Volumes landing)
df_quality.write.format("csv").mode("overwrite").option("header", "true").save(f"{BASE_PATH}/quality_inspection")
print(f"✅ quality_inspection: {df_quality.count()} rows written as CSV")
df_quality.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Summary & Verification

# COMMAND ----------

print("=" * 80)
print("SYNTHETIC DATA GENERATION COMPLETE")
print("=" * 80)

datasets = [
    ("dim_facility",          "delta",   f"{BASE_PATH}/dim_facility"),
    ("equipment_master",      "delta",   f"{BASE_PATH}/equipment_master"),
    ("iot_telemetry",         "json",    f"{BASE_PATH}/iot_telemetry"),
    ("production_orders",     "parquet", f"{BASE_PATH}/production_orders"),
    ("maintenance_records",   "delta",   f"{BASE_PATH}/maintenance_records"),
    ("quality_inspection",    "csv",     f"{BASE_PATH}/quality_inspection"),
]

for name, fmt, path in datasets:
    try:
        files = os.listdir(path)
        data_files = [f for f in files if not f.startswith("_")]
        print(f"  ✅ {name:25s} | {fmt:8s} | {len(data_files):3d} files | {path}")
    except Exception as e:
        print(f"  ❌ {name:25s} | ERROR: {str(e)}")

print("\n" + "=" * 80)
print("EDGE CASES INCLUDED:")
print("=" * 80)
print("  IoT Telemetry:       2 duplicates, 1 late-arriving, 2 out-of-range, 2 nulls, 1 invalid facility, 1 schema evolution")
print("  Production Orders:   1 null quantity, 1 invalid facility")
print("  Maintenance Records: 1 UPDATE (completed), 1 DELETE (cancelled), 2 same-equipment batch")
print("  Quality Inspection:  1 null inspection_id, 1 invalid facility")
print("=" * 80)
print("\n🚀 NEXT: Run Notebook 02 (Catalog & Governance Setup)")
