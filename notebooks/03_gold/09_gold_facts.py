# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 09 — Gold: Fact Tables
# MAGIC **Smart Manufacturing IoT Lakehouse Platform**
# MAGIC
# MAGIC Creates the star schema fact tables:
# MAGIC - `fact_sensor_readings` — IoT sensor data joined with dimension surrogate keys
# MAGIC - `fact_production_output` — Production output with defect tracking
# MAGIC - `fact_maintenance_events` — Maintenance events with cost and downtime

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. fact_sensor_readings
# MAGIC
# MAGIC Joins Silver IoT telemetry with dimension surrogate keys.

# COMMAND ----------

GOLD_FACT_SENSOR = "manufacturing_serving.fact_sensor_readings"

# Load dimensions for surrogate key lookup
df_dim_equip = spark.table("manufacturing_serving.dim_equipment").filter(col("is_current") == True)
df_dim_facility = spark.table("manufacturing_serving.dim_facility")
df_dim_time = spark.table("manufacturing_serving.dim_timestamp")

# Load Silver IoT telemetry
df_iot = spark.table("manufacturing_refined.silver_iot_telemetry")

# Join with dimensions to get surrogate keys
df_fact_sensor = (
    df_iot.alias("iot")
    # Join with dim_equipment (match on facility_id since device_id ≠ equipment_id)
    # I looked up for equipment by facility
    .join(
        df_dim_facility.alias("fac"),
        col("iot.facility_id") == col("fac.facility_id"),
        "left"
    )
    .join(
        df_dim_time.alias("dt"),
        to_date(col("iot.event_timestamp")) == col("dt.full_date"),
        "left"
    )
    .select(
        monotonically_increasing_id().alias("sensor_reading_sk"),
        col("fac.facility_sk"),
        col("dt.date_key").alias("timestamp_sk"),
        col("iot.device_id"),
        col("iot.facility_id"),
        col("iot.equipment_type"),
        col("iot.event_timestamp"),
        col("iot.event_date"),
        col("iot.temperature"),
        col("iot.vibration_x"),
        col("iot.vibration_y"),
        col("iot.vibration_z"),
        col("iot.pressure"),
        col("iot.power_consumption"),
        col("iot.rpm"),
        col("iot.latitude"),
        col("iot.longitude"),
        col("iot.firmware_version"),
        col("iot.last_calibration"),
        # Calculated fields
        (col("iot.vibration_x") + col("iot.vibration_y") + col("iot.vibration_z")).alias("total_vibration"),
        # Anomaly flags
        when(col("iot.temperature") < -50, True)
            .when(col("iot.temperature") > 150, True)
            .otherwise(False).alias("is_temperature_anomaly"),
        when((col("iot.vibration_x") > 100) | (col("iot.vibration_y") > 100), True)
            .otherwise(False).alias("is_vibration_anomaly"),
    )
)

(
    df_fact_sensor.write
    .format("delta")
    .mode("overwrite")
    .option("delta.enableChangeDataFeed", "true")
    .option("overwriteSchema", "true")
    .partitionBy("event_date", "facility_id")
    .saveAsTable(GOLD_FACT_SENSOR)
)

print(f"✅ {GOLD_FACT_SENSOR}: {df_fact_sensor.count()} records")
print(f"   Temperature anomalies: {df_fact_sensor.filter(col('is_temperature_anomaly')).count()}")
print(f"   Vibration anomalies:   {df_fact_sensor.filter(col('is_vibration_anomaly')).count()}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT device_id, facility_id, event_timestamp, temperature, vibration_x,
# MAGIC        total_vibration, is_temperature_anomaly, is_vibration_anomaly
# MAGIC FROM manufacturing_serving.fact_sensor_readings
# MAGIC LIMIT 15;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. fact_production_output
# MAGIC
# MAGIC Joins production orders with quality inspection data to track output and defects.

# COMMAND ----------

GOLD_FACT_PRODUCTION = "manufacturing_serving.fact_production_output"

df_orders = spark.table("manufacturing_refined.silver_production_orders")
df_quality = spark.table("manufacturing_refined.silver_quality_inspection")

# Join production orders with quality inspection results
df_fact_production = (
    df_orders.alias("ord")
    .join(
        df_quality.alias("qi"),
        (col("ord.order_id") == col("qi.order_id")) & 
        (col("ord.equipment_id") == col("qi.equipment_id")),
        "left"
    )
    .join(
        df_dim_facility.alias("fac"),
        col("ord.facility_id") == col("fac.facility_id"),
        "left"
    )
    .join(
        df_dim_time.alias("dt"),
        to_date(col("ord.start_time")) == col("dt.full_date"),
        "left"
    )
    .select(
        monotonically_increasing_id().alias("production_sk"),
        col("fac.facility_sk"),
        col("dt.date_key").alias("timestamp_sk"),
        col("ord.order_id"),
        col("ord.facility_id"),
        col("ord.product_sku"),
        col("ord.equipment_id"),
        col("ord.quantity_ordered"),
        col("ord.start_time"),
        col("ord.start_date"),
        col("ord.priority"),
        col("ord.priority_rank"),
        # Quality metrics (from inspection)
        col("qi.inspection_id"),
        col("qi.units_inspected"),
        col("qi.units_passed"),
        col("qi.units_rejected").alias("defect_count"),
        col("qi.defect_type"),
        col("qi.defect_severity"),
        col("qi.defect_rate"),
        col("qi.result").alias("inspection_result"),
        # Derived fields
        when(col("qi.result") == "FAIL", True).otherwise(False).alias("has_quality_issue"),
        when(col("qi.defect_severity") == "CRITICAL", True).otherwise(False).alias("is_critical_defect"),
    )
)

(
    df_fact_production.write
    .format("delta")
    .mode("overwrite")
    .option("delta.enableChangeDataFeed", "true")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_FACT_PRODUCTION)
)

print(f"✅ {GOLD_FACT_PRODUCTION}: {df_fact_production.count()} records")
print(f"   With quality issues: {df_fact_production.filter(col('has_quality_issue')).count()}")
print(f"   Critical defects:    {df_fact_production.filter(col('is_critical_defect')).count()}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT order_id, facility_id, product_sku, quantity_ordered, priority,
# MAGIC        units_inspected, defect_count, defect_rate, inspection_result
# MAGIC FROM manufacturing_serving.fact_production_output
# MAGIC LIMIT 15;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. fact_maintenance_events
# MAGIC
# MAGIC Maintenance events with cost, downtime, and technician information.

# COMMAND ----------

GOLD_FACT_MAINTENANCE = "manufacturing_serving.fact_maintenance_events"

df_maint = spark.table("manufacturing_refined.silver_maintenance").filter(~col("is_deleted"))
df_dim_tech = spark.table("manufacturing_serving.dim_technician")

df_fact_maint = (
    df_maint.alias("m")
    .join(
        df_dim_equip.alias("eq"),
        col("m.equipment_id") == col("eq.equipment_id"),
        "left"
    )
    .join(
        df_dim_tech.alias("tech"),
        col("m.maintenance_technician_id") == col("tech.technician_id"),
        "left"
    )
    .join(
        df_dim_facility.alias("fac"),
        col("m.facility_id") == col("fac.facility_id"),
        "left"
    )
    .join(
        df_dim_time.alias("dt"),
        to_date(col("m.start_datetime")) == col("dt.full_date"),
        "left"
    )
    .select(
        monotonically_increasing_id().alias("maintenance_event_sk"),
        col("eq.equipment_sk"),
        col("tech.technician_sk"),
        col("fac.facility_sk"),
        col("dt.date_key").alias("timestamp_sk"),
        col("m.maintenance_id"),
        col("m.equipment_id"),
        col("m.facility_id"),
        col("m.maintenance_type"),
        col("m.maintenance_technician_id"),
        col("m.start_datetime"),
        col("m.end_datetime"),
        col("m.downtime_minutes"),
        col("m.failure_code"),
        col("m.parts_replaced"),
        col("m.maintenance_cost_usd"),
        col("m.work_order_id"),
        # Derived fields
        when(col("m.end_datetime").isNotNull(),
             (unix_timestamp(col("m.end_datetime")) - unix_timestamp(col("m.start_datetime"))) / 3600
        ).alias("duration_hours"),
        when(col("m.maintenance_type") == "CORRECTIVE", True).otherwise(False).alias("is_unplanned"),
        when(col("m.maintenance_cost_usd") > 1000, "HIGH")
            .when(col("m.maintenance_cost_usd") > 500, "MEDIUM")
            .otherwise("LOW").alias("cost_category"),
    )
)

(
    df_fact_maint.write
    .format("delta")
    .mode("overwrite")
    .option("delta.enableChangeDataFeed", "true")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_FACT_MAINTENANCE)
)

print(f"✅ {GOLD_FACT_MAINTENANCE}: {df_fact_maint.count()} records")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT maintenance_id, equipment_id, facility_id, maintenance_type,
# MAGIC        downtime_minutes, maintenance_cost_usd, duration_hours, 
# MAGIC        is_unplanned, cost_category
# MAGIC FROM manufacturing_serving.fact_maintenance_events;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Gold Layer Summary & Analytics Queries

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top equipment by maintenance cost
# MAGIC SELECT e.equipment_id, e.equipment_type, e.facility_id,
# MAGIC        COUNT(m.maintenance_event_sk) AS total_events,
# MAGIC        SUM(m.downtime_minutes) AS total_downtime_min,
# MAGIC        ROUND(SUM(m.maintenance_cost_usd), 2) AS total_cost
# MAGIC FROM manufacturing_serving.fact_maintenance_events m
# MAGIC JOIN manufacturing_serving.dim_equipment e ON m.equipment_sk = e.equipment_sk
# MAGIC GROUP BY e.equipment_id, e.equipment_type, e.facility_id
# MAGIC ORDER BY total_cost DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Production quality summary by facility
# MAGIC SELECT facility_id, 
# MAGIC        COUNT(*) AS total_orders,
# MAGIC        SUM(CASE WHEN has_quality_issue THEN 1 ELSE 0 END) AS orders_with_issues,
# MAGIC        ROUND(AVG(defect_rate), 2) AS avg_defect_rate,
# MAGIC        SUM(defect_count) AS total_defects
# MAGIC FROM manufacturing_serving.fact_production_output
# MAGIC WHERE inspection_result IS NOT NULL
# MAGIC GROUP BY facility_id
# MAGIC ORDER BY avg_defect_rate DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sensor anomaly summary
# MAGIC SELECT facility_id,
# MAGIC        COUNT(*) AS total_readings,
# MAGIC        SUM(CASE WHEN is_temperature_anomaly THEN 1 ELSE 0 END) AS temp_anomalies,
# MAGIC        SUM(CASE WHEN is_vibration_anomaly THEN 1 ELSE 0 END) AS vibration_anomalies,
# MAGIC        ROUND(AVG(temperature), 2) AS avg_temperature,
# MAGIC        ROUND(AVG(total_vibration), 2) AS avg_total_vibration
# MAGIC FROM manufacturing_serving.fact_sensor_readings
# MAGIC GROUP BY facility_id
# MAGIC ORDER BY facility_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Complete Gold Layer Summary

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN manufacturing_serving;

# COMMAND ----------

gold_tables = {
    "Dimensions": [
        "manufacturing_serving.dim_equipment",
        "manufacturing_serving.dim_facility",
        "manufacturing_serving.dim_production_order",
        "manufacturing_serving.dim_technician",
        "manufacturing_serving.dim_timestamp",
    ],
    "Facts": [
        "manufacturing_serving.fact_sensor_readings",
        "manufacturing_serving.fact_production_output",
        "manufacturing_serving.fact_maintenance_events",
    ]
}

print("=" * 80)
print("GOLD LAYER STAR SCHEMA COMPLETE")
print("=" * 80)
for category, tables in gold_tables.items():
    print(f"\n  {category}:")
    for table in tables:
        count = spark.table(table).count()
        print(f"    ✅ {table:50s} | {count:6d} records")
print("\n" + "=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Create Security Views (RLS + Column Masking)
# MAGIC
# MAGIC Now that all Gold and Silver tables exist, I have created the governance views.

# COMMAND ----------

# --- Row-Level Security Views ---
ROLE = "data_engineer"
FACILITY = "FAC-NA-001"
REGION = "NORTH_AMERICA"

# RLS: fact_sensor_readings
spark.sql(f"""
    CREATE OR REPLACE VIEW manufacturing_governance.vw_rls_sensor_readings AS
    SELECT sr.*
    FROM manufacturing_serving.fact_sensor_readings sr
    WHERE
      CASE
        WHEN '{ROLE}' = 'data_analyst' THEN sr.facility_id = '{FACILITY}'
        WHEN '{ROLE}' = 'ml_engineer' THEN sr.facility_id IN (
          SELECT facility_id FROM manufacturing_serving.dim_facility WHERE region = '{REGION}'
        )
        WHEN '{ROLE}' = 'data_engineer' THEN TRUE
        ELSE FALSE
      END
""")

# RLS: fact_maintenance_events
spark.sql(f"""
    CREATE OR REPLACE VIEW manufacturing_governance.vw_rls_maintenance_events AS
    SELECT me.*
    FROM manufacturing_serving.fact_maintenance_events me
    WHERE
      CASE
        WHEN '{ROLE}' = 'data_analyst' THEN me.facility_id = '{FACILITY}'
        WHEN '{ROLE}' = 'ml_engineer' THEN me.facility_id IN (
          SELECT facility_id FROM manufacturing_serving.dim_facility WHERE region = '{REGION}'
        )
        WHEN '{ROLE}' = 'data_engineer' THEN TRUE
        ELSE FALSE
      END
""")

# RLS: fact_production_output
spark.sql(f"""
    CREATE OR REPLACE VIEW manufacturing_governance.vw_rls_production_output AS
    SELECT po.*
    FROM manufacturing_serving.fact_production_output po
    WHERE
      CASE
        WHEN '{ROLE}' = 'data_analyst' THEN po.facility_id = '{FACILITY}'
        WHEN '{ROLE}' = 'ml_engineer' THEN po.facility_id IN (
          SELECT facility_id FROM manufacturing_serving.dim_facility WHERE region = '{REGION}'
        )
        WHEN '{ROLE}' = 'data_engineer' THEN TRUE
        ELSE FALSE
      END
""")

print("✅ RLS views created: vw_rls_sensor_readings, vw_rls_maintenance_events, vw_rls_production_output")

# --- Column Masking Views ---
# Masked: dim_equipment
spark.sql(f"""
    CREATE OR REPLACE VIEW manufacturing_governance.vw_masked_dim_equipment AS
    SELECT
      equipment_id,
      CASE WHEN '{ROLE}' = 'data_engineer' THEN equipment_serial_number
           ELSE CONCAT('SN-***-', SHA2(equipment_serial_number, 256))
      END AS equipment_serial_number,
      facility_id, equipment_type, manufacturer, model_number,
      manufacture_date, installation_date, rated_capacity, operating_voltage,
      max_temperature_c, max_vibration_hz, maintenance_interval_days, equipment_status,
      CASE WHEN '{ROLE}' = 'data_engineer' THEN CAST(asset_value_usd AS STRING)
           ELSE '***RESTRICTED***'
      END AS asset_value_usd,
      criticality_level, region, effective_start_date, effective_end_date, is_current, version_number
    FROM manufacturing_serving.dim_equipment
""")

# Masked: maintenance (technician ID)
spark.sql(f"""
    CREATE OR REPLACE VIEW manufacturing_governance.vw_masked_maintenance AS
    SELECT
      maintenance_id, equipment_id, facility_id, maintenance_type,
      CASE WHEN '{ROLE}' = 'data_engineer' THEN maintenance_technician_id
           ELSE CONCAT('TECH-', SHA2(maintenance_technician_id, 256))
      END AS maintenance_technician_id,
      start_datetime, end_datetime, downtime_minutes, failure_code,
      parts_replaced, maintenance_cost_usd, work_order_id
    FROM manufacturing_refined.silver_maintenance
""")

# Masked: quality inspection (inspector ID)
spark.sql(f"""
    CREATE OR REPLACE VIEW manufacturing_governance.vw_masked_quality_inspection AS
    SELECT
      inspection_id, order_id, equipment_id, facility_id,
      CASE WHEN '{ROLE}' = 'data_engineer' THEN inspector_employee_id
           ELSE CONCAT('EMP-', SHA2(inspector_employee_id, 256))
      END AS inspector_employee_id,
      inspection_timestamp, product_sku, batch_id, units_inspected, units_passed,
      units_rejected, defect_type, defect_severity, inspection_method, result
    FROM manufacturing_refined.silver_quality_inspection
""")

print("✅ Column masking views created: vw_masked_dim_equipment, vw_masked_maintenance, vw_masked_quality_inspection")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify security views
# MAGIC SELECT 'vw_rls_sensor_readings' AS view_name, COUNT(*) AS row_count FROM manufacturing_governance.vw_rls_sensor_readings
# MAGIC UNION ALL
# MAGIC SELECT 'vw_rls_maintenance_events', COUNT(*) FROM manufacturing_governance.vw_rls_maintenance_events
# MAGIC UNION ALL
# MAGIC SELECT 'vw_rls_production_output', COUNT(*) FROM manufacturing_governance.vw_rls_production_output
# MAGIC UNION ALL
# MAGIC SELECT 'vw_masked_dim_equipment', COUNT(*) FROM manufacturing_governance.vw_masked_dim_equipment
# MAGIC UNION ALL
# MAGIC SELECT 'vw_masked_maintenance', COUNT(*) FROM manufacturing_governance.vw_masked_maintenance
# MAGIC UNION ALL
# MAGIC SELECT 'vw_masked_quality_inspection', COUNT(*) FROM manufacturing_governance.vw_masked_quality_inspection;

# COMMAND ----------

print("🚀 NEXT: Run Notebook 10 (Data Quality Framework)")

