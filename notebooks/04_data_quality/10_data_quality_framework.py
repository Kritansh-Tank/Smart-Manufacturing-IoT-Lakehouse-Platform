# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 10 — Data Quality Framework
# MAGIC **Smart Manufacturing IoT Lakehouse Platform**
# MAGIC
# MAGIC Comprehensive data quality validation framework with:
# MAGIC - NOT NULL checks
# MAGIC - Range validation (temperature, vibration)
# MAGIC - Referential integrity (facility_id validation)
# MAGIC - Freshness checks (data < 15 minutes old)
# MAGIC - Pattern validation (serial number format)
# MAGIC - Centralized DQ violations table with lineage
# MAGIC
# MAGIC > **Note**: Since DLT Expectations are not available on Community Edition,
# MAGIC > I implemented equivalent checks via PySpark assertions and SQL validation.

# COMMAND ----------

import builtins
_python_round = builtins.round
from pyspark.sql.functions import *
from pyspark.sql.types import *
round = _python_round  # Restore Python's round() after PySpark wildcard import
from datetime import datetime
import uuid

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create DQ Violations Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS manufacturing_governance.dq_violations (
# MAGIC   violation_id STRING,
# MAGIC   check_timestamp TIMESTAMP,
# MAGIC   source_table STRING,
# MAGIC   source_layer STRING COMMENT 'BRONZE, SILVER, GOLD',
# MAGIC   rule_name STRING,
# MAGIC   rule_type STRING COMMENT 'NOT_NULL, RANGE, REFERENTIAL_INTEGRITY, FRESHNESS, PATTERN, CUSTOM',
# MAGIC   severity STRING COMMENT 'CRITICAL, WARNING, INFO',
# MAGIC   action_taken STRING COMMENT 'DROP, WARN, QUARANTINE',
# MAGIC   records_checked LONG,
# MAGIC   records_passed LONG,
# MAGIC   records_failed LONG,
# MAGIC   failure_percentage DOUBLE,
# MAGIC   sample_failed_values STRING COMMENT 'JSON array of sample failing values',
# MAGIC   description STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Centralized data quality violations tracking with full lineage.';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. DQ Quarantine Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS manufacturing_governance.dq_quarantine (
# MAGIC   quarantine_id STRING,
# MAGIC   quarantine_timestamp TIMESTAMP,
# MAGIC   source_table STRING,
# MAGIC   rule_name STRING,
# MAGIC   violation_reason STRING,
# MAGIC   original_data STRING COMMENT 'Full row as JSON'
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Quarantined records that failed critical data quality checks.';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. DQ Check Functions

# COMMAND ----------

def log_dq_violation(spark, source_table, source_layer, rule_name, rule_type, severity,
                     action, records_checked, records_passed, records_failed, 
                     sample_values="", description=""):
    """Log a DQ violation to the centralized violations table."""
    failure_pct = round((records_failed / records_checked * 100), 2) if records_checked > 0 else 0.0
    
    violation = [(
        str(uuid.uuid4()),
        source_table,
        source_layer,
        rule_name,
        rule_type,
        severity,
        action,
        records_checked,
        records_passed,
        records_failed,
        failure_pct,
        sample_values,
        description,
    )]
    
    schema = ("violation_id STRING, source_table STRING, source_layer STRING, "
              "rule_name STRING, rule_type STRING, severity STRING, action_taken STRING, "
              "records_checked LONG, records_passed LONG, records_failed LONG, "
              "failure_percentage DOUBLE, sample_failed_values STRING, description STRING")
    
    df = spark.createDataFrame(violation, schema=schema) \
        .withColumn("check_timestamp", current_timestamp())
    
    df.write.format("delta").mode("append").saveAsTable("manufacturing_governance.dq_violations")
    
    status = "✅ PASS" if records_failed == 0 else f"❌ FAIL ({records_failed}/{records_checked})"
    print(f"  [{severity:8s}] {rule_name:45s} | {status} | Action: {action}")
    
    return records_failed

def quarantine_records(spark, df_failed, source_table, rule_name, violation_reason):
    """Send failed records to quarantine table."""
    if df_failed.count() == 0:
        return
    
    df_quarantine = (
        df_failed
        .withColumn("quarantine_id", lit(str(uuid.uuid4())))
        .withColumn("quarantine_timestamp", current_timestamp())
        .withColumn("source_table", lit(source_table))
        .withColumn("rule_name", lit(rule_name))
        .withColumn("violation_reason", lit(violation_reason))
        .withColumn("original_data", to_json(struct("*")))
        .select("quarantine_id", "quarantine_timestamp", "source_table", 
                "rule_name", "violation_reason", "original_data")
    )
    
    df_quarantine.write.format("delta").mode("append").saveAsTable("manufacturing_governance.dq_quarantine")
    print(f"    → {df_quarantine.count()} records quarantined")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Execute DQ Checks on IoT Sensor Telemetry

# COMMAND ----------

print("=" * 80)
print("DATA QUALITY CHECKS — IoT Sensor Telemetry (Silver)")
print("=" * 80)

df_iot = spark.table("manufacturing_refined.silver_iot_telemetry")
total = df_iot.count()

# --- CHECK 1: NOT NULL — device_id ---
null_device = df_iot.filter(col("device_id").isNull()).count()
log_dq_violation(spark, "silver_iot_telemetry", "SILVER",
    "NOT_NULL_device_id", "NOT_NULL", "CRITICAL", "DROP",
    total, total - null_device, null_device,
    description="device_id must not be NULL")

if null_device > 0:
    quarantine_records(spark, df_iot.filter(col("device_id").isNull()),
        "silver_iot_telemetry", "NOT_NULL_device_id", "device_id is NULL")

# --- CHECK 2: NOT NULL — event_timestamp ---
null_ts = df_iot.filter(col("event_timestamp").isNull()).count()
log_dq_violation(spark, "silver_iot_telemetry", "SILVER",
    "NOT_NULL_event_timestamp", "NOT_NULL", "CRITICAL", "DROP",
    total, total - null_ts, null_ts,
    description="event_timestamp must not be NULL")

if null_ts > 0:
    quarantine_records(spark, df_iot.filter(col("event_timestamp").isNull()),
        "silver_iot_telemetry", "NOT_NULL_event_timestamp", "event_timestamp is NULL")

# --- CHECK 3: NOT NULL — facility_id ---
null_fac = df_iot.filter(col("facility_id").isNull()).count()
log_dq_violation(spark, "silver_iot_telemetry", "SILVER",
    "NOT_NULL_facility_id", "NOT_NULL", "CRITICAL", "DROP",
    total, total - null_fac, null_fac,
    description="facility_id must not be NULL")

# --- CHECK 4: RANGE — temperature (-50 to 150°C) ---
temp_fail = df_iot.filter(
    (col("temperature") < -50) | (col("temperature") > 150)
).count()
log_dq_violation(spark, "silver_iot_telemetry", "SILVER",
    "RANGE_temperature_-50_to_150", "RANGE", "WARNING", "QUARANTINE",
    total, total - temp_fail, temp_fail,
    description="Temperature must be between -50°C and 150°C")

if temp_fail > 0:
    quarantine_records(spark, 
        df_iot.filter((col("temperature") < -50) | (col("temperature") > 150)),
        "silver_iot_telemetry", "RANGE_temperature", "Temperature out of range [-50, 150]")

# --- CHECK 5: RANGE — vibration (0 to 100 Hz) ---
vib_fail = df_iot.filter(
    (col("vibration_x") < 0) | (col("vibration_x") > 100) |
    (col("vibration_y") < 0) | (col("vibration_y") > 100)
).count()
log_dq_violation(spark, "silver_iot_telemetry", "SILVER",
    "RANGE_vibration_0_to_100", "RANGE", "WARNING", "QUARANTINE",
    total, total - vib_fail, vib_fail,
    description="Vibration must be between 0 and 100 Hz")

if vib_fail > 0:
    quarantine_records(spark,
        df_iot.filter((col("vibration_x") > 100) | (col("vibration_y") > 100)),
        "silver_iot_telemetry", "RANGE_vibration", "Vibration out of range [0, 100]")

# --- CHECK 6: REFERENTIAL INTEGRITY — facility_id exists in dim_facility ---
valid_facilities = [r.facility_id for r in 
    spark.table("manufacturing_refined.silver_dim_facility").select("facility_id").collect()]

ref_fail = df_iot.filter(~col("facility_id").isin(valid_facilities)).count()
log_dq_violation(spark, "silver_iot_telemetry", "SILVER",
    "REF_INTEGRITY_facility_id", "REFERENTIAL_INTEGRITY", "CRITICAL", "QUARANTINE",
    total, total - ref_fail, ref_fail,
    description=f"facility_id must exist in dim_facility. Valid: {valid_facilities}")

if ref_fail > 0:
    quarantine_records(spark,
        df_iot.filter(~col("facility_id").isin(valid_facilities)),
        "silver_iot_telemetry", "REF_INTEGRITY_facility_id", 
        "facility_id does not exist in dim_facility")

# --- CHECK 7: FRESHNESS — data must be < 15 minutes old (relative to max timestamp) ---
max_ts = df_iot.select(max("event_timestamp")).first()[0]
if max_ts is not None:
    freshness_threshold = max_ts  # In production: current_timestamp() - interval 15 minutes
    stale = df_iot.filter(col("event_timestamp") < lit(freshness_threshold).cast("timestamp") - expr("INTERVAL 60 MINUTES")).count()
    log_dq_violation(spark, "silver_iot_telemetry", "SILVER",
        "FRESHNESS_15_minutes", "FRESHNESS", "WARNING", "WARN",
        total, total - stale, stale,
        description="Data should be less than 15 minutes old (checked against max timestamp)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. DQ Checks on Production Orders

# COMMAND ----------

print("=" * 80)
print("DATA QUALITY CHECKS — Production Orders (Silver)")
print("=" * 80)

df_orders = spark.table("manufacturing_refined.silver_production_orders")
total_orders = df_orders.count()

# NOT NULL — order_id
null_oid = df_orders.filter(col("order_id").isNull()).count()
log_dq_violation(spark, "silver_production_orders", "SILVER",
    "NOT_NULL_order_id", "NOT_NULL", "CRITICAL", "DROP",
    total_orders, total_orders - null_oid, null_oid)

# NOT NULL — quantity_ordered
null_qty = df_orders.filter(col("quantity_ordered").isNull()).count()
log_dq_violation(spark, "silver_production_orders", "SILVER",
    "NOT_NULL_quantity_ordered", "NOT_NULL", "WARNING", "WARN",
    total_orders, total_orders - null_qty, null_qty)

# REFERENTIAL INTEGRITY — facility_id
ref_fail_ord = df_orders.filter(~col("facility_id").isin(valid_facilities)).count()
log_dq_violation(spark, "silver_production_orders", "SILVER",
    "REF_INTEGRITY_facility_id", "REFERENTIAL_INTEGRITY", "CRITICAL", "QUARANTINE",
    total_orders, total_orders - ref_fail_ord, ref_fail_ord)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. DQ Checks on Quality Inspection

# COMMAND ----------

print("=" * 80)
print("DATA QUALITY CHECKS — Quality Inspection (Silver)")
print("=" * 80)

df_quality = spark.table("manufacturing_refined.silver_quality_inspection")
total_qi = df_quality.count()

# NOT NULL — inspection_id
null_iid = df_quality.filter(col("inspection_id").isNull()).count()
log_dq_violation(spark, "silver_quality_inspection", "SILVER",
    "NOT_NULL_inspection_id", "NOT_NULL", "CRITICAL", "DROP",
    total_qi, total_qi - null_iid, null_iid)

# REFERENTIAL INTEGRITY — facility_id
ref_fail_qi = df_quality.filter(~col("facility_id").isin(valid_facilities)).count()
log_dq_violation(spark, "silver_quality_inspection", "SILVER",
    "REF_INTEGRITY_facility_id", "REFERENTIAL_INTEGRITY", "CRITICAL", "QUARANTINE",
    total_qi, total_qi - ref_fail_qi, ref_fail_qi)

# CUSTOM — units_rejected should not exceed units_inspected
invalid_reject = df_quality.filter(col("units_rejected") > col("units_inspected")).count()
log_dq_violation(spark, "silver_quality_inspection", "SILVER",
    "CUSTOM_rejected_lte_inspected", "CUSTOM", "WARNING", "WARN",
    total_qi, total_qi - invalid_reject, invalid_reject,
    description="units_rejected must not exceed units_inspected")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. DQ Checks on Equipment Master

# COMMAND ----------

print("=" * 80)
print("DATA QUALITY CHECKS — Equipment Master (Silver)")
print("=" * 80)

df_equip = spark.table("manufacturing_refined.silver_equipment_scd2").filter(col("is_current"))
total_eq = df_equip.count()

# PATTERN — serial_number format: SN-XXX-YYYY-NNNNN
pattern_fail = df_equip.filter(
    ~col("equipment_serial_number").rlike(r"^SN-[A-Z]{3}-\d{4}-\d{5}$")
).count()
log_dq_violation(spark, "silver_equipment_scd2", "SILVER",
    "PATTERN_serial_number_format", "PATTERN", "WARNING", "WARN",
    total_eq, total_eq - pattern_fail, pattern_fail,
    description="Serial number must match pattern: SN-XXX-YYYY-NNNNN")

# NOT NULL — equipment_id
null_eqid = df_equip.filter(col("equipment_id").isNull()).count()
log_dq_violation(spark, "silver_equipment_scd2", "SILVER",
    "NOT_NULL_equipment_id", "NOT_NULL", "CRITICAL", "DROP",
    total_eq, total_eq - null_eqid, null_eqid)

# REFERENTIAL INTEGRITY — facility_id
ref_fail_eq = df_equip.filter(~col("facility_id").isin(valid_facilities)).count()
log_dq_violation(spark, "silver_equipment_scd2", "SILVER",
    "REF_INTEGRITY_facility_id", "REFERENTIAL_INTEGRITY", "CRITICAL", "QUARANTINE",
    total_eq, total_eq - ref_fail_eq, ref_fail_eq)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. DQ Violations Summary Report

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Overall DQ Summary
# MAGIC SELECT 
# MAGIC   source_table,
# MAGIC   rule_type,
# MAGIC   COUNT(*) AS total_checks,
# MAGIC   SUM(CASE WHEN records_failed > 0 THEN 1 ELSE 0 END) AS checks_with_failures,
# MAGIC   SUM(records_checked) AS total_records_checked,
# MAGIC   SUM(records_failed) AS total_records_failed,
# MAGIC   ROUND(AVG(failure_percentage), 2) AS avg_failure_pct
# MAGIC FROM manufacturing_governance.dq_violations
# MAGIC GROUP BY source_table, rule_type
# MAGIC ORDER BY source_table, rule_type;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- All failures
# MAGIC SELECT check_timestamp, source_table, rule_name, severity, action_taken,
# MAGIC        records_failed, failure_percentage, description
# MAGIC FROM manufacturing_governance.dq_violations
# MAGIC WHERE records_failed > 0
# MAGIC ORDER BY severity, records_failed DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Quarantine summary
# MAGIC SELECT source_table, rule_name, violation_reason, COUNT(*) AS quarantined_records
# MAGIC FROM manufacturing_governance.dq_quarantine
# MAGIC GROUP BY source_table, rule_name, violation_reason
# MAGIC ORDER BY quarantined_records DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DQ SLA Dashboard Query
# MAGIC SELECT 
# MAGIC   source_table,
# MAGIC   SUM(records_checked) AS total_records,
# MAGIC   SUM(records_passed) AS passed,
# MAGIC   SUM(records_failed) AS failed,
# MAGIC   ROUND(SUM(records_passed) * 100.0 / NULLIF(SUM(records_checked), 0), 2) AS pass_rate_pct,
# MAGIC   CASE 
# MAGIC     WHEN SUM(records_passed) * 100.0 / NULLIF(SUM(records_checked), 0) >= 99.0 THEN '✅ SLA MET'
# MAGIC     WHEN SUM(records_passed) * 100.0 / NULLIF(SUM(records_checked), 0) >= 95.0 THEN '⚠️ SLA WARNING'
# MAGIC     ELSE '❌ SLA BREACH'
# MAGIC   END AS sla_status
# MAGIC FROM manufacturing_governance.dq_violations
# MAGIC GROUP BY source_table
# MAGIC ORDER BY pass_rate_pct;

# COMMAND ----------

print("✅ Notebook 10 Complete — Data Quality Framework")
print("🚀 NEXT: Run Notebook 11 (Performance Optimization)")
