# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 08 — Gold: Dimension Tables
# MAGIC **Smart Manufacturing IoT Lakehouse Platform**
# MAGIC
# MAGIC Creates the star schema dimension tables:
# MAGIC - `dim_equipment` (SCD Type 2 — from Silver)
# MAGIC - `dim_facility` (reference data)
# MAGIC - `dim_production_order`
# MAGIC - `dim_technician`
# MAGIC - `dim_timestamp` (pre-populated 2025–2027)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import date, timedelta

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. dim_equipment (SCD Type 2)
# MAGIC Sourced directly from Silver SCD2 table with surrogate keys.

# COMMAND ----------

GOLD_DIM_EQUIPMENT = "manufacturing_serving.dim_equipment"

df_equipment = (
    spark.table("manufacturing_refined.silver_equipment_scd2")
    .select(
        col("equipment_sk").alias("equipment_sk"),
        col("equipment_id"),
        col("equipment_serial_number"),  # PII — masked via governance views
        col("facility_id"),
        col("equipment_type"),
        col("manufacturer"),
        col("model_number"),
        col("manufacture_date"),
        col("installation_date"),
        col("rated_capacity"),
        col("operating_voltage"),
        col("max_temperature_c"),
        col("max_vibration_hz"),
        col("maintenance_interval_days"),
        col("equipment_status"),
        col("asset_value_usd"),
        col("criticality_level"),
        col("region"),
        col("effective_start_date"),
        col("effective_end_date"),
        col("is_current"),
        col("version_number"),
    )
)

(
    df_equipment.write
    .format("delta")
    .mode("overwrite")
    .option("delta.enableChangeDataFeed", "true")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_DIM_EQUIPMENT)
)

print(f"✅ {GOLD_DIM_EQUIPMENT}: {df_equipment.count()} records (including SCD2 history)")
print(f"   Current records: {df_equipment.filter(col('is_current')).count()}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT equipment_id, equipment_type, facility_id, equipment_status,
# MAGIC        is_current, version_number
# MAGIC FROM manufacturing_serving.dim_equipment
# MAGIC ORDER BY equipment_id, version_number;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. dim_facility

# COMMAND ----------

GOLD_DIM_FACILITY = "manufacturing_serving.dim_facility"

df_facility = (
    spark.table("manufacturing_refined.silver_dim_facility")
    .withColumn("facility_sk", monotonically_increasing_id() + 1)
    .select(
        "facility_sk",
        "facility_id",
        "facility_name",
        "region",
        "country",
        "city",
        "timezone",
        "facility_status",
    )
)

(
    df_facility.write
    .format("delta")
    .mode("overwrite")
    .option("delta.enableChangeDataFeed", "true")
    .saveAsTable(GOLD_DIM_FACILITY)
)

print(f"✅ {GOLD_DIM_FACILITY}: {df_facility.count()} records")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM manufacturing_serving.dim_facility;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. dim_production_order

# COMMAND ----------

GOLD_DIM_ORDER = "manufacturing_serving.dim_production_order"

df_dim_order = (
    spark.table("manufacturing_refined.silver_production_orders")
    .withColumn("order_sk", monotonically_increasing_id() + 1)
    .select(
        "order_sk",
        "order_id",
        "facility_id",
        "product_sku",
        "quantity_ordered",
        "start_time",
        "start_date",
        "equipment_id",
        "priority",
        "priority_rank",
    )
)

(
    df_dim_order.write
    .format("delta")
    .mode("overwrite")
    .option("delta.enableChangeDataFeed", "true")
    .saveAsTable(GOLD_DIM_ORDER)
)

print(f"✅ {GOLD_DIM_ORDER}: {df_dim_order.count()} records")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM manufacturing_serving.dim_production_order LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. dim_technician
# MAGIC Extract unique technicians from maintenance records.

# COMMAND ----------

GOLD_DIM_TECHNICIAN = "manufacturing_serving.dim_technician"

df_dim_tech = (
    spark.table("manufacturing_refined.silver_maintenance")
    .filter(col("maintenance_technician_id").isNotNull())
    .select("maintenance_technician_id", "facility_id")
    .distinct()
    .withColumn("technician_sk", monotonically_increasing_id() + 1)
    .withColumn("technician_name", concat(lit("Technician "), 
                regexp_extract(col("maintenance_technician_id"), r"(\d+)$", 1)))
    .withColumn("specialization", 
        when(col("maintenance_technician_id").isin("TECH-00089"), "CNC & Welding")
        .when(col("maintenance_technician_id").isin("TECH-00034"), "Hydraulic Press & Conveyor")
        .when(col("maintenance_technician_id").isin("TECH-00112"), "Pumps & Press")
        .when(col("maintenance_technician_id").isin("TECH-00056"), "Lathe & CNC")
        .otherwise("General")
    )
    .select(
        "technician_sk",
        col("maintenance_technician_id").alias("technician_id"),
        "technician_name",
        "facility_id",
        "specialization",
    )
)

(
    df_dim_tech.write
    .format("delta")
    .mode("overwrite")
    .option("delta.enableChangeDataFeed", "true")
    .saveAsTable(GOLD_DIM_TECHNICIAN)
)

print(f"✅ {GOLD_DIM_TECHNICIAN}: {df_dim_tech.count()} records")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM manufacturing_serving.dim_technician;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. dim_timestamp (Pre-populated 2025–2027)
# MAGIC Full date dimension table for 3 years.

# COMMAND ----------

GOLD_DIM_TIMESTAMP = "manufacturing_serving.dim_timestamp"

# Generate all dates from 2025-01-01 to 2027-12-31
start_date = date(2025, 1, 1)
end_date = date(2027, 12, 31)
num_days = (end_date - start_date).days + 1

date_list = [(start_date + timedelta(days=i),) for i in range(num_days)]

df_dates = spark.createDataFrame(date_list, ["full_date"])

df_dim_timestamp = (
    df_dates
    .withColumn("date_key", date_format(col("full_date"), "yyyyMMdd").cast(IntegerType()))
    .withColumn("year", year(col("full_date")))
    .withColumn("quarter", quarter(col("full_date")))
    .withColumn("month", month(col("full_date")))
    .withColumn("month_name", date_format(col("full_date"), "MMMM"))
    .withColumn("month_abbr", date_format(col("full_date"), "MMM"))
    .withColumn("week_of_year", weekofyear(col("full_date")))
    .withColumn("day_of_month", dayofmonth(col("full_date")))
    .withColumn("day_of_week", dayofweek(col("full_date")))
    .withColumn("day_name", date_format(col("full_date"), "EEEE"))
    .withColumn("day_abbr", date_format(col("full_date"), "EEE"))
    .withColumn("is_weekend", when(dayofweek(col("full_date")).isin(1, 7), True).otherwise(False))
    .withColumn("is_holiday", lit(False))  # Can be enriched with holiday calendar
    # Fiscal year (assuming fiscal year starts in April)
    .withColumn("fiscal_year", 
        when(month(col("full_date")) >= 4, year(col("full_date")))
        .otherwise(year(col("full_date")) - 1)
    )
    .withColumn("fiscal_quarter",
        when(month(col("full_date")).between(4, 6), 1)
        .when(month(col("full_date")).between(7, 9), 2)
        .when(month(col("full_date")).between(10, 12), 3)
        .otherwise(4)
    )
    .withColumn("year_month", date_format(col("full_date"), "yyyy-MM"))
    .withColumn("year_quarter", concat(col("year"), lit("-Q"), col("quarter")))
    .select(
        "date_key", "full_date", "year", "quarter", "month", "month_name", 
        "month_abbr", "week_of_year", "day_of_month", "day_of_week", 
        "day_name", "day_abbr", "is_weekend", "is_holiday",
        "fiscal_year", "fiscal_quarter", "year_month", "year_quarter"
    )
)

(
    df_dim_timestamp.write
    .format("delta")
    .mode("overwrite")
    .option("delta.enableChangeDataFeed", "true")
    .saveAsTable(GOLD_DIM_TIMESTAMP)
)

print(f"✅ {GOLD_DIM_TIMESTAMP}: {df_dim_timestamp.count()} records (2025-01-01 to 2027-12-31)")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify dim_timestamp
# MAGIC SELECT * FROM manufacturing_serving.dim_timestamp 
# MAGIC WHERE full_date = '2026-02-15';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Summary stats
# MAGIC SELECT MIN(full_date) AS start_date, MAX(full_date) AS end_date,
# MAGIC        COUNT(*) AS total_days,
# MAGIC        COUNT(DISTINCT year) AS years,
# MAGIC        SUM(CASE WHEN is_weekend THEN 1 ELSE 0 END) AS weekend_days,
# MAGIC        SUM(CASE WHEN NOT is_weekend THEN 1 ELSE 0 END) AS weekdays
# MAGIC FROM manufacturing_serving.dim_timestamp;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Summary

# COMMAND ----------

dim_tables = [
    "manufacturing_serving.dim_equipment",
    "manufacturing_serving.dim_facility",
    "manufacturing_serving.dim_production_order",
    "manufacturing_serving.dim_technician",
    "manufacturing_serving.dim_timestamp",
]

print("=" * 80)
print("GOLD DIMENSION TABLES COMPLETE")
print("=" * 80)
for table in dim_tables:
    count = spark.table(table).count()
    print(f"  ✅ {table:50s} | {count:6d} records")
print("=" * 80)
print("🚀 NEXT: Run Notebook 09 (Gold Fact Tables)")
