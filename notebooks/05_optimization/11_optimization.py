# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 11 — Performance Optimization
# MAGIC **Smart Manufacturing IoT Lakehouse Platform**
# MAGIC
# MAGIC Comprehensive optimization strategy:
# MAGIC - Partitioning (already applied during writes)
# MAGIC - Z-Ordering on high-cardinality columns
# MAGIC - Auto-compaction configuration
# MAGIC - Bloom filters
# MAGIC - VACUUM retention policy
# MAGIC - Liquid clustering evaluation
# MAGIC - ANALYZE TABLE for statistics

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. OPTIMIZE + Z-ORDER
# MAGIC
# MAGIC Z-Ordering co-locates related data in the same set of files, dramatically
# MAGIC improving query performance for filtered queries.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- OPTIMIZE fact_sensor_readings with Z-ORDER
# MAGIC -- Optimizes for queries filtering on device_id or equipment_type
# MAGIC -- ============================================================
# MAGIC OPTIMIZE manufacturing_serving.fact_sensor_readings
# MAGIC ZORDER BY (device_id, equipment_type);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- OPTIMIZE fact_production_output
# MAGIC OPTIMIZE manufacturing_serving.fact_production_output
# MAGIC ZORDER BY (order_id, product_sku);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- OPTIMIZE fact_maintenance_events
# MAGIC OPTIMIZE manufacturing_serving.fact_maintenance_events
# MAGIC ZORDER BY (equipment_id, maintenance_type);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- OPTIMIZE silver tables
# MAGIC OPTIMIZE manufacturing_refined.silver_iot_telemetry
# MAGIC ZORDER BY (device_id, equipment_type);
# MAGIC 
# MAGIC OPTIMIZE manufacturing_refined.silver_production_orders
# MAGIC ZORDER BY (order_id, product_sku);
# MAGIC 
# MAGIC OPTIMIZE manufacturing_refined.silver_equipment_scd2
# MAGIC ZORDER BY (equipment_id);

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. VACUUM — Remove Old Files
# MAGIC
# MAGIC VACUUM removes files no longer referenced by the Delta table.
# MAGIC Retention period: 7 days (168 hours) for production safety.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- VACUUM all Gold tables (7 days retention — default safe retention)
# MAGIC VACUUM manufacturing_serving.fact_sensor_readings RETAIN 168 HOURS;
# MAGIC VACUUM manufacturing_serving.fact_production_output RETAIN 168 HOURS;
# MAGIC VACUUM manufacturing_serving.fact_maintenance_events RETAIN 168 HOURS;
# MAGIC VACUUM manufacturing_serving.dim_equipment RETAIN 168 HOURS;
# MAGIC VACUUM manufacturing_serving.dim_facility RETAIN 168 HOURS;
# MAGIC VACUUM manufacturing_serving.dim_production_order RETAIN 168 HOURS;
# MAGIC VACUUM manufacturing_serving.dim_technician RETAIN 168 HOURS;
# MAGIC VACUUM manufacturing_serving.dim_timestamp RETAIN 168 HOURS;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- VACUUM Silver tables
# MAGIC VACUUM manufacturing_refined.silver_iot_telemetry RETAIN 168 HOURS;
# MAGIC VACUUM manufacturing_refined.silver_production_orders RETAIN 168 HOURS;
# MAGIC VACUUM manufacturing_refined.silver_quality_inspection RETAIN 168 HOURS;
# MAGIC VACUUM manufacturing_refined.silver_maintenance RETAIN 168 HOURS;
# MAGIC VACUUM manufacturing_refined.silver_equipment_scd2 RETAIN 168 HOURS;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. ANALYZE TABLE — Collect Statistics
# MAGIC
# MAGIC Statistics help the query optimizer choose better execution plans.

# COMMAND ----------

# MAGIC %sql
# MAGIC ANALYZE TABLE manufacturing_serving.fact_sensor_readings COMPUTE STATISTICS FOR ALL COLUMNS;
# MAGIC ANALYZE TABLE manufacturing_serving.fact_production_output COMPUTE STATISTICS FOR ALL COLUMNS;
# MAGIC ANALYZE TABLE manufacturing_serving.fact_maintenance_events COMPUTE STATISTICS FOR ALL COLUMNS;
# MAGIC ANALYZE TABLE manufacturing_serving.dim_equipment COMPUTE STATISTICS FOR ALL COLUMNS;
# MAGIC ANALYZE TABLE manufacturing_serving.dim_facility COMPUTE STATISTICS FOR ALL COLUMNS;
# MAGIC ANALYZE TABLE manufacturing_serving.dim_timestamp COMPUTE STATISTICS FOR ALL COLUMNS;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Auto-Compaction & Optimized Writes Configuration
# MAGIC
# MAGIC These settings enable automatic file compaction on Delta tables.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Enable auto-compaction for key tables
# MAGIC ALTER TABLE manufacturing_serving.fact_sensor_readings
# MAGIC SET TBLPROPERTIES (
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC   'delta.autoOptimize.autoCompact' = 'true'
# MAGIC );
# MAGIC 
# MAGIC ALTER TABLE manufacturing_serving.fact_production_output
# MAGIC SET TBLPROPERTIES (
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC   'delta.autoOptimize.autoCompact' = 'true'
# MAGIC );
# MAGIC 
# MAGIC ALTER TABLE manufacturing_serving.fact_maintenance_events
# MAGIC SET TBLPROPERTIES (
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC   'delta.autoOptimize.autoCompact' = 'true'
# MAGIC );
# MAGIC 
# MAGIC ALTER TABLE manufacturing_refined.silver_iot_telemetry
# MAGIC SET TBLPROPERTIES (
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC   'delta.autoOptimize.autoCompact' = 'true'
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Bloom Filters on High-Cardinality Columns
# MAGIC
# MAGIC Bloom filters are probabilistic data structures that speed up point lookups
# MAGIC on high-cardinality columns like device_id and equipment_id.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Note: Bloom filters may not be supported on Community Edition
# MAGIC -- These commands are documented for production use
# MAGIC 
# MAGIC -- CREATE BLOOMFILTER INDEX ON TABLE manufacturing_serving.fact_sensor_readings
# MAGIC -- FOR COLUMNS (device_id OPTIONS (fpp=0.1, numItems=100000));
# MAGIC 
# MAGIC -- CREATE BLOOMFILTER INDEX ON TABLE manufacturing_serving.fact_maintenance_events
# MAGIC -- FOR COLUMNS (equipment_id OPTIONS (fpp=0.1, numItems=10000));
# MAGIC 
# MAGIC -- CREATE BLOOMFILTER INDEX ON TABLE manufacturing_serving.fact_production_output
# MAGIC -- FOR COLUMNS (order_id OPTIONS (fpp=0.1, numItems=50000));
# MAGIC 
# MAGIC SELECT 'Bloom filter commands documented - enable in production workspace' AS status;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Liquid Clustering Evaluation
# MAGIC
# MAGIC Liquid Clustering is a next-gen alternative to partitioning + Z-ordering.
# MAGIC It automatically manages data layout without manual OPTIMIZE commands.
# MAGIC
# MAGIC > **Note**: Liquid Clustering requires Databricks Runtime 13.3+ and is not
# MAGIC > available on Community Edition. Documented for production deployment.
# MAGIC
# MAGIC ```sql
# MAGIC -- Production: Convert hot tables to liquid clustering
# MAGIC -- ALTER TABLE manufacturing_serving.fact_sensor_readings
# MAGIC -- CLUSTER BY (facility_id, device_id);
# MAGIC --
# MAGIC -- ALTER TABLE manufacturing_serving.fact_maintenance_events
# MAGIC -- CLUSTER BY (facility_id, equipment_id);
# MAGIC --
# MAGIC -- Benefits over partitioning + Z-ordering:
# MAGIC -- 1. No need to specify partition columns upfront
# MAGIC -- 2. Automatic rebalancing as data changes
# MAGIC -- 3. No small-file problem from over-partitioning
# MAGIC -- 4. Can change clustering columns without rewriting data
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Performance Benchmark Queries

# COMMAND ----------

import time

def benchmark_query(spark, name, query):
    """Run a query and measure execution time."""
    start = time.time()
    result = spark.sql(query)
    count = result.count()
    elapsed = time.time() - start
    print(f"  {name:50s} | {elapsed:6.2f}s | {count:6d} rows")
    return elapsed

print("=" * 80)
print("PERFORMANCE BENCHMARK")
print("=" * 80)

# Point lookup
benchmark_query(spark, "Point lookup: device_id filter",
    "SELECT * FROM manufacturing_serving.fact_sensor_readings WHERE device_id = 'PUMP-FAC01-0012'")

# Facility filter (partition pruning)
benchmark_query(spark, "Partition pruning: facility_id",
    "SELECT * FROM manufacturing_serving.fact_sensor_readings WHERE facility_id = 'FAC-NA-001'")

# Aggregation across facilities
benchmark_query(spark, "Aggregation: AVG temp by facility",
    """SELECT facility_id, AVG(temperature) as avg_temp 
       FROM manufacturing_serving.fact_sensor_readings 
       GROUP BY facility_id""")

# Join query
benchmark_query(spark, "Join: facts with dimensions",
    """SELECT f.facility_name, COUNT(*) as readings
       FROM manufacturing_serving.fact_sensor_readings s
       JOIN manufacturing_serving.dim_facility f ON s.facility_id = f.facility_id
       GROUP BY f.facility_name""")

# Date range query
benchmark_query(spark, "Date range filter",
    "SELECT COUNT(*) FROM manufacturing_serving.fact_sensor_readings WHERE event_date = '2026-02-15'")

print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Table Size & File Statistics

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL manufacturing_serving.fact_sensor_readings;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL manufacturing_serving.fact_maintenance_events;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY manufacturing_serving.fact_sensor_readings LIMIT 5;

# COMMAND ----------

print("✅ Notebook 11 Complete — Performance Optimization")
print("🚀 NEXT: Run Notebook 12 (Orchestration)")
