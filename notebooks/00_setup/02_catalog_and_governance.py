# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 02 — Catalog & Governance Setup
# MAGIC **Smart Manufacturing IoT Lakehouse Platform**
# MAGIC
# MAGIC This notebook sets up:
# MAGIC - Three-level namespace databases (Bronze / Silver / Gold)
# MAGIC - Row-level security (RLS) views — **deferred** until Gold tables exist
# MAGIC - Column-level security (masking) views — **deferred** until Gold tables exist
# MAGIC - RBAC role documentation
# MAGIC - Audit logging table
# MAGIC - Data lineage tracking table
# MAGIC
# MAGIC > **Note**: Security views (RLS + masking) are created as Python functions
# MAGIC > that will be called from Notebook 09 (Gold Facts) after the underlying tables exist.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create Three-Level Namespace (Medallion Architecture)
# MAGIC
# MAGIC | Unity Catalog Equivalent | Implementation | Purpose |
# MAGIC |---|---|---|
# MAGIC | `manufacturing_raw` catalog | `manufacturing_raw` database | Bronze layer |
# MAGIC | `manufacturing_refined` catalog | `manufacturing_refined` database | Silver layer |
# MAGIC | `manufacturing_serving` catalog | `manufacturing_serving` database | Gold layer |

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- BRONZE LAYER: Raw ingested data
# MAGIC -- ============================================================
# MAGIC CREATE DATABASE IF NOT EXISTS manufacturing_raw
# MAGIC COMMENT 'Bronze Layer - Raw ingested data from all sources. No transformations applied.';
# MAGIC
# MAGIC -- ============================================================
# MAGIC -- SILVER LAYER: Cleaned, validated, transformed data
# MAGIC -- ============================================================
# MAGIC CREATE DATABASE IF NOT EXISTS manufacturing_refined
# MAGIC COMMENT 'Silver Layer - Cleaned, deduplicated, validated data. Business transformations applied.';
# MAGIC
# MAGIC -- ============================================================
# MAGIC -- GOLD LAYER: Business-ready dimensional model
# MAGIC -- ============================================================
# MAGIC CREATE DATABASE IF NOT EXISTS manufacturing_serving
# MAGIC COMMENT 'Gold Layer - Star schema dimensional model for analytics and reporting.';
# MAGIC
# MAGIC -- ============================================================
# MAGIC -- GOVERNANCE: Security views, audit logs, DQ violations
# MAGIC -- ============================================================
# MAGIC CREATE DATABASE IF NOT EXISTS manufacturing_governance
# MAGIC COMMENT 'Governance Layer - Security views, audit logs, data quality violations tracking.';

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW DATABASES;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Row-Level Security (RLS) — Configuration
# MAGIC
# MAGIC Since the Gold/Silver tables don't exist yet, I defined the security
# MAGIC views as **Python functions** that will be called after Gold layer setup.
# MAGIC This avoids referencing non-existent tables.
# MAGIC
# MAGIC | Role | Access Level | Facility Filter |
# MAGIC |------|-------------|----------------|
# MAGIC | Data Analyst | Gold only | Own facility only (`FAC-NA-001`) |
# MAGIC | ML Engineer | Silver + Gold | Regional data only (`NORTH_AMERICA`) |
# MAGIC | Data Engineer | All layers | Full access, no filter |
# MAGIC | Auditor | Audit logs only | All facilities, read-only |

# COMMAND ----------

# Role simulation parameters
# In production, these would come from Unity Catalog's CURRENT_USER() function
CURRENT_ROLE = "data_engineer"
USER_FACILITY = "FAC-NA-001"
USER_REGION = "NORTH_AMERICA"

print(f"Current Role:  {CURRENT_ROLE}")
print(f"User Facility: {USER_FACILITY}")
print(f"User Region:   {USER_REGION}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Security View Creation Functions
# MAGIC These functions will be called from later notebooks once the underlying tables exist.

# COMMAND ----------

def create_rls_views(spark, role="data_engineer", facility="FAC-NA-001", region="NORTH_AMERICA"):
    """
    Create Row-Level Security views for Gold layer tables.
    Call this AFTER Gold tables (fact_sensor_readings, fact_maintenance_events,
    fact_production_output, dim_facility) have been created.
    """
    
    # RLS: fact_sensor_readings
    spark.sql(f"""
        CREATE OR REPLACE VIEW manufacturing_governance.vw_rls_sensor_readings AS
        SELECT sr.*
        FROM manufacturing_serving.fact_sensor_readings sr
        WHERE
          CASE
            WHEN '{role}' = 'data_analyst' THEN sr.facility_id = '{facility}'
            WHEN '{role}' = 'ml_engineer' THEN sr.facility_id IN (
              SELECT facility_id FROM manufacturing_serving.dim_facility
              WHERE region = '{region}'
            )
            WHEN '{role}' = 'data_engineer' THEN TRUE
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
            WHEN '{role}' = 'data_analyst' THEN me.facility_id = '{facility}'
            WHEN '{role}' = 'ml_engineer' THEN me.facility_id IN (
              SELECT facility_id FROM manufacturing_serving.dim_facility
              WHERE region = '{region}'
            )
            WHEN '{role}' = 'data_engineer' THEN TRUE
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
            WHEN '{role}' = 'data_analyst' THEN po.facility_id = '{facility}'
            WHEN '{role}' = 'ml_engineer' THEN po.facility_id IN (
              SELECT facility_id FROM manufacturing_serving.dim_facility
              WHERE region = '{region}'
            )
            WHEN '{role}' = 'data_engineer' THEN TRUE
            ELSE FALSE
          END
    """)
    
    print("✅ RLS views created: vw_rls_sensor_readings, vw_rls_maintenance_events, vw_rls_production_output")


def create_masking_views(spark, role="data_engineer"):
    """
    Create Column-Masking views for PII columns.
    Call this AFTER Silver/Gold tables exist.
    """
    
    # Masked: dim_equipment (serial number + asset value)
    spark.sql(f"""
        CREATE OR REPLACE VIEW manufacturing_governance.vw_masked_dim_equipment AS
        SELECT
          equipment_id,
          CASE
            WHEN '{role}' = 'data_engineer' THEN equipment_serial_number
            ELSE CONCAT('SN-***-', SHA2(equipment_serial_number, 256))
          END AS equipment_serial_number,
          facility_id,
          equipment_type,
          manufacturer,
          model_number,
          manufacture_date,
          installation_date,
          rated_capacity,
          operating_voltage,
          max_temperature_c,
          max_vibration_hz,
          maintenance_interval_days,
          equipment_status,
          CASE
            WHEN '{role}' = 'data_engineer' THEN CAST(asset_value_usd AS STRING)
            ELSE '***RESTRICTED***'
          END AS asset_value_usd,
          criticality_level,
          region,
          effective_start_date,
          effective_end_date,
          is_current,
          version_number
        FROM manufacturing_serving.dim_equipment
    """)
    
    # Masked: maintenance (technician ID)
    spark.sql(f"""
        CREATE OR REPLACE VIEW manufacturing_governance.vw_masked_maintenance AS
        SELECT
          maintenance_id,
          equipment_id,
          facility_id,
          maintenance_type,
          CASE
            WHEN '{role}' = 'data_engineer' THEN maintenance_technician_id
            ELSE CONCAT('TECH-', SHA2(maintenance_technician_id, 256))
          END AS maintenance_technician_id,
          start_datetime,
          end_datetime,
          downtime_minutes,
          failure_code,
          parts_replaced,
          maintenance_cost_usd,
          work_order_id
        FROM manufacturing_refined.silver_maintenance
    """)
    
    # Masked: quality inspection (inspector ID)
    spark.sql(f"""
        CREATE OR REPLACE VIEW manufacturing_governance.vw_masked_quality_inspection AS
        SELECT
          inspection_id,
          order_id,
          equipment_id,
          facility_id,
          CASE
            WHEN '{role}' = 'data_engineer' THEN inspector_employee_id
            ELSE CONCAT('EMP-', SHA2(inspector_employee_id, 256))
          END AS inspector_employee_id,
          inspection_timestamp,
          product_sku,
          batch_id,
          units_inspected,
          units_passed,
          units_rejected,
          defect_type,
          defect_severity,
          inspection_method,
          result
        FROM manufacturing_refined.silver_quality_inspection
    """)
    
    print("✅ Masking views created: vw_masked_dim_equipment, vw_masked_maintenance, vw_masked_quality_inspection")

print("✅ Security view functions defined — will be called after Gold/Silver tables exist")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. RBAC Role Definitions
# MAGIC
# MAGIC In Unity Catalog, these would be `GRANT` statements. Here I have documented the access matrix.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- RBAC CONFIGURATION (Unity Catalog equivalent GRANT statements)
# MAGIC -- These are documented for reference and would be active on
# MAGIC -- a Unity Catalog-enabled workspace.
# MAGIC -- ============================================================
# MAGIC
# MAGIC -- ROLE: Data Analyst
# MAGIC -- GRANT USAGE ON CATALOG manufacturing_serving TO `data_analyst_group`;
# MAGIC -- GRANT SELECT ON SCHEMA manufacturing_serving.gold TO `data_analyst_group`;
# MAGIC -- Access through RLS views only (vw_rls_*), own facility data only
# MAGIC -- PII columns are masked via vw_masked_* views
# MAGIC
# MAGIC -- ROLE: ML Engineer
# MAGIC -- GRANT USAGE ON CATALOG manufacturing_refined TO `ml_engineer_group`;
# MAGIC -- GRANT USAGE ON CATALOG manufacturing_serving TO `ml_engineer_group`;
# MAGIC -- GRANT SELECT ON SCHEMA manufacturing_refined.silver TO `ml_engineer_group`;
# MAGIC -- GRANT SELECT ON SCHEMA manufacturing_serving.gold TO `ml_engineer_group`;
# MAGIC -- Regional data only, full access to feature columns
# MAGIC
# MAGIC -- ROLE: Data Engineer
# MAGIC -- GRANT ALL PRIVILEGES ON CATALOG manufacturing_raw TO `data_engineer_group`;
# MAGIC -- GRANT ALL PRIVILEGES ON CATALOG manufacturing_refined TO `data_engineer_group`;
# MAGIC -- GRANT ALL PRIVILEGES ON CATALOG manufacturing_serving TO `data_engineer_group`;
# MAGIC -- Full access, no masking
# MAGIC
# MAGIC -- ROLE: Auditor
# MAGIC -- GRANT USAGE ON DATABASE manufacturing_governance TO `auditor_group`;
# MAGIC -- GRANT SELECT ON TABLE manufacturing_governance.audit_log TO `auditor_group`;
# MAGIC -- Read-only audit trail, all facilities
# MAGIC
# MAGIC SELECT 'RBAC configuration documented — see comments above' AS status;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Audit Logging Table
# MAGIC Tracks all data access events for compliance.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- AUDIT LOG TABLE: Tracks data access for governance
# MAGIC -- ============================================================
# MAGIC CREATE TABLE IF NOT EXISTS manufacturing_governance.audit_log (
# MAGIC   audit_id STRING,
# MAGIC   event_timestamp TIMESTAMP,
# MAGIC   user_role STRING,
# MAGIC   user_identity STRING,
# MAGIC   action_type STRING COMMENT 'SELECT, INSERT, UPDATE, DELETE, MERGE',
# MAGIC   target_database STRING,
# MAGIC   target_table STRING,
# MAGIC   facility_filter STRING COMMENT 'Which facility_id was filtered for RLS',
# MAGIC   rows_affected LONG,
# MAGIC   query_text STRING,
# MAGIC   execution_status STRING COMMENT 'SUCCESS or DENIED',
# MAGIC   ip_address STRING,
# MAGIC   session_id STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Audit log for all data access events. Used for compliance and governance.';

# COMMAND ----------

# Helper function to log audit events (call from other notebooks)
from pyspark.sql.functions import current_timestamp, lit
import uuid

def log_audit_event(spark, role, action, database, table, facility_filter="ALL", 
                    rows_affected=0, query_text="", status="SUCCESS"):
    """
    Log an audit event to the governance audit table.
    Call this from any notebook to track data access.
    """
    audit_data = [(
        str(uuid.uuid4()),           # audit_id
        role,                         # user_role
        f"user_{role}",              # user_identity
        action,                       # action_type
        database,                     # target_database
        table,                        # target_table
        facility_filter,             # facility_filter
        rows_affected,               # rows_affected
        query_text,                  # query_text
        status,                      # execution_status
        "127.0.0.1",                 # ip_address
        str(uuid.uuid4())[:8],       # session_id
    )]
    
    schema = "audit_id STRING, user_role STRING, user_identity STRING, action_type STRING, " \
             "target_database STRING, target_table STRING, facility_filter STRING, " \
             "rows_affected LONG, query_text STRING, execution_status STRING, " \
             "ip_address STRING, session_id STRING"
    
    df = spark.createDataFrame(audit_data, schema=schema) \
        .withColumn("event_timestamp", current_timestamp())
    
    df.write.format("delta").mode("append").saveAsTable("manufacturing_governance.audit_log")
    return df

# Example audit entries
log_audit_event(spark, "data_engineer", "CREATE", "manufacturing_raw", "*", 
                query_text="CREATE DATABASE manufacturing_raw")
log_audit_event(spark, "data_engineer", "CREATE", "manufacturing_refined", "*",
                query_text="CREATE DATABASE manufacturing_refined")
log_audit_event(spark, "data_engineer", "CREATE", "manufacturing_serving", "*",
                query_text="CREATE DATABASE manufacturing_serving")

print("✅ Audit log table created and seeded with initial entries")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM manufacturing_governance.audit_log ORDER BY event_timestamp DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Data Lineage Tracking Table
# MAGIC Tracks data movement across Bronze → Silver → Gold.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS manufacturing_governance.data_lineage (
# MAGIC   lineage_id STRING,
# MAGIC   pipeline_name STRING,
# MAGIC   source_layer STRING COMMENT 'LANDING, BRONZE, SILVER',
# MAGIC   source_table STRING,
# MAGIC   target_layer STRING COMMENT 'BRONZE, SILVER, GOLD',
# MAGIC   target_table STRING,
# MAGIC   transformation_type STRING COMMENT 'INGEST, CLEAN, AGGREGATE, MERGE, JOIN',
# MAGIC   records_read LONG,
# MAGIC   records_written LONG,
# MAGIC   records_rejected LONG,
# MAGIC   execution_timestamp TIMESTAMP,
# MAGIC   execution_duration_seconds DOUBLE,
# MAGIC   status STRING COMMENT 'SUCCESS, FAILED, PARTIAL'
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Data lineage tracking across Medallion layers.';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Summary

# COMMAND ----------

print("=" * 80)
print("CATALOG & GOVERNANCE SETUP COMPLETE")
print("=" * 80)
print("""
Databases Created:
  ✅ manufacturing_raw         (Bronze layer)
  ✅ manufacturing_refined     (Silver layer)
  ✅ manufacturing_serving     (Gold layer)
  ✅ manufacturing_governance  (Security & audit)

Security (Deferred — created after Gold tables exist):
  ⏳ Row-Level Security views  (create_rls_views function defined)
  ⏳ Column Masking views      (create_masking_views function defined)
  ✅ RBAC documented           (4 roles)
  ✅ Audit log table active
  ✅ Data lineage tracking table

Role Configuration:
  → CURRENT_ROLE:  data_engineer (controls RLS and column masking)
  → USER_FACILITY: FAC-NA-001   (facility filter for Data Analyst role)
  → USER_REGION:   NORTH_AMERICA (region filter for ML Engineer role)
""")
print("🚀 NEXT: Run Bronze Layer Notebooks (03, 04, 05, 06)")
