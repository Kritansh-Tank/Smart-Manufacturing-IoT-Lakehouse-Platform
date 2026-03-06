# Smart Manufacturing IoT Lakehouse Platform

> **Spark Wars 4.0 — Final Round Submission**
> A governed, scalable Lakehouse platform built on Databricks for real-time IoT monitoring, predictive maintenance, and production quality tracking.

---

## 📋 Table of Contents

- [Architecture Overview](#architecture-overview)
- [Prerequisites](#prerequisites)
- [Setup Instructions](#setup-instructions)
- [Pipeline Execution Guide](#pipeline-execution-guide)
- [Notebook Reference](#notebook-reference)
- [Key Design Decisions](#key-design-decisions)
- [Security & Governance](#security--governance)
- [Data Quality Framework](#data-quality-framework)
- [Performance Optimization](#performance-optimization)
- [Testing](#testing)
- [Known Limitations](#known-limitations)

---

## Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────┐
│                       DATA SOURCES (Simulated)                       │
├────────────┬──────────────┬─────────────┬───────────┬───────────────┤
│ IoT Hub    │ ADLS Gen2    │ Azure SQL   │ ADLS Gen2 │ ADLS Gen2     │
│ (Streaming)│ (Parquet)    │ (CDC)       │ (CSV)     │ (Delta)       │
│ Telemetry  │ Prod Orders  │ Maintenance │ Quality   │ Equipment     │
└─────┬──────┴──────┬───────┴──────┬──────┴─────┬─────┴───────┬───────┘
      │             │              │            │             │
      ▼             ▼              ▼            ▼             ▼
┌──────────────────────────────────────────────────────────────────────┐
│  BRONZE (manufacturing_raw)                                          │
│  Raw ingestion, schema preservation, metadata enrichment             │
│  ┌──────────┬──────────┬──────────┬──────────┬──────────────┐       │
│  │ IoT      │ Prod     │ Maint    │ Quality  │ Equipment    │       │
│  │ Telemetry│ Orders   │ Records  │ Inspect  │ Master       │       │
│  └──────────┴──────────┴──────────┴──────────┴──────────────┘       │
└──────────────────────────────┬───────────────────────────────────────┘
                               │ Cleansing, Dedup, Flattening,
                               │ Type Casting, CDC Application
                               ▼
┌──────────────────────────────────────────────────────────────────────┐
│  SILVER (manufacturing_refined)                                      │
│  Cleaned, validated, deduplicated, SCD Type 2                        │
│  ┌───────────┬──────────┬──────────┬──────────┬─────────────┐       │
│  │ IoT       │ Prod     │ Maint    │ Quality  │ Equipment   │       │
│  │ Flattened │ Enriched │ CDC      │ Cleaned  │ SCD2        │       │
│  └───────────┴──────────┴──────────┴──────────┴─────────────┘       │
└──────────────────────────────┬───────────────────────────────────────┘
                               │ Dimensional Modeling, Joins,
                               │ Aggregation, Anomaly Detection
                               ▼
┌──────────────────────────────────────────────────────────────────────┐
│  GOLD (manufacturing_serving) — Star Schema                          │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │ FACTS:  fact_sensor_readings | fact_production_output       │    │
│  │         fact_maintenance_events                              │    │
│  ├─────────────────────────────────────────────────────────────┤    │
│  │ DIMS:   dim_equipment (SCD2) | dim_facility | dim_timestamp │    │
│  │         dim_production_order | dim_technician               │    │
│  └─────────────────────────────────────────────────────────────┘    │
└──────────────────────────────────────────────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────────┐
│  GOVERNANCE (manufacturing_governance)                                │
│  ┌──────────────┬────────────────┬───────────────────┐              │
│  │ RLS Views    │ Column Masking │ Audit Log         │              │
│  │ DQ Violations│ Quarantine     │ Data Lineage      │              │
│  └──────────────┴────────────────┴───────────────────┘              │
└──────────────────────────────────────────────────────────────────────┘
```

---

## Prerequisites

- **Databricks Free Edition** account (with Unity Catalog & serverless compute)
- **Compute**: Serverless compute (auto-provisioned) or single-user cluster with DBR 14.0+
- **Language**: Python (PySpark)
- No external dependencies required — all code uses built-in Spark/Delta Lake libraries
- Unity Catalog Volumes used for data storage (DBFS is disabled on Free Edition)

---

## Setup Instructions

### Step 1: Create Databricks Account
1. Go to [Databricks](https://www.databricks.com/) and sign up for a Free Edition account
2. Log into your workspace
3. Serverless compute is available by default — no cluster setup needed
4. The notebooks will auto-detect your workspace catalog

### Step 2: Upload Notebooks
1. In the Databricks workspace, click **Import** in the left sidebar
2. Upload the entire `notebooks/` folder, maintaining the directory structure:
   ```
   notebooks/
   ├── 00_setup/
   ├── 01_bronze/
   ├── 02_silver/
   ├── 03_gold/
   ├── 04_data_quality/
   ├── 05_optimization/
   └── 06_orchestration/
   ```
3. Alternatively, upload each `.py` file individually

### Step 3: Attach Compute
1. Open any notebook
2. Serverless compute auto-attaches, or select your cluster from the dropdown
3. Notebook 01 auto-detects the workspace catalog and creates required schemas/volumes

---

## Pipeline Execution Guide

**Execute notebooks in this exact order:**

| Step | Notebook | Purpose | Expected Duration |
|------|----------|---------|-------------------|
| 1 | `01_generate_synthetic_data` | Generate all 5 data sources + edge cases | ~30s |
| 2 | `02_catalog_and_governance` | Create databases, security views, audit tables | ~20s |
| 3 | `03_bronze_iot_telemetry` | Ingest IoT streaming data → Bronze | ~30s |
| 4 | `04_bronze_production_orders` | Ingest Parquet production orders → Bronze | ~15s |
| 5 | `05_bronze_quality_inspection` | Ingest CSV quality inspections → Bronze | ~15s |
| 6 | `06_bronze_maintenance_cdc` | Ingest CDC maintenance + equipment + facility → Bronze | ~15s |
| 7 | `07_silver_transformations` | Clean, dedup, flatten, SCD2 → Silver | ~45s |
| 8 | `08_gold_dimensions` | Build dimension tables (incl. dim_timestamp 2025-2027) | ~30s |
| 9 | `09_gold_facts` | Build fact tables with dimension joins | ~30s |
| 10 | `10_data_quality_framework` | Run 15+ DQ checks, populate violations table | ~20s |
| 11 | `11_optimization` | OPTIMIZE, Z-ORDER, VACUUM, ANALYZE | ~20s |
| 12 | `12_orchestration` | View orchestration config (reference) | ~5s |

**Total estimated time: ~5 minutes**

---

## Notebook Reference

### Setup Notebooks
| # | File | Description |
|---|------|-------------|
| 01 | `01_generate_synthetic_data.py` | Generates 260+ records across 5 data sources with edge cases |
| 02 | `02_catalog_and_governance.py` | Creates 4 databases, RLS views, column masking, audit log |

### Bronze Layer (Raw Ingestion)
| # | File | Source | Format |
|---|------|--------|--------|
| 03 | `03_bronze_iot_telemetry.py` | IoT Hub (simulated) | JSON → Delta |
| 04 | `04_bronze_production_orders.py` | Landing zone | Parquet → Delta |
| 05 | `05_bronze_quality_inspection.py` | Landing zone | CSV → Delta |
| 06 | `06_bronze_maintenance_cdc.py` | Azure SQL CDC (simulated) | Delta → Delta |

### Silver Layer (Cleaned & Transformed)
| # | File | Key Operations |
|---|------|---------------|
| 07 | `07_silver_transformations.py` | Flatten JSON, dedup, CDC apply, SCD Type 2 MERGE |

### Gold Layer (Star Schema)
| # | File | Tables Created |
|---|------|---------------|
| 08 | `08_gold_dimensions.py` | dim_equipment, dim_facility, dim_order, dim_technician, dim_timestamp |
| 09 | `09_gold_facts.py` | fact_sensor_readings, fact_production_output, fact_maintenance_events |

### Quality, Optimization & Orchestration
| # | File | Description |
|---|------|-------------|
| 10 | `10_data_quality_framework.py` | 15+ DQ rules, quarantine table, SLA dashboard |
| 11 | `11_optimization.py` | Z-ORDER, VACUUM, auto-compact, Bloom filters, benchmarks |
| 12 | `12_orchestration.py` | Pipeline DAG, retry logic, Workflows JSON config |

---

## Key Design Decisions

### 1. Databricks Free Edition Adaptations
| Enterprise Feature | Our Implementation |
|---|---|
| Unity Catalog | Auto-detected workspace catalog with UC Volumes for storage |
| DBFS | Replaced with `/Volumes/<catalog>/landing/raw_data/` paths |
| Delta Live Tables | Standard PySpark batch + Structured Streaming |
| Autoloader | `spark.read` / `spark.readStream` with file paths |
| `sparkContext` (JVM) | Python file I/O for serverless compute compatibility |
| `input_file_name()` | `_metadata.file_path` (UC-compatible) |
| Row-level Security | SQL Views with Python f-string parameterization |
| Column Masking | SQL Views with SHA2() hashing |
| Workflows/Jobs | `%run` chaining + JSON config for reference |

### 2. SCD Type 2 Strategy
- Full MERGE implementation with `effective_start_date`, `effective_end_date`, `is_current`, `version_number`
- Change detection on 8 tracked columns (status, capacity, voltage, etc.)
- Handles late-arriving updates and multiple changes in a single batch
- Old records closed out before new versions inserted

### 3. Data Quality Approach
- Implemented as a separate reusable framework (not inline)
- Three actions: **DROP** (critical fails), **WARN** (logged but kept), **QUARANTINE** (moved to separate table)
- Centralized `dq_violations` table tracks all checks with lineage
- SLA dashboard query calculates pass rates per table

### 4. Streaming Simulation
- Primary ingestion is batch-mode (reliable for hackathon demo)
- Streaming mode is included as a second path using `trigger(availableNow=True)`
- Both write to the same Bronze layer format

---

## Security & Governance

### Row-Level Security
- Implemented via parameterized SQL Views in `manufacturing_governance`
- Created in Notebook 09 (after Gold tables exist) using Python f-strings
- Controlled by role variables: `CURRENT_ROLE`, `USER_FACILITY`, `USER_REGION`
- Data Analysts see only their assigned facility
- ML Engineers see only their region

### Column Masking
- `equipment_serial_number` → SHA2 hash (non-reversible)
- `maintenance_technician_id` → SHA2 hash
- `inspector_employee_id` → SHA2 hash
- Data Engineers see unmasked values

### Audit Logging
- `manufacturing_governance.audit_log` tracks all data access
- Columns: user_role, action_type, target_table, facility_filter, rows_affected

---

## Data Quality Framework

### Rules Implemented (15+)
| Rule Type | Count | Examples |
|-----------|-------|---------|
| NOT NULL | 6 | device_id, event_timestamp, facility_id, order_id, inspection_id, equipment_id |
| RANGE | 2 | Temperature [-50, 150°C], Vibration [0, 100Hz] |
| REFERENTIAL INTEGRITY | 4 | facility_id EXISTS IN dim_facility |
| FRESHNESS | 1 | Data < 15 minutes old |
| PATTERN | 1 | Serial number format validation |
| CUSTOM | 1 | units_rejected ≤ units_inspected |

### Edge Cases Tested
- NULL values in required fields
- Out-of-range sensor readings (-200°C)
- Invalid facility references
- Duplicate IoT messages
- Late-arriving events
- Schema evolution (new JSON fields)

---

## Performance Optimization

| Strategy | Implementation |
|----------|---------------|
| **Partitioning** | `event_date` + `facility_id` on fact tables |
| **Z-Ordering** | `device_id`, `equipment_id`, `equipment_type`, `product_sku` (non-partition columns) |
| **Auto-compaction** | Enabled on all fact and Silver tables |
| **VACUUM** | 7-day retention policy |
| **Statistics** | ANALYZE TABLE on all Gold tables |
| **Bloom Filters** | Documented for production (not supported in CE) |
| **Liquid Clustering** | Documented for production reference |

---

## Testing

See [tests/test_plan.md](tests/test_plan.md) for complete test scenarios covering:
- Streaming pipeline tests (dedup, late-arriving, schema evolution)
- Data quality tests (nulls, ranges, referential integrity)
- SCD Type 2 tests (spec changes, multi-change batches)
- Security tests (RLS, column masking)
- Performance tests (query benchmarks)

---

## Known Limitations

1. **Serverless Compute**: No `sparkContext`/JVM access — all file I/O uses Python native APIs
2. **Streaming**: Uses `trigger(availableNow=True)` — not continuous streaming
3. **Bloom Filters**: Documented for production use
4. **Liquid Clustering**: Documented for production reference
5. **Real IoT Hub**: Would require Azure subscription and Event Hubs endpoint
6. **JDBC CDC**: Simulated via Delta — production would use real SQL Server CDC
7. **Secrets**: Azure Key Vault integration documented but not active
8. **DBFS**: Disabled on Free Edition — Unity Catalog Volumes used instead
9. **`input_file_name()`**: Not supported in UC — replaced with `_metadata.file_path`
10. **Widget `${}` syntax**: Not supported on serverless — replaced with Python f-strings

---

## Future Improvements

1. Deploy to Azure Databricks with Unity Catalog for full governance
2. Connect to real Azure IoT Hub for actual streaming
3. Implement Delta Live Tables for declarative pipeline definitions
4. Add ML model for predictive maintenance using feature engineering
5. Build Power BI dashboards connected to Gold layer
6. Implement Azure Monitor integration for production alerting
7. Add Databricks SQL warehouse for BI queries
