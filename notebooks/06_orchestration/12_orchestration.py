# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 12 — Workflow Orchestration
# MAGIC **Smart Manufacturing IoT Lakehouse Platform**
# MAGIC
# MAGIC Master orchestration notebook that chains all pipelines in order.
# MAGIC
# MAGIC | Stage | Notebooks | Description |
# MAGIC |-------|-----------|-------------|
# MAGIC | 1 | 01 | Generate synthetic data |
# MAGIC | 2 | 02 | Catalog & governance setup |
# MAGIC | 3 | 03-06 | Bronze layer ingestion |
# MAGIC | 4 | 07 | Silver transformations |
# MAGIC | 5 | 08-09 | Gold dimensional model |
# MAGIC | 6 | 10 | Data quality framework |
# MAGIC | 7 | 11 | Performance optimization |
# MAGIC
# MAGIC > **Note**: Databricks Community Edition does not support Workflows/Jobs.
# MAGIC > I used `%run` for notebook chaining and document the production DAG config.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Pipeline Execution with Error Handling

# COMMAND ----------

import time
from datetime import datetime

class PipelineOrchestrator:
    """
    Orchestrates the execution of the Lakehouse pipeline notebooks
    with retry logic, error handling, and execution logging.
    """
    
    def __init__(self, spark):
        self.spark = spark
        self.execution_log = []
        self.run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        
    def run_notebook(self, notebook_path, timeout_seconds=600, max_retries=3):
        """
        Execute a notebook with retry logic.
        
        Args:
            notebook_path: Relative path to the notebook
            timeout_seconds: Max execution time
            max_retries: Maximum retry attempts
        """
        attempt = 0
        last_error = None
        
        while attempt < max_retries:
            attempt += 1
            start_time = time.time()
            
            try:
                print(f"\n{'='*60}")
                print(f"▶ Running: {notebook_path} (attempt {attempt}/{max_retries})")
                print(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"{'='*60}")
                
                # Execute the notebook
                result = dbutils.notebook.run(notebook_path, timeout_seconds)
                
                elapsed = round(time.time() - start_time, 2)
                self.execution_log.append({
                    "notebook": notebook_path,
                    "status": "SUCCESS",
                    "attempt": attempt,
                    "duration_seconds": elapsed,
                    "timestamp": datetime.now().isoformat(),
                    "error": None,
                })
                
                print(f"✅ SUCCESS: {notebook_path} ({elapsed}s)")
                return result
                
            except Exception as e:
                elapsed = round(time.time() - start_time, 2)
                last_error = str(e)
                
                self.execution_log.append({
                    "notebook": notebook_path,
                    "status": "FAILED",
                    "attempt": attempt,
                    "duration_seconds": elapsed,
                    "timestamp": datetime.now().isoformat(),
                    "error": last_error,
                })
                
                print(f"❌ FAILED (attempt {attempt}): {last_error}")
                
                if attempt < max_retries:
                    # Exponential backoff: 10s, 20s, 40s
                    wait_time = 10 * (2 ** (attempt - 1))
                    print(f"   Retrying in {wait_time}s...")
                    time.sleep(wait_time)
        
        raise Exception(f"Notebook {notebook_path} failed after {max_retries} attempts. "
                       f"Last error: {last_error}")
    
    def print_summary(self):
        """Print execution summary."""
        print("\n" + "=" * 80)
        print(f"PIPELINE EXECUTION SUMMARY (Run ID: {self.run_id})")
        print("=" * 80)
        
        total_time = sum(e["duration_seconds"] for e in self.execution_log)
        successes = sum(1 for e in self.execution_log if e["status"] == "SUCCESS")
        failures = sum(1 for e in self.execution_log if e["status"] == "FAILED")
        
        for entry in self.execution_log:
            status = "✅" if entry["status"] == "SUCCESS" else "❌"
            print(f"  {status} {entry['notebook']:55s} | {entry['duration_seconds']:6.1f}s | "
                  f"Attempt {entry['attempt']}")
        
        print(f"\nTotal: {successes} succeeded, {failures} failed")
        print(f"Total time: {total_time:.1f}s")
        print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Execute Full Pipeline
# MAGIC
# MAGIC Uncomment the `%run` commands below to execute the full pipeline.
# MAGIC In production, this would be a Databricks Workflow with a task DAG.
# MAGIC
# MAGIC > **⚠️ Important**: Run each notebook manually in order, or uncomment
# MAGIC > the `%run` commands below for automated execution.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Stage 1: Setup

# COMMAND ----------

# %run ./01_generate_synthetic_data

# COMMAND ----------

# %run ./02_catalog_and_governance

# COMMAND ----------

# MAGIC %md
# MAGIC ### Stage 2: Bronze Layer

# COMMAND ----------

# %run ./03_bronze_iot_telemetry

# COMMAND ----------

# %run ./04_bronze_production_orders

# COMMAND ----------

# %run ./05_bronze_quality_inspection

# COMMAND ----------

# %run ./06_bronze_maintenance_cdc

# COMMAND ----------

# MAGIC %md
# MAGIC ### Stage 3: Silver Layer

# COMMAND ----------

# %run ./07_silver_transformations

# COMMAND ----------

# MAGIC %md
# MAGIC ### Stage 4: Gold Layer

# COMMAND ----------

# %run ./08_gold_dimensions

# COMMAND ----------

# %run ./09_gold_facts

# COMMAND ----------

# MAGIC %md
# MAGIC ### Stage 5: Data Quality & Optimization

# COMMAND ----------

# %run ./10_data_quality_framework

# COMMAND ----------

# %run ./11_optimization

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Production Workflow Configuration (JSON)
# MAGIC
# MAGIC This is the Databricks Workflows/Jobs definition for production deployment.

# COMMAND ----------

import json

workflow_config = {
    "name": "Manufacturing_IoT_Lakehouse_Pipeline",
    "description": "End-to-end Lakehouse pipeline for Smart Manufacturing IoT platform",
    "schedule": {
        "quartz_cron_expression": "0 0 */4 * * ?",  # Every 4 hours
        "timezone_id": "UTC",
        "pause_status": "UNPAUSED"
    },
    "max_concurrent_runs": 1,
    "email_notifications": {
        "on_failure": ["data-engineering-team@company.com"],
        "on_success": ["data-engineering-team@company.com"],
        "no_alert_for_skipped_runs": True
    },
    "timeout_seconds": 3600,
    "tasks": [
        {
            "task_key": "01_generate_data",
            "notebook_task": {
                "notebook_path": "/Workspace/Users/tankkritansh088@gmail.com/01_generate_synthetic_data",
                "source": "WORKSPACE"
            },
            "timeout_seconds": 600,
            "max_retries": 2,
            "retry_on_timeout": True,
            "min_retry_interval_millis": 30000
        },
        {
            "task_key": "02_catalog_governance",
            "depends_on": [{"task_key": "01_generate_data"}],
            "notebook_task": {
                "notebook_path": "/Workspace/Users/tankkritansh088@gmail.com/02_catalog_and_governance"
            },
            "timeout_seconds": 300,
            "max_retries": 2
        },
        {
            "task_key": "03_bronze_iot",
            "depends_on": [{"task_key": "02_catalog_governance"}],
            "notebook_task": {
                "notebook_path": "/Workspace/Users/tankkritansh088@gmail.com/03_bronze_iot_telemetry"
            },
            "timeout_seconds": 600,
            "max_retries": 3
        },
        {
            "task_key": "04_bronze_orders",
            "depends_on": [{"task_key": "02_catalog_governance"}],
            "notebook_task": {
                "notebook_path": "/Workspace/Users/tankkritansh088@gmail.com/04_bronze_production_orders"
            },
            "timeout_seconds": 300,
            "max_retries": 2
        },
        {
            "task_key": "05_bronze_quality",
            "depends_on": [{"task_key": "02_catalog_governance"}],
            "notebook_task": {
                "notebook_path": "/Workspace/Users/tankkritansh088@gmail.com/05_bronze_quality_inspection"
            },
            "timeout_seconds": 300,
            "max_retries": 2
        },
        {
            "task_key": "06_bronze_maintenance",
            "depends_on": [{"task_key": "02_catalog_governance"}],
            "notebook_task": {
                "notebook_path": "/Workspace/Users/tankkritansh088@gmail.com/06_bronze_maintenance_cdc"
            },
            "timeout_seconds": 300,
            "max_retries": 2
        },
        {
            "task_key": "07_silver_transformations",
            "depends_on": [
                {"task_key": "03_bronze_iot"},
                {"task_key": "04_bronze_orders"},
                {"task_key": "05_bronze_quality"},
                {"task_key": "06_bronze_maintenance"}
            ],
            "notebook_task": {
                "notebook_path": "/Workspace/Users/tankkritansh088@gmail.com/07_silver_transformations"
            },
            "timeout_seconds": 600,
            "max_retries": 3
        },
        {
            "task_key": "08_gold_dimensions",
            "depends_on": [{"task_key": "07_silver_transformations"}],
            "notebook_task": {
                "notebook_path": "/Workspace/Users/tankkritansh088@gmail.com/08_gold_dimensions"
            },
            "timeout_seconds": 300,
            "max_retries": 2
        },
        {
            "task_key": "09_gold_facts",
            "depends_on": [{"task_key": "08_gold_dimensions"}],
            "notebook_task": {
                "notebook_path": "/Workspace/Users/tankkritansh088@gmail.com/09_gold_facts"
            },
            "timeout_seconds": 300,
            "max_retries": 2
        },
        {
            "task_key": "10_data_quality",
            "depends_on": [{"task_key": "09_gold_facts"}],
            "notebook_task": {
                "notebook_path": "/Workspace/Users/tankkritansh088@gmail.com/10_data_quality_framework"
            },
            "timeout_seconds": 300,
            "max_retries": 2
        },
        {
            "task_key": "11_optimization",
            "depends_on": [{"task_key": "10_data_quality"}],
            "notebook_task": {
                "notebook_path": "/Workspace/Users/tankkritansh088@gmail.com/11_optimization"
            },
            "timeout_seconds": 600,
            "max_retries": 1
        }
    ],
    "job_clusters": [
        {
            "job_cluster_key": "main_cluster",
            "new_cluster": {
                "spark_version": "14.3.x-scala2.12",
                "node_type_id": "Standard_DS3_v2",
                "num_workers": 2,
                "spark_conf": {
                    "spark.databricks.delta.optimizeWrite.enabled": "true",
                    "spark.databricks.delta.autoCompact.enabled": "true"
                }
            }
        }
    ]
}

print(json.dumps(workflow_config, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Pipeline DAG Visualization
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────┐
# MAGIC │  01_generate_data   │
# MAGIC └──────────┬──────────┘
# MAGIC            │
# MAGIC ┌──────────▼──────────┐
# MAGIC │ 02_catalog_govern   │
# MAGIC └──────────┬──────────┘
# MAGIC            │
# MAGIC     ┌──────┼──────┬──────────┐
# MAGIC     │      │      │          │
# MAGIC ┌───▼──┐┌──▼───┐┌─▼──┐  ┌───▼───┐
# MAGIC │03_iot││04_ord││05_qi│  │06_mnt │
# MAGIC └───┬──┘└──┬───┘└─┬──┘  └───┬───┘
# MAGIC     │      │      │         │
# MAGIC     └──────┼──────┼─────────┘
# MAGIC            │
# MAGIC ┌──────────▼──────────┐
# MAGIC │  07_silver_txfm     │
# MAGIC └──────────┬──────────┘
# MAGIC            │
# MAGIC ┌──────────▼──────────┐
# MAGIC │  08_gold_dims       │
# MAGIC └──────────┬──────────┘
# MAGIC            │
# MAGIC ┌──────────▼──────────┐
# MAGIC │  09_gold_facts      │
# MAGIC └──────────┬──────────┘
# MAGIC            │
# MAGIC ┌──────────▼──────────┐
# MAGIC │  10_data_quality    │
# MAGIC └──────────┬──────────┘
# MAGIC            │
# MAGIC ┌──────────▼──────────┐
# MAGIC │  11_optimization    │
# MAGIC └─────────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Alerting Configuration

# COMMAND ----------

def send_alert(alert_type, message, details=None):
    """
    Send alert notification.
    In production, this would integrate with:
    - Azure Monitor
    - Microsoft Teams webhook
    - Email via SendGrid
    - PagerDuty
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    alert = {
        "alert_type": alert_type,  # SUCCESS, FAILURE, WARNING, SLA_BREACH
        "message": message,
        "details": details,
        "timestamp": timestamp,
        "pipeline": "Manufacturing_IoT_Lakehouse",
    }
    
    print(f"\n🔔 ALERT [{alert_type}]: {message}")
    if details:
        print(f"   Details: {details}")
    print(f"   Timestamp: {timestamp}")
    
    # In production:
    # import requests
    # webhook_url = dbutils.secrets.get("keyvault-scope", "teams-webhook-url")
    # requests.post(webhook_url, json=alert)
    
    return alert

# Example alerts
send_alert("SUCCESS", "Pipeline completed successfully", 
           "All 11 notebooks executed without errors")
send_alert("WARNING", "DQ SLA approaching threshold",
           "silver_iot_telemetry pass rate: 96.5% (threshold: 95%)")

# COMMAND ----------

print("✅ Notebook 12 Complete — Workflow Orchestration")
print("📋 All notebooks are ready for execution on Databricks Community Edition!")
