# Test Plan — Smart Manufacturing IoT Lakehouse Platform

---

## 1. Streaming Pipeline Tests

### Test 1.1: Late-Arriving Events (1 hour late)
| Item | Detail |
|------|--------|
| **Scenario** | IoT event with `event_timestamp` from 07:00 arrives at 10:30 |
| **Test Data** | `PUMP-FAC01-0012` with `event_timestamp = 2026-02-15T07:00:00.000Z` |
| **Expected** | Record is ingested into Bronze and Silver (within watermark window) |
| **Verification** | `SELECT * FROM silver_iot_telemetry WHERE event_timestamp = '2026-02-15 07:00:00'` |

### Test 1.2: Duplicate Messages (same device_id + timestamp)
| Item | Detail |
|------|--------|
| **Scenario** | Two records with identical `device_id + event_timestamp` |
| **Test Data** | `PUMP-FAC01-0012` at `2026-02-15T10:23:45.123Z` (appears twice) |
| **Expected** | Only 1 record in Silver (dedup applied) |
| **Verification** | `SELECT COUNT(*) FROM silver_iot_telemetry WHERE device_id='PUMP-FAC01-0012' AND event_timestamp='2026-02-15 10:23:45.123'` → should be 1 |

### Test 1.3: Schema Evolution (new field in IoT payload)
| Item | Detail |
|------|--------|
| **Scenario** | IoT message contains new `telemetry.humidity` field |
| **Test Data** | `PUMP-FAC02-0078` with extra `humidity=45.2` and `alert_level=WARNING` |
| **Expected** | Record ingested; new fields captured in JSON but not in schema columns |
| **Verification** | Record present in Bronze; Silver flattening ignores unknown fields |

### Test 1.4: Malformed JSON Messages
| Item | Detail |
|------|--------|
| **Scenario** | NULL values in required JSON fields |
| **Test Data** | Record with `device_id = NULL` and record with `event_timestamp = NULL` |
| **Expected** | Records land in Bronze (raw), flagged in DQ framework |
| **Verification** | `SELECT * FROM dq_violations WHERE rule_name LIKE 'NOT_NULL%'` shows failures |

### Test 1.5: Out-of-Order Events
| Item | Detail |
|------|--------|
| **Scenario** | Events arrive out of chronological order |
| **Test Data** | Multiple devices with mixed timestamps |
| **Expected** | All events preserved (append-only); order doesn't matter for storage |
| **Verification** | Compare Bronze count with Silver count (should differ only by dedup) |

---

## 2. Data Quality Tests

### Test 2.1: NULL Values in Required Fields
| Item | Detail |
|------|--------|
| **Scenario** | `device_id` is NULL |
| **Test Data** | 1 record with NULL `device_id` in IoT data |
| **Expected** | DQ violation logged as CRITICAL; record quarantined |
| **Verification** | `SELECT * FROM dq_quarantine WHERE rule_name = 'NOT_NULL_device_id'` |

### Test 2.2: Temperature Outside Valid Range
| Item | Detail |
|------|--------|
| **Scenario** | Temperature reading of -200°C (valid range: -50 to 150°C) |
| **Test Data** | `PRESS-FAC03-0045` with `temperature = -200.0` |
| **Expected** | DQ violation logged as WARNING; record quarantined |
| **Verification** | `SELECT * FROM dq_quarantine WHERE rule_name = 'RANGE_temperature'` |

### Test 2.3: Vibration Outside Valid Range
| Item | Detail |
|------|--------|
| **Scenario** | Vibration of 999 Hz (valid range: 0 to 100 Hz) |
| **Test Data** | `CNC-FAC01-0031` with `vibration_x = 999.0` |
| **Expected** | DQ violation logged as WARNING; record quarantined |
| **Verification** | `SELECT * FROM dq_quarantine WHERE rule_name = 'RANGE_vibration'` |

### Test 2.4: Referential Integrity Violation
| Item | Detail |
|------|--------|
| **Scenario** | `facility_id = FAC-INVALID-999` not in dim_facility |
| **Test Data** | IoT record with `FAC-INVALID-999`, Production order with `FAC-INVALID-999` |
| **Expected** | DQ violation logged as CRITICAL; records quarantined |
| **Verification** | `SELECT * FROM dq_violations WHERE rule_name LIKE 'REF_INTEGRITY%' AND records_failed > 0` |

### Test 2.5: Data Freshness > 15 Minutes
| Item | Detail |
|------|--------|
| **Scenario** | Check if data is stale (> 15 min compared to max timestamp) |
| **Expected** | DQ check executed; WARNING logged if stale data found |
| **Verification** | `SELECT * FROM dq_violations WHERE rule_name = 'FRESHNESS_15_minutes'` |

### Test 2.6: Serial Number Pattern Validation
| Item | Detail |
|------|--------|
| **Scenario** | Equipment serial numbers must match `SN-XXX-YYYY-NNNNN` |
| **Expected** | All equipment serial numbers pass pattern check |
| **Verification** | `SELECT * FROM dq_violations WHERE rule_name = 'PATTERN_serial_number_format'` |

---

## 3. SCD Type 2 Tests

### Test 3.1: Equipment Specification Change
| Item | Detail |
|------|--------|
| **Scenario** | `EQ-PUMP-0078` status changes from `UNDER_MAINTENANCE` to `ACTIVE` |
| **Expected** | Old version: `is_current=false`, `effective_end_date` set. New version: `is_current=true`, `version_number=2` |
| **Verification** | `SELECT * FROM dim_equipment WHERE equipment_id='EQ-PUMP-0078' ORDER BY version_number` |

### Test 3.2: Capacity Upgrade
| Item | Detail |
|------|--------|
| **Scenario** | `EQ-CNC-0012` rated_capacity changes from 500 to 600 |
| **Expected** | Two versions in dim_equipment for this equipment_id |
| **Verification** | Check version 1 has `rated_capacity=500`, version 2 has `rated_capacity=600` |

### Test 3.3: Late-Arriving Historical Update
| Item | Detail |
|------|--------|
| **Scenario** | CDC UPDATE event with older `_cdc_timestamp` |
| **Test Data** | `MNT-2026-00454` with `_cdc_operation=UPDATE` |
| **Expected** | Silver maintains only the latest version per `maintenance_id` |
| **Verification** | `SELECT * FROM silver_maintenance WHERE maintenance_id='MNT-2026-00454'` → 1 row |

### Test 3.4: Multiple Changes to Same Equipment in Single Batch
| Item | Detail |
|------|--------|
| **Scenario** | Two maintenance records for `EQ-CNC-0012` in same batch |
| **Test Data** | `MNT-2026-00460` and `MNT-2026-00461` both for `EQ-CNC-0012` |
| **Expected** | Both records present in Silver; SCD2 handles sequentially |
| **Verification** | `SELECT * FROM silver_maintenance WHERE equipment_id='EQ-CNC-0012'` |

### Test 3.5: CDC DELETE Operation
| Item | Detail |
|------|--------|
| **Scenario** | Maintenance record cancelled (DELETE) |
| **Test Data** | `MNT-2026-00459` with `_cdc_operation=DELETE` |
| **Expected** | Record in Silver with `is_deleted=true` |
| **Verification** | `SELECT * FROM silver_maintenance WHERE maintenance_id='MNT-2026-00459'` |

---

## 4. Security & Governance Tests

### Test 4.1: Data Analyst — Own Facility Only
| Item | Detail |
|------|--------|
| **Setup** | Set widget `current_role = data_analyst`, `user_facility = FAC-NA-001` |
| **Expected** | RLS views return only `FAC-NA-001` data |
| **Verification** | `SELECT DISTINCT facility_id FROM vw_rls_sensor_readings` → only `FAC-NA-001` |

### Test 4.2: ML Engineer — Regional Data Only
| Item | Detail |
|------|--------|
| **Setup** | Set widget `current_role = ml_engineer`, `user_region = NORTH_AMERICA` |
| **Expected** | RLS views return `FAC-NA-001` and `FAC-NA-002` data |
| **Verification** | Query results include only NORTH_AMERICA facilities |

### Test 4.3: Column Masking — PII Fields
| Item | Detail |
|------|--------|
| **Setup** | Set widget `current_role = data_analyst` |
| **Expected** | `equipment_serial_number` shows as `SN-***-<hash>`, not raw value |
| **Verification** | `SELECT equipment_serial_number FROM vw_masked_dim_equipment LIMIT 1` |

### Test 4.4: Data Engineer — Full Access
| Item | Detail |
|------|--------|
| **Setup** | Set widget `current_role = data_engineer` |
| **Expected** | All data visible, no masking, all facilities |
| **Verification** | PII columns show original values |

### Test 4.5: Audit Log Verification
| Item | Detail |
|------|--------|
| **Expected** | All data access events logged in audit_log table |
| **Verification** | `SELECT * FROM manufacturing_governance.audit_log ORDER BY event_timestamp DESC` |

---

## 5. Performance Tests

### Test 5.1: Point Lookup on Z-Ordered Column
| Item | Detail |
|------|--------|
| **Query** | `SELECT * FROM fact_sensor_readings WHERE device_id = 'PUMP-FAC01-0012'` |
| **Expected** | Fast execution with data skipping (only relevant files scanned) |

### Test 5.2: Partition Pruning
| Item | Detail |
|------|--------|
| **Query** | `SELECT * FROM fact_sensor_readings WHERE facility_id = 'FAC-NA-001'` |
| **Expected** | Only relevant partitions read (check Spark UI for files pruned) |

### Test 5.3: Aggregation Performance
| Item | Detail |
|------|--------|
| **Query** | `SELECT facility_id, AVG(temperature) FROM fact_sensor_readings GROUP BY facility_id` |
| **Expected** | Efficient execution using column statistics |

### Test 5.4: Multi-Table Join
| Item | Detail |
|------|--------|
| **Query** | Join fact_sensor_readings with dim_facility and dim_timestamp |
| **Expected** | Broadcast join on small dimension tables |

---

## 6. Data Lineage Tests

### Test 6.1: End-to-End Traceability
| Item | Detail |
|------|--------|
| **Scenario** | Trace a specific `device_id` from landing → Bronze → Silver → Gold |
| **Steps** | 1. Count in Bronze, 2. Count in Silver (after dedup), 3. Count in Gold |
| **Expected** | Bronze ≥ Silver ≥ Gold (progressively cleaner) |

### Test 6.2: Change Data Feed Verification
| Item | Detail |
|------|--------|
| **Scenario** | Verify CDF is enabled and capturing changes |
| **Query** | `SELECT * FROM table_changes('manufacturing_serving.dim_equipment', 0)` |
| **Expected** | Shows INSERT and UPDATE operations with timestamps |
