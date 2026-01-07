# Production-Level Data Pipeline Features

## Zero Data Loss Guarantee

### Multi-Level Validation System

The pipeline implements comprehensive data validation at every stage to ensure **zero data loss**:

#### 1. **Extraction Validation** (MySQL → Iceberg)
- **Source Count**: Direct `COUNT(*)` query on MySQL source table
- **Extracted Count**: Count rows in Spark DataFrame before write
- **Iceberg Verification**: Re-read and count from Iceberg after write
- **Validation**: Source count MUST equal Iceberg count (raises exception if mismatch)

```python
# From data_validator.py
def validate_extraction(schema_name, table_name, source_count, extracted_count):
    if source_count != extracted_count:
        raise DataValidationError(f"DATA LOSS DETECTED: {source_count - extracted_count} rows lost")
```

#### 2. **Load Validation** (Iceberg → StarRocks)
- **Iceberg Count**: Count rows in Iceberg table before load
- **StarRocks Verification**: Direct `COUNT(*)` query on StarRocks after load
- **Validation**: Iceberg count MUST equal StarRocks count

#### 3. **End-to-End Validation** (MySQL → StarRocks)
- **Final Reconciliation**: Compare MySQL source vs StarRocks target
- **Accuracy Percentage**: Calculate (StarRocks rows / MySQL rows) × 100
- **Zero Data Loss Flag**: Boolean indicator of perfect data integrity

### Automatic Failure on Data Loss

```python
# Pipeline will STOP and RAISE EXCEPTION if:
- MySQL source: 100,000 rows
- Iceberg staging: 99,999 rows  ❌ VALIDATION FAILED - 1 row lost
```

## Comprehensive Metrics Tracking

### Row-Level Metrics

Every table migration tracks:

```json
{
  "row_counts": {
    "mysql_source": 42857,
    "iceberg_extracted": 42857,
    "starrocks_loaded": 42857
  },
  "validation": {
    "extraction_match": true,
    "load_match": true,
    "end_to_end_match": true,
    "accuracy_percentage": 100.0
  },
  "data_integrity": {
    "zero_data_loss": true,
    "rows_lost": 0
  }
}
```

### Performance Metrics

```json
{
  "performance": {
    "extraction_duration_seconds": 12.5,
    "load_duration_seconds": 8.3,
    "total_duration_seconds": 20.8,
    "rows_per_second": 2060.1
  }
}
```

### Schema-Level Aggregation

```json
{
  "validation": {
    "total_mysql_rows": 1250000,
    "total_starrocks_rows": 1250000,
    "zero_data_loss": true,
    "accuracy_percentage": 100.0
  },
  "table_metrics": [
    {
      "table_name": "orders",
      "metrics": {...},
      "validation": {...}
    },
    {
      "table_name": "customers",
      "metrics": {...},
      "validation": {...}
    }
  ]
}
```

## Intelligent Per-Table Partitioning

### Auto-Detection of Partition Column

The system automatically finds the best partition column for each table:

1. **Primary Key** (if single column and numeric)
2. **AUTO_INCREMENT** column
3. **Indexed INT/BIGINT** column
4. **Any INT/BIGINT** column

```sql
-- Queries information_schema to find optimal column
SELECT COLUMN_NAME, DATA_TYPE, COLUMN_KEY, EXTRA
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = 'mydb' AND TABLE_NAME = 'orders'
  AND DATA_TYPE IN ('INT', 'BIGINT', 'INTEGER')
ORDER BY
  CASE WHEN COLUMN_KEY = 'PRI' THEN 1           -- Primary key first
       WHEN EXTRA LIKE '%auto_increment%' THEN 2 -- Auto-increment second
       WHEN COLUMN_KEY = 'MUL' THEN 3           -- Indexed column third
       ELSE 4 END
LIMIT 1
```

### Dynamic Partition Count

Row count determines partition strategy:

| Row Count Range | Partitions | Strategy |
|----------------|-----------|----------|
| < 10,000 | 1 | Single-threaded (no overhead) |
| 10K - 100K | 4 | Light parallelization |
| 100K - 1M | 10 | Moderate parallelization |
| 1M - 10M | 20 | Heavy parallelization |
| > 10M | 50 | Maximum parallelization |

### Accurate Bounds

```python
# Query MIN/MAX from actual data (not guessed)
SELECT MIN(id), MAX(id), COUNT(*) FROM orders
# Result: min=1, max=42857, count=42857

# Spark will create partitions like:
# Partition 1: id >= 1 AND id < 8572
# Partition 2: id >= 8572 AND id < 17143
# Partition 3: id >= 17143 AND id < 25714
# Partition 4: id >= 25714 AND id < 34285
# Partition 5: id >= 34285 AND id <= 42857
```

### Benefits

- **No data skew**: Even distribution based on actual MIN/MAX
- **Optimal parallelism**: Right number of partitions for table size
- **Zero configuration**: Fully automatic per table
- **Better performance**: 42K rows finish faster with intelligent partitioning

## Fault Tolerance & Retry Mechanism

### Exponential Backoff Retry

Critical operations retry automatically on transient failures:

```python
# Configuration
max_attempts = 3
initial_delay = 5.0 seconds
backoff_factor = 2.0
max_delay = 60.0 seconds

# Retry timeline:
# Attempt 1: Immediate
# Attempt 2: Wait 5 seconds
# Attempt 3: Wait 10 seconds (5 × 2)
# If all fail: Raise exception
```

### Retryable Operations

1. **MySQL → Iceberg extraction** (network failures, connection timeouts)
2. **Iceberg → StarRocks load** (database locks, connection resets)
3. **Validation queries** (temporary unavailability)

### Iceberg Snapshots for Safety

Even if pipeline fails mid-execution:
- **Iceberg preserves committed snapshots** (atomic commits)
- **Can restart from last successful table**
- **No duplicate data** (createOrReplace mode)
- **Full audit trail** of what was loaded when

## Production-Grade Logging

### Structured Logging

```
2026-01-05 14:23:45 - app.services.partition_optimizer - INFO - _find_partition_column:128 -
  Selected partition column: order_id (type=INT, key=PRI)

2026-01-05 14:23:47 - app.services.spark_pipeline - INFO - extract_mysql_table_to_iceberg:260 -
  Using intelligent partitioning: column=order_id, partitions=4, rows=42857

2026-01-05 14:24:02 - app.services.data_validator - INFO - validate_extraction:73 -
  Extraction validation PASSED: orders - 42857 rows matched

2026-01-05 14:24:15 - app.services.data_validator - INFO - validate_end_to_end:195 -
  End-to-end validation PASSED: orders - 42857 rows matched (100% accuracy)
```

### Log Levels

- **INFO**: Normal operation, metrics, validation results
- **WARNING**: Retries, fallback to non-partitioned reads
- **ERROR**: Failures, data loss detection, exceptions
- **DEBUG**: Detailed SQL queries, partition configs

### Log Outputs

- **Console**: Real-time feedback during execution
- **File**: `logs/pipeline.log` (append mode, persistent across runs)

## API Response Format

### Comprehensive Response Schema

```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "success",
  "total_schemas": 1,
  "successful_schemas": 1,
  "failed_schemas": 0,
  "skipped_schemas": 0,
  "started_at": "2026-01-05T14:23:40Z",
  "completed_at": "2026-01-05T14:24:18Z",
  "total_duration_seconds": 38.2,
  "message": "Successfully processed all 1 schemas",
  "results": [
    {
      "schema_name": "production_db",
      "status": "success",
      "total_tables": 5,
      "tables_processed": 5,
      "rows_transferred": 1250000,
      "processing_time_seconds": 37.8,
      "metadata": {
        "validation": {
          "total_mysql_rows": 1250000,
          "total_starrocks_rows": 1250000,
          "zero_data_loss": true,
          "accuracy_percentage": 100.0
        },
        "table_metrics": [
          {
            "table_name": "orders",
            "metrics": {
              "row_counts": {...},
              "validation": {...},
              "performance": {...},
              "data_integrity": {
                "zero_data_loss": true,
                "rows_lost": 0
              }
            },
            "extraction_time": 12.5,
            "load_time": 8.3
          }
        ]
      }
    }
  ]
}
```

## Performance Optimizations

### Configuration Tuning

```ini
# Spark Memory (High-Performance)
SPARK_DRIVER_MEMORY=8g
SPARK_EXECUTOR_MEMORY=8g
SPARK_EXECUTOR_CORES=4

# JDBC Optimizations
JDBC_FETCH_SIZE=50000        # Fetch 50K rows per network round-trip
BATCH_SIZE=500000             # Insert 500K rows per batch to StarRocks

# API Performance
API_RELOAD=False              # Disable file watching overhead
```

### Parallel Processing Strategy

✅ **Enabled**: Per-table parallelization (multiple Spark tasks read same table)
❌ **Disabled**: Multi-table parallelization (user requested sequential table processing)

### Spark Optimizations

- **Arrow-based data transfer**: `spark.sql.execution.arrow.pyspark.enabled=true`
- **Partition-aware reads**: Splits large tables across multiple JDBC connections
- **Batch inserts**: Reduces network overhead and database load

## Data Type Accuracy

### Comprehensive MySQL → StarRocks Mapping

| MySQL Type | StarRocks Type | Notes |
|-----------|----------------|-------|
| TINYINT(1) | BOOLEAN | Auto-detected |
| TINYINT UNSIGNED | SMALLINT | Range safety |
| INT UNSIGNED | BIGINT | Range safety |
| BIGINT UNSIGNED | LARGEINT | Range safety |
| DECIMAL(p,s) | DECIMAL(p,s) | Preserves precision/scale |
| VARCHAR(n) | VARCHAR(n) | Preserves length (max 65533) |
| TEXT/LONGTEXT | STRING | Unlimited length |
| DATETIME | DATETIME | Preserves timestamps |
| JSON | JSON | Native JSON support |

### Constraint Preservation

- **NULL/NOT NULL**: Exact mapping from source
- **Precision**: DECIMAL(10,2) → DECIMAL(10,2)
- **Scale**: Maintained for numeric types
- **Length**: VARCHAR(255) → VARCHAR(255)

## Health Checks & Monitoring

### Database Connectivity

```bash
GET /health
{
  "status": "healthy",
  "mysql_connection": "ok",
  "starrocks_connection": "ok",
  "s3_access": "ok",
  "spark_session": "ready"
}
```

### Real-Time Progress

All operations log to console and file simultaneously for monitoring.

## Summary

This pipeline is **production-ready** with:

✅ **Zero data loss guarantee** (multi-level validation with automatic failure)
✅ **Comprehensive metrics** (row counts, accuracy, performance at every level)
✅ **Intelligent partitioning** (auto-detection, dynamic sizing, optimal performance)
✅ **Fault tolerance** (retry with exponential backoff, Iceberg snapshots)
✅ **Accurate schema mapping** (datatypes, constraints, precision preserved)
✅ **High performance** (8GB memory, 500K batches, 50K fetch size)
✅ **Production logging** (structured, multi-output, appropriate levels)
✅ **Complete observability** (detailed metrics in API responses)

The system will **automatically fail and alert** if even a single row is lost during migration.
