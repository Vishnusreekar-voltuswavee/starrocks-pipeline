# MySQL to StarRocks Migration Guide via Iceberg & AWS Glue

## Table of Contents
1. [Architecture Overview](#architecture-overview)
2. [Technology Stack & Versions](#technology-stack--versions)
3. [Migration Flow](#migration-flow)
4. [Detailed Component Setup](#detailed-component-setup)
5. [AWS Glue Catalog Integration](#aws-glue-catalog-integration)
6. [Error Prevention & Best Practices](#error-prevention--best-practices)
7. [MSSQL Migration Guide](#mssql-migration-guide)
8. [Troubleshooting](#troubleshooting)

---

## Architecture Overview

### High-Level Architecture
```
┌─────────────┐      ┌──────────────┐      ┌─────────────┐      ┌─────────────┐
│   MySQL     │─────▶│Apache Iceberg│─────▶│  AWS Glue   │─────▶│ StarRocks   │
│  (Source)   │      │  (S3 Layer)  │      │  Catalog    │      │(Destination)│
└─────────────┘      └──────────────┘      └─────────────┘      └─────────────┘
      │                      │                      │                    │
      │                      │                      │                    │
    JDBC              Apache Spark            Metadata Store         JDBC Write
   Extract            Processing                Registry           Bulk Insert
```

### Data Flow Pipeline

1. **Extraction Phase**: MySQL → Spark DataFrame (JDBC)
2. **Transformation Phase**: Spark DataFrame → Lowercase identifiers + Audit columns
3. **Iceberg Write Phase**: Spark → Iceberg Tables → S3 Storage
4. **Catalog Registration**: Iceberg metadata → AWS Glue Data Catalog
5. **StarRocks Load Phase**: Iceberg → Spark → StarRocks (JDBC Bulk Insert)

---

## Technology Stack & Versions

### Core Components

| Component | Version | Size | Purpose | Download Link |
|-----------|---------|------|---------|---------------|
| **Apache Spark** | 3.5.0 with Hadoop 3 | - | Distributed data processing engine | [spark-3.5.0-bin-hadoop3](https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz) |
| **Java JDK** | 11 (OpenJDK) | - | Spark runtime requirement | `sudo apt install openjdk-11-jdk` |
| **Python** | 3.10+ | - | PySpark & FastAPI application | - |

### Required JAR Dependencies

All JARs are located in: `/home/voltus-wave/starrocks/Starrocks pipeline/jars/`

#### 1. Iceberg Core Runtime
```
iceberg-spark-runtime-3.5_2.12-1.7.1.jar
Size: 41 MB
Date: Nov 22, 2024
Purpose: Iceberg table format support for Spark 3.5 with Scala 2.12
Download: https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.7.1/iceberg-spark-runtime-3.5_2.12-1.7.1.jar
Maven Coordinates: org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.1
```

#### 2. AWS Glue Catalog Integration
```
iceberg-aws-bundle-1.7.1.jar
Size: 48 MB
Date: Nov 22, 2024
Purpose: AWS Glue Catalog implementation + S3FileIO + AWS SDK dependencies
Download: https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.7.1/iceberg-aws-bundle-1.7.1.jar
Maven Coordinates: org.apache.iceberg:iceberg-aws-bundle:1.7.1
Key Classes:
  - org.apache.iceberg.aws.glue.GlueCatalog
  - org.apache.iceberg.aws.s3.S3FileIO
```

#### 3. AWS SDK
```
aws-java-sdk-bundle-1.12.648.jar
Size: 353 MB
Date: Jan 31, 2024
Purpose: AWS S3, Glue, IAM service clients
Download: https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.648/aws-java-sdk-bundle-1.12.648.jar
Maven Coordinates: com.amazonaws:aws-java-sdk-bundle:1.12.648
```

#### 4. Hadoop AWS Integration
```
hadoop-aws-3.3.4.jar
Size: 941 KB
Date: Jul 29, 2022
Purpose: Hadoop S3A filesystem implementation
Download: https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar

hadoop-common-3.3.4.jar
Size: 4.3 MB
Date: Jul 29, 2022
Purpose: Hadoop core utilities
Download: https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.4/hadoop-common-3.3.4.jar
```

#### 5. MySQL JDBC Driver
```
mysql-connector-j-8.2.0.jar
Size: 2.4 MB
Date: Oct 25, 2023
Purpose: MySQL JDBC connectivity
Download: https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.2.0/mysql-connector-j-8.2.0.jar
Maven Coordinates: com.mysql:mysql-connector-j:8.2.0
```

### Version Compatibility Matrix

| Spark Version | Iceberg Runtime | AWS Bundle | Scala Version | Status |
|---------------|----------------|------------|---------------|--------|
| 3.5.0 | 1.7.1 | 1.7.1 | 2.12 | ✅ Recommended |
| 3.5.0 | 1.4.3 | 1.4.3 | 2.12 | ⚠️ Legacy (Glue issues) |
| 3.5.0 | 1.9.2 | 1.9.2 | 2.12 | ✅ Latest (untested) |

**Important**: Always match `iceberg-spark-runtime` and `iceberg-aws-bundle` versions!

---

## Migration Flow

### Phase 1: MySQL Extraction

```python
# File: app/services/spark_pipeline.py
# Lines: 250-330

def extract_mysql_table_to_iceberg(schema_name, table_name, target_schema_name):
    """
    Extract table from MySQL to Iceberg via Spark JDBC
    """

    # Step 1: Get optimal partition configuration
    partition_config = optimizer.get_optimal_partition_config(
        schema_name, table_name
    )

    # Step 2: Read from MySQL using parallel JDBC
    df = (
        spark.read.format("jdbc")
        .option("url", mysql_jdbc_url)
        .option("dbtable", f"`{schema_name}`.`{table_name}`")
        .option("user", mysql_user)
        .option("password", mysql_password)
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("fetchsize", 50000)  # Optimized batch size
        .option("partitionColumn", partition_column)
        .option("numPartitions", num_partitions)
        .option("lowerBound", lower_bound)
        .option("upperBound", upper_bound)
        .load()
    )

    # Step 3: Add audit columns
    df = add_audit_columns(df, schema_name)

    # Step 4: Cache for performance
    df = df.cache()
    row_count = df.count()
```

**Key Configuration Parameters**:
- `fetchsize`: 50,000 rows per batch (reduces network roundtrips)
- `numPartitions`: 20 parallel readers (configurable based on table size)
- `partitionColumn`: Intelligent auto-detection (timestamp > numeric > primary key)
- `characterEncoding`: UTF-8 with emoji support (utf8mb4)

### Phase 2: Iceberg Write (AWS Glue Catalog)

```python
# File: app/services/spark_pipeline.py
# Lines: 339-365

def write_to_iceberg_glue(df, schema_name, table_name):
    """
    Write DataFrame to Iceberg with AWS Glue catalog registration
    """

    # Step 1: AWS Glue naming compliance (CRITICAL!)
    glue_schema_name = schema_name.lower()  # Database must be lowercase
    table_name_lower = table_name.lower()   # Table must be lowercase

    # Step 2: Convert column names to lowercase (Glue requirement)
    from pyspark.sql.functions import col
    df = df.select([col(c).alias(c.lower()) for c in df.columns])

    # Step 3: Create namespace if not exists
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS spark_catalog.{glue_schema_name}")

    # Step 4: Write using DataFrame API
    full_table_name = f"{glue_schema_name}.{table_name_lower}"
    df.writeTo(full_table_name).using("iceberg").createOrReplace()

    # Result: Table stored in S3 + Metadata in Glue Catalog
```

**Iceberg Table Structure**:
```
s3://bucket/warehouse/
  └── database_name/
      └── table_name/
          ├── data/
          │   ├── 00000-0-data-file-1.parquet
          │   ├── 00001-0-data-file-2.parquet
          │   └── ...
          └── metadata/
              ├── v1.metadata.json
              ├── v2.metadata.json
              └── snap-12345678.avro
```

### Phase 3: AWS Glue Catalog Registration

**Automatic Registration via Iceberg GlueCatalog**:

```python
# Spark Configuration (app/services/spark_pipeline.py: 90-110)
spark = SparkSession.builder \
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.iceberg.spark.SparkSessionCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "glue") \
    .config("spark.sql.catalog.spark_catalog.warehouse",
            "s3a://bucket/warehouse") \
    .config("spark.sql.catalog.spark_catalog.glue.region",
            "ap-south-1") \
    .getOrCreate()
```

**What Gets Registered**:
1. **Database**: `c1s1_billing_crm_dev_1_s4jnkrdr_dev_glue`
2. **Table**: `api_rate_fetch_log` (lowercase)
3. **Schema**: Column names, types, partitions
4. **Location**: `s3://bucket/warehouse/database/table/`
5. **Table Format**: `ICEBERG`

**Glue Console View**:
```
AWS Glue > Data Catalog > Databases > c1s1_billing_crm_dev_1_s4jnkrdr_dev_glue
  └── Tables
      ├── api_rate_fetch_log (27 rows)
      ├── customer_details (15,432 rows)
      └── ... (678 tables total)
```

### Phase 4: StarRocks Load

```python
# File: app/services/spark_pipeline.py
# Lines: 607-720

def load_iceberg_to_starrocks(source_schema, target_schema, table_name):
    """
    Load data from Iceberg to StarRocks
    """

    # Step 1: Create StarRocks table (preserves original MySQL casing)
    schema_mapper.create_starrocks_table(
        source_schema_name=source_schema,  # MySQL schema (for metadata)
        target_schema_name=target_schema,  # StarRocks schema
        table_name=table_name              # Original casing: API_RATE_FETCH_LOG
    )

    # Step 2: Read from Iceberg (using lowercase names)
    glue_schema_name = target_schema.lower()
    table_name_lower = table_name.lower()
    iceberg_table = f"{glue_schema_name}.{table_name_lower}"
    df = spark.table(iceberg_table)

    # Step 3: Map lowercase Iceberg columns back to original MySQL casing
    # (StarRocks uses MySQL schema, which has original case)
    column_mapping = get_mysql_column_mapping(source_schema, table_name)
    df = restore_column_casing(df, column_mapping)

    # Step 4: Bulk insert to StarRocks via JDBC
    df.write \
        .format("jdbc") \
        .option("url", starrocks_jdbc_url) \
        .option("dbtable", f"`{target_schema}`.`{table_name}`") \
        .option("user", starrocks_user) \
        .option("password", starrocks_password) \
        .option("batchsize", 500000) \
        .option("numPartitions", 8) \
        .mode("append") \
        .save()
```

**Performance Optimizations**:
- Batch size: 500,000 rows (balanced for memory & throughput)
- Parallel writes: 8 partitions (matches shuffle partitions)
- Mode: `append` (idempotent for retries)

---

## Detailed Component Setup

### 1. Environment Configuration

**File**: `.env`


### 2. Spark Session Configuration

**File**: `app/services/spark_pipeline.py`

```python
def get_spark_session() -> SparkSession:
    """
    Create Spark session with AWS Glue catalog integration
    """

    # Set AWS credentials as environment variables
    os.environ["AWS_ACCESS_KEY_ID"] = settings.aws_access_key_id
    os.environ["AWS_SECRET_ACCESS_KEY"] = settings.aws_secret_access_key
    os.environ["AWS_REGION"] = settings.aws_region

    # JAR files path
    jars_dir = os.path.join(os.path.dirname(__file__), "../../jars")
    jar_files = [
        "iceberg-spark-runtime-3.5_2.12-1.7.1.jar",
        "iceberg-aws-bundle-1.7.1.jar",
        "aws-java-sdk-bundle-1.12.648.jar",
        "hadoop-aws-3.3.4.jar",
        "hadoop-common-3.3.4.jar",
        "mysql-connector-j-8.2.0.jar",
    ]
    jars_path = ",".join([os.path.join(jars_dir, jar) for jar in jar_files])

    spark = (
        SparkSession.builder
        .appName(settings.spark_app_name)
        .master(settings.spark_master)
        .config("spark.jars", jars_path)

        # Iceberg extensions
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")

        # AWS Glue Catalog configuration
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.iceberg.spark.SparkSessionCatalog")
        .config("spark.sql.catalog.spark_catalog.type", "glue")
        .config("spark.sql.catalog.spark_catalog.warehouse",
                settings.iceberg_warehouse_path)
        .config("spark.sql.catalog.spark_catalog.glue.region",
                settings.aws_region)

        # S3 configurations
        .config("spark.hadoop.fs.s3a.access.key", settings.aws_access_key_id)
        .config("spark.hadoop.fs.s3a.secret.key", settings.aws_secret_access_key)
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{settings.aws_region}.amazonaws.com")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

        # AWS credentials provider chain
        .config("spark.hadoop.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

        # Performance optimizations
        .config("spark.driver.memory", settings.spark_driver_memory)
        .config("spark.executor.memory", settings.spark_executor_memory)
        .config("spark.executor.cores", settings.spark_executor_cores)
        .config("spark.sql.shuffle.partitions", "8")

        # Iceberg write optimizations
        .config("spark.sql.iceberg.compression-codec", "snappy")
        .config("spark.sql.parquet.compression.codec", "snappy")

        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark
```

### 3. Schema Mapping (MySQL ↔ StarRocks)

**File**: `app/services/schema_mapper.py`

**Type Mapping Table**:

| MySQL Type | StarRocks Type | Notes |
|------------|----------------|-------|
| `TINYINT(1)` | `BOOLEAN` | Single-bit boolean |
| `TINYINT` | `TINYINT` | -128 to 127 |
| `SMALLINT` | `SMALLINT` | -32,768 to 32,767 |
| `INT` | `INT` | -2B to 2B |
| `BIGINT` | `BIGINT` | Large integers |
| `DECIMAL(p,s)` | `DECIMAL(p,s)` | Preserves precision |
| `FLOAT` | `FLOAT` | 32-bit floating point |
| `DOUBLE` | `DOUBLE` | 64-bit floating point |
| `CHAR(n)` | `CHAR(n)` | Fixed length |
| `VARCHAR(n)` | `VARCHAR(n)` | Variable length (max 65,535) |
| `TEXT` | `STRING` | Unlimited text |
| `DATE` | `DATE` | Date only |
| `DATETIME` | `DATETIME` | Date + time |
| `TIMESTAMP` | `DATETIME` | Converted to DATETIME |
| `JSON` | `JSON` | Native JSON support |

### 4. Intelligent Partitioning

**File**: `app/services/partition_optimizer.py`

**Partition Strategy**:

```python
def get_optimal_partition_config(schema_name, table_name):
    """
    Auto-detect optimal partition column and calculate bounds
    """

    # Priority 1: Timestamp columns (best for time-series data)
    timestamp_columns = ["createdAt", "updatedAt", "created_at", "updated_at"]

    # Priority 2: Numeric columns (id, customer_id, etc.)
    numeric_columns = get_numeric_columns(schema_name, table_name)

    # Priority 3: Primary key
    primary_key = get_primary_key(schema_name, table_name)

    # Calculate min/max for partition bounds
    partition_column = select_best_column(timestamp_columns, numeric_columns, primary_key)
    lower_bound, upper_bound = get_min_max(schema_name, table_name, partition_column)

    # Calculate optimal partition count (based on row count)
    row_count = get_row_count(schema_name, table_name)
    num_partitions = calculate_partitions(row_count)

    return {
        "column": partition_column,
        "lowerBound": lower_bound,
        "upperBound": upper_bound,
        "numPartitions": num_partitions
    }
```

**Partition Count Formula**:
```
if row_count < 10,000:       num_partitions = 1
elif row_count < 100,000:    num_partitions = 4
elif row_count < 1,000,000:  num_partitions = 8
elif row_count < 10,000,000: num_partitions = 16
else:                        num_partitions = 20
```

---

## AWS Glue Catalog Integration

### Why AWS Glue Catalog?

| Feature | Hadoop Catalog | AWS Glue Catalog |
|---------|----------------|------------------|
| **Metadata Storage** | File-based (JSON in S3) | Managed service (DynamoDB) |
| **Multi-engine Access** | Spark only | Spark, Athena, EMR, Redshift Spectrum |
| **Concurrency** | File-based locks | Optimistic locking |
| **Schema Evolution** | Manual | Automatic versioning |
| **Production Grade** | ⚠️ Not recommended | ✅ AWS managed |
| **Cost** | Free (just S3) | $1 per 100K requests |

### Glue Catalog Configuration

**Critical Requirements**:

1. **Lowercase Naming** (MANDATORY):
   ```python
   # ❌ WRONG - Will fail with "Invalid table identifier"
   database = "c1s1_billing_crm_DEV_1_s4JNKRDR_dev_glue"
   table = "API_RATE_FETCH_LOG"

   # ✅ CORRECT - Glue requires lowercase
   database = "c1s1_billing_crm_dev_1_s4jnkrdr_dev_glue"
   table = "api_rate_fetch_log"
   ```

2. **Allowed Characters**: `[a-z0-9_]` only (lowercase letters, numbers, underscore)

3. **Length Limits**:
   - Database name: 1-255 characters (practical limit: ~40-50 for Iceberg)
   - Table name: 1-255 characters
   - Column name: 1-255 characters

### Glue Permissions (IAM)

**Required IAM Policy**:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "glue:CreateDatabase",
        "glue:GetDatabase",
        "glue:GetDatabases",
        "glue:UpdateDatabase",
        "glue:DeleteDatabase",
        "glue:CreateTable",
        "glue:GetTable",
        "glue:GetTables",
        "glue:UpdateTable",
        "glue:DeleteTable",
        "glue:BatchCreatePartition",
        "glue:BatchDeletePartition",
        "glue:BatchGetPartition",
        "glue:GetPartition",
        "glue:GetPartitions",
        "glue:UpdatePartition"
      ],
      "Resource": [
        "arn:aws:glue:ap-south-1:*:catalog",
        "arn:aws:glue:ap-south-1:*:database/*",
        "arn:aws:glue:ap-south-1:*:table/*/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::vishnu-starrockspipeline-jan5-2026/*",
        "arn:aws:s3:::vishnu-starrockspipeline-jan5-2026"
      ]
    }
  ]
}
```

### Querying via AWS Athena

Once tables are in Glue Catalog, you can query via Athena:

```sql
-- Query Iceberg table via Athena
SELECT *
FROM c1s1_billing_crm_dev_1_s4jnkrdr_dev_glue.api_rate_fetch_log
WHERE createdat >= TIMESTAMP '2025-01-01 00:00:00'
LIMIT 100;

-- Check table metadata
SHOW CREATE TABLE c1s1_billing_crm_dev_1_s4jnkrdr_dev_glue.api_rate_fetch_log;

-- List all tables
SHOW TABLES IN c1s1_billing_crm_dev_1_s4jnkrdr_dev_glue;
```

---

## Error Prevention & Best Practices

### Critical Error Categories

#### 1. JAR Dependency Errors

**Error**: `Unknown catalog type: glue`
```
Cause: Missing or wrong version of iceberg-aws-bundle.jar
Solution: Use iceberg-aws-bundle-1.7.1.jar (minimum 1.6.0)
```

**Error**: `ClassNotFoundException: org.apache.iceberg.aws.glue.GlueCatalog`
```
Cause: iceberg-aws-bundle.jar not in classpath
Solution: Add to spark.jars config and verify file exists
```

**Error**: `IllegalArgumentException: Cannot create catalog, both type and catalog-impl are set`
```
Cause: Conflicting Glue catalog configuration
Solution: Use ONLY "type=glue", remove "catalog-impl" config
```

#### 2. AWS Glue Naming Errors

**Error**: `Invalid table identifier: database.TABLE_NAME`
```
Cause: Uppercase letters in table name
Solution: Convert all identifiers to lowercase
```

**Error**: `Cannot convert namespace MyDB to Glue database name`
```
Cause: Uppercase letters in database name
Solution: database_name.lower() before creating namespace
```

#### 3. AWS Credentials Errors

**Error**: `Unable to load credentials from any provider`
```
Cause: AWS credentials not accessible to Spark/Iceberg
Solution: Set AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION as environment variables
```

**Error**: `Unable to load region from any provider`
```
Cause: AWS region not configured
Solution: Set AWS_REGION environment variable
```

#### 4. S3 Access Errors

**Error**: `403 Forbidden - Access Denied`
```
Cause: IAM permissions insufficient for S3 bucket
Solution: Add s3:GetObject, s3:PutObject, s3:ListBucket permissions
```

**Error**: `NoSuchBucket: The specified bucket does not exist`
```
Cause: S3 bucket name incorrect or wrong region
Solution: Verify bucket exists in correct region
```

### Best Practices Checklist

#### ✅ Before Starting Migration

- [ ] Verify all JAR files exist and match versions (1.7.1 for Iceberg)
- [ ] Test AWS credentials: `aws s3 ls s3://bucket-name/`
- [ ] Confirm Glue permissions: `aws glue get-databases`
- [ ] Check S3 bucket region matches `AWS_REGION` config
- [ ] Validate MySQL connectivity: `mysql -h host -u user -p`
- [ ] Validate StarRocks connectivity: `mysql -h host -P 9030 -u user -p`
- [ ] Run single-table test before full migration

#### ✅ During Migration

- [ ] Monitor Spark UI (http://localhost:4040) for job progress
- [ ] Check CloudWatch metrics for S3 PUT/GET requests
- [ ] Verify Glue Catalog tables appear in AWS Console
- [ ] Monitor StarRocks memory usage (`SHOW PROC '/backends'`)
- [ ] Check pipeline logs for warnings/errors
- [ ] Validate row counts: MySQL = Iceberg = StarRocks

#### ✅ After Migration

- [ ] Run data validation queries (checksums, row counts)
- [ ] Verify Glue Catalog metadata accuracy
- [ ] Test Athena queries on Iceberg tables
- [ ] Document any custom schema mappings
- [ ] Create reconciliation report
- [ ] Archive migration logs

### Performance Tuning Guide

**Memory Configuration**:
```bash
# For 16 GB RAM system
SPARK_DRIVER_MEMORY=8g
SPARK_EXECUTOR_MEMORY=8g

# For 32 GB RAM system
SPARK_DRIVER_MEMORY=16g
SPARK_EXECUTOR_MEMORY=16g

# For 64 GB RAM system
SPARK_DRIVER_MEMORY=24g
SPARK_EXECUTOR_MEMORY=24g
```

**Partition Tuning**:
```bash
# Small datasets (<100K rows/table)
JDBC_NUM_PARTITIONS=4
SPARK_SQL_SHUFFLE_PARTITIONS=4

# Medium datasets (100K-1M rows/table)
JDBC_NUM_PARTITIONS=8
SPARK_SQL_SHUFFLE_PARTITIONS=8

# Large datasets (>1M rows/table)
JDBC_NUM_PARTITIONS=20
SPARK_SQL_SHUFFLE_PARTITIONS=16
```

**Batch Size Optimization**:
```bash
# MySQL Extract
JDBC_FETCH_SIZE=50000  # Balance: memory vs network

# StarRocks Load
BATCH_SIZE=500000      # Large batches for bulk insert
```

---

## MSSQL Migration Guide

### Replacing MySQL with MSSQL

To migrate from **Microsoft SQL Server (MSSQL)** instead of MySQL, follow these changes:

### 1. JDBC Driver Replacement

**Remove**:
```
mysql-connector-j-8.2.0.jar
```

**Add**:
```
mssql-jdbc-12.4.2.jre11.jar
Size: 1.1 MB
Download: https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/12.4.2.jre11/mssql-jdbc-12.4.2.jre11.jar
Maven Coordinates: com.microsoft.sqlserver:mssql-jdbc:12.4.2.jre11
```

### 2. Configuration Changes

**File**: `.env`

```bash
# MSSQL Source Database
MSSQL_HOST=your-mssql-server.database.windows.net
MSSQL_PORT=1433
MSSQL_USER=admin
MSSQL_PASSWORD=YourPassword123!
MSSQL_DATABASE=your_database
MSSQL_SCHEMA=dbo  # Default schema in MSSQL
MSSQL_ENCRYPT=true
MSSQL_TRUST_SERVER_CERTIFICATE=false
```

### 3. JDBC URL Format

**MySQL**:
```python
jdbc_url = f"jdbc:mysql://{host}:{port}/{database}?useSSL=false&allowPublicKeyRetrieval=true"
driver = "com.mysql.cj.jdbc.Driver"
```

**MSSQL**:
```python
jdbc_url = f"jdbc:sqlserver://{host}:{port};databaseName={database};encrypt=true;trustServerCertificate=false"
driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
```

### 4. Schema Mapping Differences

**Type Mapping Table: MSSQL → StarRocks**:

| MSSQL Type | StarRocks Type | Notes |
|------------|----------------|-------|
| `BIT` | `BOOLEAN` | Boolean |
| `TINYINT` | `TINYINT` | Unsigned in MSSQL (0-255) |
| `SMALLINT` | `SMALLINT` | Signed |
| `INT` | `INT` | Signed |
| `BIGINT` | `BIGINT` | Signed |
| `DECIMAL(p,s)` | `DECIMAL(p,s)` | Max p=38 in both |
| `NUMERIC(p,s)` | `DECIMAL(p,s)` | Same as DECIMAL |
| `FLOAT(24)` | `FLOAT` | 32-bit |
| `FLOAT(53)` | `DOUBLE` | 64-bit |
| `REAL` | `FLOAT` | 32-bit |
| `CHAR(n)` | `CHAR(n)` | Fixed length |
| `NCHAR(n)` | `CHAR(n)` | Unicode, convert to UTF-8 |
| `VARCHAR(n)` | `VARCHAR(n)` | Variable length |
| `NVARCHAR(n)` | `VARCHAR(n)` | Unicode, convert to UTF-8 |
| `VARCHAR(MAX)` | `STRING` | Unlimited |
| `NVARCHAR(MAX)` | `STRING` | Unlimited |
| `TEXT` | `STRING` | Legacy, use VARCHAR(MAX) |
| `NTEXT` | `STRING` | Legacy, use NVARCHAR(MAX) |
| `DATE` | `DATE` | Date only |
| `DATETIME` | `DATETIME` | Precision: ~3ms |
| `DATETIME2` | `DATETIME` | Precision: 100ns → truncate |
| `SMALLDATETIME` | `DATETIME` | Precision: 1min |
| `DATETIMEOFFSET` | `VARCHAR(34)` | Store as ISO 8601 string |
| `TIME` | `VARCHAR(16)` | No TIME type in StarRocks |
| `UNIQUEIDENTIFIER` | `VARCHAR(36)` | GUIDs as strings |
| `BINARY(n)` | `VARBINARY` | Binary data |
| `VARBINARY(n)` | `VARBINARY` | Variable binary |
| `XML` | `STRING` | Store as string |
| `GEOGRAPHY` | `STRING` | WKT format |
| `GEOMETRY` | `STRING` | WKT format |

### 5. Partitioning Adjustments

**MSSQL-specific considerations**:

```python
# MSSQL partition columns (priority order)
timestamp_columns = [
    "CreatedDate",      # Common in MSSQL
    "ModifiedDate",
    "InsertedDateTime",
    "UpdatedDateTime"
]

# MSSQL identity columns for partitioning
identity_columns = get_identity_columns(schema, table)

# Partition strategy
if identity_column_exists:
    partition_column = identity_column
elif timestamp_column_exists:
    partition_column = timestamp_column
else:
    partition_column = clustered_index_first_column
```

### 6. Code Changes Required

**File**: `app/services/database.py` (new file: `mssql_manager.py`)

```python
import pymssql  # or pyodbc
from typing import List, Dict

class MSSQLManager:
    """MSSQL database manager"""

    def get_connection(self):
        """Create MSSQL connection"""
        return pymssql.connect(
            server=self.settings.mssql_host,
            port=self.settings.mssql_port,
            user=self.settings.mssql_user,
            password=self.settings.mssql_password,
            database=self.settings.mssql_database,
            charset='UTF-8',
            as_dict=True
        )

    def get_tables_in_schema(self, schema_name: str) -> List[str]:
        """Get all tables in schema"""
        query = """
            SELECT TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = %s
            AND TABLE_TYPE = 'BASE TABLE'
            ORDER BY TABLE_NAME
        """
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, (schema_name,))
                return [row['TABLE_NAME'] for row in cursor.fetchall()]

    def get_table_schema(self, schema_name: str, table_name: str) -> List[Dict]:
        """Get column metadata"""
        query = """
            SELECT
                COLUMN_NAME,
                DATA_TYPE,
                CHARACTER_MAXIMUM_LENGTH,
                NUMERIC_PRECISION,
                NUMERIC_SCALE,
                IS_NULLABLE,
                COLUMN_DEFAULT,
                COLUMNPROPERTY(OBJECT_ID(TABLE_SCHEMA + '.' + TABLE_NAME),
                               COLUMN_NAME, 'IsIdentity') AS IS_IDENTITY
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
            ORDER BY ORDINAL_POSITION
        """
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, (schema_name, table_name))
                return cursor.fetchall()

    def get_primary_key(self, schema_name: str, table_name: str) -> str:
        """Get primary key column"""
        query = """
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = %s
            AND TABLE_NAME = %s
            AND CONSTRAINT_NAME LIKE 'PK_%'
            ORDER BY ORDINAL_POSITION
        """
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, (schema_name, table_name))
                result = cursor.fetchone()
                return result['COLUMN_NAME'] if result else None
```

**File**: `app/services/spark_pipeline.py`

```python
def extract_mssql_table_to_iceberg(schema_name, table_name, target_schema_name):
    """Extract from MSSQL to Iceberg"""

    # MSSQL JDBC read
    df = (
        spark.read.format("jdbc")
        .option("url", mssql_jdbc_url)
        .option("dbtable", f"[{schema_name}].[{table_name}]")  # MSSQL bracket syntax
        .option("user", mssql_user)
        .option("password", mssql_password)
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .option("fetchsize", 50000)
        .option("partitionColumn", partition_column)
        .option("numPartitions", num_partitions)
        .option("lowerBound", lower_bound)
        .option("upperBound", upper_bound)
        # MSSQL-specific options
        .option("encrypt", "true")
        .option("trustServerCertificate", "false")
        .load()
    )

    # Rest of the code remains the same (Iceberg write, Glue catalog)
```

### 7. MSSQL-Specific Precautions

#### Handle IDENTITY Columns
```python
# IDENTITY columns can't be partitioned in Spark JDBC
# Use another column for partitioning
if column_is_identity(partition_column):
    partition_column = find_alternative_partition_column()
```

#### Unicode Handling (NCHAR/NVARCHAR)
```python
# MSSQL uses UTF-16 for NCHAR/NVARCHAR
# Spark reads as UTF-8 automatically
# Verify emoji/special characters during validation
df_validated = validate_unicode_characters(df)
```

#### DATETIMEOFFSET Timezone Handling
```python
# Convert DATETIMEOFFSET to UTC before storing
from pyspark.sql.functions import to_utc_timestamp

df = df.withColumn(
    "created_at_utc",
    to_utc_timestamp("CreatedDate", "UTC")
)
```

#### Large Object Types (LOB)
```python
# For VARBINARY(MAX), XML, large TEXT columns
# Increase fetch size and reduce partition count
.option("fetchsize", 10000)  # Smaller batches
.option("numPartitions", 4)  # Fewer partitions
```

#### Schema Qualification
```sql
-- MSSQL requires schema qualification
-- MySQL: `database`.`table`
-- MSSQL: [database].[schema].[table]

-- Example: dbo schema
jdbc_url = "jdbc:sqlserver://host:1433;databaseName=mydb"
dbtable = "[dbo].[customers]"
```

### 8. Python Dependencies

**Add to requirements.txt**:
```
# For MSSQL connectivity (choose one)
pymssql==2.2.11           # FreeTDS-based (Linux-friendly)
pyodbc==5.0.1             # ODBC-based (requires ODBC driver)
```

**Install ODBC Driver (if using pyodbc)**:
```bash
# Ubuntu/Debian
curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -
curl https://packages.microsoft.com/config/ubuntu/22.04/prod.list | sudo tee /etc/apt/sources.list.d/mssql-release.list
sudo apt-get update
sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18 unixodbc-dev
```

### 9. Testing MSSQL Connection

```python
# Test script: test_mssql_connection.py
import pymssql

conn = pymssql.connect(
    server='your-server.database.windows.net',
    port=1433,
    user='admin',
    password='Password123!',
    database='your_database'
)

cursor = conn.cursor()
cursor.execute("SELECT @@VERSION")
row = cursor.fetchone()
print(f"MSSQL Version: {row[0]}")

cursor.execute("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES")
row = cursor.fetchone()
print(f"Total tables: {row[0]}")

conn.close()
```

---

## Troubleshooting

### Common Issues & Solutions

#### Issue 1: Slow JDBC Reads

**Symptoms**: Tables with 1M+ rows take >10 minutes to extract

**Diagnosis**:
```python
# Check Spark UI (http://localhost:4040)
# Look for:
# - Task skew (some tasks much slower than others)
# - Network time vs compute time
# - Number of tasks (should match numPartitions)
```

**Solution**:
```bash
# Increase partitions for large tables
JDBC_NUM_PARTITIONS=40

# Increase fetch size (more memory, fewer network trips)
JDBC_FETCH_SIZE=100000

# Use better partition column (timestamp > id)
# Verify column has good distribution
```

#### Issue 2: Out of Memory Errors

**Symptoms**: `java.lang.OutOfMemoryError: Java heap space`

**Solution**:
```bash
# Increase driver memory
SPARK_DRIVER_MEMORY=16g

# Reduce batch size
BATCH_SIZE=250000

# Reduce shuffle partitions
SPARK_SQL_SHUFFLE_PARTITIONS=4

# Enable off-heap memory
spark.memory.offHeap.enabled=true
spark.memory.offHeap.size=4g
```

#### Issue 3: Glue Catalog Throttling

**Symptoms**: `ThrottlingException: Rate exceeded`

**Solution**:
```python
# Add retry logic with exponential backoff
from tenacity import retry, stop_after_attempt, wait_exponential

@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=30)
)
def create_glue_table(...):
    # Glue API calls
    pass

# Reduce concurrency
MAX_CONCURRENT_TABLE_WRITES=5
```

#### Issue 4: S3 403 Forbidden

**Symptoms**: `AmazonS3Exception: Access Denied (Service: Amazon S3; Status Code: 403)`

**Diagnosis**:
```bash
# Test S3 access manually
aws s3 ls s3://your-bucket/warehouse/
aws s3 cp test.txt s3://your-bucket/warehouse/test.txt
```

**Solution**:
```json
{
  "Effect": "Allow",
  "Action": [
    "s3:GetObject",
    "s3:PutObject",
    "s3:DeleteObject",
    "s3:ListBucket"
  ],
  "Resource": [
    "arn:aws:s3:::your-bucket/*",
    "arn:aws:s3:::your-bucket"
  ]
}
```

#### Issue 5: StarRocks Connection Refused

**Symptoms**: `pymysql.err.OperationalError: (2003, "Can't connect to MySQL server")`

**Solution**:
```bash
# Check network connectivity
telnet starrocks-host 9030

# Check StarRocks is running
mysql -h starrocks-host -P 9030 -u root -p

# Verify firewall rules allow port 9030
sudo ufw allow 9030/tcp

# Check StarRocks logs
tail -f /opt/starrocks/fe/log/fe.log
```

### Debug Logging

**Enable verbose logging**:
```python
# File: app/core/logging_config.py
LOG_LEVEL=DEBUG

# Spark logging
spark.sparkContext.setLogLevel("DEBUG")

# Python logging
import logging
logging.basicConfig(level=logging.DEBUG)
```

### Performance Monitoring

**Key Metrics to Track**:
```python
# Tables per hour
tables_processed = 678
time_elapsed = 4.5  # hours
throughput = tables_processed / time_elapsed  # 150 tables/hour

# Rows per second
rows_processed = 15_000_000
time_seconds = 3600
throughput = rows_processed / time_seconds  # 4,166 rows/sec

# Bytes per second (S3 writes)
bytes_written = 5 * 1024**3  # 5 GB
throughput = bytes_written / time_seconds  # 1.4 MB/sec
```

---

## Summary Checklist

### Pre-Migration
- ✅ All JAR files downloaded and verified (iceberg-spark-runtime, iceberg-aws-bundle)
- ✅ AWS credentials configured (access key, secret key, region)
- ✅ S3 bucket created and permissions granted
- ✅ Glue catalog permissions configured
- ✅ MySQL connection tested
- ✅ StarRocks connection tested
- ✅ Spark environment variables set

### During Migration
- ✅ Monitor pipeline logs for errors
- ✅ Check Glue Catalog for table creation
- ✅ Verify S3 files being written
- ✅ Monitor memory usage (Spark UI)
- ✅ Track progress (tables completed vs total)

### Post-Migration
- ✅ Run data reconciliation (MySQL vs Iceberg vs StarRocks)
- ✅ Verify row counts match across all systems
- ✅ Test Athena queries on Glue tables
- ✅ Validate data types in StarRocks
- ✅ Check for NULL values and data quality
- ✅ Archive logs and create migration report

---

## References

### Official Documentation
- [Apache Iceberg](https://iceberg.apache.org/docs/latest/)
- [Iceberg AWS Integration](https://iceberg.apache.org/docs/latest/aws/)
- [AWS Glue Data Catalog](https://docs.aws.amazon.com/glue/latest/dg/catalog-and-crawler.html)
- [Apache Spark JDBC](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html)
- [StarRocks Documentation](https://docs.starrocks.io/)

### GitHub Issues & Solutions
- [Iceberg Glue Catalog Type Error](https://github.com/apache/iceberg/issues/10078)
- [AWS Credentials Provider Chain](https://github.com/apache/iceberg/issues/12185)

### Maven Repository
- [Iceberg Releases](https://repo1.maven.org/maven2/org/apache/iceberg/)
- [AWS SDK](https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/)
- [MySQL Connector](https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/)
- [MSSQL Connector](https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/)

---

**Document Version**: 1.0
**Last Updated**: 2026-01-07
**Author**: Migration Team
**Contact**: Pipeline Support
