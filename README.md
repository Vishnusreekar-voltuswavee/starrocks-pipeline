# MySQL → Iceberg → StarRocks Pipeline API

Production-grade FastAPI application for ingesting historical data from MySQL into StarRocks using Apache Spark and Apache Iceberg as a staging layer.

## Features

- **Fault-Tolerant**: Iceberg snapshots enable safe restarts without data loss
- **Scalable**: Spark partitioned reads handle millions to billions of rows
- **Safe**: Atomic commits prevent partial data corruption
- **Observable**: Comprehensive logging and status tracking
- **RESTful**: Clean API with automatic Swagger documentation
- **Production-Ready**: Follows PEP 8, Zen of Python, and best practices

## Architecture

```
MySQL (Source) → Spark → Iceberg (S3) → StarRocks (Target)
```

### Pipeline Flow

1. **Extract**: Spark reads from MySQL using partitioned JDBC connections
2. **Stage**: Data written to Iceberg tables in S3 with snapshots
3. **Load**: Iceberg data loaded into StarRocks raw tables

**Schema Preservation**: Source schema names are preserved in Iceberg (S3) and StarRocks.

## Prerequisites

- Python 3.11+
- Apache Spark 3.5+ with Iceberg support
- MySQL 5.7+ or 8.0+
- StarRocks 2.5+
- AWS S3 bucket for Iceberg warehouse
- Java 11+ (for Spark)

## Installation

### 1. Clone the Repository

```bash
cd "/home/voltus-wave/starrocks/Starrocks pipeline"
```

### 2. Create Virtual Environment

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Download Spark Iceberg JARs

Download and place these JARs in your `$SPARK_HOME/jars` directory:

```bash
# Iceberg Spark Runtime
wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.4.3/iceberg-spark-runtime-3.5_2.12-1.4.3.jar

# AWS SDK Bundle
wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.648/aws-java-sdk-bundle-1.12.648.jar

# Hadoop AWS
wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar

# MySQL JDBC Driver
wget https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.2.0/mysql-connector-j-8.2.0.jar
```

### 5. Configure Environment Variables

Copy the example environment file and configure your credentials:

```bash
cp .env.example .env
```

Edit `.env` with your actual configuration:

```bash
# MySQL Source
MYSQL_HOST=your-mysql-host
MYSQL_PORT=3306
MYSQL_USER=your-username
MYSQL_PASSWORD=your-password

# StarRocks Target
STARROCKS_HOST=your-starrocks-host
STARROCKS_PORT=9030
STARROCKS_USER=your-username
STARROCKS_PASSWORD=your-password

# AWS S3
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key
S3_BUCKET=your-iceberg-bucket
ICEBERG_WAREHOUSE_PATH=s3a://your-iceberg-bucket/warehouse
```

## Usage

### Start the API Server

```bash
# Using Python directly
python -m app.main

# Or using uvicorn
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

The API will be available at:
- **API Docs (Swagger)**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc
- **Health Check**: http://localhost:8000/health

## API Endpoints

### 1. Process Specific Schemas

Process one or more schemas by name from MySQL to StarRocks.

**Endpoint**: `POST /api/v1/pipeline/process-schemas`

**Request Body**:
```json
{
  "schema_names": ["customers_db", "orders_db"],
  "force_reprocess": false,
  "skip_validation": false
}
```

**Response**:
```json
{
  "job_id": "123e4567-e89b-12d3-a456-426614174000",
  "status": "success",
  "total_schemas": 2,
  "successful_schemas": 2,
  "failed_schemas": 0,
  "skipped_schemas": 0,
  "results": [
    {
      "schema_name": "customers_db",
      "status": "success",
      "tables_processed": 5,
      "total_tables": 5,
      "rows_transferred": 1000000,
      "iceberg_location": "s3a://bucket/warehouse/customers_db",
      "starrocks_database": "customers_db",
      "processing_time_seconds": 120.5
    }
  ],
  "message": "Successfully processed all 2 schemas"
}
```

**cURL Example**:
```bash
curl -X POST "http://localhost:8000/api/v1/pipeline/process-schemas" \
  -H "Content-Type: application/json" \
  -d '{
    "schema_names": ["test_db"],
    "force_reprocess": false
  }'
```

### 2. Process All Schemas

Automatically discover and process all schemas from MySQL. Only processes schemas that don't already exist in StarRocks.

**Endpoint**: `POST /api/v1/pipeline/process-all-schemas`

**Request Body**:
```json
{
  "exclude_schemas": ["test_db", "temp_db"],
  "include_system_schemas": false,
  "force_reprocess": false
}
```

**Response**:
```json
{
  "job_id": "456e7890-e89b-12d3-a456-426614174000",
  "status": "success",
  "total_schemas": 10,
  "successful_schemas": 7,
  "failed_schemas": 0,
  "skipped_schemas": 3,
  "message": "Successfully processed 7 schemas, skipped 3 existing"
}
```

**cURL Example**:
```bash
curl -X POST "http://localhost:8000/api/v1/pipeline/process-all-schemas" \
  -H "Content-Type: application/json" \
  -d '{
    "exclude_schemas": ["test_db"],
    "include_system_schemas": false,
    "force_reprocess": false
  }'
```

### 3. Health Check

Check application and database connectivity health.

**Endpoint**: `GET /health`

**Response**:
```json
{
  "status": "healthy",
  "app_name": "MySQL-Iceberg-StarRocks Pipeline",
  "app_version": "1.0.0",
  "environment": "production",
  "services": {
    "mysql": "healthy",
    "starrocks": "healthy"
  }
}
```

## Project Structure

```
Starrocks pipeline/
├── app/
│   ├── __init__.py
│   ├── main.py                    # FastAPI application entry point
│   ├── api/
│   │   └── v1/
│   │       ├── endpoints/
│   │       │   └── pipeline.py    # Pipeline endpoints
│   │       └── router.py          # API router configuration
│   ├── core/
│   │   ├── config.py              # Configuration management
│   │   └── logging.py             # Logging configuration
│   ├── models/
│   │   └── schemas.py             # Pydantic models
│   ├── services/
│   │   ├── database.py            # Database managers
│   │   ├── spark_pipeline.py     # Spark job orchestration
│   │   └── pipeline_orchestrator.py  # Main orchestrator
│   └── utils/
│       └── exceptions.py          # Custom exceptions
├── tests/                         # Test suite
├── .env.example                   # Environment template
├── .gitignore                     # Git ignore rules
├── requirements.txt               # Python dependencies
├── pyproject.toml                 # Python project config
└── README.md                      # This file
```

## Configuration

### Environment Variables

All configuration is managed through environment variables in the `.env` file:

| Variable | Description | Default |
|----------|-------------|---------|
| `MYSQL_HOST` | MySQL server host | - |
| `MYSQL_PORT` | MySQL server port | 3306 |
| `MYSQL_USER` | MySQL username | - |
| `MYSQL_PASSWORD` | MySQL password | - |
| `STARROCKS_HOST` | StarRocks server host | - |
| `STARROCKS_PORT` | StarRocks server port | 9030 |
| `STARROCKS_USER` | StarRocks username | - |
| `STARROCKS_PASSWORD` | StarRocks password | - |
| `AWS_ACCESS_KEY_ID` | AWS access key | - |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key | - |
| `S3_BUCKET` | S3 bucket for Iceberg | - |
| `ICEBERG_WAREHOUSE_PATH` | Iceberg warehouse path | - |
| `SPARK_MASTER` | Spark master URL | local[*] |
| `LOG_LEVEL` | Logging level | INFO |

## Key Features

### 1. Schema Preservation

Source schema names are preserved throughout the pipeline:
- **MySQL**: `customers_db`
- **Iceberg (S3)**: `s3://bucket/warehouse/customers_db/`
- **StarRocks**: `customers_db`

### 2. Fault Tolerance

- **Iceberg Snapshots**: Each write creates a snapshot for safe restarts
- **Atomic Commits**: No partial data corruption
- **Retry Logic**: Automatic retries with exponential backoff
- **Validation**: Schema existence checks before processing

### 3. Data Fidelity

Raw data ingestion with zero transformations:
- ✅ Preserve column names, types, precision
- ✅ Add audit columns: `_ingest_time`, `_source_system`
- ❌ No type casting, timezone conversion, or business logic

### 4. Observability

- Comprehensive logging at all pipeline stages
- Health check endpoint for monitoring
- Detailed job status and metrics
- Per-schema and per-table processing results

## Development

### Code Quality

```bash
# Format code
black app/

# Sort imports
isort app/

# Lint code
flake8 app/

# Type checking
mypy app/
```

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=app --cov-report=html

# Run specific test file
pytest tests/test_pipeline.py
```

## Production Deployment

### Using Docker

Create a `Dockerfile`:

```dockerfile
FROM python:3.11-slim

WORKDIR /app

# Install Java for Spark
RUN apt-get update && apt-get install -y openjdk-11-jre-headless

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ ./app/
COPY .env .env

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

Build and run:

```bash
docker build -t mysql-iceberg-starrocks-pipeline .
docker run -p 8000:8000 --env-file .env mysql-iceberg-starrocks-pipeline
```

### Using Kubernetes

Deploy with proper resource limits and health checks:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: pipeline-api
spec:
  replicas: 2
  template:
    spec:
      containers:
      - name: api
        image: mysql-iceberg-starrocks-pipeline:latest
        ports:
        - containerPort: 8000
        envFrom:
        - secretRef:
            name: pipeline-secrets
        livenessProbe:
          httpGet:
            path: /health
            port: 8000
        resources:
          requests:
            memory: "4Gi"
            cpu: "2"
          limits:
            memory: "8Gi"
            cpu: "4"
```

## Troubleshooting

### Common Issues

**1. Spark Session Fails to Start**
- Ensure all required JARs are in `$SPARK_HOME/jars`
- Check Java version (requires Java 11+)
- Verify `SPARK_HOME` environment variable

**2. S3 Connection Errors**
- Verify AWS credentials are correct
- Check S3 bucket permissions
- Ensure bucket region matches `AWS_REGION`

**3. Database Connection Errors**
- Verify database host/port are reachable
- Check firewall rules
- Confirm credentials are correct

**4. Schema Already Exists**
- Use `force_reprocess: true` to override
- Or manually drop the schema from StarRocks first

## Best Practices

1. **Use partitioned reads** for large tables (configure `JDBC_PARTITION_COLUMN`)
2. **Monitor Iceberg snapshots** to manage S3 storage costs
3. **Set appropriate batch sizes** based on your data volume
4. **Use health checks** in production for monitoring
5. **Enable detailed logging** for troubleshooting
6. **Back up Iceberg metadata** regularly
7. **Test with small datasets** before running at scale

## License

MIT License - See LICENSE file for details

## Support

For issues, questions, or contributions:
- Create an issue in the repository
- Check the API documentation at `/docs`
- Review logs for detailed error messages

## References

- [Apache Iceberg Documentation](https://iceberg.apache.org/)
- [Apache Spark Documentation](https://spark.apache.org/)
- [StarRocks Documentation](https://docs.starrocks.io/)
- [FastAPI Documentation](https://fastapi.tiangolo.com/)
