#!/usr/bin/env python3
"""
Test script for EXCHANGE_RATE_HISTORY write - skip table creation, just write.
"""

import sys
import os
import time

# Add the app directory to the path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Initialize Spark
import findspark

SPARK_HOME = os.getenv('SPARK_HOME', '/home/voltus-wave/starrocks/spark_install/spark-3.5.0-bin-hadoop3')
JAVA_HOME = os.getenv('JAVA_HOME', '/usr/lib/jvm/java-11-openjdk-amd64')

os.environ['JAVA_HOME'] = JAVA_HOME
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

findspark.init(spark_home=SPARK_HOME)

from app.core import setup_logging, get_logger, get_settings
from pyspark.sql import SparkSession

# Setup logging
setup_logging()
logger = get_logger(__name__)

def main():
    """Test direct write to StarRocks."""

    schema_name = "c1s1_billing_crm_DEV_1_s4JNKRDR_dev"
    table_name = "EXCHANGE_RATE_HISTORY"

    settings = get_settings()

    logger.info("="*80)
    logger.info(f"Testing DIRECT write (no table creation) for {schema_name}.{table_name}")
    logger.info("="*80)

    try:
        # Create Spark session
        logger.info("Creating Spark session")

        jars_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "jars")
        jar_files = [
            os.path.join(jars_dir, "iceberg-spark-runtime-3.5_2.12-1.4.3.jar"),
            os.path.join(jars_dir, "aws-java-sdk-bundle-1.12.648.jar"),
            os.path.join(jars_dir, "hadoop-aws-3.3.4.jar"),
            os.path.join(jars_dir, "hadoop-common-3.3.4.jar"),
            os.path.join(jars_dir, "mysql-connector-j-8.2.0.jar"),
        ]
        existing_jars = [jar for jar in jar_files if os.path.exists(jar)]
        jars_path = ",".join(existing_jars)

        spark = (
            SparkSession.builder
            .appName("Test-Write-Only")
            .master("local[*]")
            .config("spark.jars", jars_path)
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.iceberg_catalog.catalog-impl", "org.apache.iceberg.hadoop.HadoopCatalog")
            .config("spark.sql.catalog.iceberg_catalog.warehouse", settings.iceberg_warehouse_path)
            .config("spark.hadoop.fs.s3a.access.key", settings.aws_access_key_id)
            .config("spark.hadoop.fs.s3a.secret.key", settings.aws_secret_access_key)
            .config("spark.hadoop.fs.s3a.endpoint", f"s3.{settings.aws_region}.amazonaws.com")
            .getOrCreate()
        )

        logger.info(f"Spark session created: {spark.sparkContext.applicationId}")

        # Read from Iceberg
        iceberg_table_name = f"iceberg_catalog.{schema_name}.{table_name}"
        logger.info(f"Reading from Iceberg: {iceberg_table_name}")

        df = spark.table(iceberg_table_name)
        df = df.cache()
        row_count = df.count()

        logger.info(f"Read {row_count} rows from Iceberg")

        # TRUNCATE the StarRocks table first to clear old data
        logger.info(f"Truncating StarRocks table {schema_name}.{table_name}")
        import pymysql
        connection = pymysql.connect(
            host=settings.starrocks_host,
            port=settings.starrocks_port,
            user=settings.starrocks_user,
            password=settings.starrocks_password,
            database=schema_name,
        )
        with connection.cursor() as cursor:
            cursor.execute(f"TRUNCATE TABLE `{table_name}`")
            connection.commit()
        connection.close()
        logger.info("Table truncated successfully")

        # StarRocks JDBC URL
        starrocks_url = f"jdbc:mysql://{settings.starrocks_host}:{settings.starrocks_port}/{schema_name}"

        # Calculate parallel partitions
        num_partitions = min(8, max(1, row_count // 10000))

        logger.info(f"Writing {row_count} rows using {num_partitions} parallel partitions")
        logger.info("JDBC options:")
        logger.info(f"  - Batch size: 100,000")
        logger.info(f"  - rewriteBatchedStatements: true")
        logger.info(f"  - isolationLevel: NONE")

        write_start = time.time()

        # Repartition and write
        df_repartitioned = df.repartition(num_partitions)

        df_repartitioned.write.format("jdbc").options(
            url=starrocks_url,
            dbtable=table_name,
            user=settings.starrocks_user,
            password=settings.starrocks_password,
            driver="com.mysql.cj.jdbc.Driver",
            batchsize="100000",
            isolationLevel="NONE",
            truncate="false",
            rewriteBatchedStatements="true",
            cachePrepStmts="true",
            useServerPrepStmts="false",
        ).mode("append").save()

        write_duration = time.time() - write_start

        df.unpersist()

        # Calculate metrics
        rows_per_sec = row_count / write_duration

        logger.info("="*80)
        logger.info("WRITE RESULTS:")
        logger.info("="*80)
        logger.info(f"Rows written: {row_count}")
        logger.info(f"Write duration: {write_duration:.2f}s")
        logger.info(f"Write speed: {rows_per_sec:.0f} rows/sec")
        logger.info(f"Parallel partitions: {num_partitions}")
        logger.info("="*80)

        # Compare with old performance
        old_speed = 9.8  # OLD: 1,988 rows in 202s = 9.8 rows/sec
        speedup = rows_per_sec / old_speed

        logger.info("PERFORMANCE COMPARISON:")
        logger.info(f"OLD JDBC (single thread): ~{old_speed:.1f} rows/sec")
        logger.info(f"NEW JDBC (parallel):       {rows_per_sec:.0f} rows/sec")
        logger.info(f"Speed improvement:         {speedup:.1f}x faster")
        logger.info("="*80)

        spark.stop()
        return 0

    except Exception as e:
        logger.error(f"Test failed: {e}", exc_info=True)
        return 1

if __name__ == "__main__":
    sys.exit(main())
