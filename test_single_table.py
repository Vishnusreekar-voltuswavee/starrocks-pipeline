#!/usr/bin/env python3
"""
Single table test: EXCHANGE_RATE_HISTORY
Measures write time and accuracy.
"""

import sys
import os
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import findspark

SPARK_HOME = os.getenv('SPARK_HOME', '/home/voltus-wave/starrocks/spark_install/spark-3.5.0-bin-hadoop3')
JAVA_HOME = os.getenv('JAVA_HOME', '/usr/lib/jvm/java-11-openjdk-amd64')

os.environ['JAVA_HOME'] = JAVA_HOME
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

findspark.init(spark_home=SPARK_HOME)

from app.core import setup_logging, get_logger, get_settings
from pyspark.sql import SparkSession
import pymysql

setup_logging()
logger = get_logger(__name__)

def main():
    schema_name = "c1s1_billing_crm_DEV_1_s4JNKRDR_dev"
    table_name = "EXCHANGE_RATE_HISTORY"

    settings = get_settings()

    print("="*80)
    print(f"TESTING: {schema_name}.{table_name}")
    print("="*80)

    try:
        # Create Spark
        logger.info("Creating Spark session...")

        jars_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "jars")
        jar_files = [
            os.path.join(jars_dir, "iceberg-spark-runtime-3.5_2.12-1.4.3.jar"),
            os.path.join(jars_dir, "aws-java-sdk-bundle-1.12.648.jar"),
            os.path.join(jars_dir, "hadoop-aws-3.3.4.jar"),
            os.path.join(jars_dir, "hadoop-common-3.3.4.jar"),
            os.path.join(jars_dir, "mysql-connector-j-8.2.0.jar"),
        ]
        jars_path = ",".join([j for j in jar_files if os.path.exists(j)])

        spark = (
            SparkSession.builder
            .appName("SingleTableTest")
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

        # Read from Iceberg
        iceberg_table = f"iceberg_catalog.{schema_name}.{table_name}"
        logger.info(f"Reading from Iceberg: {iceberg_table}")

        df = spark.table(iceberg_table)
        df = df.cache()
        iceberg_rows = df.count()

        print(f"Iceberg rows: {iceberg_rows}")

        # Create/truncate StarRocks table
        logger.info("Ensuring StarRocks table exists...")
        from app.services.schema_mapper import SchemaMapper

        schema_mapper = SchemaMapper()
        try:
            schema_mapper.create_starrocks_table(schema_name, table_name)
            print("Table created/verified")
        except Exception as e:
            logger.warning(f"Table creation warning (may already exist): {e}")

        # Truncate table
        logger.info("Truncating StarRocks table...")
        conn = pymysql.connect(
            host=settings.starrocks_host,
            port=settings.starrocks_port,
            user=settings.starrocks_user,
            password=settings.starrocks_password,
            database=schema_name,
        )
        try:
            with conn.cursor() as cursor:
                cursor.execute(f"TRUNCATE TABLE `{table_name}`")
                conn.commit()
            print("Table truncated")
        except Exception as e:
            logger.warning(f"Truncate failed (table may not exist yet): {e}")
        finally:
            conn.close()

        # Write to StarRocks
        starrocks_url = f"jdbc:mysql://{settings.starrocks_host}:{settings.starrocks_port}/{schema_name}"
        num_partitions = min(8, max(1, iceberg_rows // 10000))

        print(f"Writing {iceberg_rows} rows using {num_partitions} parallel writers...")
        print("Starting write...")

        write_start = time.time()

        df.repartition(num_partitions).write.format("jdbc").options(
            url=starrocks_url,
            dbtable=table_name,
            user=settings.starrocks_user,
            password=settings.starrocks_password,
            driver="com.mysql.cj.jdbc.Driver",
            batchsize="5000",  # StarRocks limit: max 10,000 rows per INSERT
            isolationLevel="NONE",
            truncate="false",
            rewriteBatchedStatements="true",
            cachePrepStmts="true",
            useServerPrepStmts="false",
        ).mode("append").save()

        write_duration = time.time() - write_start

        df.unpersist()

        # Verify accuracy - count rows in StarRocks
        logger.info("Verifying accuracy...")
        conn = pymysql.connect(
            host=settings.starrocks_host,
            port=settings.starrocks_port,
            user=settings.starrocks_user,
            password=settings.starrocks_password,
            database=schema_name,
        )
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) as cnt FROM `{table_name}`")
            result = cursor.fetchone()
            starrocks_rows = result[0]
        conn.close()

        # Results
        print("="*80)
        print("RESULTS:")
        print("="*80)
        print(f"Iceberg rows:    {iceberg_rows}")
        print(f"StarRocks rows:  {starrocks_rows}")
        print(f"Write duration:  {write_duration:.2f}s")
        print(f"Write speed:     {iceberg_rows/write_duration:.0f} rows/sec")
        print(f"Accuracy:        {100 * starrocks_rows / iceberg_rows:.1f}%")
        print(f"Data loss:       {iceberg_rows - starrocks_rows} rows")
        print("="*80)

        # Compare with old performance
        old_speed = 9.8  # 1,988 rows in 202s
        new_speed = iceberg_rows / write_duration
        speedup = new_speed / old_speed

        print("PERFORMANCE COMPARISON:")
        print(f"OLD: {old_speed:.1f} rows/sec")
        print(f"NEW: {new_speed:.0f} rows/sec")
        print(f"IMPROVEMENT: {speedup:.1f}x faster")
        print("="*80)

        spark.stop()

        if starrocks_rows == iceberg_rows:
            print("SUCCESS: 100% accuracy, 0 data loss")
            return 0
        else:
            print(f"WARNING: Data loss detected ({iceberg_rows - starrocks_rows} rows)")
            return 1

    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())
