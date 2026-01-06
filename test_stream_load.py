#!/usr/bin/env python3
"""
Test script for StarRocks Stream Load API implementation.

Tests loading EXCHANGE_RATE_HISTORY table from Iceberg to StarRocks.
"""

import sys
import os

# Add the app directory to the path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Initialize Spark using findspark BEFORE any PySpark imports
import findspark

SPARK_HOME = os.getenv('SPARK_HOME', '/home/voltus-wave/starrocks/spark_install/spark-3.5.0-bin-hadoop3')
JAVA_HOME = os.getenv('JAVA_HOME', '/usr/lib/jvm/java-11-openjdk-amd64')

os.environ['JAVA_HOME'] = JAVA_HOME
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

findspark.init(spark_home=SPARK_HOME)

from app.core import setup_logging, get_logger
from app.services.spark_pipeline import SparkPipelineManager

# Setup logging
setup_logging()
logger = get_logger(__name__)

def main():
    """Test Stream Load with EXCHANGE_RATE_HISTORY table."""

    schema_name = "c1s1_billing_crm_DEV_1_s4JNKRDR_dev"
    table_name = "EXCHANGE_RATE_HISTORY"

    logger.info(f"Testing Stream Load API for {schema_name}.{table_name}")

    spark_manager = SparkPipelineManager()

    try:
        # Test loading from Iceberg to StarRocks using Stream Load
        result = spark_manager.load_iceberg_to_starrocks(
            schema_name=schema_name,
            table_name=table_name,
        )

        logger.info("=" * 80)
        logger.info("STREAM LOAD TEST RESULTS:")
        logger.info("=" * 80)
        logger.info(f"Status: {result['status']}")
        logger.info(f"Iceberg rows: {result['iceberg_rows']}")
        logger.info(f"StarRocks rows: {result['starrocks_rows']}")
        logger.info(f"Filtered rows: {result['filtered_rows']}")
        logger.info(f"Total duration: {result['duration_seconds']:.2f}s")
        logger.info(f"Export duration: {result['export_duration']:.2f}s")
        logger.info(f"Load duration: {result['load_duration']:.2f}s")
        logger.info(f"File size: {result['file_size_mb']:.2f} MB")
        logger.info(f"Load speed: {result['starrocks_rows'] / result['load_duration']:.0f} rows/sec")
        logger.info(f"Data integrity: {result['data_integrity']}")
        logger.info("=" * 80)

        # Compare with OLD JDBC performance
        old_jdbc_time = 202.31  # From logs: EXCHANGE_RATE took 202s for 1,988 rows
        old_jdbc_speed = 1988 / 202.31  # ~9.8 rows/sec
        new_speed = result['starrocks_rows'] / result['load_duration']
        speedup = new_speed / old_jdbc_speed

        logger.info(f"OLD JDBC method: ~{old_jdbc_speed:.1f} rows/sec")
        logger.info(f"NEW Stream Load: {new_speed:.0f} rows/sec")
        logger.info(f"Speed improvement: {speedup:.1f}x faster")
        logger.info("=" * 80)

    except Exception as e:
        logger.error(f"Test failed: {e}", exc_info=True)
        return 1

    finally:
        spark_manager.stop_spark_session()

    return 0

if __name__ == "__main__":
    sys.exit(main())
