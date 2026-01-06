#!/usr/bin/env python3
"""
Test script for EXCHANGE_RATE_HISTORY write performance.
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
    """Test EXCHANGE_RATE_HISTORY write to StarRocks."""

    schema_name = "c1s1_billing_crm_DEV_1_s4JNKRDR_dev"
    table_name = "EXCHANGE_RATE_HISTORY"

    logger.info("="*80)
    logger.info(f"Testing optimized JDBC write for {schema_name}.{table_name}")
    logger.info("="*80)

    spark_manager = SparkPipelineManager()

    try:
        # Load from Iceberg to StarRocks
        result = spark_manager.load_iceberg_to_starrocks(
            schema_name=schema_name,
            table_name=table_name,
        )

        logger.info("="*80)
        logger.info("TEST RESULTS:")
        logger.info("="*80)
        logger.info(f"Status: {result['status']}")
        logger.info(f"Iceberg rows: {result['iceberg_rows']}")
        logger.info(f"StarRocks rows: {result['starrocks_rows']}")
        logger.info(f"Total duration: {result['duration_seconds']:.2f}s")
        logger.info(f"Write duration: {result['write_duration']:.2f}s")
        logger.info(f"Parallel partitions: {result['num_partitions']}")

        write_speed = result['starrocks_rows'] / result['write_duration']
        logger.info(f"Write speed: {write_speed:.0f} rows/sec")
        logger.info(f"Data integrity: {result['data_integrity']}")
        logger.info("="*80)

        # Compare with OLD performance
        old_jdbc_time = 202.31  # EXCHANGE_RATE: 1,988 rows in 202s
        old_jdbc_speed = 1988 / 202.31  # ~9.8 rows/sec
        speedup = write_speed / old_jdbc_speed

        logger.info("PERFORMANCE COMPARISON:")
        logger.info(f"OLD JDBC (single thread): ~{old_jdbc_speed:.1f} rows/sec")
        logger.info(f"NEW JDBC (parallel):       {write_speed:.0f} rows/sec")
        logger.info(f"Speed improvement:         {speedup:.1f}x faster")
        logger.info("="*80)

        return 0

    except Exception as e:
        logger.error(f"Test failed: {e}", exc_info=True)
        return 1

    finally:
        spark_manager.stop_spark_session()

if __name__ == "__main__":
    sys.exit(main())
