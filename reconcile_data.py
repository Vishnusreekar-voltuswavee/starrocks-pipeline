#!/usr/bin/env python3
"""
Data Reconciliation Script

Compares data across MySQL → Iceberg → StarRocks pipeline to identify:
1. Tables missing in StarRocks but exist in Iceberg (quick load)
2. Tables missing in Iceberg but exist in MySQL (full pipeline needed)
3. Tables with row count mismatches (data quality issues)
"""

import os
import sys
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Tuple

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from app.core import get_logger, get_settings
from app.services.database import MySQLManager, StarRocksManager
from pyspark.sql import SparkSession

logger = get_logger(__name__)
settings = get_settings()


class DataReconciliation:
    """Reconcile data across MySQL, Iceberg, and StarRocks."""

    def __init__(self, schema_name: str):
        self.schema_name = schema_name
        self.mysql_manager = MySQLManager()
        self.starrocks_manager = StarRocksManager()
        self.spark = None

    def init_spark(self):
        """Initialize Spark session for Iceberg access."""
        self.spark = (
            SparkSession.builder
            .appName(f"Reconciliation-{self.schema_name}")
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.iceberg_catalog.type", "hadoop")
            .config("spark.sql.catalog.iceberg_catalog.warehouse", settings.iceberg_warehouse_path)
            .config("spark.driver.memory", "2g")
            .config("spark.executor.memory", "2g")
            .getOrCreate()
        )
        self.spark.sparkContext.setLogLevel("WARN")

    def get_mysql_tables(self) -> Dict[str, int]:
        """Get all tables and row counts from MySQL."""
        logger.info(f"Fetching MySQL tables for schema: {self.schema_name}")
        tables = self.mysql_manager.get_tables_in_schema(self.schema_name)

        table_counts = {}
        for table in tables:
            query = f"SELECT COUNT(*) as cnt FROM `{self.schema_name}`.`{table}`"
            try:
                with self.mysql_manager.get_connection() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute(query)
                        result = cursor.fetchone()
                        table_counts[table] = result[0] if result else 0
            except Exception as e:
                logger.error(f"Error counting {table}: {e}")
                table_counts[table] = -1

        return table_counts

    def get_iceberg_tables(self) -> Dict[str, int]:
        """Get all tables and row counts from Iceberg."""
        logger.info(f"Fetching Iceberg tables for schema: {self.schema_name}")

        if not self.spark:
            self.init_spark()

        table_counts = {}
        iceberg_namespace = f"iceberg_catalog.{self.schema_name}"

        try:
            # List all tables in the namespace
            tables_df = self.spark.sql(f"SHOW TABLES IN {iceberg_namespace}")
            tables = [row.tableName for row in tables_df.collect()]

            for table in tables:
                try:
                    count = self.spark.sql(f"SELECT COUNT(*) as cnt FROM {iceberg_namespace}.`{table}`").collect()[0][0]
                    table_counts[table] = count
                except Exception as e:
                    logger.error(f"Error counting Iceberg {table}: {e}")
                    table_counts[table] = -1

        except Exception as e:
            logger.warning(f"Iceberg namespace {iceberg_namespace} not found or empty: {e}")

        return table_counts

    def get_starrocks_tables(self) -> Dict[str, int]:
        """Get all tables and row counts from StarRocks."""
        logger.info(f"Fetching StarRocks tables for schema: {self.schema_name}")

        # Check if database exists
        if not self.starrocks_manager.database_exists(self.schema_name):
            logger.warning(f"StarRocks database {self.schema_name} does not exist")
            return {}

        table_counts = {}
        query = f"SHOW TABLES FROM `{self.schema_name}`"

        try:
            with self.starrocks_manager.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    tables = [row[0] for row in cursor.fetchall()]

                    for table in tables:
                        count_query = f"SELECT COUNT(*) as cnt FROM `{self.schema_name}`.`{table}`"
                        try:
                            cursor.execute(count_query)
                            result = cursor.fetchone()
                            table_counts[table] = result[0] if result else 0
                        except Exception as e:
                            logger.error(f"Error counting StarRocks {table}: {e}")
                            table_counts[table] = -1

        except Exception as e:
            logger.error(f"Error fetching StarRocks tables: {e}")

        return table_counts

    def reconcile(self) -> Dict:
        """Perform full reconciliation across all three systems."""
        logger.info(f"Starting reconciliation for schema: {self.schema_name}")

        # Get data from all sources
        mysql_tables = self.get_mysql_tables()
        iceberg_tables = self.get_iceberg_tables()
        starrocks_tables = self.get_starrocks_tables()

        # Find differences
        all_tables = set(mysql_tables.keys()) | set(iceberg_tables.keys()) | set(starrocks_tables.keys())

        results = {
            'missing_in_iceberg': [],      # MySQL → Iceberg (full extraction needed)
            'missing_in_starrocks': [],    # Iceberg → StarRocks (quick load)
            'complete_match': [],          # All three match perfectly
            'count_mismatch': [],          # Exists everywhere but counts differ
            'only_in_mysql': [],           # Only in MySQL
            'only_in_iceberg': [],         # Only in Iceberg (orphaned)
            'only_in_starrocks': [],       # Only in StarRocks (orphaned)
        }

        for table in sorted(all_tables):
            mysql_count = mysql_tables.get(table)
            iceberg_count = iceberg_tables.get(table)
            starrocks_count = starrocks_tables.get(table)

            # Categorize the table
            if mysql_count is not None and iceberg_count is None and starrocks_count is None:
                results['only_in_mysql'].append((table, mysql_count))
                results['missing_in_iceberg'].append((table, mysql_count))

            elif mysql_count is not None and iceberg_count is not None and starrocks_count is None:
                results['missing_in_starrocks'].append((table, mysql_count, iceberg_count))

            elif mysql_count is None and iceberg_count is not None and starrocks_count is None:
                results['only_in_iceberg'].append((table, iceberg_count))

            elif mysql_count is None and iceberg_count is None and starrocks_count is not None:
                results['only_in_starrocks'].append((table, starrocks_count))

            elif mysql_count is not None and iceberg_count is not None and starrocks_count is not None:
                # All three exist - check if counts match
                if mysql_count == iceberg_count == starrocks_count and mysql_count >= 0:
                    results['complete_match'].append((table, mysql_count))
                else:
                    results['count_mismatch'].append((table, mysql_count, iceberg_count, starrocks_count))

        return results

    def print_report(self, results: Dict):
        """Print detailed reconciliation report."""
        print(f"\n{'='*80}")
        print(f"RECONCILIATION REPORT: {self.schema_name}")
        print(f"{'='*80}\n")

        # Summary
        print("SUMMARY:")
        print(f"  ✅ Complete matches (MySQL=Iceberg=StarRocks): {len(results['complete_match'])}")
        print(f"  ⚠️  Count mismatches: {len(results['count_mismatch'])}")
        print(f"  📥 Missing in Iceberg (need extraction): {len(results['missing_in_iceberg'])}")
        print(f"  📤 Missing in StarRocks (need load): {len(results['missing_in_starrocks'])}")
        print(f"  🔍 Only in MySQL: {len(results['only_in_mysql'])}")
        print(f"  🗑️  Only in Iceberg (orphaned): {len(results['only_in_iceberg'])}")
        print(f"  🗑️  Only in StarRocks (orphaned): {len(results['only_in_starrocks'])}")

        # Details
        if results['missing_in_starrocks']:
            print(f"\n{'='*80}")
            print(f"QUICK WIN: {len(results['missing_in_starrocks'])} tables in Iceberg, ready to load to StarRocks")
            print(f"{'='*80}")
            print(f"{'Table':<50} {'MySQL Rows':>12} {'Iceberg Rows':>12}")
            print(f"{'-'*80}")
            for table, mysql_count, iceberg_count in sorted(results['missing_in_starrocks'])[:20]:
                print(f"{table:<50} {mysql_count:>12,} {iceberg_count:>12,}")
            if len(results['missing_in_starrocks']) > 20:
                print(f"... and {len(results['missing_in_starrocks']) - 20} more")

        if results['missing_in_iceberg']:
            print(f"\n{'='*80}")
            print(f"NEED EXTRACTION: {len(results['missing_in_iceberg'])} tables need MySQL → Iceberg")
            print(f"{'='*80}")
            print(f"{'Table':<50} {'MySQL Rows':>12}")
            print(f"{'-'*80}")
            for table, count in sorted(results['missing_in_iceberg'])[:20]:
                print(f"{table:<50} {count:>12,}")
            if len(results['missing_in_iceberg']) > 20:
                print(f"... and {len(results['missing_in_iceberg']) - 20} more")

        if results['count_mismatch']:
            print(f"\n{'='*80}")
            print(f"DATA QUALITY ISSUES: {len(results['count_mismatch'])} tables with count mismatches")
            print(f"{'='*80}")
            print(f"{'Table':<40} {'MySQL':>10} {'Iceberg':>10} {'StarRocks':>10}")
            print(f"{'-'*80}")
            for table, mysql_count, iceberg_count, starrocks_count in sorted(results['count_mismatch'])[:20]:
                print(f"{table:<40} {mysql_count:>10,} {iceberg_count:>10,} {starrocks_count:>10,}")
            if len(results['count_mismatch']) > 20:
                print(f"... and {len(results['count_mismatch']) - 20} more")

        print(f"\n{'='*80}")
        print("RECOMMENDED ACTIONS:")
        print(f"{'='*80}")

        if results['missing_in_starrocks']:
            print(f"\n1. QUICK WIN - Load {len(results['missing_in_starrocks'])} tables from Iceberg → StarRocks")
            print(f"   Estimated time: ~{len(results['missing_in_starrocks']) * 0.5:.0f} minutes")

        if results['missing_in_iceberg']:
            print(f"\n2. FULL PIPELINE - Extract {len(results['missing_in_iceberg'])} tables from MySQL → Iceberg → StarRocks")
            print(f"   Estimated time: ~{len(results['missing_in_iceberg']) * 2:.0f} minutes")

        if results['count_mismatch']:
            print(f"\n3. DATA VALIDATION - Investigate {len(results['count_mismatch'])} tables with count mismatches")

        print()

    def cleanup(self):
        """Clean up resources."""
        if self.spark:
            self.spark.stop()
        self.mysql_manager.close()
        self.starrocks_manager.close()


def main():
    """Run reconciliation for a specific schema."""
    if len(sys.argv) < 2:
        print("Usage: python reconcile_data.py <schema_name>")
        print("Example: python reconcile_data.py c1s1_billing_crm_DEV_1_s4JNKRDR_dev")
        sys.exit(1)

    schema_name = sys.argv[1]

    reconciler = DataReconciliation(schema_name)

    try:
        results = reconciler.reconcile()
        reconciler.print_report(results)

        # Return exit code based on results
        if results['missing_in_starrocks'] or results['missing_in_iceberg']:
            sys.exit(1)  # Work needed
        else:
            sys.exit(0)  # All complete

    except Exception as e:
        logger.exception(f"Reconciliation failed: {e}")
        sys.exit(2)

    finally:
        reconciler.cleanup()


if __name__ == "__main__":
    main()
