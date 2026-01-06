"""
Data validation service for ensuring data integrity and zero data loss.

Provides comprehensive validation, reconciliation, and metrics tracking.
"""

from typing import Dict, Any, Optional, Tuple
import pymysql
from pymysql.cursors import DictCursor

from app.core import get_logger, get_settings
from app.utils.exceptions import DataValidationError

logger = get_logger(__name__)


class DataValidator:
    """Validates data integrity across MySQL, Iceberg, and StarRocks."""

    def __init__(self):
        """Initialize data validator."""
        self.settings = get_settings()

    def get_mysql_row_count(
        self, schema_name: str, table_name: str
    ) -> int:
        """
        Get exact row count from MySQL source table.

        Args:
            schema_name: Database schema name
            table_name: Table name

        Returns:
            int: Number of rows in source table
        """
        query = f"SELECT COUNT(*) as row_count FROM `{schema_name}`.`{table_name}`"

        try:
            connection = pymysql.connect(
                host=self.settings.mysql_host,
                port=self.settings.mysql_port,
                user=self.settings.mysql_user,
                password=self.settings.mysql_password,
                database=schema_name,
                charset=self.settings.mysql_charset,
                cursorclass=DictCursor,
            )

            with connection.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchone()

            connection.close()

            row_count = int(result['row_count']) if result else 0
            logger.info(f"MySQL source {schema_name}.{table_name}: {row_count} rows")

            return row_count

        except Exception as e:
            logger.error(f"Failed to get MySQL row count: {e}")
            raise DataValidationError(
                f"Failed to count rows in MySQL {schema_name}.{table_name}: {e}"
            )

    def get_starrocks_row_count(
        self, schema_name: str, table_name: str
    ) -> int:
        """
        Get exact row count from StarRocks target table.

        Args:
            schema_name: Database schema name
            table_name: Table name

        Returns:
            int: Number of rows in target table
        """
        query = f"SELECT COUNT(*) as row_count FROM `{schema_name}`.`{table_name}`"

        try:
            connection = pymysql.connect(
                host=self.settings.starrocks_host,
                port=self.settings.starrocks_port,
                user=self.settings.starrocks_user,
                password=self.settings.starrocks_password,
                database=schema_name,
                cursorclass=DictCursor,
            )

            with connection.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchone()

            connection.close()

            row_count = int(result['row_count']) if result else 0
            logger.info(f"StarRocks target {schema_name}.{table_name}: {row_count} rows")

            return row_count

        except Exception as e:
            logger.error(f"Failed to get StarRocks row count: {e}")
            raise DataValidationError(
                f"Failed to count rows in StarRocks {schema_name}.{table_name}: {e}"
            )

    def validate_extraction(
        self,
        schema_name: str,
        table_name: str,
        source_count: int,
        extracted_count: int,
    ) -> Dict[str, Any]:
        """
        Validate that extraction from MySQL to Iceberg was successful.

        Args:
            schema_name: Schema name
            table_name: Table name
            source_count: Row count from MySQL source
            extracted_count: Row count extracted to Iceberg

        Returns:
            Dict with validation results

        Raises:
            DataValidationError: If row counts don't match
        """
        validation_result = {
            "stage": "extraction",
            "schema_name": schema_name,
            "table_name": table_name,
            "source_rows": source_count,
            "extracted_rows": extracted_count,
            "match": source_count == extracted_count,
            "data_loss": source_count - extracted_count,
        }

        if source_count != extracted_count:
            error_msg = (
                f"DATA LOSS DETECTED in extraction: {schema_name}.{table_name} - "
                f"Source: {source_count} rows, Extracted: {extracted_count} rows, "
                f"Loss: {source_count - extracted_count} rows"
            )
            logger.error(error_msg)
            validation_result["error"] = error_msg
            raise DataValidationError(error_msg, details=validation_result)

        logger.info(
            f"Extraction validation PASSED: {schema_name}.{table_name} - "
            f"{extracted_count} rows matched"
        )

        return validation_result

    def validate_load(
        self,
        schema_name: str,
        table_name: str,
        iceberg_count: int,
        loaded_count: int,
    ) -> Dict[str, Any]:
        """
        Validate that load from Iceberg to StarRocks was successful.

        Args:
            schema_name: Schema name
            table_name: Table name
            iceberg_count: Row count from Iceberg
            loaded_count: Row count loaded to StarRocks

        Returns:
            Dict with validation results

        Raises:
            DataValidationError: If row counts don't match
        """
        validation_result = {
            "stage": "load",
            "schema_name": schema_name,
            "table_name": table_name,
            "iceberg_rows": iceberg_count,
            "loaded_rows": loaded_count,
            "match": iceberg_count == loaded_count,
            "data_loss": iceberg_count - loaded_count,
        }

        if iceberg_count != loaded_count:
            error_msg = (
                f"DATA LOSS DETECTED in load: {schema_name}.{table_name} - "
                f"Iceberg: {iceberg_count} rows, Loaded: {loaded_count} rows, "
                f"Loss: {iceberg_count - loaded_count} rows"
            )
            logger.error(error_msg)
            validation_result["error"] = error_msg
            raise DataValidationError(error_msg, details=validation_result)

        logger.info(
            f"Load validation PASSED: {schema_name}.{table_name} - "
            f"{loaded_count} rows matched"
        )

        return validation_result

    def validate_end_to_end(
        self,
        schema_name: str,
        table_name: str,
    ) -> Dict[str, Any]:
        """
        Perform end-to-end validation from MySQL source to StarRocks target.

        Args:
            schema_name: Schema name
            table_name: Table name

        Returns:
            Dict with comprehensive validation metrics

        Raises:
            DataValidationError: If validation fails
        """
        logger.info(
            f"Starting end-to-end validation: {schema_name}.{table_name}"
        )

        # Get actual row counts from both systems
        mysql_count = self.get_mysql_row_count(schema_name, table_name)
        starrocks_count = self.get_starrocks_row_count(schema_name, table_name)

        validation_result = {
            "stage": "end_to_end",
            "schema_name": schema_name,
            "table_name": table_name,
            "mysql_rows": mysql_count,
            "starrocks_rows": starrocks_count,
            "match": mysql_count == starrocks_count,
            "data_loss": mysql_count - starrocks_count,
            "accuracy_percentage": (
                (starrocks_count / mysql_count * 100) if mysql_count > 0 else 100.0
            ),
        }

        if mysql_count != starrocks_count:
            error_msg = (
                f"END-TO-END VALIDATION FAILED: {schema_name}.{table_name} - "
                f"MySQL: {mysql_count} rows, StarRocks: {starrocks_count} rows, "
                f"Difference: {mysql_count - starrocks_count} rows "
                f"({validation_result['accuracy_percentage']:.2f}% accuracy)"
            )
            logger.error(error_msg)
            validation_result["error"] = error_msg
            raise DataValidationError(error_msg, details=validation_result)

        logger.info(
            f"End-to-end validation PASSED: {schema_name}.{table_name} - "
            f"{starrocks_count} rows matched (100% accuracy)"
        )

        return validation_result

    def get_comprehensive_metrics(
        self,
        schema_name: str,
        table_name: str,
        extraction_result: Dict[str, Any],
        load_result: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Generate comprehensive metrics for a table migration.

        Args:
            schema_name: Schema name
            table_name: Table name
            extraction_result: Results from extraction phase
            load_result: Results from load phase

        Returns:
            Dict with detailed metrics and validation
        """
        # Get current counts from all systems
        mysql_count = self.get_mysql_row_count(schema_name, table_name)
        starrocks_count = self.get_starrocks_row_count(schema_name, table_name)

        metrics = {
            "schema_name": schema_name,
            "table_name": table_name,
            "row_counts": {
                "mysql_source": mysql_count,
                "iceberg_extracted": extraction_result.get("row_count", 0),
                "starrocks_loaded": starrocks_count,
            },
            "validation": {
                "extraction_match": (
                    mysql_count == extraction_result.get("row_count", 0)
                ),
                "load_match": (
                    extraction_result.get("row_count", 0) == load_result.get("row_count", 0)
                ),
                "end_to_end_match": mysql_count == starrocks_count,
                "accuracy_percentage": (
                    (starrocks_count / mysql_count * 100) if mysql_count > 0 else 100.0
                ),
            },
            "performance": {
                "extraction_duration_seconds": extraction_result.get("duration_seconds", 0),
                "load_duration_seconds": load_result.get("duration_seconds", 0),
                "total_duration_seconds": (
                    extraction_result.get("duration_seconds", 0)
                    + load_result.get("duration_seconds", 0)
                ),
                "rows_per_second": (
                    mysql_count
                    / (
                        extraction_result.get("duration_seconds", 0)
                        + load_result.get("duration_seconds", 0)
                    )
                    if (extraction_result.get("duration_seconds", 0) + load_result.get("duration_seconds", 0)) > 0
                    else 0
                ),
            },
            "data_integrity": {
                "zero_data_loss": mysql_count == starrocks_count,
                "rows_lost": mysql_count - starrocks_count,
            },
        }

        logger.info(
            f"Metrics for {schema_name}.{table_name}: "
            f"{mysql_count} rows, {metrics['performance']['rows_per_second']:.2f} rows/sec, "
            f"{metrics['validation']['accuracy_percentage']:.2f}% accuracy"
        )

        return metrics
