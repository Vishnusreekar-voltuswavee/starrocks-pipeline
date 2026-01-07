"""
Partition optimizer for Spark JDBC reads.

Automatically determines optimal partitioning strategy for each table.
"""

from typing import Dict, Optional, Tuple
import pymysql
from pymysql.cursors import DictCursor

from app.core import get_logger, get_settings

logger = get_logger(__name__)


class PartitionOptimizer:
    """Optimizes Spark partitioning for JDBC reads."""

    def __init__(self):
        """Initialize partition optimizer."""
        self.settings = get_settings()

    def get_optimal_partition_config(
        self, schema_name: str, table_name: str
    ) -> Dict[str, any]:
        """
        Determine optimal partitioning configuration for a table.

        Args:
            schema_name: Database schema name
            table_name: Table name

        Returns:
            Dict with partition_column, num_partitions, lower_bound, upper_bound
        """
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

            # Find best partition column (prefer datetime, then PK, then indexed int)
            partition_col, col_type = self._find_partition_column(
                connection, schema_name, table_name
            )

            if not partition_col:
                logger.info(
                    f"No suitable partition column found for {schema_name}.{table_name}, "
                    "will use non-partitioned read"
                )
                connection.close()
                return {"use_partitioning": False}

            # Get min/max values for partition column
            min_val, max_val, row_count = self._get_partition_bounds(
                connection, schema_name, table_name, partition_col
            )

            connection.close()

            # Calculate optimal number of partitions
            num_partitions = self._calculate_num_partitions(row_count)

            is_datetime = col_type.upper() in ('DATETIME', 'TIMESTAMP', 'DATE')

            logger.info(
                f"Partition config for {schema_name}.{table_name}: "
                f"column={partition_col} (type={col_type}), partitions={num_partitions}, "
                f"range=[{min_val}, {max_val}]{'(Unix timestamps)' if is_datetime else ''}, rows={row_count}"
            )

            return {
                "use_partitioning": True,
                "partition_column": partition_col,
                "column_type": col_type,
                "num_partitions": num_partitions,
                "lower_bound": min_val,
                "upper_bound": max_val,
                "row_count": row_count,
            }

        except Exception as e:
            logger.warning(
                f"Failed to determine partition config for {schema_name}.{table_name}: {e}"
            )
            return {"use_partitioning": False}

    def _find_partition_column(
        self, connection, schema_name: str, table_name: str
    ) -> Tuple[Optional[str], Optional[str]]:
        """
        Find the best column for partitioning.

        Priority:
        1. Primary key (if single column and numeric/datetime)
        2. AUTO_INCREMENT column
        3. Indexed DATETIME/DATE/TIMESTAMP column (createdAt, rateDate, etc.)
        4. Indexed INT/BIGINT column
        5. Any DATETIME/DATE/TIMESTAMP column
        6. Any INT/BIGINT column

        Returns:
            Tuple of (column_name, data_type) or (None, None)
        """
        try:
            with connection.cursor() as cursor:
                # Use case-insensitive matching for DATA_TYPE
                # Exclude TINYINT(1) which is boolean, and exclude BIT/BOOLEAN types
                query = """
                    SELECT
                        COLUMN_NAME,
                        DATA_TYPE,
                        COLUMN_TYPE,
                        COLUMN_KEY,
                        EXTRA
                    FROM information_schema.COLUMNS
                    WHERE TABLE_SCHEMA = %s
                    AND TABLE_NAME = %s
                    AND (
                        LOWER(DATA_TYPE) IN ('int', 'bigint', 'integer', 'mediumint', 'smallint', 'tinyint')
                        OR LOWER(DATA_TYPE) IN ('datetime', 'timestamp', 'date')
                    )
                    AND LOWER(DATA_TYPE) NOT IN ('boolean', 'bit')
                    AND NOT (LOWER(DATA_TYPE) = 'tinyint' AND COLUMN_TYPE LIKE '%%tinyint(1)%%')
                    ORDER BY
                        CASE WHEN COLUMN_KEY = 'PRI' THEN 1
                             WHEN EXTRA LIKE '%%auto_increment%%' THEN 2
                             WHEN COLUMN_KEY = 'MUL' AND LOWER(DATA_TYPE) IN ('datetime', 'timestamp', 'date') THEN 3
                             WHEN COLUMN_KEY = 'MUL' THEN 4
                             WHEN LOWER(DATA_TYPE) IN ('datetime', 'timestamp', 'date') THEN 5
                             ELSE 6
                        END,
                        ORDINAL_POSITION
                    LIMIT 1
                """
                cursor.execute(query, (schema_name, table_name))
                result = cursor.fetchone()

            if result:
                col_name = result['COLUMN_NAME']
                col_type = result['DATA_TYPE']
                logger.debug(
                    f"Selected partition column: {col_name} "
                    f"(type={col_type}, key={result['COLUMN_KEY']})"
                )
                return col_name, col_type

            return None, None

        except Exception as e:
            logger.error(f"Error finding partition column: {e}")
            return None, None

    def _get_partition_bounds(
        self, connection, schema_name: str, table_name: str, column: str
    ) -> Tuple[int, int, int]:
        """
        Get MIN, MAX, and COUNT for partition column.

        Returns:
            Tuple of (min_value, max_value, row_count)
        """
        # First check column type
        type_query = """
            SELECT DATA_TYPE
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_NAME = %s
        """

        try:
            with connection.cursor() as cursor:
                cursor.execute(type_query, (schema_name, table_name, column))
                type_result = cursor.fetchone()

            if not type_result:
                logger.error(f"Column {column} not found")
                return 0, 1, 0

            data_type = type_result['DATA_TYPE'].upper()
            is_datetime = data_type in ('DATETIME', 'TIMESTAMP', 'DATE')

            # Build query based on column type
            if is_datetime:
                # Convert datetime to Unix timestamp for Spark partitioning
                query = """
                    SELECT
                        COALESCE(UNIX_TIMESTAMP(MIN(`%s`)), 0) as min_val,
                        COALESCE(UNIX_TIMESTAMP(MAX(`%s`)), UNIX_TIMESTAMP(NOW())) as max_val,
                        COUNT(*) as row_count
                    FROM `%s`.`%s`
                """ % (column, column, schema_name, table_name)
            else:
                # Numeric column
                query = """
                    SELECT
                        COALESCE(MIN(`%s`), 0) as min_val,
                        COALESCE(MAX(`%s`), 1) as max_val,
                        COUNT(*) as row_count
                    FROM `%s`.`%s`
                """ % (column, column, schema_name, table_name)

            with connection.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchone()

            min_val = int(result['min_val']) if result['min_val'] is not None else 0
            max_val = int(result['max_val']) if result['max_val'] is not None else 1
            row_count = int(result['row_count'])

            # Ensure min < max
            if min_val >= max_val:
                max_val = min_val + 1

            logger.debug(
                f"Partition bounds for {column} (type={data_type}): "
                f"min={min_val}, max={max_val}, rows={row_count}"
            )

            return min_val, max_val, row_count

        except Exception as e:
            logger.error(f"Error getting partition bounds: {e}")
            return 0, 1, 0

    def _calculate_num_partitions(self, row_count: int) -> int:
        """
        Calculate optimal number of partitions based on row count.

        Rules:
        - < 10K rows: 1 partition (no partitioning)
        - 10K - 100K: 4 partitions
        - 100K - 1M: 10 partitions
        - 1M - 10M: 20 partitions
        - > 10M: 50 partitions
        """
        if row_count < 10000:
            return 1
        elif row_count < 100000:
            return 4
        elif row_count < 1000000:
            return 10
        elif row_count < 10000000:
            return 20
        else:
            return 50
