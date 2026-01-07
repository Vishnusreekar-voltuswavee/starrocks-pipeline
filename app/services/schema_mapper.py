"""
Schema mapping service for MySQL to StarRocks conversion.

Handles accurate datatype mapping and schema translation.
"""

from typing import Dict, List, Tuple, Any
import pymysql
from pymysql.cursors import DictCursor

from app.core import get_logger, get_settings
from app.utils.exceptions import DatabaseConnectionError

logger = get_logger(__name__)


class SchemaMapper:
    """Maps MySQL schemas to StarRocks-compatible schemas."""

    # MySQL to StarRocks datatype mapping
    DATATYPE_MAP = {
        # Integer types
        'TINYINT': 'TINYINT',
        'SMALLINT': 'SMALLINT',
        'MEDIUMINT': 'INT',
        'INT': 'INT',
        'INTEGER': 'INT',
        'BIGINT': 'BIGINT',

        # Decimal types
        'DECIMAL': 'DECIMAL',
        'NUMERIC': 'DECIMAL',
        'FLOAT': 'FLOAT',
        'DOUBLE': 'DOUBLE',
        'REAL': 'DOUBLE',

        # String types
        'CHAR': 'CHAR',
        'VARCHAR': 'VARCHAR',
        'TINYTEXT': 'STRING',
        'TEXT': 'STRING',
        'MEDIUMTEXT': 'STRING',
        'LONGTEXT': 'STRING',
        'BINARY': 'VARBINARY',
        'VARBINARY': 'VARBINARY',
        'TINYBLOB': 'VARBINARY',
        'BLOB': 'VARBINARY',
        'MEDIUMBLOB': 'VARBINARY',
        'LONGBLOB': 'VARBINARY',

        # Date and time types
        'DATE': 'DATE',
        'DATETIME': 'DATETIME',
        'TIMESTAMP': 'DATETIME',
        'TIME': 'STRING',  # StarRocks doesn't have TIME type
        'YEAR': 'SMALLINT',

        # JSON and other types
        'JSON': 'JSON',
        'ENUM': 'STRING',
        'SET': 'STRING',
        'BIT': 'BOOLEAN',
        'BOOLEAN': 'BOOLEAN',
        'BOOL': 'BOOLEAN',
    }

    def __init__(self):
        """Initialize schema mapper."""
        self.settings = get_settings()

    def get_mysql_table_schema(
        self, schema_name: str, table_name: str
    ) -> List[Dict[str, Any]]:
        """
        Get complete table schema from MySQL.

        Args:
            schema_name: Database schema name
            table_name: Table name

        Returns:
            List of column definitions with datatype, nullable, etc.
        """
        query = """
            SELECT
                COLUMN_NAME,
                DATA_TYPE,
                COLUMN_TYPE,
                IS_NULLABLE,
                COLUMN_DEFAULT,
                CHARACTER_MAXIMUM_LENGTH,
                NUMERIC_PRECISION,
                NUMERIC_SCALE,
                COLUMN_KEY,
                EXTRA
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = %s
            AND TABLE_NAME = %s
            ORDER BY ORDINAL_POSITION
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

            with connection.cursor() as cursor:
                cursor.execute(query, (schema_name, table_name))
                columns = cursor.fetchall()

            connection.close()

            logger.info(
                f"Retrieved schema for {schema_name}.{table_name}: "
                f"{len(columns)} columns"
            )

            return columns

        except Exception as e:
            logger.error(f"Failed to get table schema: {e}")
            raise DatabaseConnectionError(
                f"Failed to get schema for {schema_name}.{table_name}: {e}"
            )

    def map_mysql_to_starrocks_type(
        self, column_info: Dict[str, Any]
    ) -> str:
        """
        Map MySQL column type to StarRocks type.

        Args:
            column_info: Column information from MySQL

        Returns:
            StarRocks datatype string
        """
        data_type = column_info['DATA_TYPE'].upper()
        column_type = column_info['COLUMN_TYPE'].upper()

        # Handle DECIMAL with precision and scale
        if data_type == 'DECIMAL' or data_type == 'NUMERIC':
            precision = column_info.get('NUMERIC_PRECISION', 10)
            scale = column_info.get('NUMERIC_SCALE', 0)
            # StarRocks DECIMAL max precision is 38
            if precision > 38:
                logger.warning(f"Column {column_info['COLUMN_NAME']}: DECIMAL({precision},{scale}) exceeds StarRocks limit, capping to DECIMAL(38,{min(scale, 38)})")
                precision = 38
                scale = min(scale, 38)
            return f"DECIMAL({precision},{scale})"

        # Handle VARCHAR/CHAR with length
        if data_type in ('VARCHAR', 'CHAR'):
            length = column_info.get('CHARACTER_MAXIMUM_LENGTH')
            if length:
                # StarRocks VARCHAR max is 65533
                length = min(int(length), 65533)
                return f"{data_type}({length})"
            else:
                return 'STRING'

        # Handle TINYINT(1) as BOOLEAN
        if data_type == 'TINYINT' and '(1)' in column_type:
            return 'BOOLEAN'

        # Handle UNSIGNED integers
        if 'UNSIGNED' in column_type:
            if data_type == 'TINYINT':
                return 'SMALLINT'  # TINYINT UNSIGNED needs SMALLINT
            elif data_type == 'SMALLINT':
                return 'INT'
            elif data_type == 'INT' or data_type == 'INTEGER' or data_type == 'MEDIUMINT':
                return 'BIGINT'
            elif data_type == 'BIGINT':
                return 'LARGEINT'  # StarRocks LARGEINT for BIGINT UNSIGNED

        # Default mapping
        mapped_type = self.DATATYPE_MAP.get(data_type, 'STRING')

        logger.debug(
            f"Mapped {column_info['COLUMN_NAME']}: {column_type} -> {mapped_type}"
        )

        return mapped_type

    def generate_starrocks_create_table_sql(
        self, schema_name: str, table_name: str, columns: List[Dict[str, Any]]
    ) -> str:
        """
        Generate StarRocks CREATE TABLE SQL statement.

        Args:
            schema_name: Database name
            table_name: Table name
            columns: List of column definitions from MySQL

        Returns:
            StarRocks CREATE TABLE SQL statement
        """
        column_defs = []

        for col in columns:
            col_name = f"`{col['COLUMN_NAME']}`"
            col_type = self.map_mysql_to_starrocks_type(col)

            # Handle nullable
            nullable = 'NULL' if col['IS_NULLABLE'] == 'YES' else 'NOT NULL'

            # Build column definition
            col_def = f"{col_name} {col_type} {nullable}"

            # Add COMMENT if needed (optional)
            # col_def += f" COMMENT '{col['COLUMN_NAME']}'"

            column_defs.append(col_def)

        # Add audit columns
        column_defs.append("`_ingest_time` DATETIME NULL")
        column_defs.append("`_source_system` VARCHAR(255) NULL")

        columns_sql = ",\n    ".join(column_defs)

        # ALWAYS use first column as distribution key
        # StarRocks requires DUPLICATE KEY to be the first column in schema
        # Using PRIMARY KEY fails if PK is not the first column
        distribution_key = columns[0]['COLUMN_NAME'] if columns else None

        # Generate CREATE TABLE statement
        create_sql = f"""
CREATE TABLE IF NOT EXISTS `{schema_name}`.`{table_name}` (
    {columns_sql}
)
ENGINE = OLAP
DUPLICATE KEY (`{distribution_key}`)
DISTRIBUTED BY HASH(`{distribution_key}`)
PROPERTIES (
    "replication_num" = "1"
)
        """.strip()

        logger.debug(f"Generated CREATE TABLE SQL for {schema_name}.{table_name}")

        return create_sql

    def create_starrocks_table(
        self, source_schema_name: str, target_schema_name: str, table_name: str
    ) -> None:
        """
        Create table in StarRocks based on MySQL schema.

        Args:
            source_schema_name: MySQL source schema name (for reading metadata)
            target_schema_name: StarRocks target schema name (for creating table)
            table_name: Table name

        Raises:
            DatabaseConnectionError: If table creation fails
        """
        try:
            # Get MySQL table schema from SOURCE
            columns = self.get_mysql_table_schema(source_schema_name, table_name)

            if not columns:
                raise DatabaseConnectionError(
                    f"No columns found for {source_schema_name}.{table_name}"
                )

            # Generate CREATE TABLE SQL for TARGET schema
            create_sql = self.generate_starrocks_create_table_sql(
                target_schema_name, table_name, columns
            )

            logger.info(f"Creating table in StarRocks: {target_schema_name}.{table_name} (source: {source_schema_name})")
            logger.debug(f"SQL: {create_sql}")

            # Execute CREATE TABLE
            connection = pymysql.connect(
                host=self.settings.starrocks_host,
                port=self.settings.starrocks_port,
                user=self.settings.starrocks_user,
                password=self.settings.starrocks_password,
                database=target_schema_name,
                cursorclass=DictCursor,
            )

            with connection.cursor() as cursor:
                cursor.execute(create_sql)
                connection.commit()

            connection.close()

            logger.info(
                f"Successfully created table {target_schema_name}.{table_name} in StarRocks"
            )

        except Exception as e:
            logger.error(f"Failed to create StarRocks table: {e}")
            raise DatabaseConnectionError(
                f"Failed to create table {target_schema_name}.{table_name} in StarRocks: {e}"
            )
