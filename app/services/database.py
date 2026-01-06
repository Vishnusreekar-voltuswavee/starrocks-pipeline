"""
Database connection and management services.

Handles connections to MySQL (source) and StarRocks (target) databases.
"""

from typing import List, Dict, Any, Optional
from contextlib import contextmanager

import pymysql
from pymysql.cursors import DictCursor

from app.core import get_logger, get_settings
from app.utils.exceptions import DatabaseConnectionError, SchemaNotFoundError

logger = get_logger(__name__)


class MySQLManager:
    """Manages MySQL source database connections and operations."""

    SYSTEM_SCHEMAS = {
        "mysql",
        "information_schema",
        "performance_schema",
        "sys",
    }

    def __init__(self):
        """Initialize MySQL manager with settings."""
        self.settings = get_settings()

    @contextmanager
    def get_connection(self, schema: Optional[str] = None):
        """
        Get a MySQL database connection.

        Args:
            schema: Optional schema name to connect to

        Yields:
            pymysql.Connection: Database connection

        Raises:
            DatabaseConnectionError: If connection fails
        """
        connection = None
        try:
            connection = pymysql.connect(
                host=self.settings.mysql_host,
                port=self.settings.mysql_port,
                user=self.settings.mysql_user,
                password=self.settings.mysql_password,
                database=schema or self.settings.mysql_database,
                charset=self.settings.mysql_charset,
                cursorclass=DictCursor,
            )
            logger.info(
                f"Connected to MySQL: {self.settings.mysql_host}:"
                f"{self.settings.mysql_port}"
            )
            yield connection
        except pymysql.Error as e:
            logger.error(f"MySQL connection error: {e}")
            raise DatabaseConnectionError(
                f"Failed to connect to MySQL: {e}",
                details={"host": self.settings.mysql_host},
            )
        finally:
            if connection:
                connection.close()
                logger.debug("MySQL connection closed")

    def get_all_schemas(
        self,
        include_system: bool = False,
        exclude: Optional[List[str]] = None,
    ) -> List[str]:
        """
        Get list of all schemas from MySQL.

        Args:
            include_system: Include system schemas
            exclude: List of schemas to exclude

        Returns:
            List[str]: List of schema names

        Raises:
            DatabaseConnectionError: If query fails
        """
        exclude = exclude or []
        query = "SHOW DATABASES"

        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    results = cursor.fetchall()

            schemas = [row["Database"] for row in results]

            # Filter system schemas if needed
            if not include_system:
                schemas = [
                    s for s in schemas if s not in self.SYSTEM_SCHEMAS
                ]

            # Filter excluded schemas
            schemas = [s for s in schemas if s not in exclude]

            logger.info(f"Found {len(schemas)} schemas in MySQL")
            return sorted(schemas)

        except Exception as e:
            logger.error(f"Failed to fetch schemas: {e}")
            raise DatabaseConnectionError(
                f"Failed to fetch schemas from MySQL: {e}"
            )

    def schema_exists(self, schema_name: str) -> bool:
        """
        Check if a schema exists in MySQL.

        Args:
            schema_name: Name of the schema

        Returns:
            bool: True if schema exists
        """
        try:
            schemas = self.get_all_schemas(include_system=True)
            return schema_name in schemas
        except Exception as e:
            logger.error(f"Failed to check schema existence: {e}")
            return False

    def get_schema_metadata(self, schema_name: str) -> Dict[str, Any]:
        """
        Get metadata for a specific schema.

        Args:
            schema_name: Name of the schema

        Returns:
            Dict[str, Any]: Schema metadata including table count, size, etc.

        Raises:
            SchemaNotFoundError: If schema doesn't exist
        """
        if not self.schema_exists(schema_name):
            raise SchemaNotFoundError(
                f"Schema '{schema_name}' not found in MySQL"
            )

        query = """
            SELECT
                COUNT(*) as table_count,
                COALESCE(SUM(table_rows), 0) as total_rows,
                COALESCE(SUM(data_length + index_length), 0) as total_size_bytes
            FROM information_schema.tables
            WHERE table_schema = %s
        """

        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, (schema_name,))
                    result = cursor.fetchone()

            if not result:
                return {
                    "schema_name": schema_name,
                    "table_count": 0,
                    "total_rows": 0,
                    "total_size_bytes": 0,
                }

            return {
                "schema_name": schema_name,
                "table_count": int(result["table_count"]) if result["table_count"] else 0,
                "total_rows": int(result["total_rows"]) if result["total_rows"] else 0,
                "total_size_bytes": int(result["total_size_bytes"]) if result["total_size_bytes"] else 0,
            }

        except Exception as e:
            logger.error(f"Failed to fetch schema metadata: {e}")
            raise DatabaseConnectionError(
                f"Failed to fetch metadata for schema '{schema_name}': {e}"
            )

    def get_tables_in_schema(self, schema_name: str) -> List[str]:
        """
        Get list of tables in a schema.

        Args:
            schema_name: Name of the schema

        Returns:
            List[str]: List of table names

        Raises:
            SchemaNotFoundError: If schema doesn't exist
        """
        if not self.schema_exists(schema_name):
            raise SchemaNotFoundError(
                f"Schema '{schema_name}' not found in MySQL"
            )

        query = """
            SELECT TABLE_NAME
            FROM information_schema.tables
            WHERE TABLE_SCHEMA = %s
            AND TABLE_TYPE = 'BASE TABLE'
            ORDER BY TABLE_NAME
        """

        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, (schema_name,))
                    results = cursor.fetchall()

            tables = [row["TABLE_NAME"] for row in results]
            logger.info(
                f"Found {len(tables)} tables in schema '{schema_name}'"
            )
            return tables

        except Exception as e:
            logger.error(f"Failed to fetch tables: {e}")
            raise DatabaseConnectionError(
                f"Failed to fetch tables from schema '{schema_name}': {e}"
            )


class StarRocksManager:
    """Manages StarRocks target database connections and operations."""

    def __init__(self):
        """Initialize StarRocks manager with settings."""
        self.settings = get_settings()

    @contextmanager
    def get_connection(self, database: Optional[str] = None):
        """
        Get a StarRocks database connection.

        Args:
            database: Optional database name to connect to

        Yields:
            pymysql.Connection: Database connection

        Raises:
            DatabaseConnectionError: If connection fails
        """
        connection = None
        try:
            connection = pymysql.connect(
                host=self.settings.starrocks_host,
                port=self.settings.starrocks_port,
                user=self.settings.starrocks_user,
                password=self.settings.starrocks_password,
                database=database or self.settings.starrocks_database,
                cursorclass=DictCursor,
            )
            logger.info(
                f"Connected to StarRocks: {self.settings.starrocks_host}:"
                f"{self.settings.starrocks_port}"
            )
            yield connection
        except pymysql.Error as e:
            logger.error(f"StarRocks connection error: {e}")
            raise DatabaseConnectionError(
                f"Failed to connect to StarRocks: {e}",
                details={"host": self.settings.starrocks_host},
            )
        finally:
            if connection:
                connection.close()
                logger.debug("StarRocks connection closed")

    def database_exists(self, database_name: str) -> bool:
        """
        Check if a database exists in StarRocks.

        Args:
            database_name: Name of the database

        Returns:
            bool: True if database exists
        """
        query = "SHOW DATABASES"

        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    results = cursor.fetchall()

            databases = [row["Database"] for row in results]
            exists = database_name in databases
            logger.debug(
                f"Database '{database_name}' exists in StarRocks: {exists}"
            )
            return exists

        except Exception as e:
            logger.error(f"Failed to check database existence: {e}")
            return False

    def table_exists(self, database_name: str, table_name: str) -> bool:
        """
        Check if a table exists in StarRocks database.

        Args:
            database_name: Name of the database
            table_name: Name of the table

        Returns:
            bool: True if table exists
        """
        query = f"SHOW TABLES FROM `{database_name}` LIKE %s"

        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, (table_name,))
                    result = cursor.fetchone()

            exists = result is not None
            logger.debug(
                f"Table '{database_name}.{table_name}' exists in StarRocks: {exists}"
            )
            return exists

        except Exception as e:
            logger.error(f"Failed to check table existence: {e}")
            return False

    def create_database(self, database_name: str) -> None:
        """
        Create a database in StarRocks.

        Args:
            database_name: Name of the database to create

        Raises:
            DatabaseConnectionError: If creation fails
        """
        query = f"CREATE DATABASE IF NOT EXISTS `{database_name}`"

        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    conn.commit()

            logger.info(f"Database '{database_name}' created in StarRocks")

        except Exception as e:
            logger.error(f"Failed to create database: {e}")
            raise DatabaseConnectionError(
                f"Failed to create database '{database_name}' in StarRocks: {e}"
            )

    def get_all_databases(self) -> List[str]:
        """
        Get list of all databases from StarRocks.

        Returns:
            List[str]: List of database names

        Raises:
            DatabaseConnectionError: If query fails
        """
        query = "SHOW DATABASES"

        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    results = cursor.fetchall()

            databases = [row["Database"] for row in results]
            logger.info(f"Found {len(databases)} databases in StarRocks")
            return sorted(databases)

        except Exception as e:
            logger.error(f"Failed to fetch databases: {e}")
            raise DatabaseConnectionError(
                f"Failed to fetch databases from StarRocks: {e}"
            )
