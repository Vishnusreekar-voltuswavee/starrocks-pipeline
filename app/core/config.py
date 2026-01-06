"""
Configuration management using Pydantic Settings.

This module handles all environment variables and application configuration
following the Twelve-Factor App methodology.
"""

from typing import List, Optional
from functools import lru_cache

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    # Application Configuration
    app_name: str = Field(default="MySQL-Iceberg-StarRocks Pipeline")
    app_version: str = Field(default="1.0.0")
    app_env: str = Field(default="development")
    log_level: str = Field(default="INFO")

    # MySQL Source Database
    mysql_host: str = Field(...)
    mysql_port: int = Field(default=3306)
    mysql_user: str = Field(...)
    mysql_password: str = Field(...)
    mysql_database: str = Field(default="")
    mysql_charset: str = Field(default="utf8mb4")

    # StarRocks Target Database
    starrocks_host: str = Field(...)
    starrocks_port: int = Field(default=9030)
    starrocks_user: str = Field(...)
    starrocks_password: str = Field(...)
    starrocks_database: str = Field(default="default_catalog")

    # AWS S3 Configuration
    aws_access_key_id: str = Field(...)
    aws_secret_access_key: str = Field(...)
    aws_region: str = Field(default="us-east-1")
    s3_bucket: str = Field(...)
    s3_warehouse_path: str = Field(default="warehouse")

    # Apache Spark Configuration
    spark_master: str = Field(default="local[*]")
    spark_app_name: str = Field(default="MySQL-Iceberg-Pipeline")
    spark_driver_memory: str = Field(default="4g")
    spark_executor_memory: str = Field(default="4g")
    spark_executor_cores: int = Field(default=2)

    # Iceberg Configuration
    iceberg_catalog_type: str = Field(default="glue")  # Changed from hadoop to glue
    iceberg_warehouse_path: str = Field(...)

    # AWS Glue Catalog Configuration (optional)
    glue_catalog_id: Optional[str] = Field(default=None)  # AWS Account ID, defaults to current account

    # Schema Naming Configuration
    target_schema_suffix: Optional[str] = Field(default=None)  # Suffix to append to target schema names (e.g., "_glue")

    # JDBC Configuration
    jdbc_fetch_size: int = Field(default=10000)
    jdbc_partition_column: str = Field(default="id")
    jdbc_num_partitions: int = Field(default=10)
    jdbc_lower_bound: int = Field(default=0)
    jdbc_upper_bound: int = Field(default=1000000)

    # Pipeline Configuration
    batch_size: int = Field(default=100000)
    max_retry_attempts: int = Field(default=3)
    retry_delay_seconds: int = Field(default=5)
    process_timeout_seconds: int = Field(default=3600)

    # FastAPI Configuration
    api_host: str = Field(default="0.0.0.0")
    api_port: int = Field(default=8000)
    api_reload: bool = Field(default=True)
    cors_origins: List[str] = Field(default=["http://localhost:3000"])

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        """Validate log level is one of the standard levels."""
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        v_upper = v.upper()
        if v_upper not in valid_levels:
            raise ValueError(f"Log level must be one of {valid_levels}")
        return v_upper

    @property
    def mysql_connection_url(self) -> str:
        """Generate MySQL connection URL."""
        return (
            f"mysql+pymysql://{self.mysql_user}:{self.mysql_password}"
            f"@{self.mysql_host}:{self.mysql_port}/{self.mysql_database}"
            f"?charset={self.mysql_charset}"
        )

    @property
    def mysql_jdbc_url(self) -> str:
        """Generate MySQL JDBC URL for Spark."""
        return (
            f"jdbc:mysql://{self.mysql_host}:{self.mysql_port}/"
            f"{self.mysql_database}?useSSL=false&allowPublicKeyRetrieval=true"
        )

    @property
    def starrocks_jdbc_url(self) -> str:
        """Generate StarRocks JDBC URL."""
        return f"jdbc:mysql://{self.starrocks_host}:{self.starrocks_port}"

    @property
    def s3_warehouse_uri(self) -> str:
        """Generate S3 warehouse URI."""
        return f"s3a://{self.s3_bucket}/{self.s3_warehouse_path}"


@lru_cache()
def get_settings() -> Settings:
    """
    Get cached settings instance.

    Returns:
        Settings: Application settings instance
    """
    return Settings()
