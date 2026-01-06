"""
Custom exceptions for the pipeline application.

Provides specific exception types for better error handling and debugging.
"""


class PipelineException(Exception):
    """Base exception for all pipeline-related errors."""

    def __init__(self, message: str, details: dict = None):
        """
        Initialize pipeline exception.

        Args:
            message: Error message
            details: Additional error details
        """
        self.message = message
        self.details = details or {}
        super().__init__(self.message)


class DatabaseConnectionError(PipelineException):
    """Raised when database connection fails."""

    pass


class SchemaNotFoundError(PipelineException):
    """Raised when a schema is not found in source database."""

    pass


class SchemaExistsError(PipelineException):
    """Raised when a schema already exists in target database."""

    pass


class SparkJobError(PipelineException):
    """Raised when a Spark job fails."""

    pass


class IcebergError(PipelineException):
    """Raised when Iceberg operations fail."""

    pass


class S3Error(PipelineException):
    """Raised when S3 operations fail."""

    pass


class ValidationError(PipelineException):
    """Raised when validation fails."""

    pass


class DataValidationError(PipelineException):
    """Raised when data validation or reconciliation fails (e.g., row count mismatch)."""

    pass


class ConfigurationError(PipelineException):
    """Raised when configuration is invalid."""

    pass
