"""
Pydantic models for API request/response validation.

These models ensure type safety and automatic API documentation.
"""

from typing import List, Optional, Dict, Any
from datetime import datetime
from enum import Enum

from pydantic import BaseModel, Field, field_validator


class PipelineStatus(str, Enum):
    """Pipeline execution status."""

    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    PARTIAL_SUCCESS = "partial_success"


class SchemaProcessRequest(BaseModel):
    """Request model for processing specific schemas."""

    schema_names: List[str] = Field(
        ...,
        description="List of schema names to process from MySQL source",
        min_length=1,
    )
    force_reprocess: bool = Field(
        default=False,
        description="Force reprocessing even if schema exists in target",
    )
    skip_validation: bool = Field(
        default=False,
        description="Skip schema existence validation in target",
    )

    @field_validator("schema_names")
    @classmethod
    def validate_schema_names(cls, v: List[str]) -> List[str]:
        """Validate schema names are not empty."""
        if not v:
            raise ValueError("At least one schema name must be provided")
        # Remove duplicates while preserving order
        seen = set()
        unique_schemas = []
        for schema in v:
            schema = schema.strip()
            if not schema:
                raise ValueError("Schema names cannot be empty")
            if schema not in seen:
                seen.add(schema)
                unique_schemas.append(schema)
        return unique_schemas

    class Config:
        """Pydantic model configuration."""

        json_schema_extra = {
            "example": {
                "schema_names": ["customers_db", "orders_db", "products_db"],
                "force_reprocess": False,
                "skip_validation": False,
            }
        }


class ProcessAllSchemasRequest(BaseModel):
    """Request model for processing all available schemas."""

    exclude_schemas: List[str] = Field(
        default_factory=list,
        description="List of schema names to exclude from processing",
    )
    include_system_schemas: bool = Field(
        default=False,
        description="Include system schemas (mysql, information_schema, etc.)",
    )
    force_reprocess: bool = Field(
        default=False,
        description="Force reprocessing even if schema exists in target",
    )

    class Config:
        """Pydantic model configuration."""

        json_schema_extra = {
            "example": {
                "exclude_schemas": ["test_db", "temp_db"],
                "include_system_schemas": False,
                "force_reprocess": False,
            }
        }


class SchemaProcessResult(BaseModel):
    """Result of processing a single schema."""

    schema_name: str = Field(..., description="Name of the processed schema")
    status: PipelineStatus = Field(..., description="Processing status")
    tables_processed: int = Field(
        default=0,
        description="Number of tables successfully processed",
    )
    total_tables: int = Field(default=0, description="Total number of tables")
    rows_transferred: int = Field(
        default=0,
        description="Total rows transferred to Iceberg/StarRocks",
    )
    error_message: Optional[str] = Field(
        default=None,
        description="Error message if processing failed",
    )
    iceberg_location: Optional[str] = Field(
        default=None,
        description="S3 location of Iceberg table",
    )
    starrocks_database: Optional[str] = Field(
        default=None,
        description="StarRocks database name",
    )
    processing_time_seconds: float = Field(
        default=0.0,
        description="Time taken to process the schema",
    )
    metadata: Dict[str, Any] = Field(
        default_factory=dict,
        description="Additional metadata",
    )


class PipelineResponse(BaseModel):
    """Response model for pipeline execution."""

    job_id: str = Field(..., description="Unique job identifier")
    status: PipelineStatus = Field(..., description="Overall pipeline status")
    total_schemas: int = Field(
        default=0,
        description="Total number of schemas processed",
    )
    successful_schemas: int = Field(
        default=0,
        description="Number of successfully processed schemas",
    )
    failed_schemas: int = Field(
        default=0,
        description="Number of failed schemas",
    )
    skipped_schemas: int = Field(
        default=0,
        description="Number of skipped schemas (already exist in target)",
    )
    results: List[SchemaProcessResult] = Field(
        default_factory=list,
        description="Detailed results for each schema",
    )
    started_at: datetime = Field(
        default_factory=datetime.utcnow,
        description="Job start timestamp",
    )
    completed_at: Optional[datetime] = Field(
        default=None,
        description="Job completion timestamp",
    )
    total_duration_seconds: float = Field(
        default=0.0,
        description="Total job duration in seconds",
    )
    message: str = Field(..., description="Human-readable status message")


class HealthCheckResponse(BaseModel):
    """Health check response model."""

    status: str = Field(default="healthy")
    app_name: str
    app_version: str
    environment: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    services: Dict[str, str] = Field(
        default_factory=dict,
        description="Status of dependent services",
    )


class ErrorResponse(BaseModel):
    """Error response model."""

    error: str = Field(..., description="Error type")
    message: str = Field(..., description="Error message")
    details: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Additional error details",
    )
    timestamp: datetime = Field(default_factory=datetime.utcnow)
