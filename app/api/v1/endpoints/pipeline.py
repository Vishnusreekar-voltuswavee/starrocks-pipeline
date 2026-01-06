"""
Pipeline API endpoints.

Provides REST API endpoints for triggering and managing data pipeline jobs.
"""

from fastapi import APIRouter, HTTPException, status, BackgroundTasks
from fastapi.responses import JSONResponse

from app.core import get_logger
from app.models import (
    SchemaProcessRequest,
    ProcessAllSchemasRequest,
    PipelineResponse,
    ErrorResponse,
)
from app.services import PipelineOrchestrator
from app.utils.exceptions import (
    PipelineException,
    SchemaNotFoundError,
    SchemaExistsError,
    DatabaseConnectionError,
)

logger = get_logger(__name__)
router = APIRouter()


@router.post(
    "/process-schemas",
    response_model=PipelineResponse,
    status_code=status.HTTP_200_OK,
    summary="Process specific schemas by name",
    description=(
        "Process one or more schemas from MySQL source to StarRocks target "
        "via Iceberg staging layer. Only processes schemas that don't exist "
        "in the target database unless force_reprocess is enabled."
    ),
    responses={
        200: {
            "description": "Pipeline executed successfully",
            "model": PipelineResponse,
        },
        400: {
            "description": "Bad request - invalid schema names or parameters",
            "model": ErrorResponse,
        },
        404: {
            "description": "Schema not found in source database",
            "model": ErrorResponse,
        },
        409: {
            "description": "Schema already exists in target database",
            "model": ErrorResponse,
        },
        500: {
            "description": "Internal server error",
            "model": ErrorResponse,
        },
    },
)
async def process_specific_schemas(
    request: SchemaProcessRequest,
) -> PipelineResponse:
    """
    Process specific schemas by name from MySQL to StarRocks.

    This endpoint processes the specified schemas through the complete pipeline:
    1. Extract data from MySQL using Spark with partitioned JDBC reads
    2. Write to Iceberg staging layer in S3 with snapshots
    3. Load from Iceberg to StarRocks raw tables

    The same schema names are preserved throughout the pipeline.

    Args:
        request: Schema processing request with list of schema names

    Returns:
        PipelineResponse: Detailed results of the pipeline execution

    Raises:
        HTTPException: On validation or processing errors
    """
    orchestrator = None

    try:
        logger.info(
            f"Received request to process schemas: {request.schema_names}"
        )

        orchestrator = PipelineOrchestrator()

        # Validate schemas exist in source
        for schema_name in request.schema_names:
            if not orchestrator.mysql_manager.schema_exists(schema_name):
                raise SchemaNotFoundError(
                    f"Schema '{schema_name}' not found in MySQL source database"
                )

        # Check if schemas exist in target (unless skipping validation)
        if not request.skip_validation and not request.force_reprocess:
            for schema_name in request.schema_names:
                if orchestrator.starrocks_manager.database_exists(
                    schema_name
                ):
                    raise SchemaExistsError(
                        f"Schema '{schema_name}' already exists in StarRocks. "
                        "Use force_reprocess=true to override."
                    )

        # Execute pipeline
        result = orchestrator.process_schemas(
            schema_names=request.schema_names,
            force_reprocess=request.force_reprocess,
            skip_validation=request.skip_validation,
        )

        return result

    except SchemaNotFoundError as e:
        logger.error(f"Schema not found: {e}")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "SchemaNotFound",
                "message": str(e),
                "details": e.details if hasattr(e, "details") else None,
            },
        )

    except SchemaExistsError as e:
        logger.error(f"Schema already exists: {e}")
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "error": "SchemaExists",
                "message": str(e),
                "details": e.details if hasattr(e, "details") else None,
            },
        )

    except DatabaseConnectionError as e:
        logger.error(f"Database connection error: {e}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "error": "DatabaseConnectionError",
                "message": str(e),
                "details": e.details if hasattr(e, "details") else None,
            },
        )

    except PipelineException as e:
        logger.error(f"Pipeline error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "error": "PipelineError",
                "message": str(e),
                "details": e.details if hasattr(e, "details") else None,
            },
        )

    except Exception as e:
        logger.exception(f"Unexpected error processing schemas: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "error": "InternalServerError",
                "message": "An unexpected error occurred",
                "details": {"exception": str(e)},
            },
        )

    finally:
        if orchestrator:
            orchestrator.cleanup()


@router.post(
    "/process-all-schemas",
    response_model=PipelineResponse,
    status_code=status.HTTP_200_OK,
    summary="Process all schemas from source database",
    description=(
        "Automatically discover and process all schemas from MySQL source. "
        "Only processes schemas that don't already exist in StarRocks target "
        "unless force_reprocess is enabled. System schemas are excluded by default."
    ),
    responses={
        200: {
            "description": "Pipeline executed successfully",
            "model": PipelineResponse,
        },
        400: {
            "description": "Bad request - invalid parameters",
            "model": ErrorResponse,
        },
        500: {
            "description": "Internal server error",
            "model": ErrorResponse,
        },
    },
)
async def process_all_schemas(
    request: ProcessAllSchemasRequest,
) -> PipelineResponse:
    """
    Process all available schemas from MySQL to StarRocks.

    This endpoint automatically discovers all schemas in the MySQL source
    and processes only those that don't exist in StarRocks target.

    The pipeline:
    1. Discovers all schemas in MySQL (excluding system schemas by default)
    2. Filters out schemas that already exist in StarRocks
    3. Processes remaining schemas through MySQL -> Iceberg -> StarRocks
    4. Preserves schema names throughout the pipeline

    Args:
        request: Processing request with optional exclusion list

    Returns:
        PipelineResponse: Detailed results of the pipeline execution

    Raises:
        HTTPException: On processing errors
    """
    orchestrator = None

    try:
        logger.info("Received request to process all schemas")

        orchestrator = PipelineOrchestrator()

        # Execute pipeline for all schemas
        result = orchestrator.process_all_schemas(
            exclude_schemas=request.exclude_schemas,
            include_system_schemas=request.include_system_schemas,
            force_reprocess=request.force_reprocess,
        )

        logger.info(
            f"Processed all schemas: {result.successful_schemas} successful, "
            f"{result.failed_schemas} failed, {result.skipped_schemas} skipped"
        )

        return result

    except DatabaseConnectionError as e:
        logger.error(f"Database connection error: {e}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "error": "DatabaseConnectionError",
                "message": str(e),
                "details": e.details if hasattr(e, "details") else None,
            },
        )

    except PipelineException as e:
        logger.error(f"Pipeline error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "error": "PipelineError",
                "message": str(e),
                "details": e.details if hasattr(e, "details") else None,
            },
        )

    except Exception as e:
        logger.exception(f"Unexpected error processing all schemas: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "error": "InternalServerError",
                "message": "An unexpected error occurred",
                "details": {"exception": str(e)},
            },
        )

    finally:
        if orchestrator:
            orchestrator.cleanup()
