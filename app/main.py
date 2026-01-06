"""
Main FastAPI application entry point.

This module initializes and configures the FastAPI application with all
middleware, routers, and lifecycle events.
"""

import os
import sys
from contextlib import asynccontextmanager
from typing import Dict, Any

# Initialize Spark using findspark BEFORE any PySpark imports
import findspark

SPARK_HOME = os.getenv('SPARK_HOME', '/home/voltus-wave/starrocks/spark_install/spark-3.5.0-bin-hadoop3')
JAVA_HOME = os.getenv('JAVA_HOME', '/usr/lib/jvm/java-11-openjdk-amd64')

os.environ['JAVA_HOME'] = JAVA_HOME
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Initialize findspark with explicit SPARK_HOME
findspark.init(spark_home=SPARK_HOME)

from fastapi import FastAPI, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.core import get_logger, get_settings, setup_logging
from app.models import HealthCheckResponse
from app.api.v1.router import api_router

# Setup logging
setup_logging()
logger = get_logger(__name__)

# Get settings
settings = get_settings()

# Log Spark configuration
logger.info(f"SPARK_HOME: {os.environ.get('SPARK_HOME')}")
logger.info(f"JAVA_HOME: {os.environ.get('JAVA_HOME')}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan context manager.

    Handles startup and shutdown events.
    """
    # Startup
    logger.info(f"Starting {settings.app_name} v{settings.app_version}")
    logger.info(f"Environment: {settings.app_env}")
    logger.info(f"Log level: {settings.log_level}")

    yield

    # Shutdown
    logger.info("Shutting down application")


# Create FastAPI application
app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    description=(
        "Production-grade pipeline for ingesting historical data from MySQL "
        "into StarRocks using Apache Spark and Apache Iceberg as a staging layer. "
        "\n\n"
        "## Features\n"
        "- **Fault-tolerant**: Iceberg snapshots enable safe restarts\n"
        "- **Scalable**: Spark partitioned reads for large datasets\n"
        "- **Safe**: Atomic commits, no partial data\n"
        "- **Observable**: Comprehensive logging and status tracking\n"
        "\n\n"
        "## Pipeline Architecture\n"
        "```\n"
        "MySQL (Source) → Spark → Iceberg (S3) → StarRocks (Target)\n"
        "```"
    ),
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    lifespan=lifespan,
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Root endpoint
@app.get(
    "/",
    summary="Root endpoint",
    description="Basic information about the API",
    tags=["Health"],
)
async def root() -> Dict[str, str]:
    """Root endpoint with basic API information."""
    return {
        "application": settings.app_name,
        "version": settings.app_version,
        "status": "running",
        "docs": "/docs",
        "health": "/health",
    }


# Health check endpoint
@app.get(
    "/health",
    response_model=HealthCheckResponse,
    summary="Health check",
    description="Check the health status of the application and its dependencies",
    tags=["Health"],
)
async def health_check() -> HealthCheckResponse:
    """
    Health check endpoint.

    Returns application health status and connected services status.
    """
    from app.services.database import MySQLManager, StarRocksManager

    services_status = {}

    # Check MySQL connection
    try:
        mysql_manager = MySQLManager()
        with mysql_manager.get_connection():
            services_status["mysql"] = "healthy"
    except Exception as e:
        logger.error(f"MySQL health check failed: {e}")
        services_status["mysql"] = f"unhealthy: {str(e)}"

    # Check StarRocks connection
    try:
        starrocks_manager = StarRocksManager()
        with starrocks_manager.get_connection():
            services_status["starrocks"] = "healthy"
    except Exception as e:
        logger.error(f"StarRocks health check failed: {e}")
        services_status["starrocks"] = f"unhealthy: {str(e)}"

    # Overall status
    overall_status = (
        "healthy"
        if all("healthy" in s for s in services_status.values())
        else "degraded"
    )

    return HealthCheckResponse(
        status=overall_status,
        app_name=settings.app_name,
        app_version=settings.app_version,
        environment=settings.app_env,
        services=services_status,
    )


# Include API routers
app.include_router(api_router, prefix="/api/v1")


# Global exception handler
@app.exception_handler(Exception)
async def global_exception_handler(request, exc: Exception):
    """
    Global exception handler for unhandled exceptions.

    Args:
        request: The request that caused the exception
        exc: The exception that was raised

    Returns:
        JSONResponse: Error response
    """
    logger.exception(f"Unhandled exception: {exc}")

    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "error": "InternalServerError",
            "message": "An unexpected error occurred",
            "details": {"exception": str(exc)},
        },
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app.main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=settings.api_reload,
        log_level=settings.log_level.lower(),
    )
