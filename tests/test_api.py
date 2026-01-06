"""
API endpoint tests.

Tests for the FastAPI endpoints using httpx test client.
"""

import pytest
from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def test_root_endpoint():
    """Test the root endpoint returns expected data."""
    response = client.get("/")
    assert response.status_code == 200
    data = response.json()
    assert "application" in data
    assert "version" in data
    assert data["status"] == "running"


def test_health_check_endpoint():
    """Test the health check endpoint."""
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert "status" in data
    assert "app_name" in data
    assert "services" in data


def test_process_schemas_validation():
    """Test schema processing endpoint validates input."""
    # Test with empty schema list
    response = client.post(
        "/api/v1/pipeline/process-schemas",
        json={"schema_names": []},
    )
    assert response.status_code == 422  # Validation error


def test_process_all_schemas_endpoint_exists():
    """Test process all schemas endpoint exists."""
    response = client.post(
        "/api/v1/pipeline/process-all-schemas",
        json={
            "exclude_schemas": [],
            "include_system_schemas": False,
        },
    )
    # Will fail with connection error in test env, but endpoint exists
    assert response.status_code in [200, 500, 503]


def test_openapi_schema():
    """Test OpenAPI schema is available."""
    response = client.get("/openapi.json")
    assert response.status_code == 200
    schema = response.json()
    assert "openapi" in schema
    assert "paths" in schema
