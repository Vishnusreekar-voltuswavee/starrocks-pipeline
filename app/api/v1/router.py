"""API v1 router configuration."""

from fastapi import APIRouter

from app.api.v1.endpoints import pipeline

api_router = APIRouter()

# Include endpoint routers
api_router.include_router(
    pipeline.router,
    prefix="/pipeline",
    tags=["Pipeline Operations"],
)
