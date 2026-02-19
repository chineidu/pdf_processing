from datetime import datetime, timezone

from pydantic import Field

from src.schemas.base import BaseSchema


class HealthStatusSchema(BaseSchema):
    """Schema for health check response."""

    name: str = Field(description="Name of the service.")
    status: str = Field(description="Health status of the service.")
    version: str = Field(description="Version of the service.")
    timestamp: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat(),
        description="Timestamp of the health check in ISO 8601 format.",
    )
