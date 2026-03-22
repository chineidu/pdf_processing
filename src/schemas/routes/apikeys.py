from datetime import datetime, timedelta
from typing import Any, ClassVar

from pydantic import ConfigDict, Field, field_validator

from src.schemas.base import BaseSchema
from src.schemas.types import APIKeyScopeEnum


class APICreationSchema(BaseSchema):
    """Schema representing the creation of an API key."""

    model_config = ConfigDict(
        from_attributes=True,
        json_encoders={datetime: lambda v: v.isoformat() if v else None},
    )
    name: str = Field(description="Name of the API key to be created.")
    expires_at: datetime | None = Field(
        default=None, description="Expiration date and time of the API key."
    )
    scopes: list[APIKeyScopeEnum] = Field(
        description="List of scopes/permissions assigned to the API key."
    )

    @field_validator("expires_at", mode="before")
    @classmethod
    def validate_expiration(cls, v: datetime | None) -> datetime | None:
        """Validate that the expiration date is in the future."""
        if v and v <= datetime.now():
            raise ValueError("Expiration date must be in the future.")
        return v

    # Fetching and updating model config to add example
    _custom_model_config: ClassVar[ConfigDict] = BaseSchema.model_config.copy()
    _json_schema_extra: ClassVar[dict[str, Any]] = {
        "example": {
            "name": "My API Key",
            "expires_at": (datetime.now() + timedelta(days=30)).isoformat(),
            "scopes": [scope.value for scope in APIKeyScopeEnum],
        }
    }
    _custom_model_config.update({"json_schema_extra": _json_schema_extra})
    model_config = _custom_model_config


class APIResponseSchema(BaseSchema):
    """Schema representing an API key."""

    model_config = ConfigDict(
        from_attributes=True,
        json_encoders={datetime: lambda v: v.isoformat() if v else None},
    )

    id: int = Field(description="Unique identifier of the API key.")
    name: str = Field(description="Name of the API key.")
    prefix: str = Field(description="Prefix of the API key.")
    full_key: str = Field(description="Full API key value.")
    owner: str = Field(description="External ID of the API key owner.")
    created_at: datetime | None = Field(
        description="Creation date and time of the API key."
    )
    updated_at: datetime = Field(
        default_factory=datetime.now,
        description="Last update date and time of the API key.",
    )
    expires_at: datetime | None = Field(
        default=None, description="Expiration date and time of the API key."
    )
    scopes: list[APIKeyScopeEnum] = Field(
        description="List of scopes/permissions assigned to the API key."
    )


class APIRotationSchema(BaseSchema):
    """Request body for rotating an API key."""

    model_config = ConfigDict(
        from_attributes=True,
        json_encoders={datetime: lambda v: v.isoformat() if v else None},
    )

    grace_period_minutes: int = Field(
        default=60,
        ge=0,
        le=10080,  # max 7 days
        description="Minutes the old key stays valid after rotation. Use 0 to revoke immediately.",
    )
    new_expires_at: datetime | None = Field(
        default=None,
        description="Expiration for the new key. If None, inherits the old key's expiry.",
    )

    @field_validator("new_expires_at", mode="before")
    @classmethod
    def validate_expiration(cls, v: datetime | None) -> datetime | None:
        """Validate that the expiration date is in the future."""
        if v and v <= datetime.now():
            raise ValueError("Expiration date must be in the future.")
        return v


class APIRotationResponseSchema(APIResponseSchema):
    """Response schema for a key rotation — includes metadata about the retired key."""

    rotated_from_id: int = Field(description="ID of the old key that was rotated out.")
    old_key_expires_at: datetime | None = Field(
        default=None,
        description="When the old key expires (end of grace period). None if revoked immediately.",
    )
