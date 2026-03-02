from datetime import datetime
from typing import Any, ClassVar
from uuid import uuid4

from pydantic import ConfigDict, EmailStr, Field, SecretStr, field_validator

from src.schemas.base import BaseSchema
from src.schemas.types import (
    APIKeyScopeEnum,
    MimeTypeEnum,
    RoleTypeEnum,
    StatusTypeEnum,
    TierEnum,
    UserStatusEnum,
)


class BaseUserSchema(BaseSchema):
    """Schema representing a user."""

    model_config = ConfigDict(
        from_attributes=True,
        json_encoders={datetime: lambda v: v.isoformat() if v else None},
    )

    id: int | None = Field(default=None)
    external_id: str = Field(default_factory=lambda: str(uuid4()))
    name: str
    email: EmailStr
    tier: TierEnum = Field(default=TierEnum.FREE)
    roles: list[RoleTypeEnum] = Field(default_factory=list)
    credits: float = Field(default=0.0, le=1_000_000.0, ge=0.0)
    status: UserStatusEnum = Field(default=UserStatusEnum.ACTIVE)
    is_active: bool = Field(default=True)
    created_at: datetime | None = Field(default=None)
    updated_at: datetime | None = Field(default=None)

    @field_validator("roles", mode="before")
    @classmethod
    def convert_roles(cls, v: Any) -> list[RoleTypeEnum]:
        """Convert DBRole objects or strings to RoleTypeEnum."""
        if not v:
            return []

        result = []
        for role in v:
            if isinstance(role, str):
                result.append(RoleTypeEnum(role))
            elif hasattr(role, "name"):  # DBRole object
                result.append(RoleTypeEnum(role.name))
            else:
                result.append(role)
        return result


class GuestUserSchema(BaseSchema):
    """Schema representing a guest/anonymous user with limited access."""

    model_config = ConfigDict(
        from_attributes=True,
        json_encoders={datetime: lambda v: v.isoformat() if v else None},
    )

    id: int | None = None
    external_id: str = "guest"
    name: str = "guest"
    email: EmailStr = Field(default="guest@anonymous.local")
    tier: TierEnum = Field(default=TierEnum.GUEST)
    credits: float = Field(default=0.0)
    status: UserStatusEnum = Field(default=UserStatusEnum.ACTIVE)
    is_active: bool = Field(default=True)
    created_at: datetime | None = None
    updated_at: datetime | None = None


class UserCreateSchema(BaseUserSchema):
    """Schema representing a database user with password."""

    password: SecretStr = Field(
        description="Plaintext password for the user. This will be hashed before storage.",
        min_length=8,
        max_length=20,
    )

    # Fetching and updating model config to add example
    _custom_model_config: ClassVar[ConfigDict] = BaseSchema.model_config.copy()
    _json_schema_extra: ClassVar[dict[str, Any]] = {
        "example": {
            "name": "example_user",
            "email": "user@example.com",
            "password": "securepassword123",
        }
    }
    _custom_model_config.update({"json_schema_extra": _json_schema_extra})
    model_config = _custom_model_config


class UserSchema(BaseUserSchema):
    """Schema representing a database user."""

    password_hash: str


class APIUpdateSchema(BaseSchema):
    """Schema representing the update of an API key."""

    model_config = ConfigDict(
        from_attributes=True,
        json_encoders={datetime: lambda v: v.isoformat() if v else None},
    )

    id: int | None = Field(
        default=None, description="Unique identifier of the API key."
    )
    name: str | None = Field(default=None, description="Name of the API key.")
    requests_per_minute: int | None = Field(
        default=None, description="Request limit per minute for the API key."
    )
    expires_at: datetime | None = Field(
        default=None, description="Expiration date and time of the API key."
    )
    is_active: bool | None = Field(
        default=None, description="Active status of the API key."
    )


class APIKeySchema(APIUpdateSchema):
    """Schema representing a database API key."""

    user_id: int | None = Field(description="ID of the user owning the API key.")
    key_prefix: str = Field(description="Prefix of the API key.")
    key_hash: str = Field(description="Hashed value of the API key.")
    scopes: list[APIKeyScopeEnum] = Field(
        default_factory=list,
        description="List of scopes/permissions assigned to the API key.",
    )

    # System Managed Fields
    created_at: datetime | None = Field(
        default=None, description="Creation date and time of the API key."
    )
    updated_at: datetime | None = Field(
        default=None, description="Last update date and time of the API key."
    )
    last_used_at: datetime | None = Field(
        default=None, description="Last used date and time of the API key."
    )


class RoleSchema(BaseSchema):
    """Role schema."""

    model_config = ConfigDict(
        from_attributes=True,
        json_encoders={datetime: lambda v: v.isoformat() if v else None},
    )

    id: int | None = Field(default=None, description="Unique identifier of the role.")
    name: RoleTypeEnum
    description: str | None = Field(
        default=None, description="Description of the role."
    )
    created_at: datetime | None = Field(
        default=None, description="Creation date and time of the role."
    )
    updated_at: datetime | None = Field(
        default=None, description="Last update date and time of the role."
    )


ROLES: dict[str, RoleSchema] = {
    "admin": RoleSchema(
        name=RoleTypeEnum.ADMIN, description="Administrator with full access"
    ),
    "user": RoleSchema(
        name=RoleTypeEnum.USER, description="Regular user with standard access"
    ),
    "guest": RoleSchema(
        name=RoleTypeEnum.GUEST, description="Guest user with limited access"
    ),
}


class TaskSchema(BaseSchema):
    """Task schema."""

    model_config = ConfigDict(
        from_attributes=True,
        json_encoders={datetime: lambda v: v.isoformat() if v else None},
    )

    # Identifiers
    id: int | None = Field(default=None, description="Unique identifier of the task.")
    task_id: str = Field(description="Unique identifier of the Celery task.")
    user_id: int | None = Field(
        default=None, description="ID of the user who initiated the task."
    )

    # Status and type
    status: StatusTypeEnum = Field(
        description="Current status of the task (e.g., pending, processing, completed, failed, etc)."
    )

    # File information
    file_page_count: int | None = Field(
        default=None,
        description="Number of pages in the uploaded PDF file (if applicable).",
    )
    file_upload_key: str = Field(
        description="S3 key of the uploaded file associated with the task."
    )
    file_result_key: str | None = Field(
        default=None, description="S3 key of the processed result file."
    )
    file_size_bytes: int = Field(description="Size of the uploaded file in bytes.")
    file_type: MimeTypeEnum | None = Field(
        description="MIME type of the uploaded file."
    )
    etag: str | None = Field(
        default=None, description="ETag of the uploaded file in S3."
    )

    # Error information
    error_message: str | None = Field(
        default=None, description="Error message if the task failed."
    )

    # Webhook information
    webhook_url: str | None = Field(
        default=None, description="URL to send webhook notifications to."
    )

    # Timestamps
    created_at: datetime | None = Field(
        default=None, description="Creation date and time of the task."
    )
    updated_at: datetime | None = Field(
        default=None, description="Last update date and time of the task."
    )
    completed_at: datetime | None = Field(
        default=None, description="Completion date and time of the task."
    )

    @field_validator("status", mode="before")
    @classmethod
    def convert_status(cls, v: Any) -> StatusTypeEnum:
        """Convert DBRole objects or strings to StatusTypeEnum."""
        if isinstance(v, str):
            return StatusTypeEnum(v)
        return v

    @field_validator("file_type", mode="before")
    @classmethod
    def convert_file_type(cls, v: Any) -> MimeTypeEnum:
        """Convert DBRole objects or strings to MimeTypeEnum."""
        if isinstance(v, str):
            return MimeTypeEnum(v)
        return v
