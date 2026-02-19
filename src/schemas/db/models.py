from datetime import datetime
from typing import Any, ClassVar
from uuid import uuid4

from pydantic import ConfigDict, EmailStr, Field, SecretStr, field_validator

from src.schemas.base import BaseSchema
from src.schemas.types import APIKeyScopeEnum, ClientStatusEnum, RoleTypeEnum, TierEnum


class BaseClientSchema(BaseSchema):
    """Schema representing a client."""

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
    status: ClientStatusEnum = Field(default=ClientStatusEnum.ACTIVE)
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


class GuestClientSchema(BaseSchema):
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
    status: ClientStatusEnum = Field(default=ClientStatusEnum.ACTIVE)
    is_active: bool = Field(default=True)
    created_at: datetime | None = None
    updated_at: datetime | None = None


class ClientCreateSchema(BaseClientSchema):
    """Schema representing a database client with password."""

    password: SecretStr = Field(
        description="Plaintext password for the client. This will be hashed before storage.",
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


class ClientSchema(BaseClientSchema):
    """Schema representing a database client."""

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

    client_id: int | None = Field(description="ID of the client owning the API key.")
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
