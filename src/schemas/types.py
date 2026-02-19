from enum import StrEnum
from typing import Final, TypedDict


class EnvironmentEnum(StrEnum):
    DEVELOPMENT = "development"
    PRODUCTION = "production"
    SANDBOX = "sandbox"
    STAGING = "staging"
    TESTING = "testing"


class ErrorCodeEnum(StrEnum):
    CIRCUIT_OPEN_ERROR = "circuit_open_error"
    HTTP_ERROR = "http_error"
    INTERNAL_SERVER_ERROR = "internal_server_error"
    INVALID_INPUT = "invalid_input"
    MAX_RETRIES_EXCEEDED = "max_retries_exceeded"
    RATE_LIMIT_ERROR = "rate_limit_error"
    RESOURCES_NOT_FOUND = "resources_not_found"
    SERVICE_UNAVAILABLE = "service_unavailable"
    TIMEOUT_ERROR = "timeout_error"
    UNAUTHORIZED = "unauthorized"
    UNEXPECTED_ERROR = "unexpected_error"


class ResourceEnum(StrEnum):
    """The type of resource to use."""

    BACKEND_REGISTRY = "backend_registry"
    CACHE = "cache"
    DATABASE = "database"
    RATE_LIMITER = "rate_limiter"
    SERVICE_REGISTRY = "service_registry"
    STORAGE = "storage"


class StatusEnum(StrEnum):
    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


class CircuitBreakerStateEnum(StrEnum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class TierEnum(StrEnum):
    """Client subscription tier."""

    GUEST = "guest"  # guest users with minimal access, read-only
    FREE = "free"  # low limits, short TTLs and basic support
    PLUS = "plus"  # moderate limits, medium TTLs and standard support
    PRO = "pro"  # high limits, long TTLs and premium support


class ClientStatusEnum(StrEnum):
    """Client account status."""

    # Onboarding
    PENDING_VERIFICATION = "pending_verification"  # Awaiting email/admin approval

    # Normal Operation
    ACTIVE = "active"  # Fully functional

    # User-Initiated (The user wants to stop)
    PAUSED = "paused"  # Temporarily stopped by the client
    ARCHIVED = "archived"  # Soft-deleted

    # Admin/System-Initiated (You stopped them)
    SUSPENDED = "suspended"  # Temporarily blocked (e.g. unpaid bill, rate limit abuse)
    BANNED = "banned"  # Permanently blocked


class RoleTypeEnum(StrEnum):
    """User role types."""

    GUEST = "guest"
    ADMIN = "admin"
    USER = "user"


class APIKeyScopeEnum(StrEnum):
    """API key scopes using resource:action naming convention"""

    # ----- Core data access -----
    DATA_READ = "data:read"  # Read any data / most resources
    DATA_WRITE = "data:write"  # Create/update/delete most data

    # ----- Admin & dangerous -----
    ADMIN_FULL = "admin:full"
    API_KEYS_MANAGE = "api_keys:manage"

    # ----- Utility / special -----
    EXPORT_DATA = "export:data"
    JOBS_RUN = "jobs:run"
    ANALYTICS_READ = "analytics:read"
    LOGS_READ = "logs:read"

    # Very narrow / internal
    HEALTH_CHECK = "health:check"
    RATE_LIMIT_EXEMPT = "rate_limit:exempt"


# Convenience sets (not enum members)
COMMON_READ_SCOPES: Final = frozenset(
    [
        APIKeyScopeEnum.DATA_READ,
        # add more granular ones as needed: users:read, projects:read, ...
        APIKeyScopeEnum.ANALYTICS_READ,
        APIKeyScopeEnum.LOGS_READ,
        APIKeyScopeEnum.HEALTH_CHECK,
    ]
)

WRITE_SCOPES: Final = frozenset(
    [
        APIKeyScopeEnum.DATA_WRITE,
        # add more granular ones as needed: users:write, projects:write, ...
        APIKeyScopeEnum.JOBS_RUN,
    ]
)

DANGEROUS_SCOPES: Final = frozenset(
    [
        APIKeyScopeEnum.ADMIN_FULL,
        APIKeyScopeEnum.API_KEYS_MANAGE,
    ]
)


class DocumentValidationResult(TypedDict):
    valid: bool
    errors: list[str]
