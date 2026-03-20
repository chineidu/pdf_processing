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


class UserStatusEnum(StrEnum):
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


class StatusTypeEnum(StrEnum):
    """General status types."""

    # --- Pre-Processing Phase ---
    PENDING = "pending"  # Presigned URL generated; awaiting MinIO webhook.
    UPLOADED = "uploaded"  # MinIO webhook received.
    VALIDATING = "validating"  # Gateway is checking ETag and file type.

    # --- Active Phase ---
    PROCESSING = "processing"  # Celery worker is actively running docling OCR.

    # --- Terminal Success States ---
    COMPLETED = "completed"  # Pipeline ran successfully OR an identical ETag was found (Cache hit).

    # --- Terminal Failure States ---
    SKIPPED = "skipped"  # Valid file, but business logic rejected it (e.g., encrypted PDF, zero text).
    UNPROCESSABLE = (
        "unprocessable"  # Invalid/corrupt data that cannot be parsed. Do not retry.
    )
    FAILED = "failed"  # System/Infrastructure error (e.g., RMQ disconnect, DB timeout). Eligible for retry.


IDEMPOTENCY_ACTIVE_STATUSES: Final = frozenset(
    [
        # --------------------------------------------------------------------
        # --- In-Flight States (Prevent Race Conditions) ---------------------
        # --------------------------------------------------------------------
        StatusTypeEnum.UPLOADED.value,
        StatusTypeEnum.VALIDATING.value,
        StatusTypeEnum.PROCESSING.value,
        # --------------------------------------------------------------------
        # --- Deterministic Terminal States (Prevent Wasted Compute) ---------
        # --------------------------------------------------------------------
        StatusTypeEnum.COMPLETED.value,
        StatusTypeEnum.SKIPPED.value,
        StatusTypeEnum.UNPROCESSABLE.value,
    ]
)


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


class DBUpdateReasonEnum(StrEnum):
    """Reasons for updating a database record"""

    ENCRYPTED_FILE = "encrypted_file"  # Document is encrypted and cannot be processed
    EXCEEDS_PAGE_LIMIT = "exceeds_page_limit"  # Document has more pages than allowed
    EXCEEDS_SIZE_LIMIT = (
        "exceeds_size_limit"  # Document file size exceeds allowed limit
    )
    IDENTICAL_DATA = (
        "identical_data"  # New data is identical to existing (e.g., same ETag)
    )
    INVALID_FILE_TYPE = "invalid_file_type"  # Document has an unsupported file type
    OCR_FAILURE = (
        "ocr_failure"  # OCR process failed (e.g., unreadable content, corrupted file)
    )
    PROCESSED = (
        "processed"  # Document was successfully processed and data was extracted
    )
    PROCESSING_FAILED = (
        "processing_failed"  # Document processing failed due to an error
    )
    SYSTEM_ERROR = "system_error"  # Unexpected system error during processing
    ZERO_TEXT = "zero_text"  # Document contains no extractable text


class DocumentValidationResult(TypedDict):
    valid: bool
    errors: list[str]


class ExportFormat(StrEnum):
    ALL = "all"
    DOCUMENT_TAGS = "document_tags"
    JSON = "json"
    MARKDOWN = "markdown"
    TABLE = "table"
    TEXT = "text"


class MimeTypeEnum(StrEnum):
    PDF = "application/pdf"
    TEXT = "text/plain"
    JSON = "application/json"


class PoolType(StrEnum):
    """Celery worker pool strategies.

    Notes
    -----
    `PREFORK`: Process-based workers (separate Python processes). Provides strong
    isolation and true parallelism for CPU-bound or non-thread-safe tasks, but
    increases memory usage (model loaded per process).

    `THREADS`: Thread-based workers (single process). Lower memory usage since
    models can be shared; required for GPU/CUDA workloads to avoid context
    conflicts. The Python GIL may limit pure-Python parallelism, though many ML
    runtimes release the GIL during inference.

    Selection (short):
        - Prefer `THREADS` for GPUs, large models, ONNX/optimized ML inference,
          or fast startup.
        - Prefer `PREFORK` for isolation, CPU-bound work, or non-thread-safe code.
    """

    PREFORK = "prefork"
    THREADS = "threads"


class PriorityEnum(StrEnum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


PDFOutputValue = bytes | str | None


class PDFOutputResult(TypedDict):
    doctags: PDFOutputValue
    json: PDFOutputValue
    markdown: PDFOutputValue
    tables: dict[str, PDFOutputValue]  # keys like "table_1", "table_2", ...
    text: PDFOutputValue
