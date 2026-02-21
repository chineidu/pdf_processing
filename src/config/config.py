from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from omegaconf import DictConfig, OmegaConf
from pydantic import BaseModel, Field

from src import ROOT


@dataclass(slots=True, kw_only=True)
class CORS:
    """CORS configuration class."""

    allow_origins: list[str] = field(
        default_factory=list, metadata={"description": "Allowed origins for CORS."}
    )
    allow_credentials: bool = field(
        metadata={"description": "Allow credentials for CORS."}
    )
    allow_methods: list[str] = field(
        default_factory=list, metadata={"description": "Allowed methods for CORS."}
    )
    allow_headers: list[str] = field(
        default_factory=list, metadata={"description": "Allowed headers for CORS."}
    )


@dataclass(slots=True, kw_only=True)
class Middleware:
    """Middleware configuration class."""

    cors: CORS = field(metadata={"description": "CORS configuration."})


@dataclass(slots=True, kw_only=True)
class APIConfig:
    """API-level configuration."""

    title: str = field(metadata={"description": "The title of the API."})
    name: str = field(metadata={"description": "The name of the API."})
    description: str = field(metadata={"description": "The description of the API."})
    version: str = field(metadata={"description": "The version of the API."})
    status: str = field(metadata={"description": "The current status of the API."})
    prefix: str = field(metadata={"description": "The prefix for the API routes."})
    auth_prefix: str = field(
        metadata={"description": "The prefix for the authentication routes."}
    )
    middleware: Middleware = field(
        metadata={"description": "Middleware configuration."}
    )


@dataclass(slots=True, kw_only=True)
class DatabaseConfig:
    """Database configuration class."""

    pool_size: int = field(
        default=30, metadata={"description": "Number of connections to keep in pool"}
    )
    max_overflow: int = field(
        default=10, metadata={"description": "Number of extra connections allowed"}
    )
    pool_timeout: int = field(
        default=20, metadata={"description": "Seconds to wait for a connection"}
    )
    pool_recycle: int = field(
        default=1800,
        metadata={"description": "Seconds after which to recycle connections"},
    )
    pool_pre_ping: bool = field(
        default=True, metadata={"description": "Whether to test connections before use"}
    )
    expire_on_commit: bool = field(
        default=False, metadata={"description": "Whether to expire objects on commit"}
    )


@dataclass(slots=True, kw_only=True)
class PDFProcessingConfig:
    """PDF processing configuration class."""

    max_num_pages: int = field(
        default=20,
        metadata={
            "description": "Maximum number of pages to process in a PDF document."
        },
    )
    max_file_size_bytes: int = field(
        default=10 * 1024 * 1024,  # 10 MB
        metadata={"description": "Maximum file size in bytes for PDF processing."},
    )
    perform_ocr: bool = field(
        default=False,
        metadata={"description": "Whether to perform OCR on PDF documents."},
    )
    use_gpu: bool = field(
        default=True, metadata={"description": "Whether to use GPU for OCR processing."}
    )


@dataclass(slots=True, kw_only=True)
class EndpointPoliciesConfig:
    """Configuration for endpoint policies such as rate limits and credit costs."""

    ratelimits: dict[str, dict[str, Any]] = field(
        default_factory=dict,
        metadata={"description": "Rate limit configuration for different user roles."},
    )
    credit_costs: dict[str, float] = field(
        default_factory=dict,
        metadata={"description": "Credit costs for different API endpoints."},
    )


@dataclass(slots=True, kw_only=True)
class PrioritySizes:
    """Priority sizes configuration dataclass."""

    highest_priority: int = field(
        default=600, metadata={"description": "Size of the highest priority queue"}
    )
    high_priority: int = field(
        default=1500, metadata={"description": "Size of the high priority queue"}
    )
    normal_priority: int = field(
        default=2500, metadata={"description": "Size of the normal priority queue"}
    )
    low_priority: int = field(
        default=4000, metadata={"description": "Size of the low priority queue"}
    )


@dataclass(slots=True, kw_only=True)
class PriorityWeights:
    """Priority weights configuration dataclass."""

    highest_priority: int = field(
        default=9, metadata={"description": "Weight for the highest priority queue"}
    )
    high_priority: int = field(
        default=7, metadata={"description": "Weight for the high priority queue"}
    )
    normal_priority: int = field(
        default=5, metadata={"description": "Weight for the normal priority queue"}
    )
    low_priority: int = field(
        default=3, metadata={"description": "Weight for the low priority queue"}
    )


@dataclass(slots=True, kw_only=True)
class PriorityConfig:
    """Priority configuration dataclass."""

    sizes: PrioritySizes = field(
        metadata={"description": "Priority sizes configuration"}
    )
    weights: PriorityWeights = field(
        metadata={"description": "Priority weights configuration"}
    )


@dataclass(slots=True, kw_only=True)
class QueueConfig:
    """Queue configuration dataclass."""

    high_priority_ml: str = field(
        default="high_priority_ml",
        metadata={"description": "Queue for high priority machine learning tasks"},
    )
    normal_priority_ml: str = field(
        default="normal_priority_ml",
        metadata={"description": "Queue for normal priority machine learning tasks"},
    )
    low_priority_ml: str = field(
        default="low_priority_ml",
        metadata={"description": "Queue for low priority machine learning tasks"},
    )
    cleanups: str = field(
        default="periodic", metadata={"description": "Queue for cleanup tasks"}
    )
    notifications: str = field(
        default="user_notifications",
        metadata={"description": "Queue for user notifications"},
    )
    priority_config: PriorityConfig = field(
        metadata={"description": "Priority configuration"}
    )


@dataclass(slots=True, kw_only=True)
class QueueName:
    queue: str


@dataclass(slots=True, kw_only=True)
class TaskConfig:
    """Task configuration class."""

    task_serializer: str
    result_serializer: str
    accept_content: list[str]
    timezone: str
    enable_utc: bool


@dataclass(slots=True, kw_only=True)
class WorkerConfig:
    """Worker configuration class."""

    worker_prefetch_multiplier: int = field(
        metadata={"description": "Worker prefetch multiplier"}
    )
    task_acks_late: bool = field(
        metadata={"description": "Acknowledge tasks after completion"}
    )
    worker_max_tasks_per_child: int = field(
        metadata={"description": "Max tasks a worker can process before being replaced"}
    )
    worker_max_memory_per_child: int = field(
        metadata={
            "description": "Max memory (in bytes) a worker can use before being replaced"
        }
    )


@dataclass(slots=True, kw_only=True)
class TaskAndSchedule:
    """Task and schedule class."""

    task: str
    schedule: int


@dataclass(slots=True, kw_only=True)
class BeatSchedule:
    """Beat schedule class."""

    cleanup_old_records: TaskAndSchedule


@dataclass(slots=True, kw_only=True)
class BeatConfig:
    """Beat configuration class."""

    beat_schedule: BeatSchedule
    health_check: TaskAndSchedule


@dataclass(slots=True, kw_only=True)
class OtherConfig:
    """Other configuration class."""

    result_expires: int
    task_compression: str | None
    result_compression: str | None
    result_backend_always_retry: bool
    result_persistent: bool
    result_backend_max_retries: int


@dataclass(slots=True, kw_only=True)
class CeleryConfig:
    """Celery configuration class."""

    task_config: TaskConfig = field(metadata={"description": "Task configuration"})
    task_routes: dict[str, QueueName] = field(
        metadata={"description": "Dictionary of task routes"}
    )
    worker_config: WorkerConfig = field(
        metadata={"description": "Worker configuration"}
    )
    beat_config: BeatConfig = field(metadata={"description": "Beat configuration"})
    other_config: OtherConfig = field(metadata={"description": "Other configuration"})


class AppConfig(BaseModel):
    """Application configuration with validation."""

    api_config: APIConfig = Field(description="Configuration settings for the API")
    database_config: DatabaseConfig = Field(
        description="Configuration settings for the database"
    )
    pdf_processing_config: PDFProcessingConfig = Field(
        description="Configuration settings for PDF processing"
    )
    endpoint_policies_config: EndpointPoliciesConfig = Field(
        description="Configuration settings for endpoint policies"
    )
    queue_config: QueueConfig = Field(
        description="Configuration settings for task queues"
    )
    celery_config: CeleryConfig = Field(description="Celery configuration")


config_path: Path = ROOT / "src/config/config.yaml"
config: DictConfig = OmegaConf.load(config_path).config
resolved_cfg = OmegaConf.to_container(config, resolve=True)
app_config: AppConfig = AppConfig(**dict(resolved_cfg))  # type: ignore
