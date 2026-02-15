from dataclasses import dataclass, field
from pathlib import Path

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


class AppConfig(BaseModel):
    """Application configuration with validation."""

    api_config: APIConfig = Field(description="Configuration settings for the API")
    database_config: DatabaseConfig = Field(
        description="Configuration settings for the database"
    )
    pdf_processing_config: PDFProcessingConfig = Field(
        description="Configuration settings for PDF processing"
    )


config_path: Path = ROOT / "src/config/config.yaml"
config: DictConfig = OmegaConf.load(config_path).config
resolved_cfg = OmegaConf.to_container(config, resolve=True)
app_config: AppConfig = AppConfig(**dict(resolved_cfg))  # type: ignore
