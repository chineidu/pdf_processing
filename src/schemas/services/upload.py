from dataclasses import dataclass, field
from pathlib import Path

from src.schemas.types import StatusTypeEnum


@dataclass(slots=True, kw_only=True)
class UploadedResult:
    """Data class representing the result of an uploaded file processing task."""

    status: StatusTypeEnum = field(
        metadata={
            "description": "Status of the uploaded file processing task (e.g., 'uploaded', 'failed', etc.)"
        }
    )
    task_id: str = field(
        metadata={
            "description": "Unique identifier of the processing task associated with the uploaded file."
        }
    )
    filepath: str = field(
        metadata={"description": "Original file path of the uploaded file."}
    )
    filename: str | None = field(
        default=None,
        metadata={"description": "Original filename of the uploaded file."},
    )
    file_size_bytes: int | None = field(
        default=None,
        metadata={"description": "Size of the uploaded file in bytes."},
    )
    content_type: str | None = field(
        default=None,
        metadata={"description": "Content type used for the upload request."},
    )
    etag: str | None = field(
        default=None,
        metadata={
            "description": (
                "ETag of the uploaded file in S3. Null if the upload failed or ETag is not available."
            )
        },
    )
    page_count: int | None = field(
        default=None,
        metadata={
            "description": (
                "Number of pages in the PDF file (if applicable). "
                "Used for queue routing without re-downloading."
            )
        },
    )
    error: str | None = field(
        default=None,
        metadata={
            "description": (
                "Error message if the file upload or processing failed. Null if the operation was successful."
            )
        },
    )

    def __post_init__(self) -> None:
        """Validate the status field to ensure it is a valid StatusTypeEnum value."""
        if isinstance(self.status, StatusTypeEnum):
            try:
                # Convert string status to StatusTypeEnum value
                self.status = self.status.value
            except ValueError:
                raise ValueError(
                    f"Invalid status value: {self.status}. Must be a valid StatusTypeEnum."
                ) from None
        if isinstance(self.filepath, Path):
            self.filepath = str(self.filepath)
