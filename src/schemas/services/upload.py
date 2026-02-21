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
    error: str | None = field(
        default=None,
        metadata={
            "description": "Error message if the file upload or processing failed. Null if the operation was successful."
        },
    )

    def __post_init__(self) -> None:
        """Validate the status field to ensure it is a valid StatusTypeEnum value."""
        if isinstance(self.status, str):
            try:
                self.status = StatusTypeEnum(self.status)
            except ValueError:
                raise ValueError(
                    f"Invalid status value: {self.status}. Must be a valid StatusTypeEnum."
                ) from None
        if isinstance(self.filepath, Path):
            self.filepath = str(self.filepath)
