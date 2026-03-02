from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True, kw_only=True)
class ProcessDataTaskResult:
    """Data class representing the result of a PDF processing task."""

    status: str = field(
        metadata={
            "description": "Status of the PDF processing task (e.g., 'completed', 'failed', etc.)"
        }
    )
    success: bool = field(
        metadata={
            "description": "Indicates whether the PDF processing task was successful or not."
        }
    )
    task_id: str = field(
        metadata={
            "description": "Unique identifier of the processing task associated with the uploaded file."
        }
    )
    completed_at: str = field(
        metadata={
            "description": "Timestamp indicating when the PDF processing task was completed "
            "(in ISO 8601 format)."
        }
    )
    file_result_url: dict[str, Any] | None = field(
        default=None,
        metadata={
            "description": "Dictionary mapping format names to S3 URLs of the processed files."
        },
    )
    error: str | None = field(
        default=None,
        metadata={
            "description": (
                "Error message if the PDF processing task failed. Null if the operation was successful."
            )
        },
    )
