from datetime import datetime

from pydantic import Field

from src.schemas.base import BaseSchema
from src.schemas.types import MimeTypeEnum, StatusTypeEnum


class PresignedURLResponse(BaseSchema):
    """Schema representing the response from the presigned URL generation endpoint."""

    task_id: str = Field(
        description="The unique identifier of the processing task associated with the uploaded file."
    )
    url: str = Field(
        description="The presigned URL to which the file should be uploaded."
    )
    expires_at: str = Field(
        description="The expiration time of the presigned URL in ISO 8601 format."
    )
    content_type: MimeTypeEnum | None = Field(
        description="The MIME type of the file to be uploaded."
    )


class TaskStatusResponse(BaseSchema):
    """Schema representing the response from the task status endpoint."""

    task_id: str = Field(description="Unique identifier of the task.")
    status: StatusTypeEnum = Field(description="Current status of the task.")
    file_type: MimeTypeEnum | None = Field(
        description="MIME type of the uploaded file."
    )
    file_size_bytes: int = Field(description="Size of the uploaded file in bytes.")
    created_at: datetime | None = Field(description="Task creation timestamp.")
    updated_at: datetime | None = Field(description="Task last update timestamp.")
    completed_at: datetime | None = Field(description="Task completion timestamp.")
    error_message: str | None = Field(description="Error message if the task failed.")
    download_urls: dict[str, str] | None = Field(
        description="Mapping of output format names to download URLs."
    )
