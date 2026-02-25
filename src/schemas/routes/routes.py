from pydantic import Field

from src.schemas.base import BaseSchema
from src.schemas.types import MimeTypeEnum


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
