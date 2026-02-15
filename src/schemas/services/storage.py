from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass(slots=True, kw_only=True)
class S3UploadMetadata:
    task_id: str
    uploaded_at: str
    correlation_id: str
    environment: str
    service: str = "taskflow"
    log_level: str = "INFO"


@dataclass(slots=True, kw_only=True)
class UploadResultExtraArgs:
    """The extra arguments for S3 upload.

    Note
    ----
    The field names MUST match the expected keys in boto3's `ExtraArgs` parameter.
    """

    Metadata: S3UploadMetadata
    # AccessControlList
    ACL: str
    ContentType: str

    def model_dump(self) -> dict[str, Any]:
        """Convert to a dictionary.

        Returns
        -------
        dict[str, Any]
            Dictionary representation.
        """
        return asdict(self)


@dataclass(slots=True, kw_only=True)
class LogStorageUploadResult:
    attempts: int
    error: str | None = field(
        default=None, metadata={"description": "Error message if upload failed."}
    )
    s3_key: str | None = field(
        default=None, metadata={"description": "S3 key where the task logs are stored."}
    )
    s3_url: str | None = field(
        default=None, metadata={"description": "S3 URL where the task logs are stored."}
    )
