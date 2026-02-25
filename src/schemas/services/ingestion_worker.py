from dataclasses import dataclass, field
from typing import Any

import pendulum
from pydantic import AliasChoices, AliasPath, Field

from src.schemas.base import BaseSchema


# ----- RabbitMQ Queue Arguments -----
@dataclass(slots=True, kw_only=True)
class QueueArguments:
    x_dead_letter_exchange: str | None = field(
        default=None, metadata={"description": "The name of the dead-letter exchange."}
    )
    x_max_priority: int | None = field(
        default=10,
        metadata={"description": "The maximum priority level for the queue."},
    )
    x_expires: int | None = field(
        default=None,
        metadata={
            "description": "Time in milliseconds for the queue to expire after inactivity."
        },
    )

    def model_dump(self) -> dict[str, Any]:
        """Convert the instance to a dictionary.

        Returns
        -------
        dict[str, Any]
            Dictionary representation.
        """
        data: dict[str, Any] = {}
        if self.x_dead_letter_exchange:
            data["x-dead-letter-exchange"] = self.x_dead_letter_exchange
        if self.x_max_priority:
            data["x-max-priority"] = self.x_max_priority
        if self.x_expires:
            data["x-expires"] = self.x_expires
        return data


# ----- Storage Event Payload -----
class StorageObject(BaseSchema):
    """Represents an object stored in a storage service (e.g. S3)."""

    # Accept both lowercase keys and MinIO/S3-style capitalized keys.
    key: str = Field(
        description="The key of the object.",
        validation_alias=AliasChoices("key", "Key"),
    )
    size: int | None = Field(
        default=None,
        description="The size of the object in bytes.",
        validation_alias=AliasChoices("size", "Size"),
    )
    etag: str = Field(
        description="The ETag of the object.",
        validation_alias=AliasChoices("eTag", "ETag"),
    )


class StorageEntity(BaseSchema):
    """Represents a storage entity (e.g. S3 bucket) involved in the event."""

    # Bucket name may come as:
    # - bucketName (camelCase) i.e. {"bucketName": "my-bucket"}
    # - name (flat key) i.e. {"name": "my-bucket"}
    # - bucket.name (nested object) i.e. {"bucket": {"name": "my-bucket"}}
    bucket_name: str = Field(
        description="The name of the storage bucket.",
        validation_alias=AliasChoices(
            "bucketName", "name", AliasPath("bucket", "name")
        ),
    )
    object: StorageObject = Field(description="The storage object details.")


class StorageRecord(BaseSchema):
    """Represents a single record in a storage event notification."""

    # Event fields vary by producer casing, so we accept both forms.
    event_name: str = Field(
        description="The name of the storage event (e.g. ObjectCreated:Put).",
        validation_alias=AliasChoices("eventName", "EventName"),
    )
    storage_entity: StorageEntity = Field(
        description="The storage entity details.", alias="s3"
    )
    created_at: str = Field(
        default_factory=lambda: pendulum.now().to_iso8601_string(),
        description="The timestamp when the event was created.",
        validation_alias=AliasChoices("eventTime", "EventTime"),
    )


class StorageEventPayload(BaseSchema):
    """Represents the payload of a storage event notification."""

    # Top-level records key can also be sent as `Records`.
    records: list[StorageRecord] = Field(
        description="List of storage event records.",
        validation_alias=AliasChoices("records", "Records"),
    )
