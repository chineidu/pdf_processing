"""S3/MinIO storage service for managing task execution logs."""

import asyncio
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, AsyncGenerator

import boto3
from botocore.client import Config
from botocore.exceptions import BotoCoreError, ClientError

from src import create_logger
from src.config import app_config, app_settings
from src.schemas.services.storage import (
    S3StorageUploadResult,
    S3UploadMetadata,
    UploadResultExtraArgs,
)

logger = create_logger("storage")
MAX_ATTEMPTS: int = 3
MAX_FILE_SIZE_BYTES = app_config.pdf_processing_config.max_file_size_bytes


# ---- Helper functions for S3 operations ----
def _upload_to_s3(
    client: Any,
    filepath: str | Path,
    bucket_name: str,
    object_name: str,
    extra_args: UploadResultExtraArgs,
) -> bool:
    task_id: str = extra_args.Metadata.task_id
    try:
        client.upload_file(
            str(filepath),
            bucket_name,
            object_name,
            ExtraArgs=extra_args.model_dump(),
        )
        return True

    except ClientError as e:
        logger.error(f"[x] Failed to upload log for task {task_id}: {e}")
        return False

    except BotoCoreError as e:
        logger.error(f"[x] BotoCore error uploading log for task {task_id}: {e}")
        return False

    except Exception as exc:
        logger.error(f"[x] Unexpected error uploading log for task {task_id}: {exc}")
        return False


def _download_from_s3(
    client: Any, filepath: str | Path, bucket_name: str, object_name: str
) -> bool:
    try:
        client.download_file(bucket_name, object_name, filepath)
        return True

    except Exception as exc:
        logger.error(f"[x] Download failed: {exc}")
        return False


def _get_s3_stream(client: Any, bucket_name: str, key: str) -> Any:
    """Get a streaming body from S3.

    Parameters
    ----------
    client : Any
        Boto3 S3 client.
    bucket_name : str
        Name of the S3 bucket.
    key : str
        Object key in S3.

    Returns
    -------
    Any
        The streaming body object.

    Raises
    ------
    ClientError
        If the object does not exist or there's an S3 error.
    """
    try:
        response = client.get_object(Bucket=bucket_name, Key=key)
        return response["Body"]
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code")
        if error_code == "NoSuchKey":
            logger.error(f"[x] Object not found in S3: bucket={bucket_name}, key={key}")
        else:
            logger.error(
                f"[x] S3 error retrieving object: bucket={bucket_name}, key={key}, error={e}"
            )
        raise
    except Exception as e:
        logger.error(
            f"[x] Unexpected error retrieving object from S3: bucket={bucket_name}, key={key}, error={e}"
        )
        raise


def _generate_presigned_url(
    client: Any, bucket_name: str, object_name: str, expiration: int = 3600
) -> str:
    """Generate a presigned URL for the given S3 object."""
    try:
        return client.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket_name, "Key": object_name},
            ExpiresIn=expiration,
        )
    except ClientError as e:
        logger.error(f"Failed to generate presigned URL: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error generating presigned URL: {e}")
        raise


# ---- S3 Storage Service and Policy Classes ----
class S3StorageService:
    """
    Service for uploading, downloading, and deleting files from S3-compatible storage.

    Supports AWS S3, MinIO and other S3-compatible backends.
    """

    def __init__(self) -> None:
        """Initialize S3 client with settings from configuration."""
        try:
            self.s3_client = boto3.client(
                "s3",
                aws_access_key_id=app_settings.AWS_ACCESS_KEY_ID.get_secret_value(),
                aws_secret_access_key=app_settings.AWS_SECRET_ACCESS_KEY.get_secret_value(),
                region_name=app_settings.AWS_DEFAULT_REGION,
                config=Config(
                    retries={"max_attempts": MAX_ATTEMPTS, "mode": "adaptive"}
                ),
                # ---- Required for MinIO or other S3-compatible services ----
                endpoint_url=app_settings.aws_s3_endpoint_url,
            )
            self.bucket_name = app_settings.AWS_S3_BUCKET
            self.max_size_bytes = MAX_FILE_SIZE_BYTES

            logger.info(
                f"[+] {self.__class__.__name__} initialized: endpoint={app_settings.aws_s3_endpoint_url}, "
                f"bucket={self.bucket_name}"
            )
        except Exception as e:
            logger.error(f"[x] Failed to initialize S3StorageService: {e}")
            raise

    async def aupload_file_to_s3(
        self,
        *,
        filepath: str | Path,
        task_id: str,
        correlation_id: str,
        environment: str,
        max_allowed_size_bytes: int = MAX_FILE_SIZE_BYTES,
    ) -> bool:
        """Upload a file to S3 asynchronously.

        Parameters
        ----------
        filepath : str | Path
            Path to the file to upload.
        task_id : str
            Unique task identifier.
        correlation_id : str
            Correlation ID for tracing.
        environment : str
            Application environment (e.g., development, production).
        max_allowed_size_bytes : int, optional
            Maximum allowed file size in bytes, by default
            app_config.pdf_processing_config.max_file_size_bytes

        Returns
        -------
        bool
            True if upload succeeded, False otherwise.

        Raises
        ------
        ValueError
            If the file size exceeds the maximum allowed size.
        RuntimeError
            If the upload fails.
        """
        filepath = Path(filepath) if isinstance(filepath, str) else filepath
        # Check the file size
        file_size = filepath.stat().st_size
        if file_size > max_allowed_size_bytes:
            raise ValueError(
                f"Log size: {file_size:,} bytes exceeds max: {max_allowed_size_bytes:,} bytes"
            )

        object_name = self.get_object_name(task_id)
        extra_args = UploadResultExtraArgs(
            Metadata=S3UploadMetadata(
                task_id=task_id,
                uploaded_at=datetime.now(timezone.utc).isoformat(),
                correlation_id=correlation_id,
                environment=environment,
            ),
            ACL="private",
            ContentType="text/plain",
        )
        success = await self._aupload_to_s3(
            filepath,
            object_name,
            extra_args,
        )
        if not success:
            raise RuntimeError("S3 upload failed")

        if success:
            logger.info(
                f"[+] Uploaded '{filepath}' to 's3://{self.bucket_name}/{object_name}'"
            )

        return success

    async def adownload_file_from_s3(
        self,
        *,
        filepath: str | Path,
        task_id: str,
    ) -> bool:
        """Download a file to S3 asynchronously.

        Parameters
        ----------
        filepath : str | Path
            Path to the file to download.
        task_id : str
            Unique task identifier.

        Returns
        -------
        bool
            True if download succeeded, False otherwise.

        Raises
        ------
        RuntimeError
            If the download fails.
        """
        filepath = Path(filepath) if isinstance(filepath, str) else filepath
        object_name: str = self.get_object_name(task_id)
        success: bool = await self._adownload_from_s3(filepath, object_name)
        if not success:
            raise RuntimeError("S3 download failed")

        if success:
            logger.info(f"[+] Downloaded and saved to '{filepath.absolute()}'")

        return success

    async def aget_s3_stream(self, task_id: str) -> Any:
        """Asynchronously get a streaming body from S3."""
        object_name = self.get_object_name(task_id)
        return await asyncio.to_thread(
            _get_s3_stream, self.s3_client, self.bucket_name, object_name
        )

    async def as3_stream_generator(
        self, streaming_body: Any
    ) -> AsyncGenerator[bytes, None]:
        """Generator to yield chunks from S3 body without blocking the loop."""
        while True:
            # Read in 128KB chunks
            chunk = await asyncio.to_thread(streaming_body.read, 1024 * 128)
            if not chunk:
                break
            yield chunk

    async def acheck_bucket_exists(self) -> bool:
        """Check if the bucket exists and is accessible."""
        try:
            await asyncio.to_thread(self.s3_client.head_bucket, Bucket=self.bucket_name)
            logger.info(f"[+] Successfully verified bucket: {self.bucket_name}")
            return True

        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code")
            if error_code == "404":
                logger.error(f"Bucket {self.bucket_name} does not exist.")
            elif error_code == "403":
                logger.error(f"Access denied to bucket {self.bucket_name}.")
            else:
                logger.error(f"S3 Error: {e}")
            return False

    def get_object_name(self, task_id: str) -> str:
        """Get the S3 object name for a given task ID."""
        return f"logs/{task_id}.log"

    def get_s3_object_url(self, task_id: str) -> str:
        """Get the S3 object URL format."""
        object_name: str = self.get_object_name(task_id)
        return f"{self.s3_client.meta.endpoint_url}/{self.bucket_name}/{object_name}"

    async def aget_presigned_url(self, task_id: str, expiration: int = 3600) -> str:
        """Asynchronously get a presigned URL for the given task ID."""
        object_name = self.get_object_name(task_id)
        return await asyncio.to_thread(
            _generate_presigned_url,
            self.s3_client,
            self.bucket_name,
            object_name,
            expiration,
        )

    async def _aupload_to_s3(
        self,
        filepath: str | Path,
        object_name: str,
        extra_args: UploadResultExtraArgs,
    ) -> bool:
        """Helper function to upload a file to S3 asynchronously."""
        return await asyncio.to_thread(
            _upload_to_s3,
            self.s3_client,
            filepath,
            self.bucket_name,
            object_name,
            extra_args,
        )

    async def _adownload_from_s3(
        self,
        filepath: str | Path,
        object_name: str,
    ) -> bool:
        """Helper function to download a file from S3 asynchronously."""
        return await asyncio.to_thread(
            _download_from_s3,
            self.s3_client,
            filepath,
            self.bucket_name,
            object_name,
        )


# --- S3 File Upload Policy Class with Retry Logic ---
class S3StorageFileUploadPolicy:
    """Policy class for handling S3 file uploads."""

    def __init__(
        self,
        storage_service: S3StorageService,
        max_attempts: int = 3,
        base_delay: float = 1.0,
        raise_on_failure: bool = False,
    ) -> None:
        """Initialize the S3FileUploadPolicy.

        Parameters
        ----------
        storage_service : S3StorageService
            The S3 storage service instance.
        max_attempts : int, optional
            Maximum number of upload attempts, by default 3
        base_delay : float, optional
            Base delay in seconds for exponential backoff, by default 1.0
        raise_on_failure : bool, optional
            Whether to raise an exception on final failure, by default False
        """
        self.storage_service = storage_service
        self.max_attempts = max_attempts
        self.base_delay = base_delay
        self.raise_on_failure = raise_on_failure

    async def aupload_with_retry(
        self,
        filepath: str | Path,
        task_id: str,
        correlation_id: str,
        environment: str,
        max_allowed_size_bytes: int = MAX_FILE_SIZE_BYTES,
    ) -> S3StorageUploadResult:
        """Upload a file to S3 with retry logic.

        Parameters
        ----------
        filepath : str | Path
            Path to the file to upload.
        task_id : str
            Unique task identifier.
        correlation_id : str
            Correlation ID for tracing.
        environment : str
            Application environment (e.g., development, production).
        max_allowed_size_bytes : int, optional
            Maximum allowed file size in bytes, by default
            app_config.pdf_processing_config.max_file_size_bytes

        Returns
        -------
        S3StorageUploadResult
            Result of the upload attempt, including success status and any error messages.
        """
        last_exception: Exception | None = None

        for attempt in range(1, self.max_attempts + 1):
            try:
                success = await self.storage_service.aupload_file_to_s3(
                    filepath=filepath,
                    task_id=task_id,
                    correlation_id=correlation_id,
                    environment=environment,
                    max_allowed_size_bytes=max_allowed_size_bytes,
                )
                if success:
                    return S3StorageUploadResult(
                        attempts=attempt,
                        s3_key=self.storage_service.get_object_name(task_id),
                        s3_url=self.storage_service.get_s3_object_url(task_id),
                    )
            except Exception as e:
                last_exception = e

            if attempt < self.max_attempts:
                delay: float = self.base_delay * attempt
                logger.warning(
                    f"[-] Upload attempt {attempt} for task_id={task_id} failed: {last_exception}. "
                    f"Retrying in {delay} seconds..."
                )
                await asyncio.sleep(delay)

        # All attempts failed
        error_message = (
            f"All {self.max_attempts} upload attempts failed for task_id={task_id}. "
            f"Last error: {last_exception}"
        )
        logger.error(error_message)

        if self.raise_on_failure and last_exception is not None:
            raise last_exception

        return S3StorageUploadResult(
            attempts=self.max_attempts,
            error=error_message,
        )
