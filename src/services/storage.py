"""S3/MinIO storage service for managing cloud storage operations."""

import asyncio
from pathlib import Path
from typing import Any, AsyncGenerator, BinaryIO

import boto3
import pendulum
from botocore.client import Config
from botocore.exceptions import BotoCoreError, ClientError

from src import create_logger
from src.config import app_config, app_settings
from src.schemas.services.storage import (
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
) -> str | None:
    task_id: str = extra_args.Metadata.task_id
    try:
        client.upload_file(
            str(filepath),
            bucket_name,
            object_name,
            ExtraArgs=extra_args.model_dump(),
        )
        # Retrieve object metedata to capture the ETag (content hash) and confirm upload
        response = _get_metadata(client, bucket_name, object_name)
        # ETags are usually enclosed in quotes, so we strip them for consistency
        return response.get("ETag", "").strip('"')

    except ClientError as e:
        logger.error(f"[x] Failed to upload log for task {task_id}: {e}")
        return None

    except BotoCoreError as e:
        logger.error(f"[x] BotoCore error uploading log for task {task_id}: {e}")
        return None

    except Exception as exc:
        logger.error(f"[x] Unexpected error uploading log for task {task_id}: {exc}")
        return None


def _upload_fileobj_to_s3(
    client: Any,
    fileobj: BinaryIO,
    bucket_name: str,
    object_name: str,
    extra_args: UploadResultExtraArgs,
) -> str | None:
    task_id: str = extra_args.Metadata.task_id

    try:
        fileobj.seek(0)

        client.upload_fileobj(
            Fileobj=fileobj,
            Bucket=bucket_name,
            Key=object_name,
            ExtraArgs=extra_args.model_dump(),
        )

        response = _get_metadata(client, bucket_name, object_name)
        return response.get("ETag", "").strip('"')

    except ClientError as e:
        logger.error(f"[x] Failed to upload log for task {task_id}: {e}")
        return None

    except BotoCoreError as e:
        logger.error(f"[x] BotoCore error uploading log for task {task_id}: {e}")
        return None

    except Exception as exc:
        logger.error(f"[x] Unexpected error uploading log for task {task_id}: {exc}")
        return None


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
    client: Any,
    bucket_name: str,
    object_name: str,
    expiration: int = 3600,
    content_type: str | None = None,
    metadata: dict[str, str] | None = None,
) -> dict[str, str]:
    """Generate a presigned URL for the given S3 object."""
    try:
        params: dict[str, Any] = {"Bucket": bucket_name, "Key": object_name}
        if content_type:
            params["ContentType"] = content_type
        if metadata:
            params["Metadata"] = metadata
        url = client.generate_presigned_url(
            "put_object",
            Params=params,
            ExpiresIn=expiration,
        )
        return {
            "url": url,
            "expires_at": pendulum.now("UTC").add(seconds=expiration).isoformat(),
        }
    except ClientError as e:
        logger.error(f"Failed to generate presigned URL: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error generating presigned URL: {e}")
        raise


def _get_metadata(client: Any, bucket_name: str, object_name: str) -> dict[str, Any]:
    """Helper function to get metadata from S3."""
    return client.head_object(Bucket=bucket_name, Key=object_name)


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
                    max_pool_connections=50,  # Connection pooling for better performance
                    retries={"max_attempts": MAX_ATTEMPTS, "mode": "adaptive"},
                    signature_version="s3v4",
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
        operation: str = "input",
        max_allowed_size_bytes: int = MAX_FILE_SIZE_BYTES,
    ) -> str | None:
        """Upload a file to S3 asynchronously.

        Wraps synchronous boto3 upload in asyncio.to_thread() to avoid blocking the event loop.
        File extension is extracted from filepath; S3 object key is derived as
        `uploads/<task_id>/<operation>/<task_id>.<extension>`.

        Parameters
        ----------
        filepath : str | Path
            Path to the file to upload. Extension is automatically extracted.
        task_id : str
            Unique task identifier used in the S3 object key and metadata.
        correlation_id : str
            Correlation ID for tracing; stored in S3 object metadata.
        operation : str, optional
            Operation type: 'input', 'output', 'temp', etc., by default "input"
        max_allowed_size_bytes : int, optional
            Maximum allowed file size in bytes, by default
            app_config.pdf_processing_config.max_file_size_bytes

        Returns
        -------
        str | None
            The ETag of the uploaded object if successful, or None if the upload failed.

        Raises
        ------
        ValueError
            If the file size exceeds the maximum allowed size.
        RuntimeError
            If the upload fails after size validation.

        Notes
        -----
        - File metadata (task_id, correlation_id, timestamp) is embedded in S3 object metadata.
        - Uses asyncio.to_thread() to run blocking boto3 calls asynchronously.
        - ETag is extracted from HEAD object response after upload confirmation.
        """
        filepath = Path(filepath) if isinstance(filepath, str) else filepath
        file_extension = filepath.suffix  # e.g., '.pdf'
        # Check the file size
        file_size = filepath.stat().st_size
        if file_size > max_allowed_size_bytes:
            raise ValueError(
                f"Log size: {file_size:,} bytes exceeds max: {max_allowed_size_bytes:,} bytes"
            )

        object_name = self.get_object_name(task_id, file_extension, operation)
        extra_args = UploadResultExtraArgs(
            Metadata=S3UploadMetadata(
                task_id=task_id,
                uploaded_at=pendulum.now("UTC").isoformat(),
                correlation_id=correlation_id,
                service="pdf_processing",
            ),
            ACL="private",
            ContentType="text/plain",
        )
        etag = await self._aupload_to_s3(
            filepath,
            object_name,
            extra_args,
        )
        if not etag:
            raise RuntimeError("S3 upload failed")

        if etag:
            logger.info(
                f"[+] Uploaded '{filepath}' to 's3://{self.bucket_name}/{object_name}'"
            )

        return etag

    async def aupload_fileobj_to_s3(
        self,
        *,
        fileobj: BinaryIO,
        task_id: str,
        correlation_id: str,
        file_extension: str,
        operation: str = "input",
        max_allowed_size_bytes: int = MAX_FILE_SIZE_BYTES,
    ) -> str | None:
        """Upload a file-like object to S3 asynchronously.

        Wraps synchronous boto3 upload in asyncio.to_thread() to avoid blocking the event loop.
        File size is determined by seeking to EOF; pointer is reset by the helper before upload.
        S3 object key is derived as `uploads/<task_id>/<operation>/<task_id>.<extension>`.

        Parameters
        ----------
        fileobj : BinaryIO
            File-like object to upload. Pointer position will be modified during size check
            and upload; callers should not assume pointer position after this call.
        task_id : str
            Unique task identifier used in the S3 object key and metadata.
        correlation_id : str
            Correlation ID for tracing; stored in S3 object metadata.
        file_extension : str
            File extension (e.g., '.pdf', '.csv', '.json'). Leading dot optional.
        operation : str, optional
            Operation type: 'input', 'output', 'temp', etc., by default "input"
        max_allowed_size_bytes : int, optional
            Maximum allowed file size in bytes, by default
            app_config.pdf_processing_config.max_file_size_bytes

        Returns
        -------
        str | None
            The ETag of the uploaded object if successful, or None if the upload failed.

        Raises
        ------
        ValueError
            If the file size exceeds the maximum allowed size.
        RuntimeError
            If the upload fails after size validation.

        Notes
        -----
        - File pointer position is modified: seeked to EOF for size check, then reset to 0 by helper.
        - File metadata (task_id, correlation_id, timestamp) is embedded in S3 object metadata.
        - Uses asyncio.to_thread() to run blocking boto3 calls asynchronously.
        - ETag is extracted from HEAD object response after upload confirmation.
        - Do not assume file pointer position after this call—seek or reset as needed.
        """
        # Check the file size by seeking to the end and getting the position
        fileobj.seek(0, 2)  # Move to end of file
        file_size = fileobj.tell()
        if file_size > max_allowed_size_bytes:
            raise ValueError(
                f"Log size: {file_size:,} bytes exceeds max: {max_allowed_size_bytes:,} bytes"
            )

        # Normalize file extension
        if not file_extension.startswith("."):
            file_extension = f".{file_extension}"
        object_name = self.get_object_name(task_id, file_extension, operation)
        extra_args = UploadResultExtraArgs(
            Metadata=S3UploadMetadata(
                task_id=task_id,
                uploaded_at=pendulum.now("UTC").isoformat(),
                correlation_id=correlation_id,
                service="pdf_processing",
            ),
            ACL="private",
            ContentType="text/plain",
        )
        etag = await asyncio.to_thread(
            _upload_fileobj_to_s3,
            self.s3_client,
            fileobj,
            self.bucket_name,
            object_name,
            extra_args,
        )
        if not etag:
            raise RuntimeError("S3 upload failed")

        if etag:
            logger.info(
                f"[+] Uploaded file-like object to 's3://{self.bucket_name}/{object_name}'"
            )

        return etag

    async def adownload_file_from_s3(
        self,
        *,
        filepath: str | Path,
        task_id: str,
        file_extension: str,
        operation: str = "input",
    ) -> bool:
        """Download a file from S3 asynchronously.

        S3 object key is derived as `uploads/<task_id>/<operation>/<task_id>.<extension>`.

        Parameters
        ----------
        filepath : str | Path
            Path where downloaded file will be saved.
        task_id : str
            Unique task identifier.
        file_extension : str
            File extension (e.g., '.pdf', '.csv'). Leading dot optional.
        operation : str, optional
            Operation type: 'input', 'output', 'temp', etc., by default "input"


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
        object_name: str = self.get_object_name(task_id, file_extension, operation)
        success: bool = await self._adownload_from_s3(filepath, object_name)
        if not success:
            raise RuntimeError("S3 download failed")

        if success:
            logger.info(f"[+] Downloaded and saved to '{filepath.absolute()}'")

        return success

    async def aget_s3_stream(
        self, task_id: str, file_extension: str, operation: str = "input"
    ) -> Any:
        """Asynchronously get a streaming body from S3.

        Parameters
        ----------
        task_id : str
            Unique task identifier.
        file_extension : str
            File extension (e.g., '.pdf', '.csv').
        operation : str, optional
            Operation type: 'input', 'output', 'temp', etc., by default "input"
        """
        object_name = self.get_object_name(task_id, file_extension, operation)
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

    def get_object_name(
        self, task_id: str, file_extension: str, operation: str = "input"
    ) -> str:
        """Get the S3 object name for a given task ID, file extension, and operation.

        Parameters
        ----------
        task_id : str
            Unique task identifier.
        file_extension : str
            File extension (e.g., '.pdf', '.csv'). Leading dot optional.
        operation : str, optional
            Operation type: 'input', 'output', 'temp', etc., by default "input"

        Returns
        -------
        str
            S3 object key: `uploads/{operation}/{task_id}{file_extension}`

        Examples
        --------
        >>> get_object_name('abc-123', '.pdf')
        'uploads/input/abc-123.pdf'

        >>> get_object_name('abc-123', 'csv', operation='output')
        'uploads/output/abc-123.csv'
        """
        if not file_extension.startswith("."):
            file_extension = f".{file_extension}"
        return f"uploads/{operation}/{task_id}{file_extension}"

    def get_s3_object_url(
        self, task_id: str, file_extension: str, operation: str = "input"
    ) -> str:
        """Get the S3 object URL format.

        Parameters
        ----------
        task_id : str
            Unique task identifier.
        file_extension : str
            File extension (e.g., '.pdf', '.csv').
        operation : str, optional
            Operation type: 'input', 'output', 'temp', etc., by default "input"

        Returns
        -------
        str
            Full S3 URL: `{endpoint}/{bucket}/{object_key}`
        """
        object_name: str = self.get_object_name(task_id, file_extension, operation)
        return f"{self.s3_client.meta.endpoint_url}/{self.bucket_name}/{object_name}"

    async def aget_presigned_url(
        self,
        task_id: str,
        file_extension: str,
        operation: str = "input",
        expiration: int = 3600,
        content_type: str | None = None,
        metadata: dict[str, str] | None = None,
    ) -> dict[str, str]:
        """Asynchronously get a presigned URL for the given task ID.

        Parameters
        ----------
        task_id : str
            Unique task identifier.
        file_extension : str
            File extension (e.g., '.pdf', '.csv').
        operation : str, optional
            Operation type: 'input', 'output', 'temp', etc., by default "input"
        expiration : int, optional
            URL expiration time in seconds, by default 3600
        content_type : str, optional
            MIME type for the object, by default None
        metadata : dict[str, str] | None, optional
            S3 object metadata to sign with the upload, by default None
        """
        object_name = self.get_object_name(task_id, file_extension, operation)
        return await asyncio.to_thread(
            _generate_presigned_url,
            self.s3_client,
            self.bucket_name,
            object_name,
            expiration,
            content_type,
            metadata,
        )

    async def aget_object_metadata(
        self,
        *,
        task_id: str,
        file_extension: str,
        operation: str = "input",
    ) -> dict[str, str]:
        """Get object metadata without downloading the file.

        Returns
        -------
        dict[str, str]
            Metadata keys and values. Returns empty dict if not found.
        """
        object_name = self.get_object_name(task_id, file_extension, operation)
        try:
            response = await asyncio.to_thread(
                _get_metadata,
                self.s3_client,
                self.bucket_name,
                object_name,
            )
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code")
            if error_code == "404":
                logger.warning(
                    f"[x] Object not found for metadata lookup: bucket={self.bucket_name}, key={object_name}"
                )
                return {}
            logger.error(
                f"[x] S3 error reading metadata: bucket={self.bucket_name}, key={object_name}, error={e}"
            )
            return {}
        return response.get("Metadata", {}) or {}

    async def _aupload_to_s3(
        self,
        filepath: str | Path,
        object_name: str,
        extra_args: UploadResultExtraArgs,
    ) -> str | None:
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
