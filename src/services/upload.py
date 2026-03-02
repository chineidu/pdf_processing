"""This module provides functionality to upload files (via HTTP) to S3 using presigned URLs."""

from pathlib import Path
from typing import Any
from urllib.parse import urlencode

from anyio import open_file

from src import ROOT, create_logger
from src.config import app_settings
from src.db.models import aget_db_session
from src.db.repositories.task_repository import TaskRepository
from src.schemas.services.upload import UploadedResult
from src.schemas.types import StatusTypeEnum
from src.utilities.client import HTTPXClient
from src.utilities.utils import get_pdf_page_count_from_bytes

logger = create_logger(name=__name__)


async def _aget_presigned_url(
    client: HTTPXClient,
    url: str,
    token: str,
    content_type: str | None = None,
    webhook_url: str | None = None,
    page_count: int | None = None,
) -> dict[str, Any]:
    """Helper function to get a presigned URL for uploading a file to S3.

    Parameters
    ----------
    client : HTTPXClient
        HTTP client for making requests.
    url : str
        Presigned URL endpoint.
    token : str
        Bearer token for authentication.
    content_type : str | None, optional
        MIME type of the file, by default None
    webhook_url : str | None, optional
        Webhook URL for notifications, by default None
    page_count : int | None, optional
        Page count for PDF files (for queue routing), by default None
    """
    headers: dict[str, str] = {
        "accept": "application/json",
        "Authorization": f"Bearer {token}",
    }

    if not client:
        raise ValueError("HTTPXClient is not initialized.")

    # Generate the presigned URL for uploading the file
    _url: str = url
    query_params: dict[str, str] = {}

    if content_type:
        query_params["content_type"] = content_type
    if webhook_url:
        query_params["webhook_url"] = webhook_url
    if page_count is not None:
        query_params["page_count"] = str(page_count)
    if query_params:
        _url = f"{url}?{urlencode(query_params)}"

    result: dict[str, Any] = await client.post(_url, headers=headers)
    return result


async def upload_file_to_s3(
    client: HTTPXClient,
    filepath: str | Path,
    url: str,
    token: str,
    content_type: str | None = None,
    analysis_id: str | None = "unknown",
    webhook_url: str | None = None,
) -> UploadedResult:
    """Uploads a file to S3 and returns the result as a dictionary.

    Detects PDF page count at upload time to enable efficient queue routing
    without requiring re-downloads by workers.
    """
    if isinstance(filepath, str):
        filepath = Path(filepath)
    filename = filepath.name
    file_size_bytes = filepath.stat().st_size

    # Read file bytes once
    async with await open_file(filepath, "rb") as f:
        file_data = await f.read()

    # Detect page count for PDFs (once, at upload time)
    page_count: int | None = None
    if content_type == "application/pdf" or filename.lower().endswith(".pdf"):
        # Run in a thread to avoid blocking the event loop with CPU-bound operation
        page_count = await asyncio.to_thread(get_pdf_page_count_from_bytes, file_data)
        if page_count:
            logger.info(f"Detected {page_count} pages in {filename} for queue routing")

    # Step 1: Get a presigned URL for uploading the file
    result: dict[str, Any] = await _aget_presigned_url(
        client, url, token, content_type, webhook_url, page_count
    )

    # Step 2: Upload the file to S3 using the presigned URL
    response: dict[str, Any] = {}
    if result["success"]:
        presigned_url = result["data"]["url"]
        task_id = result["data"]["taskId"]
        try:
            # Use ONLY the upload headers from the presigned URL response which are already part of
            # the signature
            upload_headers: dict[str, str] = (
                result["data"].get("uploadHeaders", {}).copy()
                if result["data"].get("uploadHeaders")
                else {}
            )

            # Only send headers that are in the PresignedURLResponse
            # Do NOT add extra headers that weren't signed, or S3 will reject with "headers not signed"
            upload_result: dict[str, Any] = await client.put(
                presigned_url,
                content=file_data,
                headers=upload_headers,
            )
            etag = None
            if upload_result.get("headers"):
                etag_value = upload_result.get("headers").get("etag")  # type:ignore
                if etag_value:
                    etag = etag_value.replace('"', "")

            # Step 3: Update the task status in the database based on the upload result
            if upload_result["success"]:
                async with aget_db_session() as session:
                    task_repo = TaskRepository(db=session)
                    update_data: dict[str, Any] = {
                        "status": StatusTypeEnum.UPLOADED,
                        "file_page_count": page_count,
                        "etag": etag,
                        "file_size_bytes": file_size_bytes,
                    }
                    if content_type is not None:
                        update_data["file_type"] = content_type
                    await task_repo.aupdate_task(
                        task_id=task_id,
                        update_data=update_data,
                    )
                logger.info(
                    f"File {filename} uploaded successfully to S3 for analysis_id: {analysis_id}"
                )
                response: dict[str, Any] = {
                    "status": StatusTypeEnum.UPLOADED,
                    "task_id": task_id,
                    "filepath": filepath,
                    "filename": filename,
                    "file_size_bytes": file_size_bytes,
                    "content_type": content_type,
                    "page_count": page_count,
                    "etag": etag,
                }
            else:
                logger.error(
                    f"Failed to upload file {filename} to S3: {upload_result.get('error', 'Unknown error')}"
                )
                response = {
                    "status": StatusTypeEnum.FAILED,
                    "task_id": task_id,
                    "filepath": filepath,
                    "filename": filename,
                    "file_size_bytes": file_size_bytes,
                    "content_type": content_type,
                    "page_count": page_count,
                    "etag": etag,
                    "error": upload_result.get("error", "Unknown error"),
                }
        except Exception as e:
            logger.error(f"Error uploading file {filename} to S3: {e}")
            response = {
                "status": StatusTypeEnum.FAILED,
                "task_id": task_id,
                "filepath": filepath,
                "filename": filename,
                "file_size_bytes": None,
                "content_type": content_type,
                "page_count": None,
                "etag": None,
                "error": str(e),
            }
    else:
        logger.error(
            f"Failed to get presigned URL: {result.get('error', 'Unknown error')}"
        )
        response = {
            "status": StatusTypeEnum.FAILED,
            "task_id": None,
            "filepath": filepath,
            "filename": filename,
            "file_size_bytes": file_size_bytes,
            "content_type": content_type,
            "page_count": page_count,
            "etag": None,
            "error": result.get("error", "Failed to get presigned URL"),
        }
    return UploadedResult(**response)


# -------------------------------------------------------------------
# For testing purposes
# -------------------------------------------------------------------
client = HTTPXClient()


async def test_upload() -> None:
    """Test the upload_file_to_s3 function with a sample file."""

    filepath: Path = ROOT / Path("data/football_report.pdf")

    analysis_id = "unknown"
    base_url: str = "http://0.0.0.0:8000"
    url = f"{base_url}/api/v1/presigned-urls"
    content_type = "application/pdf"

    async with client:
        # OAuth2PasswordRequestForm expects form-encoded data, not JSON
        token_dict = await client.post(
            f"{base_url}/api/v1/auth/token",
            data={"username": "example_user", "password": "password"},
        )
        if not token_dict["success"]:
            logger.error(f"Authentication failed: {token_dict}")
            return

        token = token_dict["data"]["access_token"]
        result = await upload_file_to_s3(
            client,
            filepath,
            url,
            token,
            content_type,
            analysis_id,
            webhook_url=app_settings.WEBHOOK_URL,
        )
        print(result)


if __name__ == "__main__":
    import asyncio

    # Example usage
    asyncio.run(test_upload())
