from pathlib import Path
from typing import Any
from urllib.parse import urlencode

from anyio import open_file

from src import ROOT, create_logger
from src.db.models import aget_db_session
from src.db.repositories.task_repository import TaskRepository
from src.schemas.services.upload import UploadedResult
from src.schemas.types import StatusTypeEnum
from src.utilities.client import HTTPXClient

logger = create_logger(name=__name__)


async def upload_file_to_s3(
    client: HTTPXClient,
    filepath: str | Path,
    url: str,
    token: str,
    content_type: str | None = None,
    analysis_id: str | None = "unknown",
) -> UploadedResult:
    """Uploads a file to S3 and returns the result as a dictionary."""

    headers: dict[str, str] = {
        "accept": "application/json",
        "Authorization": f"Bearer {token}",
    }

    if not client:
        raise ValueError("HTTPXClient is not initialized.")

    # Generate the presigned URL for uploading the file
    presign_url = url
    if content_type:
        presign_url = f"{url}?{urlencode({'content_type': content_type})}"

    result: dict[str, Any] = await client.post(presign_url, headers=headers)

    # Upload the file to S3 using the presigned URL
    if result["success"]:
        presigned_url = result["data"]["url"]
        task_id = result["data"]["taskId"]
        try:
            if isinstance(filepath, str):
                filepath = Path(filepath)
            filename = filepath.name
            file_size_bytes = filepath.stat().st_size
            async with await open_file(filepath, "rb") as f:
                file_data = await f.read()
            upload_headers = {"Content-Type": content_type} if content_type else None
            upload_result = await client.put(
                presigned_url, content=file_data, headers=upload_headers
            )
            etag = (
                upload_result.get("headers").get("etag").replace('"', "")  # type:ignore
                if upload_result.get("headers")
                else None
            )

            if upload_result["success"]:
                async with aget_db_session() as session:
                    task_repo = TaskRepository(db=session)
                    update_data: dict[str, Any] = {
                        "status": StatusTypeEnum.UPLOADED,
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
                    f"File {filepath} uploaded successfully to S3 for analysis_id: {analysis_id}"
                )
                response: dict[str, Any] = {
                    "status": StatusTypeEnum.UPLOADED,
                    "task_id": task_id,
                    "filepath": filepath,
                    "filename": filename,
                    "file_size_bytes": file_size_bytes,
                    "content_type": content_type,
                    "etag": etag,
                }
            else:
                logger.error(
                    f"Failed to upload file {filepath} to S3: {upload_result['error']}"
                )
                response = {
                    "status": StatusTypeEnum.FAILED,
                    "task_id": task_id,
                    "filepath": filepath,
                    "filename": filename,
                    "file_size_bytes": file_size_bytes,
                    "content_type": content_type,
                    "etag": etag,
                    "error": upload_result["error"],
                }
        except Exception as e:
            logger.error(f"Error uploading file {filepath} to S3: {e}")
            response = {
                "status": StatusTypeEnum.FAILED,
                "task_id": task_id,
                "filepath": filepath,
                "filename": Path(filepath).name
                if isinstance(filepath, (str, Path))
                else None,
                "file_size_bytes": None,
                "content_type": content_type,
                "etag": None,
                "error": str(e),
            }
    return UploadedResult(**response)  # type:ignore


# For testing purposes
client = HTTPXClient()


async def test_upload() -> None:
    """Test the upload_file_to_s3 function with a sample file."""

    filepath: Path = ROOT / Path("data/ML_role.pdf")
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
            client, filepath, url, token, content_type, analysis_id
        )
        print(result)


if __name__ == "__main__":
    import asyncio

    # Example usage
    asyncio.run(test_upload())
