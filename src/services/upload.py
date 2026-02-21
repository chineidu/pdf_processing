from pathlib import Path
from typing import Any
from urllib.parse import urlencode

from anyio import open_file

from src import ROOT, create_logger
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
        try:
            if isinstance(filepath, str):
                filepath = Path(filepath)
            async with await open_file(filepath, "rb") as f:
                file_data = await f.read()
            upload_headers = {"Content-Type": content_type} if content_type else None
            upload_result = await client.put(
                presigned_url, content=file_data, headers=upload_headers
            )
            if upload_result["success"]:
                logger.info(
                    f"File {filepath} uploaded successfully to S3 for analysis_id: {analysis_id}"
                )
                response: dict[str, Any] = {
                    "status": StatusTypeEnum.UPLOADED,
                    "task_id": analysis_id,
                    "filepath": filepath,
                }
            else:
                logger.error(
                    f"Failed to upload file {filepath} to S3: {upload_result['error']}"
                )
                response = {
                    "status": StatusTypeEnum.FAILED,
                    "task_id": analysis_id,
                    "filepath": filepath,
                    "error": upload_result["error"],
                }
        except Exception as e:
            logger.error(f"Error uploading file {filepath} to S3: {e}")
            response = {
                "status": StatusTypeEnum.FAILED,
                "task_id": analysis_id,
                "filepath": filepath,
                "error": str(e),
            }
    return UploadedResult(**response)  # type:ignore


# For testing purposes
client = HTTPXClient()


async def test_upload() -> None:
    """Test the upload_file_to_s3 function with a sample file."""

    filepath: Path = ROOT / Path(
        "data/Machine Learning System Design With end-to-end examples.pdf"
    )
    analysis_id = "unknown"
    url = "http://0.0.0.0:8000/api/v1/presigned-urls"
    content_type = "application/pdf"
    token = (
        "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJleGFtcGxlX3VzZXIiLCJ0aWVyIjoiZnJlZSIsImV4cCI6MTc"
        "3MTYwNDAwMn0.eu8aWYyG05FalUncn7SDQBK4sXtZ-S5Dtm-98fg3idI"
    )
    async with client:
        result = await upload_file_to_s3(
            client, filepath, url, token, content_type, analysis_id
        )
        print(result)


if __name__ == "__main__":
    import asyncio

    # Example usage
    asyncio.run(test_upload())
