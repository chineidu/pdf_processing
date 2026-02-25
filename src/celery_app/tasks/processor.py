import asyncio
import json
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from celery import shared_task

from src import create_logger
from src.celery_app import CustomTask
from src.db.repositories.task_repository import TaskRepository
from src.schemas.types import ExportFormat, StatusTypeEnum
from src.services.storage import S3StorageService

logger = create_logger(name=__name__)
logger.propagate = False  # This prevents double logging to the root logger


async def adownload_file(
    s3_service: S3StorageService,
    filepath: str,
    task_id: str,
    file_extension: str,
    operation: str = "input",
) -> None:
    """Helper to download a file from S3 using S3StorageService.

    This is used within the Celery task to fetch the file for processing.
    """
    success = await s3_service.adownload_file_from_s3(
        filepath=filepath,
        task_id=task_id,
        file_extension=file_extension,
        operation=operation,
    )
    if success:
        logger.info("Downloaded file!")
    else:
        logger.error("Failed to download file from S3.")


# Note: When `bind=True`, celery automatically passes the task instance as the first argument
# meaning that we need to use `self` and this provides additional functionality like retries, etc
@shared_task(bind=True, base=CustomTask)
def process_data(
    self,  # noqa: ANN001
    etag: str,
    task_id: str,
    analysis_id: str = "unknown",
) -> dict[str, Any]:
    """Celery task to process data"""
    processor = self.processor
    s3_service = self.s3_service

    # Check if task_id is present in the DB
    # Download the file from S3 using the task_id (which is the same as the S3 key)
    # Validate the file (e.g. check if it's a valid PDF, check file size, etc.)
    # Process the file (e.g. extract text, run analysis, etc.)
    # Store the results metadata back in the DB (e.g. status, file size, number of pages, etc.)
    # Store the raw results back in S3 (e.g. extracted text, analysis results, etc.)
    # Return the summary of the results (e.g. status, file size, number of pages, etc.)

    logger.info(
        f"Started processing task_id: {task_id}, analysis_id: {analysis_id} with etag: {etag}"
    )
    try:
        time.sleep(1)  # Simulate some processing time

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            uploads_dir = tmp_path / "uploads"
            uploads_dir.mkdir(parents=True, exist_ok=True)
            filepath: str = str(uploads_dir / f"{task_id}.pdf")

            # Download the file from S3 to a temporary location for processing
            asyncio.run(
                adownload_file(
                    s3_service=s3_service,
                    filepath=filepath,
                    task_id=task_id,
                    file_extension=".pdf",
                    operation="input",
                )
            )

            # Process the file and upload results back to S3
            uploaded_files = asyncio.run(
                processor.aprocess_data_and_upload(
                    source=filepath,
                    s3_service=s3_service,
                    task_id=task_id,
                    export_format=ExportFormat.MARKDOWN,
                )
            )
        # Use the sync db_session context manager from CustomTask
        with self.db_session() as session:
            task_repo = TaskRepository(db=session)
            # Run async method synchronously
            asyncio.run(
                task_repo.aupdate_task(
                    task_id=task_id,
                    update_data={
                        "status": StatusTypeEnum.COMPLETED,
                        "file_result_key": json.dumps(uploaded_files),
                    },
                    add_completed_at=True,
                )
            )

        success = True
        return {
            "status": StatusTypeEnum.COMPLETED.value
            if success
            else StatusTypeEnum.FAILED.value,
            "success": success,
            "task_id": task_id,
            "analysis_id": analysis_id,
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "download_urls": uploaded_files,
        }

    except Exception as e:
        logger.error(f"Error processing task_id {task_id}: {e}", exc_info=True)
        raise self.retry(exc=e) from e
