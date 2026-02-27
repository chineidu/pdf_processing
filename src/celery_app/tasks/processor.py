import tempfile
from pathlib import Path
from typing import Any

import pendulum
from celery import shared_task

from src import create_logger
from src.celery_app import CustomTask
from src.celery_app.event_loop import _get_worker_event_loop
from src.db.repositories.task_repository import TaskRepository
from src.schemas.types import ExportFormat, StatusTypeEnum
from src.services.storage import S3StorageService
from src.utilities.utils import MSGSPEC_ENCODER

logger = create_logger(name=__name__)
logger.propagate = False  # This prevents double logging to the root logger


# -------------------------------------------------------------------
# Helper functions for async operations within the Celery task
# -------------------------------------------------------------------
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


async def aupdate_task_metadata(
    task_id: str, uploaded_files: dict[str, str]
) -> str | None:
    """Update task metadata in database after async processing completes.

    Parameters
    ----------
    task_id : str
        Task identifier.
    uploaded_files : dict[str, str]
        Dictionary mapping format names to S3 URLs.

    Returns
    -------
    str | None
        Webhook URL stored on the task, if present.
    """
    from src.db.models import aget_db_session

    async with aget_db_session() as session:
        task_repo = TaskRepository(db=session)
        updated_task = await task_repo.aupdate_task(
            task_id=task_id,
            update_data={
                "status": StatusTypeEnum.COMPLETED,
                "file_result_key": MSGSPEC_ENCODER.encode(uploaded_files).decode(),
            },
            add_completed_at=True,
        )

        if updated_task is None:
            return None

        return updated_task.webhook_url


async def aupdate_webhook_delivered_at(task_id: str) -> None:
    """Store the timestamp for a successfully delivered webhook."""
    from src.db.models import aget_db_session

    async with aget_db_session() as session:
        task_repo = TaskRepository(db=session)
        await task_repo.aupdate_task(
            task_id=task_id,
            update_data={"webhook_delivered_at": pendulum.now("UTC")},
        )


# -------------------------------------------------------------------
# Main processing task
# -------------------------------------------------------------------
# Note: When `bind=True`, celery automatically passes the task instance as the first argument
# meaning that we need to use `self` and this provides additional functionality like retries, etc
@shared_task(bind=True, base=CustomTask)
def process_data(
    self,  # noqa: ANN001
    etag: str,
    task_id: str,
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

    logger.info(f"Started processing task_id: {task_id} with etag: {etag}")
    try:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            uploads_dir = tmp_path / "uploads"
            uploads_dir.mkdir(parents=True, exist_ok=True)
            filepath: str = str(uploads_dir / f"{task_id}.pdf")

            # Get the stable worker event loop
            loop = _get_worker_event_loop()

            # Run all async operations in the same event loop
            async def aprocess_and_update() -> dict[str, Any]:
                """Combined async workflow: download, process, and update database."""
                # Download the file from S3
                await adownload_file(
                    s3_service=s3_service,
                    filepath=filepath,
                    task_id=task_id,
                    file_extension=".pdf",
                    operation="input",
                )

                # Process the file and upload results back to S3
                uploaded_files = await processor.aprocess_data_and_upload(
                    source=filepath,
                    s3_service=s3_service,
                    task_id=task_id,
                    export_format=ExportFormat.MARKDOWN,
                )

                # Update task metadata in database
                webhook_url = await aupdate_task_metadata(task_id, uploaded_files)

                completed_at = pendulum.now("UTC").isoformat()
                payload = {
                    "task_id": task_id,
                    "status": StatusTypeEnum.COMPLETED.value,
                    "completed_at": completed_at,
                    "etag": etag,
                    "download_urls": uploaded_files,
                    "event_id": f"{task_id}:{completed_at}",
                }

                async with self.webhook_service as webhook_service:
                    webhook_sent = await webhook_service.asend_webhook(
                        payload=payload,
                        webhook_url=webhook_url,
                        event_name="task.completed",
                    )

                if webhook_sent:
                    await aupdate_webhook_delivered_at(task_id)

                return {
                    "uploaded_files": uploaded_files,
                    "completed_at": completed_at,
                }

            result: dict[str, Any] = loop.run_until_complete(aprocess_and_update())
            uploaded_files = result["uploaded_files"]
            completed_at = result["completed_at"]

        success = True
        return {
            "status": StatusTypeEnum.COMPLETED.value
            if success
            else StatusTypeEnum.FAILED.value,
            "success": success,
            "task_id": task_id,
            "completed_at": completed_at,
            "download_urls": uploaded_files,
        }

    except Exception as e:
        logger.error(f"Error processing task_id {task_id}: {e}", exc_info=True)

        # Check if we've exhausted all retries (terminal failure)
        is_final_failure = self.request.retries >= self.max_retries

        if is_final_failure:
            # Terminal failure - send webhook and update database before giving up
            logger.error(
                f"Terminal failure for task {task_id} after {self.max_retries} retries"
            )

            async def asend_failure_notification(error: Exception) -> None:
                """Update DB and send failure webhook on terminal failure."""
                from src.db.models import aget_db_session

                # Update task status to failed
                async with aget_db_session() as session:
                    task_repo = TaskRepository(db=session)
                    updated_task = await task_repo.aupdate_task(
                        task_id=task_id,
                        update_data={
                            "status": StatusTypeEnum.FAILED.value,
                            "error_message": str(error),
                        },
                        add_completed_at=True,
                    )

                    webhook_url = updated_task.webhook_url if updated_task else None

                # Send failure webhook
                completed_at = pendulum.now("UTC").isoformat()
                payload = {
                    "task_id": task_id,
                    "status": StatusTypeEnum.FAILED.value,
                    "completed_at": completed_at,
                    "error": str(error),
                    "retries": self.request.retries,
                    "event_id": f"{task_id}:{completed_at}",
                }

                async with self.webhook_service as webhook_service:
                    webhook_sent = await webhook_service.asend_webhook(
                        payload=payload,
                        webhook_url=webhook_url,
                        event_name="task.failed",
                    )

                if webhook_sent:
                    await aupdate_webhook_delivered_at(task_id)

            loop = _get_worker_event_loop()
            loop.run_until_complete(asend_failure_notification(e))

        raise self.retry(exc=e) from e
