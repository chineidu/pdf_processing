# src/celery_app/tasks/processor.py
import tempfile
from pathlib import Path
from typing import Any

import pendulum
from celery import shared_task

from src import create_logger
from src.celery_app import CustomTask
from src.celery_app.event_loop import _get_worker_event_loop
from src.config import app_config
from src.db.models import aget_db_session
from src.db.repositories.task_repository import TaskRepository
from src.schemas.db.models import MetadataResult, TaskSchema
from src.schemas.tasks.processor import ProcessDataTaskResult
from src.schemas.types import ExportFormat, StatusTypeEnum
from src.utilities.utils import MSGSPEC_ENCODER

logger = create_logger(name=__name__)
logger.propagate = False  # This prevents double logging to the root logger

MAX_PAGES: int = app_config.pdf_processing_config.max_num_pages
MAX_SIZE_BYTES: int = app_config.pdf_processing_config.max_file_size_bytes


# -------------------------------------------------------------------
# Helper functions for async operations within the Celery task
# -------------------------------------------------------------------
async def aupdate_task_metadata(
    task_id: str, uploaded_files: dict[str, str], status: StatusTypeEnum
) -> str | None:
    """Update task metadata in database after async processing completes.

    Parameters
    ----------
    task_id : str
        Task identifier.
    uploaded_files : dict[str, str]
        Dictionary mapping format names to S3 URLs.
    status : StatusTypeEnum
        The status to set for the task (e.g., COMPLETED or FAILED).

    Returns
    -------
    str | None
        Webhook URL stored on the task, if present.
    """

    async with aget_db_session() as session:
        task_repo = TaskRepository(db=session)
        updated_task = await task_repo.aupdate_task(
            task_id=task_id,
            update_data={
                "status": status.value,
                "file_result_key": MSGSPEC_ENCODER.encode(uploaded_files).decode(),
            },
            add_completed_at=True,
        )

        if updated_task is None:
            return None

        return updated_task.webhook_url


async def aupdate_task_status(task_id: str, status: StatusTypeEnum) -> None:
    """Update the status of a task without touching other fields."""
    async with aget_db_session() as session:
        task_repo = TaskRepository(db=session)
        await task_repo.aupdate_task(
            task_id=task_id,
            update_data={"status": status.value},
        )


async def aupdate_webhook_delivered_at(task_id: str) -> None:
    """Store the timestamp for a successfully delivered webhook."""

    async with aget_db_session() as session:
        task_repo = TaskRepository(db=session)
        await task_repo.aupdate_task(
            task_id=task_id,
            update_data={"webhook_delivered_at": pendulum.now("UTC")},
        )


async def afetch_task(etag: str) -> TaskSchema | None:
    """Fetch the first completed task by ETag."""

    async with aget_db_session() as session:
        task_repo = TaskRepository(db=session)
        # Get all completed tasks with this etag
        tasks = await task_repo.aget_tasks_by_etag(etag=etag)

        # Return the first completed task if any exist
        if tasks:
            first_task = tasks[0] if isinstance(tasks, list) else tasks
            return TaskSchema.model_validate(first_task)
        return None


async def asend_success_notification(
    etag: str,
    task_id: str,
    uploaded_files: dict[str, str],
    webhook_url: str | None,
    webhook_service: Any,
    metadata: MetadataResult | None = None,
    retries: int | None = None,  # noqa: ARG001 Added for signature consistency with failure notification
) -> dict[str, Any]:
    """Send webhook on task completion."""

    payload = {
        "task_id": task_id,
        "status": StatusTypeEnum.COMPLETED.value,
        "metadata": metadata,
        "completed_at": pendulum.now("UTC").isoformat(),
        "etag": etag,
        "file_result_url": uploaded_files,
        "event_id": f"{task_id}:{pendulum.now('UTC').isoformat()}",
    }
    # Send webhook without using context manager since webhook_service is a singleton
    webhook_sent = await webhook_service.asend_webhook(
        payload=payload,
        webhook_url=webhook_url,
        event_name="task.completed",
    )

    if webhook_sent:
        await aupdate_webhook_delivered_at(task_id)

    return {
        "uploaded_files": uploaded_files,
        "completed_at": pendulum.now("UTC").isoformat(),
    }


async def asend_failure_notification(
    etag: str,
    task_id: str,
    webhook_url: str | None,
    error: Exception,
    webhook_service: Any,
    metadata: MetadataResult | None = None,
    retries: int | None = None,
) -> None:
    """Update DB and send failure webhook on terminal failure."""

    completed_at = pendulum.now("UTC").isoformat()
    payload = {
        "task_id": task_id,
        "status": StatusTypeEnum.FAILED.value,
        "metadata": metadata,
        "completed_at": completed_at,
        "etag": etag,
        "error": str(error),
        "retries": retries,
        "event_id": f"{task_id}:{completed_at}",
    }

    # Send failure webhook without using context manager since webhook_service is a singleton
    webhook_sent = await webhook_service.asend_webhook(
        payload=payload,
        webhook_url=webhook_url,
        event_name="task.failed",
    )

    if webhook_sent:
        await aupdate_webhook_delivered_at(task_id)


async def avalidation_checks(
    etag: str,
    task_id: str,
    metadata: MetadataResult | None,
    webhook_service: Any,
    max_pages: int,
) -> dict[str, Any] | None:
    """Check if page count exceeds maximum allowed pages and handle accordingly.

    Parameters
    ----------
    etag : str
        Entity tag for the task.
    task_id : str
        Task identifier.
    metadata : MetadataResult | None
        Metadata containing page count information.
    webhook_service : Any
        Service for sending webhooks.
    max_pages : int
        Maximum allowed number of pages.

    Returns
    -------
    dict[str, Any] | None
        Result dictionary with uploaded_files and completed_at timestamp, or
        None if page count is within limits.
    """

    if metadata is None:
        return None

    try:
        page_count_value = metadata["page_count"]
        file_size_bytes = metadata["file_size_bytes"]

        if page_count_value is None or file_size_bytes is None:
            return None
        page_count: int = int(page_count_value)
        file_size: int = int(file_size_bytes)

        if page_count > max_pages or file_size > MAX_SIZE_BYTES:
            logger.warning(
                f"Task {task_id} has page count {page_count} which exceeds the maximum of "
                f"{max_pages} OR file size {file_size:,} which exceeds the maximum of "
                f"{MAX_SIZE_BYTES:,} bytes. Marking as completed without processing."
            )

            # Update task metadata in database
            webhook_url = await aupdate_task_metadata(
                task_id, {}, StatusTypeEnum.COMPLETED
            )
            return await asend_success_notification(
                etag=etag,
                task_id=task_id,
                uploaded_files={},
                webhook_url=webhook_url,
                webhook_service=webhook_service,
                metadata=MetadataResult(**metadata),
            )

        # Else:
        # Page count is within limits - continue processing
        logger.info(
            f"Task {task_id} has {page_count} pages (within limit of {max_pages}) "
            f"OR file size {file_size:,} bytes (within limit of {MAX_SIZE_BYTES:,} bytes). "
            "Continuing with processing."
        )
        return None

    except (ValueError, KeyError) as e:
        logger.warning(
            f"Invalid page count in metadata for task_id {task_id}: {metadata.get('page_count')}. Error: {e}"
        )
        # Continue processing despite invalid metadata
        return None


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
    metadata: MetadataResult | None = None,
) -> ProcessDataTaskResult:
    """Celery task to process data"""

    processor = self.processor
    s3_service = self.s3_service
    uploaded_files: dict[str, Any] = {}

    logger.info(f"Started processing task_id: {task_id} with etag: {etag}")

    # Mark task as PROCESSING immediately so idempotency TTL kicks in correctly
    loop = _get_worker_event_loop()
    loop.run_until_complete(
        aupdate_task_status(task_id=task_id, status=StatusTypeEnum.PROCESSING)
    )

    try:
        # Check the page count from metadata if available
        if metadata and metadata["page_count"] is not None:
            metadata_copy = metadata.copy()

            # Add metadata
            page_count_value = metadata_copy["page_count"]
            if isinstance(page_count_value, str):
                page_count_int = int(page_count_value)
            elif isinstance(page_count_value, int):
                page_count_int = page_count_value
            metadata_copy["page_count"] = page_count_int
            file_size_value = metadata_copy["file_size_bytes"]
            file_size_int = int(file_size_value) if file_size_value is not None else 0
            metadata_copy["file_size_bytes"] = file_size_int
            # If page count exceeds limit or file size exceeds limit
            metadata_copy["reason"] = (
                "exceeds_page_limit"
                if page_count_int > MAX_PAGES
                else "exceeds_size_limit"
            )

            loop = _get_worker_event_loop()
            early_termination_result = loop.run_until_complete(
                avalidation_checks(
                    etag=etag,
                    task_id=task_id,
                    metadata=metadata_copy,
                    webhook_service=self.webhook_service,
                    max_pages=MAX_PAGES,
                )
            )
            # Page count exceeds limit
            if early_termination_result:
                uploaded_files = early_termination_result.get("uploaded_files", {})
                completed_at = early_termination_result.get(
                    "completed_at", pendulum.now("UTC").isoformat()
                )

                return ProcessDataTaskResult(
                    status=StatusTypeEnum.COMPLETED.value,
                    success=True,
                    task_id=task_id,
                    completed_at=completed_at,
                    file_result_url=uploaded_files,
                )

        # Else: If no page count metadata, proceed with processing but log a warning
        logger.info(
            f"No page count metadata for task_id {task_id}. Proceeding with processing without "
            "page count check."
        )

        # ------ Normal processing workflow (download, process, upload) ------
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            uploads_dir = tmp_path / "uploads"
            uploads_dir.mkdir(parents=True, exist_ok=True)
            filepath: str = str(uploads_dir / f"{task_id}.pdf")

            # Run all async operations in the same event loop
            async def aprocess_and_update() -> dict[str, Any]:
                """Combined async workflow: download, process, and update database."""

                await s3_service.adownload_file_from_s3(
                    filepath=filepath,
                    task_id=task_id,
                    file_extension=".pdf",
                    operation="input",
                )

                # Process the file and upload results back to S3
                uploaded_files: dict[
                    str, Any
                ] = await processor.aprocess_data_and_upload(
                    source=filepath,
                    s3_service=s3_service,
                    task_id=task_id,
                    export_format=ExportFormat.MARKDOWN,
                )

                # Update task metadata in database
                webhook_url = await aupdate_task_metadata(
                    task_id, uploaded_files, StatusTypeEnum.COMPLETED
                )
                task_metadata = metadata.copy() if metadata is not None else {}
                task_metadata["reason"] = "processed"

                return await asend_success_notification(
                    etag=etag,
                    task_id=task_id,
                    uploaded_files=uploaded_files,
                    webhook_url=webhook_url,
                    webhook_service=self.webhook_service,
                    metadata=MetadataResult(**task_metadata),
                )

            # Get the stable worker event loop
            loop = _get_worker_event_loop()
            result: dict[str, Any] = loop.run_until_complete(aprocess_and_update())

        return ProcessDataTaskResult(
            status=StatusTypeEnum.COMPLETED.value,
            success=True,
            task_id=task_id,
            completed_at=result["completed_at"],
            file_result_url=result["uploaded_files"],
        )

    except Exception as exc:
        logger.error(f"Error processing task_id {task_id}: {exc}", exc_info=True)

        # Check if we've exhausted all retries (terminal failure)
        is_final_failure = self.request.retries >= self.max_retries

        if is_final_failure:
            # Terminal failure - send webhook and update database before giving up
            logger.error(
                f"Terminal failure for task {task_id} after {self.max_retries} retries"
            )

            async def aprocess_failure(error: Exception) -> None:
                """Async workflow to update DB and send failure notification on terminal failure."""
                webhook_url = await aupdate_task_metadata(
                    task_id, uploaded_files={}, status=StatusTypeEnum.FAILED
                )
                await asend_failure_notification(
                    etag=etag,
                    task_id=task_id,
                    webhook_url=webhook_url,
                    error=error,
                    webhook_service=self.webhook_service,
                    retries=self.request.retries,
                )
                return

            loop = _get_worker_event_loop()
            loop.run_until_complete(aprocess_failure(exc))

        raise self.retry(exc=exc) from exc
