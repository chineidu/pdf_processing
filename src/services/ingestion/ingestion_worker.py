"""Ingestion Worker Service"""

import asyncio
import signal
import sys
from datetime import datetime, timedelta, timezone
from pathlib import PurePosixPath
from typing import TYPE_CHECKING, Any, Awaitable, Callable, TypeAlias
from urllib.parse import unquote

import aio_pika
from sqlalchemy import select

from src import create_logger
from src.celery_app.app import celery_app
from src.celery_app.tasks.processor import (
    asend_failure_notification,
    asend_success_notification,
)
from src.config import app_config, app_settings
from src.db.models import DBTask, aget_db_session
from src.db.repositories.task_repository import TaskRepository
from src.schemas.db.models import MetadataResult
from src.schemas.services.ingestion_worker import QueueArguments, StorageEventPayload
from src.schemas.types import (
    IDEMPOTENCY_ACTIVE_STATUSES,
    DBUpdateReasonEnum,
    StatusTypeEnum,
)
from src.services.ingestion.base import BaseRabbitMQ
from src.services.ingestion.utilities import (
    get_queue_and_priority,
    map_priority_enum_to_int,
)
from src.services.storage import S3StorageService
from src.services.webhook import WebhookService
from src.utilities.utils import MSGSPEC_DECODER

webhook_service = WebhookService()

if TYPE_CHECKING:
    from src.config.config import AppConfig

logger = create_logger(name=__name__)

# ----- Configuration -----
STORAGE_RMQ_URL: str = app_settings.rabbitmq_storage_url
DEFAULT_NUM_PAGES: int = 50  # Default page count if metadata is missing
DEFAULT_FILE_SIZE: int = 10_000_000  # Default file size if metadata is missing

# Parse inflight recheck delays from config (e.g., "2,3" -> [2, 3])
INFLIGHT_RECHECK_DELAYS: list[int] = [
    int(d.strip()) for d in app_settings.INFLIGHT_RECHECK_DELAYS.split(",") if d.strip()
]

CallbackType: TypeAlias = Callable[
    [StorageEventPayload, dict[str, Any]], Awaitable[None]
]


def _decode_file_result_key(raw: str | None, task_id: str) -> dict[str, str]:
    """Safely decode a JSON string. If decoding fails, logs a warning and
    returns an empty dict to prevent downstream errors.
    """
    if not raw:
        return {}
    try:
        return MSGSPEC_DECODER.decode(raw)
    except Exception as exc:
        logger.warning(
            f"Could not decode for task {task_id} "
            f"(value truncated or invalid): {exc}. Falling back to empty dict."
        )
        return {}


def _extract_task_id_from_storage_key(storage_key: str) -> str | None:
    """Extract task_id from an object key like `uploads/<task_id>.pdf`.

    Handles keys that may include bucket prefix such as
    `pdf-processor/uploads/<task_id>.pdf`.
    """
    # URL-decode the storage key to handle any encoded characters (e.g. spaces, special chars)
    normalized_key = unquote(storage_key)
    name = PurePosixPath(normalized_key).name
    if not name.lower().endswith(".pdf"):
        return None
    task_id = name[:-4]
    return task_id or None


async def _await_and_recheck_inflight(
    idempotent_task: DBTask,
    derived_task_id: str,
    etag: str,
    task_repo: TaskRepository,
) -> bool:
    """Wait for an in-flight task to complete using exponential backoff.

    When a duplicate upload arrives while the original task is still PROCESSING
    or VALIDATING, this function waits with exponential backoff delays to see
    if the original completes. If it does, we copy its result to the new task
    instead of dispatching duplicate work.

    Parameters
    ----------
    idempotent_task : DBTask
        The original in-flight task found by idempotency check.
    derived_task_id : str
        The task_id of the new duplicate task.
    etag : str
        The file etag shared by both tasks.
    task_repo : TaskRepository
        Database repository for task updates.

    Returns
    -------
    bool
        True if the task was handled (completed/skipped/failed), False if it
        still needs to be dispatched.
    """
    total_wait = sum(INFLIGHT_RECHECK_DELAYS)
    logger.info(
        f"Task with etag={etag} is in-flight (status={idempotent_task.status}). "
        f"Waiting up to {total_wait}s to check if it completes..."
    )

    for attempt, delay in enumerate(INFLIGHT_RECHECK_DELAYS, start=1):
        await asyncio.sleep(delay)
        elapsed = sum(INFLIGHT_RECHECK_DELAYS[:attempt])

        # Re-query the ORIGINAL task to see its current state
        updated_task = await task_repo.aget_task_by_task_id(idempotent_task.task_id)

        # If the original task disappeared (e.g., DB issue), we should not wait further as
        # it may never complete. Log a warning and proceed to dispatch.
        if updated_task is None:
            logger.warning(
                f"Original task {idempotent_task.task_id} not found after {elapsed}s. "
                f"Dispatching duplicate {derived_task_id} as backup."
            )
            return False

        # A: Original completed successfully - copy result
        if updated_task.status == StatusTypeEnum.COMPLETED.value:
            logger.info(
                f"Original task {idempotent_task.task_id} completed after {elapsed}s. "
                f"Copying result to {derived_task_id}."
            )
            # Task completion
            await task_repo.aupdate_task(
                task_id=derived_task_id,
                update_data={
                    "file_result_key": updated_task.file_result_key,
                    "status": StatusTypeEnum.COMPLETED.value,
                    "_metadata": MetadataResult(
                        page_count=updated_task.file_page_count,
                        file_size_bytes=updated_task.file_size_bytes,
                        reason=DBUpdateReasonEnum.IDENTICAL_DATA.value,
                    ),
                },
                add_completed_at=True,
            )
            await asend_success_notification(
                etag=etag,
                task_id=derived_task_id,
                uploaded_files=_decode_file_result_key(
                    updated_task.file_result_key, updated_task.task_id
                ),
                webhook_url=updated_task.webhook_url,
                webhook_service=webhook_service,
                metadata=MetadataResult(
                    page_count=updated_task.file_page_count,
                    file_size_bytes=updated_task.file_size_bytes,
                    reason=DBUpdateReasonEnum.IDENTICAL_DATA.value,
                ),
            )
            return True

        # B: Original failed deterministically - copy terminal state
        if updated_task.status in {
            StatusTypeEnum.SKIPPED.value,
            StatusTypeEnum.UNPROCESSABLE.value,
        }:
            logger.info(
                f"Original task {idempotent_task.task_id} reached terminal status "
                f"{updated_task.status} after {elapsed}s. Copying to {derived_task_id}."
            )
            # Task completion
            await task_repo.aupdate_task(
                task_id=derived_task_id,
                update_data={
                    "file_result_key": updated_task.file_result_key,
                    "status": StatusTypeEnum.SKIPPED.value,
                    "_metadata": MetadataResult(
                        page_count=updated_task.file_page_count,
                        file_size_bytes=updated_task.file_size_bytes,
                        reason=DBUpdateReasonEnum.IDENTICAL_DATA.value,
                    ),
                },
                add_completed_at=True,
            )
            await asend_failure_notification(
                etag=etag,
                task_id=derived_task_id,
                error=Exception(updated_task.error_message),
                webhook_url=updated_task.webhook_url,
                webhook_service=webhook_service,
                metadata=MetadataResult(
                    page_count=updated_task.file_page_count,
                    file_size_bytes=updated_task.file_size_bytes,
                    reason=DBUpdateReasonEnum.IDENTICAL_DATA.value,
                ),
            )
            return True

        # Task still in-flight; dispatch (safety net)
        if updated_task.status not in {
            StatusTypeEnum.PROCESSING.value,
            StatusTypeEnum.VALIDATING.value,
            StatusTypeEnum.UPLOADED.value,
        }:
            logger.info(
                f"Original task {idempotent_task.task_id} now has status "
                f"{updated_task.status} after {elapsed}s. Dispatching {derived_task_id} as retry."
            )
            return False

        # Still in-flight, continue to next backoff iteration
        logger.info(
            f"Original task {idempotent_task.task_id} still {updated_task.status} "
            f"after {elapsed}s, continuing to wait..."
        )

    # Exhausted all backoff attempts, still in-flight - dispatch as backup
    logger.info(
        f"Original task {idempotent_task.task_id} still in-flight after {total_wait}s. "
        f"Dispatching {derived_task_id} as backup to prevent orphaning."
    )
    return False


async def acheck_idempotency(
    etag: str, current_task_id: str | None = None
) -> DBTask | None:
    """Check if a task with the same etag is already being processed or has been completed.

    Parameters
    ----------
    etag : str
        The file etag to check for duplicates.
    current_task_id : str | None, optional
        The task_id of the current task being processed — excluded from
        the duplicate check to avoid self-matching, by default None.

    Returns
    -------
    DBTask | None
        An existing active/completed task with the same etag, or None.
    """
    async with aget_db_session() as session:
        cutoff = datetime.now(timezone.utc) - timedelta(days=10)
        conditions = [
            DBTask.etag == etag,
            DBTask.status.in_(IDEMPOTENCY_ACTIVE_STATUSES),
            DBTask.created_at >= cutoff,
        ]
        # Exclude the current task to avoid self-matching after
        # the worker sets it to VALIDATING above
        if current_task_id:
            conditions.append(DBTask.task_id != current_task_id)

        stmt = (
            select(DBTask)
            .where(*conditions)
            .order_by(DBTask.created_at.desc())
            .limit(1)
        )
        result = await session.execute(stmt)
        return result.scalar_one_or_none()


def dispatch_to_celery_process_data_task(
    etag: str,
    task_id: str,
    metadata: MetadataResult | None = None,
    queue: str = "celery",
    priority: int = 5,
) -> None:
    """Synchronous function to send the task to Celery. This will be executed in a thread pool.

    Parameters
    ----------
    etag : str
        The file etag for idempotency tracking.
    task_id : str
        The task identifier.
    metadata : MetadataResult | None, optional
        Additional metadata for the task, by default None
    queue : str, optional
        The queue to dispatch to, by default "celery"
    priority : int, optional
        The priority level (1-10, where 10 is highest), by default 5
    """
    celery_app.send_task(
        "src.celery_app.tasks.processor.orchestrate_pdf_processing",
        kwargs={
            "task_id": task_id,
            "etag": etag,
            "metadata": metadata,
        },
        queue=queue,
        priority=priority,
    )
    logger.info(
        f"Successfully dispatched Celery task for S3 key: {etag} to queue: {queue}"
    )


# ----- Business logic callback -----
async def aprocess_pdf_documents(
    payload: StorageEventPayload, context: dict[str, Any]
) -> None:
    """
    Processes each record in the payload, performs idempotency check,
    determines queue based on page count, and dispatches to Celery.
    """
    task_id: str = context.get("task_id", "unknown")
    correlation_id: str = context.get("correlation_id", "unknown")
    timestamp: str = context.get("timestamp", "unknown")

    logger.info(
        f"Processing message | task_id={task_id} | correlation_id={correlation_id} | time={timestamp}"
    )

    async with aget_db_session() as session:
        task_repo = TaskRepository(db=session)

        for record in payload.records:
            etag: str = record.storage_entity.object.etag.replace('"', "").strip()
            storage_key: str = record.storage_entity.object.key
            object_size_bytes: int | None = record.storage_entity.object.size
            derived_task_id: str | None = (
                _extract_task_id_from_storage_key(storage_key) or task_id
            )

            # Skip output files (only process input files. i.e. files uploaded by the client,
            # not files generated by the processing pipeline)
            if "/output/" in storage_key:
                logger.warning(f"Skipping output file: {storage_key}")
                continue

            # Filetype check (PDF only)
            if not storage_key.lower().endswith(".pdf"):
                logger.warning(f"Skipping non-PDF file: {storage_key}")
                continue

            # Update state
            await task_repo.aupdate_task(
                task_id=derived_task_id,
                update_data={"status": StatusTypeEnum.VALIDATING.value},
            )
            # Yield to ensure DB update is committed before idempotency check
            await asyncio.sleep(0.1)

            # ----- Idempotency check  -----
            # This checks for any active task (VALIDATING, PROCESSING, or COMPLETED)
            logger.info(
                f"Checking for duplicates: task_id={derived_task_id}, etag={etag}"
            )
            idempotent_task = await acheck_idempotency(
                etag, current_task_id=derived_task_id
            )
            is_idempotent = idempotent_task is not None
            logger.info(
                f"Idempotency check for task_id={derived_task_id}, etag={etag}: "
                f"Is duplicate? {is_idempotent}. "
            )
            if idempotent_task:
                # A: If completed, skip
                if idempotent_task.status == StatusTypeEnum.COMPLETED.value:
                    logger.info(
                        f"Duplicate detected for task_id={derived_task_id} with etag={etag}. "
                        f"Existing status: {idempotent_task.status}."
                    )
                    # Task completion
                    await task_repo.aupdate_task(
                        task_id=derived_task_id,
                        update_data={
                            "file_result_key": idempotent_task.file_result_key,
                            "status": StatusTypeEnum.COMPLETED.value,
                            "_metadata": MetadataResult(
                                page_count=idempotent_task.file_page_count,
                                file_size_bytes=idempotent_task.file_size_bytes,
                                reason=DBUpdateReasonEnum.IDENTICAL_DATA.value,
                            ),
                        },
                        add_completed_at=True,
                    )
                    await asend_success_notification(
                        etag=etag,
                        task_id=derived_task_id,
                        uploaded_files=_decode_file_result_key(
                            idempotent_task.file_result_key, idempotent_task.task_id
                        ),
                        webhook_url=idempotent_task.webhook_url,
                        webhook_service=webhook_service,
                        metadata=MetadataResult(
                            page_count=idempotent_task.file_page_count,
                            file_size_bytes=idempotent_task.file_size_bytes,
                            reason=DBUpdateReasonEnum.IDENTICAL_DATA.value,
                        ),
                    )
                    continue

                # B: If the existing task skipped/unprocessable, we can also skip re-processing.
                if idempotent_task.status in {
                    StatusTypeEnum.SKIPPED.value,
                    StatusTypeEnum.UNPROCESSABLE.value,
                }:
                    logger.info(
                        f"Existing task with etag={etag} has terminal status {idempotent_task.status}. "
                        f"Skipping re-processing."
                    )
                    # Task completion
                    await task_repo.aupdate_task(
                        task_id=derived_task_id,
                        update_data={
                            "file_result_key": idempotent_task.file_result_key,
                            "status": StatusTypeEnum.SKIPPED.value,
                            "_metadata": MetadataResult(
                                page_count=idempotent_task.file_page_count,
                                file_size_bytes=idempotent_task.file_size_bytes,
                                reason=DBUpdateReasonEnum.IDENTICAL_DATA.value,
                            ),
                        },
                        add_completed_at=True,
                    )
                    await asend_failure_notification(
                        etag=etag,
                        task_id=derived_task_id,
                        error=Exception(idempotent_task.error_message),
                        webhook_url=idempotent_task.webhook_url,
                        webhook_service=webhook_service,
                        metadata=MetadataResult(
                            page_count=idempotent_task.file_page_count,
                            file_size_bytes=idempotent_task.file_size_bytes,
                            reason=DBUpdateReasonEnum.IDENTICAL_DATA.value,
                        ),
                    )
                    continue

                # C1: PROCESSING/VALIDATING - wait and recheck to avoid duplicate processing
                if idempotent_task.status in {
                    StatusTypeEnum.PROCESSING.value,
                    StatusTypeEnum.VALIDATING.value,
                }:
                    # Wait with exponential backoff to see if original completes
                    handled = await _await_and_recheck_inflight(
                        idempotent_task=idempotent_task,
                        derived_task_id=derived_task_id,
                        etag=etag,
                        task_repo=task_repo,
                    )
                    if handled:
                        # Task was completed/skipped by original, no dispatch needed
                        continue
                    # else: still in-flight after all retries, fall through to dispatch

                # C2: UPLOADED - immediate dispatch (race condition recovery, no wait)
                if idempotent_task.status == StatusTypeEnum.UPLOADED.value:
                    logger.info(
                        f"Task with etag={etag} found in UPLOADED state (likely race condition). "
                        f"Dispatching immediately to recover."
                    )

                # Shared dispatch logic for C1 (after wait) and C2 (immediate)
                # Runs if _await_and_recheck_inflight returned False (i.e. it was not handled) or
                # if the status was UPLOADED to begin with.
                if idempotent_task.status in {
                    StatusTypeEnum.UPLOADED.value,
                    StatusTypeEnum.PROCESSING.value,
                    StatusTypeEnum.VALIDATING.value,
                }:
                    # Use existing task's page count for routing
                    num_pages = idempotent_task.file_page_count or DEFAULT_NUM_PAGES
                    file_size_bytes = idempotent_task.file_size_bytes

                    # If file size is missing from DB, fall back to S3 metadata if available
                    if (
                        (file_size_bytes is None or file_size_bytes <= 0)
                        and object_size_bytes is not None
                        and object_size_bytes > 0
                    ):
                        file_size_bytes = object_size_bytes

                    try:
                        queue_info = get_queue_and_priority(num_pages)
                        metadata: dict[str, Any] = {"page_count": num_pages}
                        if file_size_bytes is not None and file_size_bytes > 0:
                            metadata["file_size_bytes"] = file_size_bytes

                        await asyncio.to_thread(
                            dispatch_to_celery_process_data_task,
                            etag=etag,
                            task_id=derived_task_id,
                            metadata=MetadataResult(**metadata),
                            queue=queue_info.queue_name,
                            priority=queue_info.priority,
                        )
                        logger.info(
                            f"Successfully dispatched in-flight recovery task {derived_task_id} "
                            f"to queue {queue_info.queue_name}"
                        )
                    except Exception as e:
                        logger.error(
                            f"Failed to dispatch in-flight recovery task {derived_task_id}: {e}",
                            exc_info=True,
                        )
                    continue

                # Else (safety net): idempotent_task exists but has an unrecognised status.
                # Log a warning and re-dispatch rather than silently orphaning the task.
                logger.warning(
                    f"Unhandled idempotent task status '{idempotent_task.status}' "
                    f"for task_id={derived_task_id} (etag={etag}). "
                    f"Re-dispatching as a safety measure."
                )
                num_pages = idempotent_task.file_page_count or DEFAULT_NUM_PAGES
                file_size_bytes = idempotent_task.file_size_bytes

                # If file size is missing from DB, fall back to S3 metadata if available
                if (
                    (file_size_bytes is None or file_size_bytes <= 0)
                    and object_size_bytes is not None
                    and object_size_bytes > 0
                ):
                    file_size_bytes = object_size_bytes
                try:
                    queue_info = get_queue_and_priority(num_pages)
                    safety_metadata: dict[str, Any] = {"page_count": num_pages}
                    if file_size_bytes is not None and file_size_bytes > 0:
                        safety_metadata["file_size_bytes"] = file_size_bytes
                    await asyncio.to_thread(
                        dispatch_to_celery_process_data_task,
                        etag=etag,
                        task_id=derived_task_id,
                        metadata=MetadataResult(**safety_metadata),
                        queue=queue_info.queue_name,
                        priority=queue_info.priority,
                    )
                except Exception as e:
                    logger.error(
                        f"Safety-net re-dispatch failed for task {derived_task_id}: {e}",
                        exc_info=True,
                    )

            else:
                logger.info(
                    f"No duplicates found for task_id={derived_task_id}, proceeding with processing."
                )

                # ----- For fresh tasks -----
                # Use existing task data if available, otherwise fetch from S3 metadata
                metadata: dict[str, Any] = {}
                new_task = await task_repo.aget_task_by_task_id(derived_task_id)
                # new_task = await aget_new_task(derived_task_id)

                is_new_task = new_task is not None
                logger.info(
                    f"Idempotency check for task_id={derived_task_id}, etag={etag}: "
                    f"Is new task? {is_new_task}. "
                )

                try:
                    num_pages: int | None = None
                    file_size_bytes: int | None = None

                    # Get info
                    if new_task:
                        num_pages = new_task.file_page_count
                        file_size_bytes = new_task.file_size_bytes
                        logger.info(
                            f"New task {derived_task_id} [{new_task.status}]: "
                            f"{num_pages} pages, {file_size_bytes} bytes"
                        )

                    if num_pages is None:
                        logger.warning(
                            f"Missing page count for task_id={derived_task_id}. "
                            f"Falling back to default page count: {DEFAULT_NUM_PAGES}."
                        )
                        num_pages = DEFAULT_NUM_PAGES

                    metadata["page_count"] = num_pages

                    # If file size is missing from DB, fall back to S3 metadata if available
                    if (
                        (file_size_bytes is None or file_size_bytes <= 0)
                        and object_size_bytes is not None
                        and object_size_bytes > 0
                    ):
                        file_size_bytes = object_size_bytes

                    if file_size_bytes is None or file_size_bytes <= 0:
                        logger.warning(
                            f"Missing file size for task_id={derived_task_id}. "
                            f"Falling back to default file size: {DEFAULT_FILE_SIZE}."
                        )
                        file_size_bytes = DEFAULT_FILE_SIZE

                    metadata["file_size_bytes"] = file_size_bytes

                    queue_info = get_queue_and_priority(num_pages)

                    logger.info(
                        f"task_id: {derived_task_id}: {num_pages} pages -> "
                        f"queue: {queue_info.queue_name}, priority: {queue_info.priority}"
                    )

                    # Dispatch to Celery in a thread to avoid blocking the event loop
                    await asyncio.to_thread(
                        dispatch_to_celery_process_data_task,
                        etag=etag,
                        task_id=derived_task_id,
                        metadata=MetadataResult(**metadata),
                        queue=queue_info.queue_name,
                        priority=queue_info.priority,
                    )

                except Exception as e:
                    logger.error(
                        f"Error routing PDF {derived_task_id}: {e}",
                        exc_info=True,
                    )
                    # Task completion
                    await task_repo.aupdate_task(
                        task_id=derived_task_id,
                        update_data={
                            "status": StatusTypeEnum.FAILED.value,
                            "_metadata": MetadataResult(
                                reason=DBUpdateReasonEnum.SYSTEM_ERROR.value
                            ),
                            "error_message": str(e)[
                                :1_000
                            ],  # Truncate error message to prevent DB issues
                        },
                        add_completed_at=True,
                    )


# ----- RabbitMQ Consumer -----
class IngestionWorker(BaseRabbitMQ):
    """Worker that listens to RabbitMQ for storage events."""

    def __init__(self, config: "AppConfig", url: str = STORAGE_RMQ_URL) -> None:
        super().__init__(config=config, url=url)

        # Event to signal shutdown across async tasks
        self._shutdown_event = asyncio.Event()

    async def consume(
        self, queue_name: str, callback: CallbackType, durable: bool = True
    ) -> None:
        """Processes incoming storage events from RabbitMQ.

        Parameters
        ----------
        queue_name : str
            The name of the RabbitMQ queue to consume from.
        callback : CallbackType
            The async callback function to process each consumed message.
        durable : bool, optional
            Whether the queue should be durable (survive broker restarts), by default True.

        Returns
        -------
        None
        """

        # --------------- Infrastructure Setup ---------------
        await self.aconnect()
        assert self.channel is not None, "Channel is not established."

        await self.aensure_dlq(
            dlq_name=self.config.rabbitmq_config.dlq_config.dlq_name,
            dlx_name=self.config.rabbitmq_config.dlq_config.dlx_name,
        )

        priority = self.config.rabbitmq_config.queue_priority
        priority_int = map_priority_enum_to_int(priority)
        max_retries: int = self.config.rabbitmq_config.max_retries

        # --------------- Setup queues ---------------
        queue = await self.aensure_queue(
            queue_name=queue_name,
            # Attach dead-letter exchange for failed messages
            arguments=QueueArguments(
                x_dead_letter_exchange=self.config.rabbitmq_config.dlq_config.dlx_name,
                x_max_priority=priority_int,
            ),
            durable=durable,
        )
        # Wait queue with N ttl for retries
        delay_queue_name = f"{queue_name}_delay"
        await self.aensure_delay_queue(
            delay_queue_name=delay_queue_name,
            target_queue_name=queue_name,
            ttl_ms=self.config.rabbitmq_config.dlq_config.ttl,
        )

        # Topic exchange: Routes messages based on routing key patterns
        # MinIO publishes events like "s3.objectcreated.put" -> we subscribe to "s3.objectcreated.#"
        # This lets us receive all S3 object creation events regardless of the upload method
        routing_key: str = "s3.objectcreated.#"
        storage_topic: str = self.config.rabbitmq_config.topic_names.storage_topic
        storage_exchange = await self.aensure_topic(storage_topic, durable=False)
        await queue.bind(storage_exchange, routing_key=routing_key)
        logger.info(
            f"Bound queue '{queue_name}' to exchange '{storage_topic}' with routing key '{routing_key}'"
        )

        # Manually consume messages with an async iterator with more control over
        # acknowledgment and error handling
        logger.info(
            f"Storage worker is ready and listening for MinIO events on queue '{queue_name}'"
        )
        async with queue.iterator(no_ack=False) as queue_iter:
            async for message in queue_iter:
                logger.info(f"📨 Received message: routing_key={message.routing_key}")

                # Check if shutdown was requested
                if self._shutdown_event.is_set():
                    logger.info("[+] Shutdown detected, breaking message loop")
                    break

                # Extract metadata for logging
                task_id: str = "unknown"
                headers: dict[str, Any] = message.headers or {}
                task_id = str(headers.get("task_id", "unknown"))
                correlation_id: str = message.correlation_id or "unknown"
                timestamp: datetime | None = message.timestamp

                context: dict[str, Any] = {
                    "task_id": task_id,
                    "correlation_id": correlation_id,
                    "timestamp": timestamp,
                }

                try:
                    # Parse and validate the incoming message
                    raw_data: str = message.body.decode()
                    logger.info(
                        f"📦 Raw message payload: {raw_data[:500]} ...[TRUNCATED]"
                    )
                    event_payload = StorageEventPayload.model_validate_json(raw_data)
                    logger.info(
                        "Successfully parsed storage event payload, invoking callback..."
                    )

                    # ----------- Invoke the business logic callback -----------
                    await callback(event_payload, context)
                    await (
                        message.ack()
                    )  # Acknowledge message only after successful processing
                    logger.info("Message processed and acknowledged successfully")

                except Exception as e:
                    logger.error(f"Error processing storage event: {e}", exc_info=True)
                    retry_count_raw = (message.headers or {}).get("x-retry-count", 0)
                    try:
                        if isinstance(retry_count_raw, (bytes, bytearray)):
                            retry_count = int(retry_count_raw.decode())
                        else:
                            retry_count = int(str(retry_count_raw))
                    except (TypeError, ValueError, UnicodeDecodeError):
                        retry_count = 0
                    if retry_count >= max_retries:
                        logger.error(
                            f"Retries exhausted for task_id={task_id} "
                            f"(retry_count={retry_count}, max_retries={max_retries}). "
                            "Routing message to DLQ."
                        )
                        await message.nack(requeue=False)  # -> DLQ
                    else:
                        next_retry_count: int = retry_count + 1
                        retry_headers: dict[str, Any] = dict(message.headers or {})
                        retry_headers["x-retry-count"] = next_retry_count

                        retry_message = aio_pika.Message(
                            body=message.body,
                            headers=retry_headers,
                            content_type=message.content_type,
                            content_encoding=message.content_encoding,
                            correlation_id=message.correlation_id,
                            message_id=message.message_id,
                            timestamp=message.timestamp,
                            type=message.type,
                            app_id=message.app_id,
                            priority=message.priority,
                            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                        )

                        try:
                            assert self.channel is not None, (
                                "Channel is not established."
                            )
                            await self.channel.default_exchange.publish(
                                retry_message,
                                routing_key=delay_queue_name,
                            )
                            # ACK original only after delayed retry message is safely republished.
                            await message.ack()
                            logger.warning(
                                f"Scheduled retry {next_retry_count}/{max_retries} for task_id={task_id} "
                                f"via delay queue '{delay_queue_name}'."
                            )
                        except Exception as publish_error:
                            logger.error(
                                f"Failed to publish delayed retry for task_id={task_id}: {publish_error}. "
                                "Routing original message to DLQ.",
                                exc_info=True,
                            )
                            await message.nack(requeue=False)  # -> DLQ


async def run_worker(callback: CallbackType) -> None:
    """Run the RabbitMQ consumer worker with graceful shutdown.

    Parameters
    ----------
    callback : CallbackType
        The async callback function to process each consumed message.

    Returns
    -------
    None
    """

    consumer = IngestionWorker(app_config)

    # Setup signal handlers for graceful shutdown
    def signal_handler(sig: int) -> None:
        """Handle shutdown signals. This sets the shutdown event to stop consuming.

        Parameters
        ----------
        sig : int
            The signal number received.
        """
        logger.info(f"[+] Received signal {sig}, initiating graceful shutdown...")
        consumer._shutdown_event.set()

    # Register signal handlers
    loop = asyncio.get_event_loop()
    # Ctrl+C and termination signals
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda s=sig: signal_handler(s))

    try:
        # --------------- Start Consuming Messages ---------------
        # Consume with async callback using context manager
        async with consumer.aconnection_context():
            # Create consume task for consuming messages from task queue and processing via callback
            consume_task = asyncio.create_task(
                consumer.consume(
                    queue_name=app_config.rabbitmq_config.queue_names.task_queue,
                    callback=callback,
                    durable=True,  # Queue survives broker restarts
                )
            )

            # Wait/block the event loop for shutdown event. If a shutdown signal is NOT received, this will
            # run indefinitely keeping the consumer alive to process messages.
            await consumer._shutdown_event.wait()

            # --------------- Graceful Shutdown ---------------
            # Cancel consume task
            logger.info("[+] Cancelling consume task...")
            consume_task.cancel()
            try:
                await consume_task
            except asyncio.CancelledError:
                logger.info("[+] Consume task cancelled successfully")

        logger.info("[+] Consumer shutdown complete")

    except asyncio.CancelledError:
        logger.info("[+] Consumer cancelled, cleaning up...")

    except Exception as e:
        logger.error(f"[x] Error in main: {e}", exc_info=True)
        raise


async def main(callback: CallbackType) -> None:
    """Entry point to run the consumer with S3 bucket check.

    Parameters
    ----------
    callback : CallbackType
        The async callback function to process each consumed message.

    Returns
    -------
    None
    """
    s3_service = S3StorageService()
    if not await s3_service.acheck_bucket_exists():
        logger.error("[x] S3 bucket not accessible")
        sys.exit(1)
    logger.info("[+] S3 bucket is accessible, starting consumer...")

    await run_worker(callback)


if __name__ == "__main__":
    try:
        asyncio.run(main(callback=aprocess_pdf_documents))
        logger.info("[+] Exiting gracefully")

    except KeyboardInterrupt:
        logger.info("[+] Received KeyboardInterrupt, exiting...")
        sys.exit(0)

    except Exception as e:
        logger.error(f"[x] Fatal error running consumer: {e}")
        sys.exit(1)
