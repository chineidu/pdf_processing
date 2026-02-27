"""Ingestion Worker Service"""

import asyncio
import signal
import sys
from datetime import datetime, timedelta, timezone
from pathlib import PurePosixPath
from typing import TYPE_CHECKING, Any, Awaitable, Callable
from urllib.parse import unquote

from sqlalchemy import select

from src import create_logger
from src.celery_app.app import celery_app
from src.config import app_config, app_settings
from src.db.models import DBTask, aget_db_session
from src.db.repositories.task_repository import TaskRepository
from src.schemas.services.ingestion_worker import QueueArguments, StorageEventPayload
from src.schemas.types import IDEMPOTENCY_ACTIVE_STATUSES, StatusTypeEnum
from src.services.ingestion.base import BaseRabbitMQ
from src.services.ingestion.utilities import map_priority_enum_to_int
from src.services.storage import S3StorageService

if TYPE_CHECKING:
    from src.config.config import AppConfig

logger = create_logger(name=__name__)

# ----- Configuration -----
STORAGE_RMQ_URL: str = app_settings.rabbitmq_storage_url

CallbackType = Callable[[StorageEventPayload, dict[str, Any]], Awaitable[None]]


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


async def check_idempotency(etag: str) -> bool:
    """Queries the database using the indexed etag. Returns True if the file has already
    been processed or is pending."""
    async with aget_db_session() as session:
        cutoff = datetime.now(timezone.utc) - timedelta(days=10)
        stmt = (
            select(DBTask.id)
            .where(
                DBTask.etag == etag,
                DBTask.status.in_(IDEMPOTENCY_ACTIVE_STATUSES),
                DBTask.created_at >= cutoff,
            )
            .limit(1)
        )

        result = await session.execute(stmt)
        return result.scalar_one_or_none() is not None


def dispatch_to_celery(task_id: str, etag: str) -> None:
    """Synchronous function to send the task to Celery. This will be executed in a thread pool."""
    celery_app.send_task(
        "src.celery_app.tasks.processor.process_data",
        kwargs={
            "task_id": task_id,
            "etag": etag,
        },
    )
    logger.info(f"Successfully dispatched Celery task for S3 key: {etag}")


# ----- Business logic callback -----
async def aprocess_pdf_documents(
    payload: StorageEventPayload, context: dict[str, Any]
) -> None:
    """
    Processes each record in the payload, performs idempotency check,
    and dispatches valid PDF files to Celery.
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
            etag: str = record.storage_entity.object.etag
            storage_key: str = record.storage_entity.object.key
            derived_task_id = _extract_task_id_from_storage_key(storage_key)
            task_id_to_update = derived_task_id or task_id

            # Skip output files (only process input files)
            if "/output/" in storage_key:
                logger.info(f"Skipping output file: {storage_key}")
                continue

            # Filetype check (PDF only)
            if not storage_key.lower().endswith(".pdf"):
                logger.warning(f"Skipping non-PDF file: {storage_key}")
                continue

            # Idempotency check using the etag
            is_duplicate = await check_idempotency(etag)
            if is_duplicate:
                logger.info(
                    f"Duplicate file detected (etag: {etag}), skipping processing."
                )
                await task_repo.aupdate_task(
                    task_id=task_id_to_update,
                    update_data={"status": StatusTypeEnum.SKIPPED.value},
                )
                continue

            # Update to validating only if not duplicate
            await task_repo.aupdate_task(
                task_id=task_id_to_update,
                update_data={"status": StatusTypeEnum.VALIDATING.value},
            )

            # Dispatch to Celery in a thread to avoid blocking the event loop
            await asyncio.to_thread(dispatch_to_celery, task_id_to_update, etag)


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

        priority = app_config.rabbitmq_config.queue_priority
        priority_int = map_priority_enum_to_int(priority)

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
        logger.info("🔄 Starting message consumption loop...")
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
                    # Re-raising the exception will cause the message to be NACKed, allowing for retries
                    raise


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
    from src.config import app_config

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
