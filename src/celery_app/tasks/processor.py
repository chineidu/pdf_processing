import asyncio
import base64
import tempfile
from pathlib import Path
from typing import Any

import fitz  # PyMuPDF
import pendulum
from celery import chord, group, shared_task
from celery.exceptions import Ignore
from celery.result import allow_join_result

from src import create_logger
from src.celery_app import CustomTask
from src.celery_app.event_loop import _get_worker_event_loop
from src.config import app_config
from src.db.models import aget_db_session
from src.db.repositories.task_repository import TaskRepository
from src.schemas.db.models import MetadataResult, TaskSchema
from src.schemas.tasks.processor import ProcessDataTaskResult
from src.schemas.types import (
    DBUpdateReasonEnum,
    ExportFormat,
    PDFOutputResult,
    StatusTypeEnum,
    TaskProgressMessageEnum,
)
from src.utilities.utils import MSGSPEC_DECODER, json_dumps

logger = create_logger(name=__name__)
logger.propagate = False  # This prevents double logging to the root logger

MAX_PAGES: int = app_config.pdf_processing_config.max_num_pages
CHUNK_SIZE: int = app_config.pdf_processing_config.chunk_size
MAX_SIZE_BYTES: int = app_config.pdf_processing_config.max_file_size_bytes


def _normalize_progress_percentage(progress_percentage: int) -> int:
    """Clamp progress percentage to the inclusive range [0, 100]."""
    return max(0, min(100, int(progress_percentage)))


def _get_task_routing_options(task: Any) -> dict[str, Any]:
    """Infer queue/routing key/priority from the currently executing Celery task."""

    request = getattr(task, "request", None)
    if request is None:
        return {}

    delivery_info = getattr(request, "delivery_info", {})
    properties = getattr(request, "properties", {})

    routing_key = delivery_info.get("routing_key") or delivery_info.get("queue")
    options: dict[str, Any] = {}

    if routing_key:
        options["queue"] = routing_key
        options["routing_key"] = routing_key

    priority = properties.get("priority")
    if priority is None:
        priority = delivery_info.get("priority")

    if priority is not None:
        options["priority"] = int(priority)

    return options


def _encode_processed_outputs(outputs: dict[str, bytes]) -> dict[str, str]:
    """Convert bytes outputs into base64 strings for broker-safe task payloads."""
    # Encode bytes to base64 strings so Celery/RabbitMQ can transport them safely
    return {
        key: base64.b64encode(value).decode("ascii") for key, value in outputs.items()
    }


def _decode_processed_outputs(outputs: dict[str, str]) -> dict[str, bytes]:
    """Decode base64 task payload outputs back to raw bytes."""
    # Decode base64-encoded outputs back to raw bytes for post-processing
    return {
        key: base64.b64decode(value.encode("ascii")) for key, value in outputs.items()
    }


def _normalize_processed_outputs(outputs: PDFOutputResult) -> dict[str, bytes]:
    """Flatten PDFOutputResult into broker-safe byte payloads.

    Celery transports a flat key/value map, so nested table outputs are expanded
    to top-level table_N keys and empty values are dropped.
    """

    normalized_outputs: dict[str, bytes] = {}

    for key in ("doctags", "json", "markdown", "text"):
        value = outputs.get(key)
        if value is None:
            # Skip empty values to avoid unnecessary task payload bloat
            continue
        # Else: encode string values to bytes if needed, and include in payload
        normalized_outputs[key] = (
            value.encode("utf-8") if isinstance(value, str) else value
        )

    for table_key, table_value in outputs.get("tables", {}).items():
        if table_value is None:
            continue
        normalized_outputs[table_key] = (
            table_value.encode("utf-8") if isinstance(table_value, str) else table_value
        )

    return normalized_outputs


def _chunk_page_ranges(total_pages: int, chunk_size: int) -> list[tuple[int, int, int]]:
    """Build (chunk_idx, start_page, end_page) ranges from total page count."""
    if total_pages <= 0:
        return []

    safe_chunk_size = max(1, chunk_size)
    # chunk_idx, start_page, end_page
    ranges: list[tuple[int, int, int]] = []
    chunk_idx = 1
    start_page = 0

    while start_page < total_pages:
        end_page = min(start_page + safe_chunk_size - 1, total_pages - 1)
        ranges.append((chunk_idx, start_page, end_page))
        chunk_idx += 1
        # Move to the next page (of the next chunk)
        start_page = end_page + 1

    return ranges


def _sort_table_keys(output_keys: list[str]) -> list[str]:
    """Sort table keys like table_1, table_2 numerically."""

    def _table_index(key: str) -> int:
        try:
            return int(key.split("_", 1)[1])
        except (IndexError, ValueError):
            return 0

    # Ensure table keys are ordered numerically (table_1, table_2, ...)
    return sorted(output_keys, key=_table_index)


# -------------------------------------------------------------------
# Helper functions for async operations within the Celery task
# -------------------------------------------------------------------
async def aupdate_task_metadata(
    task_id: str,
    uploaded_files: dict[str, str],
    status: StatusTypeEnum,
    update_data: dict[str, Any] | None = None,
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
    update_data : dict[str, Any] | None
        Additional fields to update in the task record, if any.

    Returns
    -------
    str | None
        Webhook URL stored on the task, if present.
    """
    # Persist final file URLs and status to the DB, return any webhook URL
    terminal_metadata_message = (
        TaskProgressMessageEnum.COMPLETED
        if status == StatusTypeEnum.COMPLETED
        else TaskProgressMessageEnum.FAILED
    )
    merged_update_data: dict[str, Any] = dict(update_data or {})
    existing_metadata = merged_update_data.get("_metadata") or {}
    terminal_metadata: dict[str, Any] = dict(existing_metadata)

    if "progress_percentage" not in terminal_metadata:
        terminal_metadata["progress_percentage"] = 100
    if "progress_message" not in terminal_metadata:
        terminal_metadata["progress_message"] = terminal_metadata_message

    merged_update_data["_metadata"] = terminal_metadata

    async with aget_db_session() as session:
        task_repo = TaskRepository(db=session)
        updated_task = await task_repo.aupdate_task(
            task_id=task_id,
            update_data={
                "status": status.value,
                "file_result_key": json_dumps(uploaded_files),
                **merged_update_data,
            },
            add_completed_at=True,
        )

        if updated_task is None:
            return None

        return updated_task.webhook_url


async def aupdate_task_status(task_id: str, status: StatusTypeEnum) -> bool:
    """Update the status of a task without touching other fields."""
    # Lightweight status-only update used to mark processing states
    async with aget_db_session() as session:
        task_repo = TaskRepository(db=session)
        result = await task_repo.aupdate_task(
            task_id=task_id,
            update_data={"status": status.value},
        )
        return result is not None


async def aupdate_task_progress(
    task_id: str,
    progress_percentage: int,
    progress_message: TaskProgressMessageEnum | None = None,
) -> bool:
    """Persist coarse task progress in metadata for live status monitoring."""

    async with aget_db_session() as session:
        task_repo = TaskRepository(db=session)
        db_task = await task_repo.aget_task_by_task_id(task_id=task_id)
        if db_task is None:
            return False

        metadata = dict(getattr(db_task, "_metadata", {}) or {})
        existing_progress = metadata.get("progress_percentage")
        next_progress = _normalize_progress_percentage(progress_percentage)

        if isinstance(existing_progress, int):
            metadata["progress_percentage"] = max(existing_progress, next_progress)
        else:
            metadata["progress_percentage"] = next_progress

        if progress_message:
            metadata["progress_message"] = progress_message

        result = await task_repo.aupdate_task(
            task_id=task_id,
            update_data={"_metadata": metadata},
        )
        return result is not None


async def aupdate_webhook_delivered_at(task_id: str) -> None:
    """Store the timestamp for a successfully delivered webhook."""
    # Record the time we successfully delivered a webhook for auditing

    async with aget_db_session() as session:
        task_repo = TaskRepository(db=session)
        await task_repo.aupdate_task(
            task_id=task_id,
            update_data={"webhook_delivered_at": pendulum.now("UTC")},
        )


async def afetch_task(etag: str) -> TaskSchema | None:
    """Fetch the first completed task by ETag."""
    # Helper to find an existing completed task for idempotency checks

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
        # Failure notifications are sent on terminal errors to inform callers
        await aupdate_webhook_delivered_at(task_id)


async def avalidation_checks(
    etag: str,
    task_id: str,
    metadata: MetadataResult | None,
    webhook_service: Any,
) -> dict[str, Any] | None:
    """Check if file size exceeds maximum allowed size and handle accordingly.

    Parameters
    ----------
    etag : str
        Entity tag for the task.
    task_id : str
        Task identifier.
    metadata : MetadataResult | None
        Metadata containing file size information.
    webhook_service : Any
        Service for sending webhooks.
    Returns
    -------
    dict[str, Any] | None
        Result dictionary with uploaded_files and completed_at timestamp, or
        None if page count is within limits.
    """

    # If no metadata was provided, there are no pre-checks to run
    if metadata is None:
        return None

    try:
        file_size_bytes = metadata["file_size_bytes"]

        if file_size_bytes is None:
            return None
        file_size: int = int(file_size_bytes)

        if file_size > MAX_SIZE_BYTES:
            logger.warning(
                f"Task {task_id} has file size {file_size:,} which exceeds the maximum of "
                f"{MAX_SIZE_BYTES:,} bytes. Marking as completed without processing."
            )

            # When file is too large, mark as completed without processing
            # and trigger the success webhook with an empty result set
            metadata_copy = metadata.copy()
            metadata_copy["reason"] = DBUpdateReasonEnum.EXCEEDS_SIZE_LIMIT.value
            webhook_url = await aupdate_task_metadata(
                task_id,
                {},
                StatusTypeEnum.COMPLETED,
                update_data={"_metadata": metadata_copy},
            )
            return await asend_success_notification(
                etag=etag,
                task_id=task_id,
                uploaded_files={},
                webhook_url=webhook_url,
                webhook_service=webhook_service,
                metadata=MetadataResult(**metadata_copy),
            )

        # File size within limits — continue with processing
        logger.info(
            f"Task {task_id} file size {file_size:,} bytes is within limit of {MAX_SIZE_BYTES:,} bytes."
        )
        return None

    except (ValueError, KeyError) as e:
        logger.warning(
            f"Invalid file size metadata for task_id {task_id}: {metadata.get('file_size_bytes')}. Error: {e}"
        )
        # Continue processing despite invalid metadata
        return None


async def adelete_temp_chunk_objects(
    task_id: str,
    chunk_task_ids: list[str],
    s3_service: Any,
) -> None:
    """Best-effort cleanup for temporary chunk PDFs stored in object storage."""
    if not chunk_task_ids:
        return

    for chunk_task_id in chunk_task_ids:
        object_name = s3_service.get_object_name(
            task_id=chunk_task_id,
            file_extension=".pdf",
            operation="temp",
        )
        try:
            await asyncio.to_thread(
                s3_service.s3_client.delete_object,
                Bucket=s3_service.bucket_name,
                Key=object_name,
            )
        except Exception as exc:
            logger.warning(
                f"Task {task_id} failed to delete temporary chunk object '{object_name}': {exc}"
            )


async def aprocess_and_update(
    etag: str,
    task_id: str,
    filepath: str,
    metadata: MetadataResult | None,
    processor: Any,
    s3_service: Any,
    webhook_service: Any,
    routing_options: dict[str, Any],
) -> dict[str, Any]:
    """Combined async workflow: split once, process chunk files, combine, upload, update DB."""

    # Download file from S3-compatible storage into the worker temp dir
    await s3_service.adownload_file_from_s3(
        filepath=filepath,
        task_id=task_id,
        file_extension=".pdf",
        operation="input",
    )

    with fitz.open(filepath) as pdf_doc:
        total_pages = pdf_doc.page_count

    # Compute page chunks for processing; empty if no pages
    if total_pages <= 0:
        logger.warning(
            f"Task {task_id} PDF has zero pages; skipping processing outputs"
        )
        # (chunk_idx, start_page, end_page)
        chunk_ranges: list[tuple[int, int, int]] = []

    elif total_pages > MAX_PAGES:
        logger.warning(
            f"Task {task_id} has {total_pages} pages which exceeds the maximum of "
            f"{MAX_PAGES} pages. Marking as completed without processing."
        )
        metadata_copy = metadata.copy() if metadata is not None else {}
        metadata_copy["reason"] = DBUpdateReasonEnum.EXCEEDS_SIZE_LIMIT.value
        webhook_url = await aupdate_task_metadata(
            task_id,
            {},
            StatusTypeEnum.COMPLETED,
            update_data={"_metadata": metadata_copy},
        )
        return await asend_success_notification(
            etag=etag,
            task_id=task_id,
            uploaded_files={},
            webhook_url=webhook_url,
            webhook_service=webhook_service,
            metadata=MetadataResult(**metadata_copy),
        )

    else:
        # (chunk_idx, start_page, end_page)
        chunk_ranges = _chunk_page_ranges(
            total_pages=total_pages,
            chunk_size=CHUNK_SIZE,
        )

    logger.info(
        f"Processing task {task_id} in {len(chunk_ranges)} chunk(s) for {total_pages} page(s)"
    )
    await aupdate_task_progress(
        task_id=task_id,
        progress_percentage=10,
        progress_message=TaskProgressMessageEnum.SPLITTING_CHUNKS,
    )

    # Pre-split chunk PDFs once and upload each chunk to object storage.
    # Chunk workers then download their specific chunk file directly.

    # (chunk_idx, start_page, end_page, chunk_task_id)
    chunk_payloads: list[tuple[int, int, int, str]] = []
    chunk_task_ids: list[str] = []
    try:
        if chunk_ranges:
            with fitz.open(filepath) as source_doc:
                for chunk_idx, start_page, end_page in chunk_ranges:
                    chunk_task_id = f"{task_id}-chunk-{chunk_idx}"
                    with tempfile.NamedTemporaryFile(suffix=".pdf") as chunk_tmp_file:
                        chunk_file_path = Path(chunk_tmp_file.name)
                        # Create a new PDF for the chunk by copying the relevant pages from the source PDF
                        with fitz.open() as chunk_doc:
                            chunk_doc.insert_pdf(
                                source_doc,
                                from_page=start_page,
                                to_page=end_page,
                            )
                            chunk_doc.save(str(chunk_file_path))

                        await s3_service.aupload_file_to_s3(
                            filepath=str(chunk_file_path),
                            task_id=chunk_task_id,
                            correlation_id=etag,
                            operation="temp",
                            max_allowed_size_bytes=MAX_SIZE_BYTES,
                        )

                    chunk_task_ids.append(chunk_task_id)
                    chunk_payloads.append(
                        (chunk_idx, start_page, end_page, chunk_task_id)
                    )

                    chunk_split_progress = 10 + int(
                        25 * (chunk_idx / max(1, len(chunk_ranges)))
                    )
                    await aupdate_task_progress(
                        task_id=task_id,
                        progress_percentage=chunk_split_progress,
                        progress_message=TaskProgressMessageEnum.SPLITTING_CHUNKS,
                    )

        chunk_signatures = [
            process_single_chunk.s(
                etag=etag,
                task_id=task_id,
                chunk_idx=chunk_idx,
                start_page=start_page,
                end_page=end_page,
                chunk_task_id=chunk_task_id,
                export_format=ExportFormat.MARKDOWN.value,
            ).set(**routing_options)
            for chunk_idx, start_page, end_page, chunk_task_id in chunk_payloads
        ]

        # Orchestrator blocks here intentionally — allow_join_result() permits .get()
        # inside a worker. It ensures this task runs on a dedicated queue to avoid
        # deadlocking with process_single_chunk workers competing for the same slots.
        # `.get` is used because we need the results to proceed with combining and uploading.
        if chunk_signatures:
            with allow_join_result():
                encoded_outputs: dict[str, str] = (
                    chord(
                        group(chunk_signatures),
                        combine_processed_chunks.s().set(**routing_options),
                    )
                    .apply_async()
                    .get()
                )
            await aupdate_task_progress(
                task_id=task_id,
                progress_percentage=80,
                progress_message=TaskProgressMessageEnum.COMBINING_OUTPUTS,
            )
        else:
            encoded_outputs = {}
    finally:
        await adelete_temp_chunk_objects(
            task_id=task_id,
            chunk_task_ids=chunk_task_ids,
            s3_service=s3_service,
        )

    # Decode merged outputs and upload processed artifacts to S3
    combined_outputs: dict[str, bytes] = _decode_processed_outputs(encoded_outputs)
    await aupdate_task_progress(
        task_id=task_id,
        progress_percentage=90,
        progress_message=TaskProgressMessageEnum.UPLOADING_OUTPUTS,
    )
    uploaded_files: dict[str, str] = await processor.aupload(
        s3_service=s3_service,
        processed_outputs=combined_outputs,
        task_id=task_id,
    )

    # Persist uploaded file locations and mark task as completed
    task_metadata = metadata.copy() if metadata is not None else {}
    task_metadata["reason"] = DBUpdateReasonEnum.PROCESSED.value
    webhook_url = await aupdate_task_metadata(
        task_id,
        uploaded_files,
        status=StatusTypeEnum.COMPLETED,
        update_data={"_metadata": task_metadata},
    )

    return await asend_success_notification(
        etag=etag,
        task_id=task_id,
        uploaded_files=uploaded_files,
        webhook_url=webhook_url,
        webhook_service=webhook_service,
        metadata=MetadataResult(**task_metadata),
    )


async def aprocess_failure(
    etag: str,
    task_id: str,
    uploaded_files: dict[str, str],
    exc: Exception,
    retries: int,
    webhook_service: Any,
) -> None:
    """Async workflow to update DB and send failure notification on terminal failure."""
    webhook_url = await aupdate_task_metadata(
        task_id,
        uploaded_files=uploaded_files,
        status=StatusTypeEnum.FAILED,
        update_data={
            "_metadata": MetadataResult(
                reason=DBUpdateReasonEnum.PROCESSING_FAILED.value
            ),
            "error_message": str(exc)[:1_000],
        },  # Truncate error message to prevent DB issues
    )
    await asend_failure_notification(
        etag=etag,
        task_id=task_id,
        webhook_url=webhook_url,
        error=exc,
        webhook_service=webhook_service,
        retries=retries,
    )
    return


# -------------------------------------------------------------------
# Individual chunk processing
# -------------------------------------------------------------------
# Note: When `bind=True`, celery automatically passes the task instance as the first argument
# meaning that we need to use `self` and this provides additional functionality like retries, etc
@shared_task(bind=True, base=CustomTask)
def process_single_chunk(  # noqa: ANN201
    self,  # noqa: ANN001
    etag: str,
    task_id: str,
    chunk_idx: int,
    start_page: int,
    end_page: int,
    chunk_task_id: str,
    export_format: str = ExportFormat.MARKDOWN.value,
) -> dict[str, Any]:
    """Process a single PDF chunk and return broker-safe serialized chunk outputs."""

    # Download the pre-split chunk PDF from object storage, run processing,
    # and encode outputs as base64 strings so they're safe to pass through the broker.

    processor = self.processor
    s3_service = self.s3_service
    loop = _get_worker_event_loop()

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)
        chunk_file = tmp_path / f"{task_id}-chunk-{chunk_idx}.pdf"

        async def aprocess_chunk() -> dict[str, Any]:
            await s3_service.adownload_file_from_s3(
                filepath=str(chunk_file),
                task_id=chunk_task_id,
                file_extension=".pdf",
                operation="temp",
            )

            processed_outputs: PDFOutputResult = await processor.aprocess_data(
                source=chunk_file,
                task_id=f"{task_id}-chunk-{chunk_idx}",
                export_format=ExportFormat(export_format),
            )
            return {
                "chunk_idx": chunk_idx,
                "start_page": start_page,
                "end_page": end_page,
                "outputs": _encode_processed_outputs(
                    _normalize_processed_outputs(processed_outputs)
                ),
                "etag": etag,
            }

        return loop.run_until_complete(aprocess_chunk())


# -------------------------------------------------------------------
# Task to combine chunk results
# -------------------------------------------------------------------
@shared_task(bind=True, base=CustomTask)
def combine_processed_chunks(  # noqa: ANN201
    self,  # noqa: ANN001, ARG001
    chunk_results: list[dict[str, Any]],
) -> dict[str, str]:
    """Merge chunk outputs into a single base64-encoded map.

    Tables are renumbered sequentially (table_1, table_2, ...).
    Non-table values are concatenated with newlines, except 'json' keys
    which are merged into a JSON array.
    """

    ordered_chunks = sorted(chunk_results, key=lambda result: int(result["chunk_idx"]))
    table_payloads: list[bytes] = []
    non_table_payloads: dict[str, list[bytes]] = {}

    for chunk in ordered_chunks:
        # It contains export_format and the content in bytes. e.g.
        # decoded_outputs = {"markdown": b"...", "json": b"...", "table_1": b"...", "table_2": b"...", etc.}
        decoded_outputs: dict[str, bytes] = _decode_processed_outputs(chunk["outputs"])

        # Extract and sort table keys for this specific chunk
        table_keys: list[str] = [
            key for key in decoded_outputs if key.startswith("table_")
        ]
        table_payloads.extend(
            [decoded_outputs[table_key] for table_key in _sort_table_keys(table_keys)]
        )

        for key, value in decoded_outputs.items():
            if not key.startswith("table_"):
                non_table_payloads.setdefault(key, []).append(value)

    def _apply_merge(
        table_payloads: list[bytes],
        non_table_payloads: dict[str, list[bytes]],
    ) -> dict[str, bytes]:
        """Build the final merged output payload from grouped chunk data."""
        merged_outputs: dict[str, bytes] = {}

        for idx, table_bytes in enumerate(table_payloads, start=1):
            merged_outputs[f"table_{idx}"] = table_bytes

        for key, chunks in non_table_payloads.items():
            if len(chunks) == 1:
                merged_outputs[key] = chunks[0]
            elif key == "json":
                merged_json_items = [MSGSPEC_DECODER.decode(chunk) for chunk in chunks]
                merged_outputs[key] = json_dumps(merged_json_items).encode("utf-8")
            else:
                merged_outputs[key] = b"\n".join(chunks)

        return merged_outputs

    return _encode_processed_outputs(_apply_merge(table_payloads, non_table_payloads))


# -------------------------------------------------------------------
# Main processing task
# -------------------------------------------------------------------
@shared_task(bind=True, base=CustomTask)
def orchestrate_pdf_processing(
    self,  # noqa: ANN001
    etag: str,
    task_id: str,
    metadata: MetadataResult | None = None,
) -> ProcessDataTaskResult:
    """Orchestrate chunk processing with Celery chord and upload merged outputs."""

    processor = self.processor
    s3_service = self.s3_service
    uploaded_files: dict[str, Any] = {}

    logger.info(f"Started processing task_id: {task_id} with etag: {etag}")

    # Mark task as PROCESSING immediately so idempotency TTL kicks in correctly
    loop = _get_worker_event_loop()
    task_found = loop.run_until_complete(
        aupdate_task_status(task_id=task_id, status=StatusTypeEnum.PROCESSING)
    )
    loop.run_until_complete(
        aupdate_task_progress(
            task_id=task_id,
            progress_percentage=5,
            progress_message=TaskProgressMessageEnum.PROCESSING_STARTED,
        )
    )
    # Drop broker events for unknown task IDs (e.g., temp chunk object events) without retries.
    if not task_found:
        logger.debug(f"Task {task_id} not found in DB — discarding spurious invocation")
        raise Ignore()

    try:
        # Run centralized file-size validation (early-exit for oversized files).
        early_termination_result = loop.run_until_complete(
            avalidation_checks(
                etag=etag,
                task_id=task_id,
                metadata=metadata,
                webhook_service=self.webhook_service,
            )
        )
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

        # If page count metadata isn't present, continue but log for visibility
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

            result: dict[str, Any] = loop.run_until_complete(
                aprocess_and_update(
                    etag=etag,
                    task_id=task_id,
                    filepath=filepath,
                    metadata=metadata,
                    processor=processor,
                    s3_service=s3_service,
                    webhook_service=self.webhook_service,
                    routing_options=_get_task_routing_options(self),
                )
            )

        # Return the standard task result schema expected by callers
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

            loop.run_until_complete(
                aprocess_failure(
                    etag=etag,
                    task_id=task_id,
                    uploaded_files={},
                    exc=exc,
                    retries=self.request.retries,
                    webhook_service=self.webhook_service,
                )
            )

        raise self.retry(exc=exc) from exc
