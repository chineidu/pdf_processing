"""Routes for managing PDF processing tasks, including status retrieval and upload confirmation."""

import asyncio
from typing import Any

from fastapi import APIRouter, Depends, status
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession

from src import create_logger
from src.api.core.auth import get_current_user
from src.api.core.exceptions import HTTPError
from src.api.core.responses import MsgSpecJSONResponse
from src.db.models import DBTask, aget_db
from src.db.repositories.task_repository import TaskRepository
from src.schemas.db.models import GuestUserSchema, UserSchema
from src.schemas.routes.routes import TaskStatusResponse, TaskUploadConfirmRequest
from src.schemas.types import (
    MimeTypeEnum,
    StatusTypeEnum,
    TaskProgressMessageEnum,
    TaskStreamEventEnum,
)
from src.utilities.utils import json_dumps, json_loads

logger = create_logger(name=__name__)
router = APIRouter(tags=["tasks"], default_response_class=MsgSpecJSONResponse)

TERMINAL_STATUSES = {
    StatusTypeEnum.COMPLETED.value,
    StatusTypeEnum.FAILED.value,
    StatusTypeEnum.SKIPPED.value,
    StatusTypeEnum.UNPROCESSABLE.value,
}


def _extract_progress_metadata(
    task: DBTask,
) -> tuple[int | None, TaskProgressMessageEnum | None]:
    """Extract progress details from task metadata, if present.

    Parameters
    ----------
    task : DBTask
        The database task object from which to extract progress metadata.

    Returns
    -------
    tuple[int | None, TaskProgressMessageEnum | None]
        A tuple containing the progress percentage (0-100) and the progress message enum,
         or None if not available or invalid.
    """
    metadata = getattr(task, "_metadata", None) or {}

    if not isinstance(metadata, dict):
        return (None, None)

    raw_progress = metadata.get("progress_percentage")
    progress_percentage: int | None = None
    if raw_progress is not None:
        try:
            progress_percentage = max(0, min(100, int(raw_progress)))
        except (TypeError, ValueError):
            progress_percentage = None

    raw_message = metadata.get("progress_message")
    progress_message: TaskProgressMessageEnum | None = None
    if isinstance(raw_message, str):
        try:
            progress_message = TaskProgressMessageEnum(raw_message)
        except ValueError:
            progress_message = None
    return (progress_percentage, progress_message)


def _decode_download_urls(task: DBTask, task_id: str) -> dict[str, str] | None:
    """Decode serialized download URLs from task payload."""
    download_urls: dict[str, str] | None = None
    if task.file_result_key:
        try:
            download_urls = json_loads(task.file_result_key)
        except Exception as exc:  # noqa: BLE001
            logger.warning(f"Failed to decode download URLs for task {task_id}: {exc}")
    return download_urls


def _status_value(status_value: StatusTypeEnum | str) -> str:
    """Return normalized status value from enum/string."""
    if isinstance(status_value, StatusTypeEnum):
        return status_value.value
    return status_value


def _to_task_status_response(
    task: DBTask,
    download_urls: dict[str, str] | None = None,
) -> TaskStatusResponse:
    progress_percentage, progress_message = _extract_progress_metadata(task)
    return TaskStatusResponse(
        task_id=task.task_id,
        status=StatusTypeEnum(task.status)
        if isinstance(task.status, str)
        else task.status,
        file_type=MimeTypeEnum(task.file_type)
        if isinstance(task.file_type, str)
        else task.file_type,
        file_page_count=task.file_page_count,
        file_size_bytes=task.file_size_bytes,
        created_at=task.created_at,
        updated_at=task.updated_at,
        completed_at=task.completed_at,
        error_message=task.error_message,
        progress_percentage=progress_percentage,
        progress_message=progress_message,
        download_urls=download_urls,
    )


@router.get("/tasks/{task_id}", status_code=status.HTTP_200_OK)
async def get_task_status(
    task_id: str,
    user: UserSchema | GuestUserSchema = Depends(get_current_user),
    db: AsyncSession = Depends(aget_db),
) -> TaskStatusResponse:
    """Fetch task status and results for the requesting user."""
    task_repo = TaskRepository(db=db)
    task = await task_repo.aget_task_by_task_id(task_id)

    if task is None:
        raise HTTPError(details="Task not found", status_code=status.HTTP_404_NOT_FOUND)

    if hasattr(user, "id") and task.user_id != user.id:
        raise HTTPError(details="Task not found", status_code=status.HTTP_404_NOT_FOUND)

    download_urls = _decode_download_urls(task=task, task_id=task_id)

    return _to_task_status_response(task, download_urls=download_urls)


@router.get("/tasks/{task_id}/stream", status_code=status.HTTP_200_OK)
async def stream_task_status(
    task_id: str,
    user: UserSchema | GuestUserSchema = Depends(get_current_user),
    db: AsyncSession = Depends(aget_db),
) -> StreamingResponse:
    """Stream task status updates using Server-Sent Events (SSE)."""
    task_repo = TaskRepository(db=db)
    task = await task_repo.aget_task_by_task_id(task_id)

    if task is None:
        raise HTTPError(details="Task not found", status_code=status.HTTP_404_NOT_FOUND)

    if hasattr(user, "id") and task.user_id != user.id:
        raise HTTPError(details="Task not found", status_code=status.HTTP_404_NOT_FOUND)

    async def event_generator() -> Any:
        last_payload: str | None = None

        # Instruct clients to retry every 3 seconds on disconnect.
        yield "retry: 3000\n\n"

        while True:
            current_task = await task_repo.aget_task_by_task_id(task_id)
            if current_task is None:
                yield (
                    f"event: {TaskStreamEventEnum.ERROR.value}\n"
                    f"data: {json_dumps({'detail': 'Task not found'})}\n\n"
                )
                # Stop the generator
                break

            if hasattr(user, "id") and current_task.user_id != user.id:
                yield (
                    f"event: {TaskStreamEventEnum.ERROR.value}\n"
                    f"data: {json_dumps({'detail': 'Task not found'})}\n\n"
                )
                # Stop the generator
                break

            download_urls: dict[str, str] | None = _decode_download_urls(
                current_task, task_id
            )
            payload = _to_task_status_response(
                task=current_task,
                download_urls=download_urls,
            ).model_dump(mode="json", by_alias=True)

            payload_json: str = json_dumps(payload)
            # Send update only on change; otherwise emit `keepalive`.
            if payload_json != last_payload:
                yield (
                    f"event: {TaskStreamEventEnum.TASK_STATUS.value}\ndata: {payload_json}\n\n"
                )
                last_payload = payload_json
            else:
                # Keep the connection alive through proxies/load balancers.
                yield f": {TaskStreamEventEnum.KEEPALIVE.value}\n\n"

            if _status_value(current_task.status) in TERMINAL_STATUSES:
                break

            await asyncio.sleep(1.5)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.post("/tasks/{task_id}/uploaded", status_code=status.HTTP_200_OK)
async def confirm_task_uploaded(
    task_id: str,
    payload: TaskUploadConfirmRequest,
    user: UserSchema | GuestUserSchema = Depends(get_current_user),
    db: AsyncSession = Depends(aget_db),
) -> TaskStatusResponse:
    """Confirm successful direct upload and persist transition from pending to uploaded."""
    task_repo = TaskRepository(db=db)
    task = await task_repo.aget_task_by_task_id(task_id)

    if task is None:
        raise HTTPError(details="Task not found", status_code=status.HTTP_404_NOT_FOUND)

    if hasattr(user, "id") and task.user_id != user.id:
        raise HTTPError(details="Task not found", status_code=status.HTTP_404_NOT_FOUND)

    current_status = task.status.value if hasattr(task.status, "value") else task.status
    terminal_statuses = {
        StatusTypeEnum.COMPLETED.value,
        StatusTypeEnum.FAILED.value,
        StatusTypeEnum.SKIPPED.value,
        StatusTypeEnum.UNPROCESSABLE.value,
    }

    if current_status not in terminal_statuses:
        update_data: dict[str, object] = {}

        # Move fresh tasks to UPLOADED, but still allow metadata enrichment for
        # non-terminal tasks (VALIDATING/PROCESSING) to avoid race-condition data loss.
        if current_status == StatusTypeEnum.PENDING.value:
            update_data["status"] = StatusTypeEnum.UPLOADED.value

        current_size = task.file_size_bytes
        if payload.file_size_bytes > 0 and (current_size is None or current_size <= 0):
            update_data["file_size_bytes"] = payload.file_size_bytes

        current_page_count = task.file_page_count
        if payload.file_page_count is not None and (
            current_page_count is None or current_page_count <= 0
        ):
            update_data["file_page_count"] = payload.file_page_count

        if payload.file_type is not None and task.file_type is None:
            update_data["file_type"] = payload.file_type

        if payload.etag and not task.etag:
            update_data["etag"] = payload.etag

        if update_data:
            await task_repo.aupdate_task(task_id=task_id, update_data=update_data)
            task = await task_repo.aget_task_by_task_id(task_id)

    if task is None:
        raise HTTPError(
            details="Task not found after update",
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )

    return _to_task_status_response(task)
