from fastapi import APIRouter, Depends, status
from sqlalchemy.ext.asyncio import AsyncSession

from src import create_logger
from src.api.core.auth import get_current_user
from src.api.core.exceptions import HTTPError
from src.api.core.responses import MsgSpecJSONResponse
from src.db.models import DBTask, aget_db
from src.db.repositories.task_repository import TaskRepository
from src.schemas.db.models import GuestUserSchema, UserSchema
from src.schemas.routes.routes import TaskStatusResponse, TaskUploadConfirmRequest
from src.schemas.types import MimeTypeEnum, StatusTypeEnum
from src.utilities.utils import MSGSPEC_DECODER

logger = create_logger(name=__name__)
router = APIRouter(tags=["tasks"], default_response_class=MsgSpecJSONResponse)


def _to_task_status_response(
    task: DBTask,
    download_urls: dict[str, str] | None = None,
) -> TaskStatusResponse:
    return TaskStatusResponse(
        task_id=task.task_id,
        status=StatusTypeEnum(task.status)
        if isinstance(task.status, str)
        else task.status,
        file_type=MimeTypeEnum(task.file_type)
        if isinstance(task.file_type, str)
        else task.file_type,
        file_size_bytes=task.file_size_bytes,
        created_at=task.created_at,
        updated_at=task.updated_at,
        completed_at=task.completed_at,
        error_message=task.error_message,
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

    download_urls: dict[str, str] | None = None
    if task.file_result_key:
        try:
            download_urls = MSGSPEC_DECODER.decode(task.file_result_key.encode())
        except Exception as exc:  # noqa: BLE001
            logger.warning(f"Failed to decode download URLs for task {task_id}: {exc}")

    return _to_task_status_response(task, download_urls=download_urls)


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
    if current_status == StatusTypeEnum.PENDING.value:
        update_data = {
            "status": StatusTypeEnum.UPLOADED.value,
            "file_size_bytes": payload.file_size_bytes,
        }
        if payload.file_type is not None:
            update_data["file_type"] = payload.file_type
        if payload.file_page_count is not None:
            update_data["file_page_count"] = payload.file_page_count
        if payload.etag:
            update_data["etag"] = payload.etag

        await task_repo.aupdate_task(task_id=task_id, update_data=update_data)
        task = await task_repo.aget_task_by_task_id(task_id)

    if task is None:
        raise HTTPError(
            details="Task not found after update",
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )

    return _to_task_status_response(task)
