from fastapi import APIRouter, Depends, status
from sqlalchemy.ext.asyncio import AsyncSession

from src import create_logger
from src.api.core.auth import get_current_user
from src.api.core.exceptions import HTTPError
from src.api.core.responses import MsgSpecJSONResponse
from src.db.models import aget_db
from src.db.repositories.task_repository import TaskRepository
from src.schemas.db.models import GuestUserSchema, UserSchema
from src.schemas.routes.routes import TaskStatusResponse
from src.schemas.types import MimeTypeEnum, StatusTypeEnum
from src.utilities.utils import MSGSPEC_DECODER

logger = create_logger(name=__name__)
router = APIRouter(tags=["tasks"], default_response_class=MsgSpecJSONResponse)


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
