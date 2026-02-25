import uuid
from typing import TYPE_CHECKING

from fastapi import APIRouter, Depends, Query, Request, status
from sqlalchemy.ext.asyncio import AsyncSession

from src import create_logger
from src.api.core.auth import get_current_user
from src.api.core.dependencies import get_s3_service
from src.api.core.exceptions import HTTPError
from src.api.core.ratelimit import get_rate_limiter
from src.api.core.responses import MsgSpecJSONResponse
from src.db.models import aget_db
from src.db.repositories.task_repository import TaskRepository
from src.schemas.db.models import GuestUserSchema, TaskSchema, UserSchema
from src.schemas.routes.routes import PresignedURLResponse
from src.schemas.types import MimeTypeEnum, StatusTypeEnum
from src.utilities.validators import DocumentValidator

if TYPE_CHECKING:
    from src.services.storage import S3StorageService
logger = create_logger(name=__name__)
router = APIRouter(tags=["presigned_urls"], default_response_class=MsgSpecJSONResponse)

# Create validator instance (It can also be created once in lifespan and reused
# across requests for better performance)
doc_validator = DocumentValidator(max_size=25 * 1024 * 1024)  # 25MB limit
# Create upload directory
EXPIRATION_SECONDS: int = 600  # 10 minutes


@router.post("/presigned-urls", status_code=status.HTTP_200_OK)
async def generate_presigned_urls(
    request: Request,  # Required for caching  # noqa: ARG001
    s3_service: "S3StorageService" = Depends(
        get_s3_service
    ),  # Required by caching decorator  # noqa: ARG001
    rate_limiter=Depends(get_rate_limiter),  # noqa: ANN001, ARG001
    user: UserSchema | GuestUserSchema = Depends(get_current_user),  # noqa: ARG001
    content_type: MimeTypeEnum | None = Query(
        None,
        description="Optional content type for the file to be uploaded (e.g. application/pdf)",
    ),
    db: AsyncSession = Depends(aget_db),  # noqa: ARG001
) -> PresignedURLResponse:
    """Route for generating presigned URLs for file uploads"""
    task_repo = TaskRepository(db=db)

    if not task_repo:
        raise HTTPError(
            details="Database connection is not available",
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        )

    task_id = str(uuid.uuid4())

    # Determine file extension from content_type or use default
    file_extension = ".pdf"
    if content_type:
        # Map common MIME types to extensions
        mime_to_ext = {
            "application/pdf": ".pdf",
            "text/csv": ".csv",
            "application/json": ".json",
            "text/plain": ".txt",
            "text/markdown": ".md",
        }
        file_extension = mime_to_ext.get(
            content_type.value if hasattr(content_type, "value") else content_type,
            ".pdf",
        )

    presigned_url_data: dict[str, str] = await s3_service.aget_presigned_url(
        task_id=task_id,
        file_extension=file_extension,
        expiration=EXPIRATION_SECONDS,
        content_type=content_type,
    )

    if not presigned_url_data:
        raise HTTPError(
            details="Failed to generate presigned URL",
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )

    task = TaskSchema(
        task_id=task_id,
        user_id=user.id if hasattr(user, "id") else None,
        status=StatusTypeEnum.PENDING,
        file_upload_key="",
        file_result_key="",
        file_size_bytes=0,
        file_type=content_type if content_type else None,
    )

    await task_repo.acreate_task(task)

    return PresignedURLResponse(
        task_id=task_id,
        url=presigned_url_data["url"],
        expires_at=presigned_url_data["expires_at"],
        content_type=content_type,
    )


@router.post("/webhooks/s3-event", status_code=status.HTTP_200_OK)
async def handle_s3_event(request: Request) -> dict[str, str]:
    """Handle S3 event notifications."""
    try:
        body = await request.json()
        records = body.get("Records", [])
        for record in records:
            s3_info = record.get("s3", {})
            s3_key = s3_info.get("object", {}).get("key")
            if not s3_key or not s3_key.startswith("uploads/"):
                continue
            parts = s3_key.split("/")
            if len(parts) >= 2:
                task_id = parts[1]
                logger.info(f"Triggering processing for task {task_id}")
                # Trigger Celery task asynchronously
                from src.celery_app.tasks.processor import process_data

                process_data.delay(task_id, analysis_id=task_id)
        return {"status": "ok"}
    except Exception as e:
        logger.error(f"Error handling S3 event: {e}", exc_info=True)
        return {"status": "error", "message": str(e)}
