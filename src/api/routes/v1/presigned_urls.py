import uuid
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

from fastapi import APIRouter, Depends, File, Request, UploadFile, status

from src import ROOT, create_logger
from src.api.core.auth import get_current_user
from src.api.core.dependencies import get_s3_service
from src.api.core.exceptions import HTTPError
from src.api.core.ratelimit import get_rate_limiter
from src.api.core.responses import MsgSpecJSONResponse
from src.schemas.db.models import ClientSchema, GuestClientSchema
from src.utilities.utils import MSGSPEC_ENCODER
from src.utilities.validators import DocumentValidator

if TYPE_CHECKING:
    from src.services.storage import S3StorageService
logger = create_logger(name=__name__)
router = APIRouter(tags=["presigned_urls"], default_response_class=MsgSpecJSONResponse)
# Define a chunk size to control memory usage (e.g., 1MB)
CHUNK_SIZE = 1024 * 1024
# Create validator instance (It can also be created once in lifespan and reused
# across requests for better performance)
doc_validator = DocumentValidator(max_size=25 * 1024 * 1024)  # 25MB limit
# Create upload directory
UPLOAD_DIR = ROOT / "uploads"
try:
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    logger.info(f"Upload directory: {UPLOAD_DIR.absolute()}")
except PermissionError:
    logger.warning(
        f"Permission denied when creating 'uploads' directory at {UPLOAD_DIR}. File uploads may fail."
    )
except Exception as e:
    logger.error(f"Unexpected error creating 'uploads' directory at {UPLOAD_DIR}: {e}")


@router.post("/presigned-urls", status_code=status.HTTP_200_OK)
async def generate_presigned_urls(
    request: Request,  # Required for caching  # noqa: ARG001
    s3_service: "S3StorageService" = Depends(
        get_s3_service
    ),  # Required by caching decorator  # noqa: ARG001
    rate_limiter=Depends(get_rate_limiter),  # noqa: ANN001, ARG001
    client: ClientSchema | GuestClientSchema = Depends(get_current_user),  # noqa: ARG001
) -> Any:
    """Route for generating presigned URLs for file uploads"""
    task_id = str(uuid.uuid4())
    expires_at = datetime.now(timezone.utc) + timedelta(seconds=600)
    url = await s3_service.aget_presigned_url(task_id=task_id, expiration=600)

    if not url:
        raise HTTPError(
            details="Failed to generate presigned URL",
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
    return {"task_id": task_id, "url": url, "expires_at": expires_at}


@router.post("/upload/single")
async def upload_single_file(
    file: UploadFile = File(...),
    rate_limiter=Depends(get_rate_limiter),  # noqa: ANN001, ARG001
    client: ClientSchema | GuestClientSchema = Depends(get_current_user),  # noqa: ARG001
) -> dict[str, Any]:
    """Upload a single file with validation"""
    task_id: str = str(uuid.uuid4())
    # Validate the file first
    validation = await doc_validator.validate_file(file)

    if not validation["valid"]:
        raise HTTPError(
            status_code=status.HTTP_400_BAD_REQUEST,
            details=MSGSPEC_ENCODER.encode(validation["errors"]),  # type:ignore
        )

    # Upload to S3 in the background
    # Celery task will handle the actual upload using the presigned URL generated earlier
    # ...

    return {
        "status": "queued",
        "task_id": task_id,
        "upload": {
            "original_filename": file.filename,
            "content_type": file.content_type,
            "received_at": datetime.now().isoformat(),
            "size": file.size,
        },
        "processing": {
            "state": "queued",
            "enqueue_time": datetime.now().isoformat(),
            "estimated_start": None,
        },
    }
