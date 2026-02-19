from aiocache import Cache
from fastapi import APIRouter, Depends, Request, Response, status

from src import create_logger
from src.api.core.auth import get_current_user_or_guest
from src.api.core.cache import cached
from src.api.core.dependencies import get_cache
from src.api.core.exceptions import HTTPError
from src.api.core.ratelimit import get_rate_limiter
from src.api.core.responses import MsgSpecJSONResponse
from src.config import app_config
from src.schemas.db.models import ClientSchema, GuestClientSchema
from src.schemas.routes.health import HealthStatusSchema

logger = create_logger(name=__name__)
router = APIRouter(tags=["health"], default_response_class=MsgSpecJSONResponse)
TTL: int = 30  # seconds


@router.get("/health", status_code=status.HTTP_200_OK)
@cached(ttl=TTL, key_prefix="health")  # type: ignore
async def health_check(
    request: Request,  # Required for caching  # noqa: ARG001
    response: Response,  # Required to set cache headers  # noqa: ARG001, F821
    cache: Cache = Depends(get_cache),  # Required by caching decorator  # noqa: ARG001
    rate_limiter=Depends(get_rate_limiter),  # noqa: ANN001, ARG001
    client: ClientSchema | GuestClientSchema = Depends(get_current_user_or_guest),  # noqa: ARG001
) -> HealthStatusSchema:
    """Route for health checks"""

    result = HealthStatusSchema(
        name=app_config.api_config.title,
        status=app_config.api_config.status,
        version=app_config.api_config.version,
    )

    if not result:
        raise HTTPError(
            details="Health check failed",
        )

    return result
