"""Custom rate limiting implementation using Redis as the backend. This supports
distributed rate limiting across multiple instances of the FastAPI application and
per client (via API key) rate limiting.

Adapted from:
https://github.com/benavlabs/FastAPI-boilerplate/blob/main/src/app/core/utils/rate_limit.py
"""

from datetime import UTC, datetime
from typing import Any

from fastapi import Depends, Request
from redis.asyncio import ConnectionPool, Redis

from src import create_logger
from src.api.core.auth import get_current_user_or_guest
from src.api.core.exceptions import RateLimitError
from src.config import app_config, app_settings
from src.schemas.db.models import BaseUserSchema, GuestUserSchema
from src.schemas.types import TierEnum

logger = create_logger(name=__name__)


# ---------- Utility Functions ----------
def sanitize_path(path: str) -> str:
    """Sanitize the request path to be used as part of the Redis key.

    Parameters
    ----------
    path : str
        The request path.

    Examples
    --------
    "/api/v1/users" → "api_v1_users"
    "/posts/{id}" → "posts_{id}"
    """
    return path.strip("/").replace("/", "_")


def get_limit_and_period(tier: str, path: str) -> tuple[int, int]:
    """
    Get the request limit and period for a given tier and endpoint path.

    Parameters
    ----------
    tier : str
        The client tier (e.g., "free", "plus", "pro").
    path : str
        The API endpoint path.

    Returns
    -------
    tuple[int, int]
        A tuple containing the request limit and period in seconds.
    """

    default_limit = 5  # requests
    default_period = 60  # seconds

    data: dict[str, dict[str, Any]] = app_config.endpoint_policies_config.ratelimits
    tier = tier.value if isinstance(tier, TierEnum) else tier
    tier_info = data.get(tier)
    if not tier_info:
        raise ValueError(f"Unknown tier: {tier}")

    special_endpoints = tier_info.get("special_endpoints", {})
    if path in special_endpoints:
        limit_info = special_endpoints[path]
        return limit_info["limit"], limit_info["period"]

    # Check for prefix matches in special endpoints
    s_paths = [p for p in special_endpoints.keys() if path.startswith(p)]
    # If the path is not a special endpoint, return the default limit for the tier
    if not s_paths:
        return (
            data.get(tier, {}).get("requests_per_minute", default_limit),
            default_period,
        )

    return (default_limit, default_period)


# ---------- Rate Limiting ----------

# Lua script for atomic INCR + EXPIRE.
# Guarantees a single atomic operation, preventing race conditions
# and ensuring keys expire automatically to avoid unbounded growth.
_RATE_LIMIT_LUA = """
local current = redis.call("INCR", KEYS[1])
if tonumber(current) == 1 then
    redis.call("EXPIRE", KEYS[1], ARGV[1])
end
return current
"""


class RateLimiter:
    _instance: "RateLimiter | None" = None
    _pool: ConnectionPool | None = None
    _client: Redis | None = None
    _is_initialized: bool = False

    def __new__(cls) -> "RateLimiter":
        """Singleton implementation to ensure only one instance of RateLimiter exists."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)

        return cls._instance

    def __init__(self) -> None:
        """Initialize the Redis connection pool and client."""
        if not self._is_initialized:
            self._pool = ConnectionPool(
                host=app_settings.REDIS_HOST,
                port=app_settings.REDIS_PORT,
                password=app_settings.REDIS_PASSWORD.get_secret_value() or None,
                db=app_settings.REDIS_RATE_LIMIT_DB,
                decode_responses=True,
            )
            self._client = Redis(connection_pool=self._pool)
            self._is_initialized = True
            logger.info("RateLimiter initialized with Redis backend.")

    def get_client(self) -> Redis:
        """Get the Redis client instance.

        Returns
        -------
        Redis
            The Redis client.
        """
        if self._client is None:
            raise RuntimeError("RateLimiter not initialized properly.")
        return self._client

    async def ais_rate_limited(
        self,
        *,
        user_id: str | int | None,
        path: str,
        limit: int,
        period: int,
    ) -> tuple[bool, int]:
        """Check if a user has exceeded the rate limit for a specific path.

        Parameters
        ----------
        user_id : str | int
            The unique identifier for the user (e.g., API key).
        path : str
            The request path.
        limit : int
            The maximum number of allowed requests in the given period.
        period : int
            The time window in seconds for the rate limit.
        """
        user_id = user_id or "anonymous"
        client = self.get_client()

        # Fixed window algorithm: window starts at the beginning of the current period
        # This ensures all requests in the same period share the same window. e.g.
        # For a period of 60 seconds, if the current time is 12:01:45, the window starts at 12:01:00
        # and ends at 12:01:59.
        now = int(datetime.now(UTC).timestamp())
        window_start = now - (now % period)

        sanitized_path: str = sanitize_path(path)
        key: str = f"ratelimit:{user_id}:{sanitized_path}:{window_start}"

        try:
            current_count: int = await client.eval(_RATE_LIMIT_LUA, 1, key, period)  # type: ignore

            # Check if the current count exceeds the limit
            if current_count > limit:
                retry_after: int = period - (now - window_start)
                return (True, max(retry_after, 0))

            # Else, within limit
            return (False, 0)

        except Exception:
            # Traceback logging with sanitized user_id
            _user_id: str = (
                str(user_id) if len(str(user_id)) <= 8 else str(user_id)[:8] + "..."
            )
            logger.exception(
                f"Rate limiting check failed (user_id={_user_id}, path={sanitized_path})"
            )
            # Fail open: allow the request if Redis is unavailable
            return (False, 0)


# Singleton instance
rate_limiter = RateLimiter()


async def get_rate_limiter(
    request: Request,
    user: BaseUserSchema | GuestUserSchema | None = Depends(get_current_user_or_guest),
) -> None:
    """
    Enforces rate limits per user tier and API path.

    - Identifies user (or defaults to IP-based anonymous rate limit)
    - Finds tier-specific limit for the request path
    - Checks Redis counter to determine if request should be allowed
    """
    path = sanitize_path(request.url.path)

    # Identify user: Use guest (ip address) or authenticated client
    if isinstance(user, GuestUserSchema):
        user_id = request.client.host or "anonymous"  # type: ignore
    else:
        user_id = (
            getattr(user, "external_id", None) or request.client.host or "anonymous"  # type: ignore
        )  # type: ignore

    # Determine user tier (default to "free" or anonymous)
    if user and getattr(user, "tier", None):
        tier = user.tier
    else:
        tier = TierEnum.GUEST

    # Find specific rate limit rule for this path + tier
    limit, period = get_limit_and_period(tier=tier, path=path)

    # Check rate limit in Redis
    is_limited, retry_after = await rate_limiter.ais_rate_limited(
        user_id=user_id,
        path=path,
        limit=limit,
        period=period,
    )

    if is_limited:
        raise RateLimitError(
            f"Rate limit exceeded for path '{path}'. Try again in {retry_after} seconds."
        )
