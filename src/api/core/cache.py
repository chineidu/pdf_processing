"""
Caching utilities for FastAPI endpoints using aiocache with Redis backend.
"""

import hashlib
import json
import warnings
from functools import wraps
from typing import Any, Callable, Coroutine

from aiocache import Cache
from aiocache.serializers import JsonSerializer
from fastapi import Request, Response
from fastapi.encoders import jsonable_encoder

from src import create_logger
from src.config import app_settings
from src.utilities.utils import MSGSPEC_ENCODER, sort_dict

logger = create_logger(name=__name__)
type CacheDecorator = Callable[..., Callable[..., Coroutine[Any, Any, Any]]]


def setup_cache() -> Cache:
    """
    Initialize Redis cache (call once at startup).

    Connects to Redis using the REDIS_URL environment variable.
    Falls back to in-memory cache if Redis is not available.

    Returns:
        Cache: Configured cache instance (Redis or MEMORY fallback)
    """
    redis_url: str = app_settings.redis_url

    password = app_settings.REDIS_PASSWORD.get_secret_value()
    db: int = app_settings.REDIS_DB

    try:
        # Create Redis cache
        cache_kwargs: dict[str, Any] = {
            "endpoint": app_settings.REDIS_HOST,
            "port": app_settings.REDIS_PORT,
            "serializer": JsonSerializer(),
            "namespace": "main",
        }
        if password:
            cache_kwargs["password"] = password
        if db != 0:
            cache_kwargs["db"] = db

        return Cache(Cache.REDIS, **cache_kwargs)

    except Exception as e:
        warnings.warn(
            f"Failed to connect to Redis ({redis_url}): {e}. Falling back to MEMORY cache.",
            stacklevel=2,
        )
        # Fallback to in-memory cache
        return Cache(Cache.MEMORY, serializer=JsonSerializer(), namespace="main")


def cached(
    ttl: int = 300, key_prefix: str = "", payload_key: str | None = None
) -> Callable[[CacheDecorator], CacheDecorator]:
    """
    Decorator for caching endpoint responses.

    Parameters
    ----------
    ttl: Time to live in seconds (default 5 minutes)
    key_prefix: Prefix for cache key (useful for namespacing)
    payload_key: Optional key in request body to include in cache key generation

    Usage
    -----
        @cached(ttl=60, key_prefix="products", payload_key="input_data")
        async def get_products():
            ...
    """

    def decorator(func: CacheDecorator) -> Any:
        @wraps(func)
        async def wrapper(*args, **kwargs) -> Any:  # noqa: ANN002, ANN003
            # Extract request and cache from kwargs
            request: Request | None = kwargs.get("request")
            response: Response | None = kwargs.get("response")
            cache: Cache | None = kwargs.get("cache")
            payload_dict: dict[str, Any] = {}

            if not cache:
                # If no cache available, just call the function
                return await func(*args, **kwargs)  # type: ignore

            # Extract payload for cache key if specified
            if payload_key and payload_key in kwargs:
                payload = kwargs.get(payload_key)

                # Handle Pydantic models or dataclasses
                if hasattr(payload, "model_dump"):
                    payload_dict = payload.model_dump()
                elif hasattr(payload, "dict"):
                    payload_dict = payload.dict()

            # Generate cache key from endpoint path and query params
            if request is None:
                raise ValueError("Request object is required for caching")
            cache_key: str = _generate_cache_key(
                request.url.path,
                dict(request.query_params),
                key_prefix,
                payload=payload_dict if payload_dict else None,
            )

            # Try to get from cache
            cached_response = await cache.get(cache_key)
            if cached_response:
                logger.info(f"Cache hit for key: {cache_key}")

                # Set cached response headers if possible
                if response:
                    response.headers["X-Cache"] = "HIT"
                # Some backends/serializers may return a JSON string. Attempt to
                # deserialize so FastAPI gets native Python types (list/dict).
                try:
                    if isinstance(cached_response, str):
                        return json.loads(cached_response)
                except Exception:
                    # If deserialization fails, return the raw cached value
                    return cached_response
                return cached_response

            # Cache miss - call the actual function
            func_response = await func(*args, **kwargs)  # type: ignore
            # Set cache miss header if possible
            if response:
                response.headers["X-Cache"] = "MISS"

            try:
                # Serialize the response to a JSON-compatible format
                serialized = jsonable_encoder(func_response)
                # Store in cache
                await cache.set(cache_key, serialized, ttl=ttl)

            except Exception as e:
                logger.warning(f"Skipping cache for key {cache_key}: {e}")

            return func_response

        return wrapper

    return decorator


def _generate_cache_key(
    path: str,
    params: dict[str, Any],
    prefix: str = "",
    payload: dict[str, Any] | None = None,
) -> str:
    """Generate a unique cache key from path and parameters.

    This function creates a stable, short, and unique string (an MD5 hash)
    based on the combination of the request path and its parameters.

    Parameters
    ----------
    path : str
        The **base path** or endpoint identifier for the request, e.g., "/api/users".
    params : dict[str, Any]
        The **query or body parameters** of the request (e.g., {'limit': 10, 'offset': 0}).
        These are serialized and hashed to ensure uniqueness.
    prefix : str, optional
        An **optional string prefix** to prepend to the generated hash, useful
        for namespacing keys (e.g., 'user_cache'), by default "".
    payload : dict[str, Any] | None, optional
        An **optional payload** to include in the cache key generation, by default None.

    Returns
    -------
    str
        A **unique, deterministic cache key** string, which is either an MD5 hash
        or a prefixed MD5 hash (e.g., 'user_cache:abcdef1234567890').
    """
    # Create a deterministic string from params
    params_str: str = MSGSPEC_ENCODER.encode(sort_dict(params)).decode()
    key_content: str = f"{path}:{params_str}"

    if payload:
        serialized_payload = MSGSPEC_ENCODER.encode(sort_dict(payload)).decode()
        key_content += f":{serialized_payload}"

    # Hash for shorter keys
    key_hash: str = hashlib.md5(key_content.encode()).hexdigest()

    if prefix:
        return f"{prefix}:{key_hash}"
    return key_hash


async def invalidate_cache(cache: Cache, pattern: str | None = None) -> None:
    """
    Invalidate cache entries. Use after data updates.

    Parameters
    ----------
    cache: Cache
        Cache instance
    pattern: str | None
        Pattern to match keys (None = clear all)

    Returns
    -------
    None
    """
    try:
        if pattern:
            await cache.clear(namespace=pattern.rstrip("*"))  # type: ignore
        else:
            await cache.clear()  # type: ignore
    except AttributeError:
        logger.warning(
            "Cache backend does not support clear operation for the given pattern."
        )
