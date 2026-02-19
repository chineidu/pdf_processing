"""Custom middleware for request ID assignment, logging, error handling, and credit deduction."""

import time
from collections.abc import Awaitable, Callable
from typing import Any
from uuid import uuid4

from fastapi import HTTPException, Request, Response, status
from opentelemetry import trace
from starlette.background import BackgroundTask
from starlette.middleware.base import BaseHTTPMiddleware

from src import create_logger
from src.api.core.exceptions import (
    CircuitOpenError,
    HTTPError,
    RateLimitError,
    ServiceUnavailableError,
    UnauthorizedError,
    UnexpectedError,
)
from src.api.core.metrics import CACHE_REQUEST_COUNT, REQUEST_DURATION_SECONDS
from src.api.core.responses import MsgSpecJSONResponse
from src.config import app_settings
from src.schemas.types import ErrorCodeEnum
from src.services.billing import adeduct_credits_background
from src.utilities.utils import MSGSPEC_ENCODER

logger = create_logger(name=__name__)


class RequestIDMiddleware(BaseHTTPMiddleware):
    """Middleware to add a unique request ID to each incoming request.
    The request ID is used for tracing and logging purposes.
    """

    async def dispatch(
        self, request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        """Add a unique request ID to the request and response headers."""
        # Check for existing request ID from client
        client_req_id: str | None = request.headers.get("X-Request-ID", None)

        if client_req_id and len(client_req_id) <= 128:
            request_id = client_req_id.strip()
        else:
            # Generate a new UUID if not provided or invalid
            request_id = str(uuid4())

        request.state.request_id = request_id
        response = await call_next(request)
        response.headers["X-Request-ID"] = request_id

        return response


class LoggingMiddleware(BaseHTTPMiddleware):
    """Middleware to log incoming requests and outgoing responses."""

    async def dispatch(
        self, request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        """Log request and response details."""
        start_time: float = time.perf_counter()

        request_id = getattr(request.state, "request_id", "N/A")
        response: Response = await call_next(request)
        # in milliseconds
        process_time: float = round(((time.perf_counter() - start_time) * 1000), 2)
        response.headers["X-Process-Time-MS"] = str(process_time)

        log: dict[str, Any] = {
            "method": request.method,
            "path": request.url.path,
            "status_code": response.status_code,
            "process_time_ms": process_time,
            "request_id": request_id,
        }

        # Use msgspec for optimized serialization
        logger.info(MSGSPEC_ENCODER.encode(log).decode("utf-8"))

        return response


class ErrorHandlingMiddleware(BaseHTTPMiddleware):
    """Middleware to handle exceptions and return standardized error responses."""

    async def dispatch(
        self, request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        """Catch exceptions and return standardized error responses."""
        try:
            response: Response = await call_next(request)
            return response

        except (HTTPError, HTTPException) as exc:
            return self._create_error_response(exc, ErrorCodeEnum.HTTP_ERROR, request)
        except UnauthorizedError as exc:
            return self._create_error_response(exc, exc.error_code, request)
        except CircuitOpenError as exc:
            return self._create_error_response(exc, exc.error_code, request)
        except RateLimitError as exc:
            return self._create_error_response(exc, exc.error_code, request)
        except ServiceUnavailableError as exc:
            return self._create_error_response(exc, exc.error_code, request)
        except UnexpectedError as exc:
            return self._create_error_response(exc, exc.error_code, request)
        except Exception as exc:
            logger.exception(f"Unhandled exception in middleware: {exc}")
            return MsgSpecJSONResponse(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                content={
                    "status": "error",
                    "error": {
                        "message": "An unexpected server error occurred.",
                        "code": ErrorCodeEnum.UNEXPECTED_ERROR,
                    },
                    "request_id": getattr(request.state, "request_id", "N/A"),
                    "path": str(request.url.path),
                },
                headers=getattr(exc, "headers", None),
            )

    def _create_error_response(
        self, exc: Any, code: str, request: Request
    ) -> MsgSpecJSONResponse:
        """Helper to reduce code duplication in exception handling."""
        msg = getattr(exc, "message", None) or getattr(exc, "detail", "HTTP error")
        return MsgSpecJSONResponse(
            status_code=exc.status_code,
            content={
                "status": "error",
                "error": {"message": msg, "code": code},
                "request_id": getattr(request.state, "request_id", "N/A"),
                "path": str(request.url.path),
            },
            headers=getattr(exc, "headers", None),
        )


class BillingMiddleware(BaseHTTPMiddleware):
    """
    Middleware to handle usage recording and billing.
    Crucial for handling requests that hit the Cache (which skip the route handler).
    """

    async def dispatch(
        self, request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        """Dispatch the request and attach billing background task if applicable."""
        # Execute the request
        response = await call_next(request)

        # Check conditions: Success (200 OK) AND User Identity Found in State
        if not self._should_bill(request, response):
            return response

        # Extract billing info from request state
        billing_info = self._extract_billing_info(request)
        if not billing_info:
            return response

        # Attach billing background task to response
        self._attach_billing_task(response, billing_info)
        return response

    def _should_bill(self, request: Request, response: Response) -> bool:
        """Determine if billing should be applied based on request and response."""
        # Bill only for successful requests with user identity
        return (response.status_code == status.HTTP_200_OK) and hasattr(
            request.state, "api_key_client_id"
        )

    def _extract_billing_info(self, request: Request) -> dict[str, Any] | None:
        """Extract billing-related information from the request state."""
        # Note: 'api_key_client_id' and other attributes are set by the 'get_current_api_key' dependency
        client_id = getattr(request.state, "api_key_client_id", None)
        key_id = getattr(request.state, "api_key_id", None)
        cost = getattr(
            request.state, "api_key_cost", app_settings.CREDIT_COST_PER_REQUEST
        )

        # Both client_id and key_id are required
        if not client_id or not key_id:
            return None

        return {"client_id": client_id, "key_id": key_id, "cost": cost}

    def _attach_billing_task(
        self, response: Response, billing_info: dict[str, Any]
    ) -> None:
        """Attach the billing background task to the response."""
        # Create the billing background task
        billing_task = BackgroundTask(
            adeduct_credits_background,
            client_id=billing_info["client_id"],
            key_id=billing_info["key_id"],
            cost=billing_info["cost"],
        )

        # CRITICAL: Chain with existing background tasks
        # If route handlers already attached tasks (e.g., cache updates),
        # we must run both sequentially to avoid losing operations
        if response.background:
            original_task = response.background

            async def run_chained_tasks() -> None:
                """Execute original task first, then billing task."""
                await original_task()
                await billing_task()

            response.background = BackgroundTask(run_chained_tasks)
        else:
            # No existing task - simply attach billing task
            response.background = billing_task


class TracingMiddleware(BaseHTTPMiddleware):
    """Middleware to enrich OpenTelemetry spans with custom attributes and business context."""

    async def dispatch(
        self, request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        """Add custom attributes to OpenTelemetry spans."""
        span = trace.get_current_span()

        # Add request metadata to span
        if span.is_recording():
            # Add attributes
            span.set_attribute(
                "http.request_id", request.headers.get("x-request-id", "")
            )
            span.set_attribute("http.user_agent", request.headers.get("user-agent", ""))

            # Add custom business context
            if "model_type" in request.path_params:
                span.set_attribute("ml.model_type", request.path_params["model_type"])

        start_time = time.perf_counter()

        # Execute request
        response = await call_next(request)
        duration_seconds = time.perf_counter() - start_time

        # Add response and authentication metadata
        if span.is_recording():
            # Add response status
            span.set_attribute("http.status_code", response.status_code)

            # Add authentication context
            if hasattr(request.state, "api_key_client_id"):
                span.set_attribute("auth.client_id", request.state.api_key_client_id)
            if hasattr(request.state, "api_key_id"):
                span.set_attribute("auth.key_id", request.state.api_key_id)

            # Mark cache hits
            if response.headers.get("x-cache") == "HIT":
                span.set_attribute("cache.hit", True)
                span.add_event("cache_hit")
            else:
                span.set_attribute("cache.hit", False)

        # Record cache status metrics for Prometheus
        cache_status = (response.headers.get("x-cache") or "UNKNOWN").upper()
        model_type = request.path_params.get("model_type", "unknown")
        CACHE_REQUEST_COUNT.labels(
            cache_status=cache_status,
            model_type=model_type,
            method=request.method,
            status=str(response.status_code),
        ).inc()

        REQUEST_DURATION_SECONDS.labels(
            model_type=model_type,
            method=request.method,
            status=str(response.status_code),
        ).observe(duration_seconds)

        return response


# ===== Define the stack of middleware =====
# REQUEST FLOW:
# RequestIDMiddleware (Outermost) -> TracingMiddleware
# -> LoggingMiddleware -> ErrorHandlingMiddleware -> BillingMiddleware -> [Endpoint]
#
# RESPONSE FLOW:
# [Endpoint] -> BillingMiddleware -> ErrorHandlingMiddleware
# -> LoggingMiddleware -> TracingMiddleware -> RequestIDMiddleware (Outermost)
MIDDLEWARE_STACK: list[type[BaseHTTPMiddleware]] = [
    RequestIDMiddleware,  # 1. Touches request first
    TracingMiddleware,  # 1a. Adds tracing info right after Request ID
    LoggingMiddleware,  # 2. Touches request second
    ErrorHandlingMiddleware,  # 3. Touches request third
    # BillingMiddleware,  # 4. Handles Usage (Runs after Cache/Router)
]
# Reverse the middleware stack to maintain the correct order! (LIFO: Last In, First Out for requests)
MIDDLEWARE_STACK.reverse()
