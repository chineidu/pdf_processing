"""Custom exceptions for the API module."""

from fastapi import Request, status

from src.api.core.responses import MsgSpecJSONResponse
from src.schemas.types import ErrorCodeEnum


class BaseAPIError(Exception):
    """Base exception for API-related errors."""

    def __init__(
        self,
        message: str,
        status_code: int = status.HTTP_500_INTERNAL_SERVER_ERROR,
        error_code: str = ErrorCodeEnum.INTERNAL_SERVER_ERROR,
        headers: dict[str, str] | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        self.error_code = error_code or self.__class__.__name__
        self.headers = headers or {}
        super().__init__(message)


class UnauthorizedError(BaseAPIError):
    """Exception raised for unauthorized access."""

    def __init__(self, details: str, headers: dict[str, str] | None = None) -> None:
        message = f"Unauthorized access: {details}"
        self.headers = {"WWW-Authenticate": "Bearer"}
        super().__init__(
            message,
            status_code=status.HTTP_401_UNAUTHORIZED,
            error_code=ErrorCodeEnum.UNAUTHORIZED,
            headers=headers,
        )


class HTTPError(BaseAPIError):
    """Exception raised for HTTP error."""

    def __init__(
        self,
        details: str,
        status_code: int = status.HTTP_503_SERVICE_UNAVAILABLE,
        headers: dict[str, str] | None = None,
    ) -> None:
        message = f"HTTP error: {details}"
        super().__init__(
            message,
            status_code=status_code,
            error_code=ErrorCodeEnum.HTTP_ERROR,
            headers=headers,
        )


class CircuitOpenError(BaseAPIError):
    """Exception raised when the circuit breaker is open and requests are blocked."""

    def __init__(self, details: str, headers: dict[str, str] | None = None) -> None:
        message = f"Circuit breaker open: {details}"
        super().__init__(
            message,
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            error_code=ErrorCodeEnum.CIRCUIT_OPEN_ERROR,
            headers=headers,
        )


class ServiceUnavailableError(BaseAPIError):
    """Exception raised when no healthy service instances are available."""

    def __init__(
        self, service_name: str, headers: dict[str, str] | None = None
    ) -> None:
        message = f"No healthy instances available for service: {service_name}"
        super().__init__(
            message,
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            error_code=ErrorCodeEnum.SERVICE_UNAVAILABLE,
            headers=headers,
        )


class RateLimitError(BaseAPIError):
    """Exception raised when a rate limit is exceeded."""

    def __init__(self, details: str, headers: dict[str, str] | None = None) -> None:
        message = f": {details}"
        super().__init__(
            message,
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            error_code=ErrorCodeEnum.RATE_LIMIT_ERROR,
            headers=headers,
        )


class UnexpectedError(BaseAPIError):
    """Exception raised for unexpected errors."""

    def __init__(self, details: str, headers: dict[str, str] | None = None) -> None:
        message = f"An unexpected error occurred: {details}"
        super().__init__(
            message,
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_code=ErrorCodeEnum.UNEXPECTED_ERROR,
            headers=headers,
        )


# ====== API Exception Handler ====== #
async def api_error_handler(request: Request, exc: BaseAPIError) -> MsgSpecJSONResponse:
    """Handle all custom API errors.

    This handler processes all exceptions that inherit from BaseAPIError,
    returning a consistent JSON response format with appropriate status codes.

    Parameters
    ----------
    request : Request
        The incoming request object.
    exc : BaseAPIError
        The exception that was raised.

    Returns
    -------
    MsgSpecJSONResponse
        A JSON response with error details, status code, and optional headers.
    """
    response_content = {
        "status": "error",
        "error": {
            "message": exc.message,
            "code": exc.error_code,
        },
        "request_id": request.headers.get("X-Request-ID", "unknown"),
        "path": request.url.path,
    }

    # Include headers if the exception has them (e.g., UnauthorizedError)
    headers = getattr(exc, "headers", None)

    return MsgSpecJSONResponse(
        status_code=exc.status_code,
        content=response_content,
        headers=headers,
    )
