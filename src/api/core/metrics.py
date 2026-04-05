from prometheus_client import Counter, Histogram
from prometheus_fastapi_instrumentator.metrics import Info

# ========================================
# HTTP Request Metrics (for SLO tracking)
# ========================================

# 1. Define the metric globally so it persists across requests
# We use a distinct name to avoid conflicts with default metrics
# Note: `model_type` is obtained dynamically per request from the path
MODEL_REQUEST_COUNT = Counter(
    "http_requests_by_model_total",
    "Total HTTP requests grouped by model type",
    labelnames=["model_type", "method", "status"],
)

CACHE_REQUEST_COUNT = Counter(
    "http_requests_by_cache_status_total",
    "Total HTTP requests grouped by cache status",
    labelnames=["cache_status", "model_type", "method", "status"],
)

REQUEST_DURATION_SECONDS = Histogram(
    "http_request_duration_seconds",
    "Request duration in seconds",
    labelnames=["model_type", "method", "status"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)

# SLO-specific HTTP metrics
HTTP_REQUESTS_TOTAL = Counter(
    "http_requests_total",
    "Total HTTP requests (SLO tracking)",
    labelnames=["endpoint", "method", "status", "endpoint_category"],
)

HTTP_REQUEST_ERRORS_TOTAL = Counter(
    "http_request_errors_total",
    "Total HTTP request errors (5xx and 4xx) for SLO tracking",
    labelnames=["endpoint", "method", "status", "endpoint_category"],
)

HTTP_REQUEST_LATENCY_SECONDS = Histogram(
    "http_request_latency_seconds",
    "HTTP request latency in seconds (SLO tracking)",
    labelnames=["endpoint", "method", "endpoint_category"],
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)


def model_label(info: Info) -> None:
    """Add model_type label to Prometheus metrics if available in the request path."""
    # 2. Extract the path param (e.g., from /v1/{model_type}/predict)
    # Note: path_params is only populated if the router matched the request
    model_type = info.request.path_params.get("model_type")

    # 3. Only record if model_type is present
    if model_type:
        status_code = info.response.status_code if info.response else 500
        MODEL_REQUEST_COUNT.labels(
            model_type=model_type,
            method=info.method,
            status=str(status_code),
        ).inc()


def slo_metrics(info: Info) -> None:
    """Record SLO-specific metrics for all endpoints."""
    if not info.response:
        return

    status_code = info.response.status_code
    method = info.method
    endpoint = info.request.url.path or "unknown"

    # Determine endpoint category based on path
    endpoint_category = _categorize_endpoint(endpoint)

    # Record total requests
    HTTP_REQUESTS_TOTAL.labels(
        endpoint=endpoint,
        method=method,
        status=str(status_code),
        endpoint_category=endpoint_category,
    ).inc()

    # Record errors (4xx and 5xx)
    if status_code >= 400:
        HTTP_REQUEST_ERRORS_TOTAL.labels(
            endpoint=endpoint,
            method=method,
            status=str(status_code),
            endpoint_category=endpoint_category,
        ).inc()

    # Record latency if available
    if hasattr(info, "response") and hasattr(info.response, "__dict__"):
        # Duration is in seconds
        duration = info.modified_duration / 1000.0 if info.modified_duration else 0
        HTTP_REQUEST_LATENCY_SECONDS.labels(
            endpoint=endpoint,
            method=method,
            endpoint_category=endpoint_category,
        ).observe(duration)


def _categorize_endpoint(path: str) -> str:
    """Categorize endpoint by its path for SLO tracking."""
    if "/tasks" in path:
        return "task-management"
    if "/presigned-urls" in path:
        return "upload"
    if "/webhooks" in path:
        return "webhooks"
    if "/health" in path or "/metrics" in path:
        return "observability"
    if "/api-keys" in path:
        return "authentication"
    if "/auth" in path:
        return "authentication"
    return "other"
