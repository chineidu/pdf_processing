from prometheus_client import Counter, Histogram
from prometheus_fastapi_instrumentator.metrics import Info

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
