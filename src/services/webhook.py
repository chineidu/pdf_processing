"""Webhook service for sending notifications with HMAC signatures."""

import asyncio
import hashlib
import hmac
import json
import time
from typing import Any

import httpx
from prometheus_client import Counter, Histogram

from src import create_logger
from src.config import app_settings

logger = create_logger(__name__)


# ========================================
# Webhook Metrics (for SLO tracking)
# ========================================
WEBHOOK_ATTEMPTS_TOTAL = Counter(
    "webhook_attempts_total",
    "Total webhook delivery attempts",
    labelnames=["event_type"],
)

WEBHOOK_SUCCESSES_TOTAL = Counter(
    "webhook_successes_total",
    "Total successful webhook deliveries",
    labelnames=["event_type"],
)

WEBHOOK_FAILURES_TOTAL = Counter(
    "webhook_failures_total",
    "Total failed webhook deliveries",
    labelnames=["event_type", "error_type"],
)

WEBHOOK_DELIVERY_LATENCY_SECONDS = Histogram(
    "webhook_delivery_latency_seconds",
    "Webhook delivery latency in seconds",
    labelnames=["event_type"],
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)


class WebhookService:
    """Service for sending webhook notifications with HMAC signatures."""

    def __init__(self) -> None:
        self.webhook_url = app_settings.WEBHOOK_URL
        self.secret_key: bytes = (
            app_settings.WEBHOOK_SECRET_KEY.get_secret_value().encode()
        )
        self.max_retries = app_settings.WEBHOOK_MAX_RETRIES
        self.timeout = app_settings.WEBHOOK_TIMEOUT_SECONDS

        # Single client instance for connection pooling and efficiency
        self.client = httpx.AsyncClient(
            timeout=self.timeout,
            limits=httpx.Limits(
                max_connections=20,
                max_keepalive_connections=10,
            ),
        )

    async def __aenter__(self) -> "WebhookService":
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:  # noqa: ANN001
        """Async context manager exit - ensures client is closed."""
        await self.aclose()

    async def aclose(self) -> None:
        """Close the HTTP client session."""
        await self.client.aclose()

    def generate_signature(self, payload: str) -> str:
        """Generate HMAC signature for the given payload."""
        return hmac.new(self.secret_key, payload.encode(), hashlib.sha256).hexdigest()

    async def asend_webhook(
        self,
        payload: dict[str, Any],
        webhook_url: str | None = None,
        event_name: str | None = None,
    ) -> bool:
        """Asynchronously send a webhook notification with retries."""
        target_url = webhook_url or self.webhook_url
        if not target_url:
            logger.info("Webhook URL not configured. Skipping delivery.")
            return False

        payload_str = json.dumps(payload, separators=(",", ":"))
        signature = self.generate_signature(payload_str)

        resolved_event = event_name
        if not resolved_event:
            resolved_event = (
                "task.completed"
                if payload.get("status") == "completed"
                else "task.failed"
            )

        headers = {
            "Content-Type": "application/json",
            "X-Webhook-Signature": signature,
            "X-Webhook-Event": resolved_event,
        }

        start_time = time.time()

        for attempt in range(1, self.max_retries + 1):
            try:
                # Record attempt
                WEBHOOK_ATTEMPTS_TOTAL.labels(event_type=resolved_event).inc()

                response = await self.client.post(
                    target_url,
                    content=payload_str,
                    headers=headers,
                )
                response.raise_for_status()

                # Record success
                duration = time.time() - start_time
                WEBHOOK_SUCCESSES_TOTAL.labels(event_type=resolved_event).inc()
                WEBHOOK_DELIVERY_LATENCY_SECONDS.labels(
                    event_type=resolved_event
                ).observe(duration)

                logger.info(f"Webhook sent successfully on attempt {attempt}")
                return True

            except httpx.HTTPStatusError as e:
                error_type = f"http_{e.response.status_code}"
                logger.error(
                    f"HTTP error on attempt {attempt}: {e.response.status_code} - {e.response.text}"
                )
                if attempt == self.max_retries:
                    WEBHOOK_FAILURES_TOTAL.labels(
                        event_type=resolved_event,
                        error_type=error_type,
                    ).inc()
            except httpx.RequestError as e:
                error_type = "request_error"
                logger.error(f"Request error on attempt {attempt}: {e}")
                if attempt == self.max_retries:
                    WEBHOOK_FAILURES_TOTAL.labels(
                        event_type=resolved_event,
                        error_type=error_type,
                    ).inc()
            except Exception as e:
                error_type = type(e).__name__
                logger.error(f"Unexpected error on attempt {attempt}: {e}")
                if attempt == self.max_retries:
                    WEBHOOK_FAILURES_TOTAL.labels(
                        event_type=resolved_event,
                        error_type=error_type,
                    ).inc()

            if attempt < self.max_retries:
                delay = 2**attempt
                logger.info(f"Retrying webhook in {delay} seconds...")
                await asyncio.sleep(delay)

        # If we exhaust all retries, log the failure
        logger.error(f"Failed to send webhook after {self.max_retries} attempts.")
        return False

    def verify_signature(self, payload: str, received_signature: str) -> bool:
        """Verify the HMAC signature of an incoming webhook request."""
        expected_signature = self.generate_signature(payload)
        return hmac.compare_digest(expected_signature, received_signature)
