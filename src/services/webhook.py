"""Webhook service for sending notifications with HMAC signatures."""

import asyncio
import hashlib
import hmac
import json
from typing import Any

import httpx

from src import create_logger
from src.config import app_settings

logger = create_logger(__name__)


class WebhookService:
    """Service for sending webhook notifications with HMAC signatures."""

    def __init__(self) -> None:
        self.webhook_url = app_settings.WEBHOOK_URL
        self.secret_key: bytes = (
            app_settings.WEBHOOK_SECRET_KEY.get_secret_value().encode()
        )
        self.max_retries = app_settings.WEBHOOK_MAX_RETRIES
        self.timeout = app_settings.WEBHOOK_TIMEOUT_SECONDS

    def generate_signature(self, payload: str) -> str:
        """Generate HMAC signature for the given payload."""
        return hmac.new(self.secret_key, payload.encode(), hashlib.sha256).hexdigest()

    async def asend_webhook(self, payload: dict[str, Any]) -> bool:
        """Asynchronously send a webhook notification with retries."""
        payload_str = json.dumps(payload)
        signature = self.generate_signature(payload_str)
        headers = {
            "Content-Type": "application/json",
            "X-Webhook-Signature": signature,
            "X-Webhook-Event": "task.completed"
            if payload.get("status") == "completed"
            else "task.failed",
        }

        async with httpx.AsyncClient() as client:
            for attempt in range(1, self.max_retries + 1):
                try:
                    response = await client.post(
                        self.webhook_url,
                        data=payload,
                        headers=headers,
                        timeout=self.timeout,
                    )
                    response.raise_for_status()
                    logger.info(f"Webhook sent successfully on attempt {attempt}")
                    return True

                except httpx.HTTPStatusError as e:
                    logger.error(
                        f"HTTP error on attempt {attempt}: {e.response.status_code} - {e.response.text}"
                    )
                except httpx.RequestError as e:
                    logger.error(f"Request error on attempt {attempt}: {e}")
                except Exception as e:
                    logger.error(f"Unexpected error on attempt {attempt}: {e}")

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
