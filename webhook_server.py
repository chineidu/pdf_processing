"""
Simple webhook server for testing PDF pipeline notifications.

Usage:
    uv run webhook_server.py

Then use this URL when creating upload slots:
    - Local: http://localhost:3000/webhook
    - With ngrok: https://your-ngrok-url.ngrok-free.app/webhook
"""

import hashlib
import hmac
import json
from datetime import datetime
from typing import Any

import uvicorn
from fastapi import FastAPI, Header, HTTPException, Request

from src.config import app_settings

app = FastAPI(title="PDF Pipeline Webhook Server")

# Store received webhooks (for demo purposes)
received_webhooks = []

# Your secret key (must match WEBHOOK_SECRET_KEY in .env)
SECRET_KEY = app_settings.WEBHOOK_SECRET_KEY.get_secret_value()


def verify_signature(payload: bytes, signature: str) -> bool:
    """Verify webhook signature."""
    expected = hmac.new(SECRET_KEY.encode("utf-8"), payload, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature)


@app.get("/")
async def root() -> dict[str, Any]:
    """Root endpoint - show received webhooks."""
    return {
        "message": "PDF Pipeline Webhook Server",
        "endpoint": "/webhook",
        "received_count": len(received_webhooks),
        "recent_webhooks": received_webhooks[-5:],  # Last 5
    }


@app.post("/webhook")
async def handle_webhook(
    request: Request,
    x_webhook_signature: str = Header(None),
    x_webhook_event: str = Header(None),
) -> dict[str, Any]:
    """
    Receive webhook notifications from PDF pipeline.

    This endpoint:
    1. Verifies the HMAC signature
    2. Logs the webhook payload
    3. Stores it for inspection
    4. Returns 200 OK quickly
    """
    # Get raw body for signature verification
    body = await request.body()

    # Verify signature (security check)
    if x_webhook_signature:
        if not verify_signature(body, x_webhook_signature):
            print("❌ Invalid webhook signature!")
            raise HTTPException(status_code=401, detail="Invalid signature")
        print("✅ Signature verified")
    else:
        print("⚠️  No signature provided (not recommended for production)")

    # Parse payload
    payload = json.loads(body)
    print("📥 Webhook payload:", json.dumps(payload, indent=2))

    # Extract webhook data
    task_id = payload.get("task_id")
    status = payload.get("status")
    event = x_webhook_event or "unknown"

    print("\n" + "=" * 60)
    print(f"📬 WEBHOOK RECEIVED at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    print(f"Event Type: {event}")
    print(f"Task ID: {task_id}")
    print(f"Status: {status}")

    if status == "completed":
        result_url = payload.get("file_result_url")
        completed_at = payload.get("completed_at")
        print("✅ Processing completed!")
        print(f"Metadata: {payload.get('metadata')}")
        print(f"Results URL: {result_url}")
        print(f"Completed at: {completed_at}")

        # Here you would:
        # - Download the results from result_url
        # - Update your database
        # - Notify the user
        # - Trigger next pipeline step

    elif status == "failed":
        error = payload.get("error")
        completed_at = payload.get("completed_at")
        print("❌ Processing failed!")
        print(f"Error: {error}")
        print(f"Failed at: {completed_at}")

        # Here you would:
        # - Log the error
        # - Notify the user
        # - Potentially retry

    print("=" * 60 + "\n")

    # Store webhook for inspection (in production, save to database)
    received_webhooks.append(
        {
            "received_at": datetime.now().isoformat(),
            "event": event,
            "payload": payload,
            "signature_valid": x_webhook_signature is not None,
        }
    )

    # IMPORTANT: Return 200 quickly (within 10 seconds)
    return {
        "status": "received",
        "task_id": task_id,
        "timestamp": datetime.now().isoformat(),
    }


@app.get("/webhooks")
async def list_webhooks() -> dict[str, Any]:
    """List all received webhooks (for debugging)."""
    return {"count": len(received_webhooks), "webhooks": received_webhooks}


@app.delete("/webhooks")
async def clear_webhooks() -> dict[str, Any]:
    """Clear webhook history."""
    received_webhooks.clear()
    return {"status": "cleared"}


if __name__ == "__main__":
    print("=" * 60)
    print("🚀 Starting PDF Pipeline Webhook Server")
    print("=" * 60)
    print()
    print("Server will run on: http://localhost:3000")
    print("Webhook endpoint: http://localhost:3000/webhook")
    print()
    print("To use with the PDF pipeline:")
    print()
    print("  curl -X POST http://localhost:8000/api/upload-slot \\")
    print('    -H "Content-Type: application/json" \\')
    print('    -d \'{"webhook_url": "http://localhost:3000/webhook"}\'')
    print()
    print("Or expose via ngrok for external access:")
    print("  ngrok http 3000")
    print()
    print("=" * 60)
    print()

    uvicorn.run(app, host="0.0.0.0", port=3000, log_level="info")
