"""
-----------------------------------------------------------
Global singleton event loop for this worker process
-----------------------------------------------------------

Purpose
-------
  Celery workers (especially in prefork mode) can create/destroy
  many event loops during task execution, retries, timeouts, etc.
  Repeatedly creating and closing loops is expensive and can lead
  to warnings, resource leaks, or strange behaviour with libraries
  that expect a long-lived loop (e.g. aiohttp, httpx with async).

Strategy
--------
  -> Keep ONE stable event loop alive for the entire lifetime of
    this worker process.
  -> Recreate it **only** if it gets closed unexpectedly.
"""

import asyncio

from src import create_logger

logger = create_logger(__name__)
# Global event loop
_EVENT_LOOP: asyncio.AbstractEventLoop | None = None


def _get_worker_event_loop() -> asyncio.AbstractEventLoop:
    """Get or lazily initialize a stable, long-lived event loop for this worker.

    This function should be called whenever an async operation is about to run
    inside a Celery task (or any other code running in a worker process).

    Returns
    -------
    asyncio.AbstractEventLoop
        A stable event loop for the worker process.
    """
    global _EVENT_LOOP

    # Step 1: Check if there's already a current loop in this thread
    try:
        current_loop = asyncio.get_event_loop()
    except RuntimeError:
        # No loop is set in this thread yet; we'll create one later
        current_loop = None

    # Step 2: If we have a current loop AND it's still alive; prefer it
    if current_loop is not None and not current_loop.is_closed():
        return current_loop

    # Step 3: Fall back to our global stable loop (if it exists and is healthy)
    if _EVENT_LOOP is not None and not _EVENT_LOOP.is_closed():
        # Make sure it's set as the current loop for this thread
        asyncio.set_event_loop(_EVENT_LOOP)
        return _EVENT_LOOP

    # Step 4: No usable loop exists; create a fresh one and store it globally
    # - This should only happen:
    # - First time this worker needs async
    # - or after the previous loop was unexpectedly closed
    logger.debug("Creating new persistent event loop for worker")
    _EVENT_LOOP = asyncio.new_event_loop()
    asyncio.set_event_loop(_EVENT_LOOP)

    return _EVENT_LOOP
