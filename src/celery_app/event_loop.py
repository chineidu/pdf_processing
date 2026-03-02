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

# src/celery_app/event_loop.py
import asyncio
import threading

from src import create_logger

logger = create_logger(__name__)

# Thread-local storage: each thread gets its own _EVENT_LOOP instance
_thread_local = threading.local()


def _get_worker_event_loop() -> asyncio.AbstractEventLoop:
    """Get or lazily initialize a stable, long-lived event loop for this thread.

    With Celery's thread pool, multiple tasks run in separate threads.
    Each thread must have its OWN event loop — sharing one loop across
    threads causes 'This event loop is already running' errors.

    Returns
    -------
    asyncio.AbstractEventLoop
        A stable, thread-local event loop.
    """
    loop: asyncio.AbstractEventLoop | None = getattr(_thread_local, "event_loop", None)

    # Reuse if healthy
    if loop is not None and not loop.is_closed():
        return loop

    # Create a new loop for this thread
    logger.debug(
        f"Creating new persistent event loop for thread {threading.current_thread().name}"
    )
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    _thread_local.event_loop = loop

    return loop
