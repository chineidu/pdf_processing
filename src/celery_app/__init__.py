from typing import TYPE_CHECKING

from celery import Task

from src import create_logger
from src.celery_app.app import celery_app

if TYPE_CHECKING:
    pass
logger = create_logger(name="celery_app")

__all__ = ["BaseCustomTask", "celery_app"]

# Import signals to register them (this ensures signal handlers are connected)
from src.celery_app import signals  # noqa: E402, F401


class BaseCustomTask(Task):
    """
    A custom base task class for Celery tasks with automatic retry configuration.

    Attributes
    ----------
    autoretry_for : tuple
        A tuple of exception types for which the task should automatically retry.
    throws : tuple
        A tuple of exception types for which full traceback should be logged on retry.
    default_retry_delay : int
        The default delay between retries in seconds.
    max_retries : int
        The maximum number of retries allowed for the task.
    retry_backoff : bool
        Enables exponential backoff for retry delays.
    retry_backoff_max : int
        The maximum delay in seconds for exponential backoff.
    """

    autoretry_for = (Exception,)
    throws = (Exception,)  # Log full traceback on retry # type: ignore
    default_retry_delay = 30  # 30 seconds
    max_retries = 5
    retry_backoff = True
