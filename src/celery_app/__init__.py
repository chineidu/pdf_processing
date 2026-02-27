from functools import cached_property
from typing import TYPE_CHECKING

from celery import Task

from src import create_logger
from src.celery_app.app import celery_app

if TYPE_CHECKING:
    from src.services.pdf_processor import PDFProcessor
    from src.services.storage import S3StorageService
    from src.services.webhook import WebhookService
logger = create_logger(name="celery_app")

__all__ = ["BaseCustomTask", "CustomTask", "celery_app"]

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
    throws = (Exception,)  # Log full traceback on retry
    default_retry_delay = 30  # 30 seconds
    max_retries = 5
    retry_backoff = True


class CustomTask(BaseCustomTask):
    """Custom task class with sync database session support for Celery tasks.

    Provides singleton instances of other services through cached properties.
    Since Celery Task subclasses are singletons, these instances are shared across all task executions.
    """

    @cached_property
    def processor(self) -> "PDFProcessor":
        """Lazily initialize and return the singleton PDFProcessor instance.

        Returns
        -------
        PDFProcessor
            Cached processor instance, created on first access.
        """
        from src.services.pdf_processor import PDFProcessor

        return PDFProcessor()

    @cached_property
    def s3_service(self) -> "S3StorageService":
        """Lazily initialize and return the singleton S3StorageService instance.

        Returns
        -------
        S3StorageService
            Cached S3 service instance, created on first access.
        """
        from src.services.storage import S3StorageService

        return S3StorageService()

    @cached_property
    def webhook_service(self) -> "WebhookService":
        """Lazily initialize and return the singleton WebhookService instance.

        Returns
        -------
        WebhookService
            Cached webhook service instance, created on first access.
        """
        from src.services.webhook import WebhookService

        return WebhookService()
