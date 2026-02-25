import asyncio
from contextlib import contextmanager
from functools import cached_property
from typing import TYPE_CHECKING, Generator

from celery import Task
from sqlalchemy.ext.asyncio import AsyncSession

from src import create_logger
from src.celery_app.app import celery_app
from src.db.models import aget_db_pool

if TYPE_CHECKING:
    from src.services.pdf_processor import PDFProcessor
    from src.services.storage import S3StorageService
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

    @contextmanager
    def db_session(self) -> Generator[AsyncSession, None, None]:
        """
        Get a database session context manager for sync Celery tasks.

        This wraps async session operations to work in a synchronous Celery task context.

        Yields
        ------
        AsyncSession
            A database session

        Example
        -------
            @shared_task(base=CustomTask)
            def my_task():
                with self.db_session() as session:
                    task_repo = TaskRepository(db=session)
                    task = asyncio.run(task_repo.aupdate_task_status(...))
        """

        async def _aget_session() -> AsyncSession:
            pool = await aget_db_pool()
            async with pool.aget_session() as session:
                return session

        session = asyncio.run(_aget_session())
        try:
            yield session
        finally:
            # Session is auto-closed by the context manager in pool.aget_session()
            pass

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
