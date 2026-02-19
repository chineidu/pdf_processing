from contextlib import asynccontextmanager
from typing import AsyncGenerator

from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from src import create_logger
from src.config import app_config, app_settings
from src.schemas.types import EnvironmentEnum

logger = create_logger(name=__name__)


class AsyncDatabasePool:
    """AsyncDatabase connection pool with automatic reconnection."""

    def __init__(self, database_url: str) -> None:
        """Initialize"""
        self.database_url: str = database_url
        self._engine: AsyncEngine | None = None
        self._session_factory: async_sessionmaker[AsyncSession] | None = None
        self._setup_engine()

    def _setup_engine(self) -> None:
        """Set up the async database engine and session factory.

        Note
        -----
        database_url must start with postgresql+asyncpg://
        """
        IS_DEV: bool = (
            True if app_settings.ENV == EnvironmentEnum.DEVELOPMENT else False
        )
        self._engine = create_async_engine(
            self.database_url,
            pool_size=app_config.database_config.pool_size,  # Keep 30 connections in pool
            max_overflow=app_config.database_config.max_overflow,  # Allow N extra connections
            pool_timeout=app_config.database_config.pool_timeout,  # Wait N seconds for connection
            pool_recycle=app_config.database_config.pool_recycle,  # Recycle connections after 30 minutes
            pool_pre_ping=app_config.database_config.pool_pre_ping,  # Test connections before use
            # Log SQL in non-prod to catch issues like N+1 queries, etc.
            echo=IS_DEV,
        )

        self._session_factory = async_sessionmaker(
            bind=self._engine,
            # Recommended for async to avoid implicit I/O
            expire_on_commit=app_config.database_config.expire_on_commit,
        )
        logger.info("AsyncDatabase connection pool initialized")

    @asynccontextmanager
    async def aget_session(self) -> AsyncGenerator[AsyncSession, None]:
        """Get an async database session with automatic commit/rollback."""
        if not self._session_factory:
            raise RuntimeError("Session factory not initialized")

        async with self._session_factory() as session:
            try:
                yield session
                await session.commit()

            except Exception:
                await session.rollback()
                raise

            finally:
                # context manager will close the session
                pass

    async def ahealth_check(self) -> bool:
        """Check if async database is healthy."""
        try:
            async with self.aget_session() as session:
                await session.execute(text("SELECT 1"))
            return True
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False

    async def close(self) -> None:
        """Close connection pool."""
        if self._engine:
            await self._engine.dispose()
            logger.info("AsyncDatabase pool closed")

    @property
    def engine(self) -> AsyncEngine:
        """Get AsyncDatabase engine."""
        if self._engine is None:
            raise RuntimeError("AsyncDatabase engine is not initialized.")
        return self._engine
