"""
Async Database initialization utilities.
"""

from src import create_logger
from src.db.models import Base, aget_db_pool

logger = create_logger(name=__name__)


async def ainit_db() -> None:
    """Initialize the asynchronous database."""
    db_pool = await aget_db_pool()
    # Create all tables in the database that are defined by the Base's subclasses
    async with db_pool.engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    logger.info("AsyncDatabase tables initialized successfully")
