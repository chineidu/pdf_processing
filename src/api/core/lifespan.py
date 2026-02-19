import time
import warnings
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, AsyncGenerator

from fastapi import FastAPI

from src import create_logger
from src.api.core.cache import setup_cache
from src.config import app_settings
from src.db.init import ainit_db
from src.services.storage import S3StorageService

if TYPE_CHECKING:
    pass

warnings.filterwarnings("ignore")
logger = create_logger(name=__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:  # noqa: ARG001
    """Initialize and cleanup FastAPI application lifecycle.

    This context manager handles the initialization of required resources
    during startup and cleanup during shutdown.
    """
    try:
        start_time: float = time.perf_counter()
        logger.info(f"ENVIRONMENT: {app_settings.ENV} | DEBUG: {app_settings.DEBUG} ")
        logger.info("Starting up application and loading model...")

        # ====================================================
        # ================= Load Dependencies ================
        # ====================================================

        # --------- Setup Database ----------
        await ainit_db()

        # ---------- Setup cache ----------
        app.state.cache = setup_cache()
        logger.info("✅ Cache initialized")

        logger.info(
            f"Application startup completed in {time.perf_counter() - start_time:.2f} seconds"
        )

        # ---------- Setup storage service ----------
        app.state.s3_service = S3StorageService()

        # Yield control to the application
        yield

    # ====================================================
    # =============== Cleanup Dependencies ===============
    # ====================================================
    except Exception as e:
        logger.error("❌ Application startup failed")
        logger.error(f"   Reason: {e}")
        raise

    finally:
        logger.info("Shutting down application...")

        # ---------- Cleanup cache ----------
        if hasattr(app.state, "cache") and app.state.cache:
            try:
                await app.state.cache.aclose()  # ty:ignore[unresolved-attribute]
                logger.info("🚨 Cache shutdown.")

            except Exception as e:
                logger.error(f"❌ Error shutting down the cache: {e}")
