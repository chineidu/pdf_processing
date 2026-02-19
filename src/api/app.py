"""Application factory for FastAPI app instance."""

import sys
import warnings

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from prometheus_fastapi_instrumentator import Instrumentator

from src import create_logger
from src.api.core.exceptions import BaseAPIError, api_error_handler
from src.api.core.lifespan import lifespan
from src.api.core.metrics import model_label
from src.api.core.middleware import MIDDLEWARE_STACK

# from src.api.routes import admin, apikeys, auth, health, predict, services
from src.api.routes import auth, health, presigned_urls
from src.config import app_config, app_settings
from src.observability.telemetry import setup_telemetry

warnings.filterwarnings("ignore")

# ---------------------------------------------------------
# LOGGING INITIALIZATION (runs once per worker process)
# ---------------------------------------------------------
logger = create_logger(
    "src.api.app",
    structured=False,
    log_file=None,
)
logger.info("Initializing FastAPI application")


def create_application() -> FastAPI:
    """Create and configure a FastAPI application instance.

    This function initializes a FastAPI application with custom configuration settings,
    adds CORS middleware, and includes API route handlers.

    Returns
    -------
    FastAPI
        A configured FastAPI application instance.
    """
    prefix: str = app_config.api_config.prefix
    auth_prefix: str = app_config.api_config.auth_prefix

    app = FastAPI(
        title=app_config.api_config.title,
        description=app_config.api_config.description,
        version=app_config.api_config.version,
        docs_url="/docs",
        redoc_url="/redoc",
        lifespan=lifespan,
    )

    # Add custom middleware (LIFO: Last In, First Out for requests)
    # These are added first (innermost layer, closest to routes)
    for mdlware in MIDDLEWARE_STACK:
        app.add_middleware(mdlware)  # ty:ignore[invalid-argument-type]

    # Configure CORS middleware (outer layer to ensure error responses have CORS headers)
    app.add_middleware(
        CORSMiddleware,  # type: ignore
        allow_origins=app_config.api_config.middleware.cors.allow_origins,
        allow_credentials=app_config.api_config.middleware.cors.allow_credentials,
        allow_methods=app_config.api_config.middleware.cors.allow_methods,
        allow_headers=app_config.api_config.middleware.cors.allow_headers,
    )

    # Add GZip middleware for response compression (outermost layer)
    app.add_middleware(GZipMiddleware, minimum_size=1000)  # ty:ignore[invalid-argument-type]

    # Include routers
    # Auth routes
    # app.include_router(apikeys.router, prefix=auth_prefix)
    app.include_router(auth.router, prefix=auth_prefix)
    # # Other routes
    # app.include_router(admin.router, prefix=prefix)
    app.include_router(health.router, prefix=prefix)
    app.include_router(presigned_urls.router, prefix=prefix)
    # app.include_router(predict.router, prefix=prefix)
    # app.include_router(services.router, prefix=prefix)

    # Add exception handlers
    app.add_exception_handler(BaseAPIError, api_error_handler)  # type: ignore

    # Initialize Prometheus instrumentation (Metrics)
    instrumentator = Instrumentator(
        should_group_status_codes=False,
        should_ignore_untemplated=True,
        should_instrument_requests_inprogress=True,
        excluded_handlers=["/metrics", "/health", "/docs", "/redoc", "/openapi.json"],
    )
    instrumentator.add(model_label)
    instrumentator.instrument(app).expose(app)

    #  Setup OpenTelemetry
    setup_telemetry(app)
    logger.info("OpenTelemetry initialized successfully")

    return app


# Module-level app for ASGI servers (Required)
app: FastAPI = create_application()

if __name__ == "__main__":
    try:
        uvicorn.run(
            "src.api.app:app",
            host=app_settings.HOST,
            port=app_settings.PORT,
            workers=app_settings.WORKERS,
            reload=app_settings.RELOAD,
            loop="uvloop",  # Use uvloop for better async performance
        )
    except (Exception, KeyboardInterrupt) as e:
        print(f"Error creating application: {e}")
        print("Exiting gracefully...")
        sys.exit(1)
