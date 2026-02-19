"""
OpenTelemetry configuration for distributed tracing. Provides automatic instrumentation for FastAPI,
HTTP clients, Redis, and SQLAlchemy. Traces are exported to Jaeger/OTLP collector for analysis.
"""

from typing import Any

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from opentelemetry.instrumentation.redis import RedisInstrumentor
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter

from src import create_logger
from src.config import app_settings

logger = create_logger(name=__name__)


def setup_telemetry(app: Any) -> None:
    """
    Initialize OpenTelemetry tracing with automatic instrumentation.

    Sets up:
    - FastAPI request/response tracing
    - HTTPX client tracing (backend calls)
    - Redis operation tracing
    - SQLAlchemy database tracing
    - Span export to OTLP collector or console

    Parameters
    ----------
    app : Any
        FastAPI application instance to instrument.

    Returns
    -------
    None
    """

    # Create tracer provider with service metadata
    resource = Resource(
        attributes={
            SERVICE_NAME: app_settings.OTEL_SERVICE_NAME,
            "environment": app_settings.ENV,
            "version": app_settings.OTEL_SERVICE_VERSION,
        }
    )

    provider = TracerProvider(resource=resource)
    trace.set_tracer_provider(provider)

    # Configure span exporters
    # Export to OTLP collector (Jaeger, Tempo, etc.)
    if app_settings.OTEL_EXPORTER_OTLP_ENDPOINT:
        otlp_exporter = OTLPSpanExporter(
            endpoint=app_settings.OTEL_EXPORTER_OTLP_ENDPOINT,
            insecure=True,  # Use False with TLS in production
        )
        provider.add_span_processor(BatchSpanProcessor(otlp_exporter))
        logger.info(
            f"OTLP exporter configured: {app_settings.OTEL_EXPORTER_OTLP_ENDPOINT}"
        )

    # Console exporter for development/debugging
    if app_settings.OTEL_CONSOLE_EXPORTER_ENABLED:
        console_exporter = ConsoleSpanExporter()
        provider.add_span_processor(BatchSpanProcessor(console_exporter))
        logger.info("Console span exporter enabled")

    # Instrument FastAPI application
    # Automatically creates spans for all HTTP requests
    FastAPIInstrumentor.instrument_app(
        app,
        tracer_provider=provider,
        # Exclude health check endpoints from tracing
        excluded_urls="health,metrics",
    )
    logger.info("FastAPI instrumentation enabled")

    # Instrument HTTP client (for backend calls)
    # Traces all httpx requests and propagates trace context
    # Ensure HTTPXClientInstrumentor is instantiated correctly
    httpx_instrumentor = HTTPXClientInstrumentor()
    if httpx_instrumentor is not None:
        httpx_instrumentor.instrument(
            tracer_provider=provider,
        )
        logger.info("HTTPX client instrumentation enabled")
    else:
        logger.warning("HTTPXClientInstrumentor is None and cannot be instrumented.")
    logger.info("HTTPX client instrumentation enabled")

    # Instrument Redis (for caching operations)
    redis_instrumentor = RedisInstrumentor()
    if redis_instrumentor is not None:
        redis_instrumentor.instrument(
            tracer_provider=provider,
        )
        logger.info("Redis instrumentation enabled")
    else:
        logger.warning("RedisInstrumentor is None and cannot be instrumented.")
    # Instrument SQLAlchemy (for database operations)
    # Note: This instruments the engine, so call after creating engine
    # SQLAlchemyInstrumentor().instrument(
    #     tracer_provider=provider,
    #     engine=your_db_engine,  # Pass your engine here
    # )
    logger.info("SQLAlchemy instrumentation ready (instrument after engine creation)")
