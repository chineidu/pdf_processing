# pdf_processing

PDF processing service with an API, background workers, and storage-backed ingestion.

## Table of contents

- [Overview](#overview)
- [Technologies used](#technologies-used)
- [Quick start](#quick-start)
- [Running the code](#running-the-code)
- [Data flow (architecture)](#data-flow-architecture)
- [Code style guidelines](#code-style-guidelines)
- [Repository layout](#repository-layout)

## Overview

This project exposes a FastAPI service for uploads and task orchestration, then uses Celery workers to process PDFs and persist results. The local development stack includes PostgreSQL, RabbitMQ, Redis, and MinIO, plus Jaeger for tracing.

## Technologies used

Application and API:

- fastapi
- pydantic-settings
- msgspec
- omegaconf

Async, workers, and messaging:

- celery
- aio-pika

Storage and databases:

- boto3
- sqlalchemy
- asyncpg
- psycopg-binary
- redis
- aioredis
- aiocache

PDF and OCR:

- docling
- easyocr
- python-magic

Auth and security:

- passlib
- python-jose

Observability:

- opentelemetry-api
- opentelemetry-sdk
- opentelemetry-exporter-otlp-proto-grpc
- opentelemetry-instrumentation-fastapi
- opentelemetry-instrumentation-httpx
- opentelemetry-instrumentation-redis
- opentelemetry-instrumentation-sqlalchemy
- prometheus-client
- prometheus-fastapi-instrumentator

Dev tooling:

- ruff
- pyrefly
- ty
- prek

## Quick start

1. Install dependencies:

   ```sh
   make install
   ```

1. Start local services (Postgres, RabbitMQ, Redis, MinIO, Jaeger):

   ```sh
   make up
   ```

1. Run the API and worker in separate terminals:

```sh
make api-run
```

```sh
make worker-run
```

## Running the code

Local services:

- `make up` - start the stack
- `make down` - stop the stack
- `make restart` - restart the stack
- `make logs` - view logs
- `make status` - show container status
- `make health-check` - check service health

App processes:

- `make api-run` - start FastAPI with Uvicorn
- `make api-run-gunicorn` - start FastAPI with Gunicorn
- `make worker-run` - start the Celery worker
- `make storage-worker-run` - start the storage ingestion worker

Testing and quality:

- `make test`
- `make test-verbose`
- `make lint`
- `make format`
- `make type-check`

## Data flow (architecture)

High-level pipeline:

1. Client calls the FastAPI service to request uploads or start processing.
2. API writes metadata to PostgreSQL and uses Redis for caching and rate-limiting.
3. Files are stored in MinIO (S3-compatible) and bucket events publish to RabbitMQ.
4. Celery workers consume RabbitMQ events, fetch the file from storage, and run PDF/OCR processing.
5. Results are persisted to PostgreSQL and, if configured, webhook notifications are emitted.
6. Traces and metrics are exported via OpenTelemetry and Prometheus instrumentation.

Key components:

- API: FastAPI app in `src/api/app.py`
- Workers: Celery app and tasks in `src/celery_app` and `src/celery_app/tasks`
- Storage: S3/MinIO integration in `src/services/storage.py`
- Processing: PDF pipeline in `src/services/pdf_processor.py`

## Code style guidelines

- Use Python 3.11+ syntax and type hints throughout the codebase.
- Format and lint with Ruff (`make format`, `make lint`).
- Keep business logic in `src/services` and transport logic in `src/api`.
- Prefer async I/O for network and database operations.
- Add docstrings for public functions and classes; keep them concise.
- Handle errors explicitly with structured exceptions in `src/api/core/exceptions.py`.

## Repository layout

- `src/api` - FastAPI app, routes, and middleware
- `src/celery_app` - Celery configuration and task wiring
- `src/services` - domain services (storage, processing, webhooks)
- `src/db` - database models and repositories
- `docker/` - container init scripts
- `docs/` - operational notes
