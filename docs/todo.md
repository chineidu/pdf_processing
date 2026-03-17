# Project TODO and Gap Analysis

This document tracks what already exists and what is still missing to make this project production-ready.

## Current Feature Coverage

### Platform and Runtime

- FastAPI service with middleware stack, metrics endpoint integration, and telemetry bootstrap (`src/api/app.py`).
- Celery workers with priority queues and task routing (`src/celery_app/app.py`, `src/celery_app/tasks/processor.py`).
- Local environment via Docker Compose (PostgreSQL, RabbitMQ, Redis, MinIO, Jaeger) and helper scripts (`docker-compose.yaml`, `docker/`).

### Product Features

- API routes for auth, health, presigned upload URLs, and task status (`src/api/routes/v1/`).
- Web UI for auth and upload flow (`src/api/routes/ui.py`, `templates/auth.html`, `templates/upload.html`).
- PDF processing service with validation, inspection, OCR integration, and export handling (`src/services/pdf_processor.py`).
- Storage-backed ingestion worker for event-driven processing (`src/services/ingestion/ingestion_worker.py`).
- Webhook notification service and task metadata updates (`src/services/webhook.py`, `src/celery_app/tasks/processor.py`).

### Data and Infrastructure

- SQLAlchemy models and repository pattern for users, tasks, and API entities (`src/db/models.py`, `src/db/repositories/`).
- Config system using Pydantic settings + YAML config files (`src/config/`).
- Observability baseline via Prometheus + OpenTelemetry (`src/observability/telemetry.py`).

## Key Missing Features

These are the highest-value gaps found in the current repository.

### P0: Must Have (Reliability and Security)

- **Automated test suite is missing**: no `tests/` directory found. Add unit, integration, and worker flow tests.
- **CI pipeline is missing**: no `.github/workflows/` found. Add PR checks for lint, type-check, and tests.
- **Database migrations are missing**: no `alembic/` or migration workflow found. Add migration tooling and versioned schema changes.
- **Webhook hardening**: add strict URL allowlist/validation and signed webhook payloads to reduce SSRF and spoofing risks.
- **Operational failure controls**: add dead-letter queues, explicit retry policies, and poison-message handling for ingestion/worker paths.

### P1: Should Have (Scale and Operability)

- **Idempotency guarantees across the full pipeline**: ensure repeated storage events or retries do not duplicate work or billing.
- **Progress model for long tasks**: support progress percentage/state transitions and expose it in API/UI.
- **SLO-style monitoring and alerting**: define latency/error SLOs and alert rules, not just raw metrics export.
- **Backpressure and concurrency controls**: tune queue QoS, worker concurrency, and task size limits by queue class.
- **Cost and usage controls**: enforce per-user quotas/rate limits and audit usage against billing credits.

### P2: Nice to Have (Product Maturity)

- **Real-time status updates in UI**: WebSocket or SSE updates instead of manual polling.
- **Admin and support tooling**: admin endpoints/UI for failed tasks, replays, and manual task repair.
- **Versioned API contract artifacts**: OpenAPI snapshot/versioning plus client SDK generation.
- **Benchmark suite**: repeatable throughput/latency benchmarks for representative PDF sizes and OCR-heavy jobs.

## Recommended Execution Plan

### Phase 1 (1-2 weeks)

- Create `tests/` with smoke tests for API routes and unit tests for `PDFProcessor` and storage service.
- Add CI workflow to run `make lint`, `make type-check`, and `make test` on pull requests.
- Introduce Alembic migration scaffold and initial baseline migration.

### Phase 2 (2-4 weeks)

- Add queue DLQ/retry policy and idempotency keys around ingestion-to-worker transitions.
- Harden webhook delivery (signature, retries with backoff, endpoint validation).
- Add task progress tracking in DB and API response model.

### Phase 3 (4+ weeks)

- Implement real-time task updates in UI.
- Add alerting dashboards and runbook docs for incidents.
- Add performance benchmark suite and capacity targets.

## Concrete Next Tasks

- [ ] Create `tests/unit/test_pdf_processor.py` with happy-path and invalid-file cases.
- [ ] Create `tests/integration/test_presigned_urls.py` for auth + upload URL issuance.
- [ ] Add `.github/workflows/ci.yml` for lint/type-check/tests.
- [ ] Initialize `alembic/` and commit the first schema migration.
- [ ] Add webhook signature verification and request signing support.
- [ ] Add DLQ and poison-message monitoring for worker queues.

---
Last updated: 2026-03-14
