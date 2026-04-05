# Service Level Indicators (SLIs) and Objectives (SLOs)

This document defines how we measure and manage reliability for the PDF processing service. By tracking SLIs (actual metrics) against SLOs (targets), we make data-driven decisions about when to invest in stability versus new features.

---

## Quick Definitions

**Service Level Indicator (SLI)**: A quantitative metric reflecting what users actually experience. It answers: *How is the service performing right now?*

Examples: "% of API requests returning 2xx status", "% of uploads validated within 60 seconds", "% of webhooks delivered successfully".

**Service Level Objective (SLO)**: A target or goal for one or more SLIs over a specific time period (e.g., 30 days). It answers: *How good do we need to be?*

Examples: "99.5% of requests must succeed", "95% of responses must complete under 300ms", "99% of webhooks must deliver within 5 minutes".

**Error Budget**: The "allowed failure" portion of your SLO. If your SLO is 99.5% availability, you have a 0.5% error budget—enough room for occasional incidents without violating the SLO. Teams use this budget to decide: stabilize now, or ship new features?

---

## Critical SLIs for PDF Processing

We measure five core SLIs tied to the key user workflows: uploading, validating, processing, tracking status, and receiving results.

### 1. **Presigned URL & API Availability**

**What we measure**: % of HTTP requests returning a 2xx status code across all API endpoints.

**Why it matters**: Users can't interact with the service if the API is unavailable.

**How we track it**: Prometheus counter `http_requests_total` (per status code) at `/metrics`. FastAPI middleware captures all endpoint responses.

**SLO Target**: **99.5% availability** over any 30-day rolling window.

- Allows ~3.6 minutes of downtime per month.
- Example: If 1,000,000 requests/month, up to 5,000 failures are acceptable.

**Associated workflows**:

- POST `/api/v1/presigned-urls` (request upload slot)
- GET `/api/v1/tasks/{task_id}` (poll status)
- POST `/api/v1/tasks/{task_id}/uploaded` (confirm upload complete)

---

### 2. **API Response Latency**

**What we measure**: % of API responses completing under a latency threshold.

**Why it matters**: Slow APIs degrade user experience; users expect responses in under 300ms for interactive endpoints.

**How we track it**: Prometheus histogram `http_request_duration_seconds` (per endpoint) at `/metrics`.

**SLO Target**: **95% of requests must complete under 300ms** over any 30-day rolling window.

- Reflects normal network + processing time under regular load.
- Presigned URL generation and status polling are typically fast (direct DB queries or S3 API calls).
- Allows 5% of requests to exceed 300ms during traffic spikes or degraded database performance.

**Associated workflows**:

- Presigned URL endpoint (should be <50ms typically; allows caching via Redis).
- Status polling endpoint (direct DB read; typically <100ms).

---

### 3. **Upload Validation Success Rate**

**What we measure**: % of uploaded PDFs that are successfully validated and transitioned from PENDING → UPLOADED → VALIDATING within a time threshold.

**Why it matters**: If uploads fail validation, users see errors and can't proceed. This measures the reliability of the validation pipeline.

**How we track it**: Task status transitions via Celery task counters (`celery_task_started`, `celery_task_succeeded`, `celery_task_failed`) at `/metrics`, plus database query of task status distribution.

**SLO Target**: **99% of uploads must validate successfully within 60 seconds** of confirmation.

- Allows 1% to fail due to corrupted PDFs, unsupported formats, or validation service errors.
- 60-second window covers typical Celery task dispatch + validation logic (<5 seconds) with headroom for queue depth.

**Associated workflows**:

- Task created in PENDING state after presigned URL generation.
- After user uploads to S3, POST `/api/v1/tasks/{task_id}/uploaded` confirms and queues validation.
- Celery worker picks up validation task and transitions to PROCESSING.

---

### 4. **Task Processing Completion Rate**

**What we measure**: % of tasks that reach a terminal state (COMPLETED, FAILED, UNPROCESSABLE, or SKIPPED) within a reasonable time.

**Why it matters**: Users need to know when their PDF has finished processing. Long-running stuck tasks indicate system issues (crashed workers, queue backlog, storage failures).

**How we track it**: Task status in PostgreSQL; query tasks created 24+ hours ago to measure how many reached terminal state. Celery task duration histogram at `/metrics`.

**SLO Target**: **98% of tasks must reach a terminal state within 24 hours** of validation completion.

- Accounts for queue depth, worker availability, and large PDFs with slow OCR/extraction.
- 2% allowance covers edge cases: extremely large PDFs (1000+ pages), worker crashes requiring retry, or temporary service degradation.

**Associated workflows**:

- Task transitions: PENDING → UPLOADED → VALIDATING → PROCESSING → COMPLETED/FAILED/SKIPPED/UNPROCESSABLE.
- Celery workers subscribe to priority queues; long queue times impact SLI but are within user's visibility (status polling).

---

### 5. **Webhook Delivery Success Rate**

**What we measure**: % of task completion webhooks that are successfully delivered to the customer's endpoint within a time threshold.

**Why it matters**: Webhooks provide real-time async notification; failed delivery means users miss result updates and may miss time-sensitive actions.

**How we track it**: Webhook service tracks delivery success/failure in PostgreSQL (`webhooks` table); Prometheus counter for webhook delivery attempts and successes.

**SLO Target**: **99% of webhooks must be delivered successfully within 5 minutes** of task completion.

- Includes retries with exponential backoff for transient failures.
- 1% allowance covers permanent customer endpoint failures (404, 410, auth issues) that are outside our control.

**Associated workflows**:

- Customer registers webhook URL when creating presigned URL (optional).
- After task completes (success or failure), webhook service sends POST with HMAC signature to registered endpoint.
- Retries at: 1s, 10s, 30s, 2m, 5m with backoff; after 5 retries mark as failed.

---

## Relating SLIs to SLOs

| SLI | SLO Target | Type | Monitoring |
|-----|-----------|------|------------|
| API requests returning 2xx | 99.5% over 30 days | Availability | `/metrics` counter by status code |
| Requests completing <300ms | 95% over 30 days | Latency | `/metrics` histogram by endpoint |
| Uploads validating successfully | 99% within 60s of confirmation | Speed + Success | Celery task counters + DB query |
| Tasks reaching terminal state | 98% within 24 hours | Reliability | Task status distribution histogram |
| Webhooks delivered successfully | 99% within 5 minutes | Delivery | Webhook service counter + retry log |

---

## Error Budgets and Decision-Making

**How to use error budgets**:

1. **Calculate** your "budget" from the SLO. Example: 99.5% availability SLO = 0.5% error budget.
2. **Track actual** SLI performance daily/weekly. Example: If you hit 98% actual availability, you've burned 1% of monthly budget.
3. **Decide**:

   - **Budget ahead of time?** -> Ship features, run experiments.
   - **Budget nearly exhausted?** -> Focus on stability: pay down technical debt, improve observability, add redundancy.
   - **SLO violated?** -> Post-incident review; prioritize root-cause fixes before new work.

**Example scenario** (Presigned URL SLO = 99.5%):

- Month 1: 99.7% actual (0.2% burned; 0.3% remaining).
- Month 2: 99.2% actual (0.8% burned; 0.2% remaining).
- Month 3 projection: Only 0.2% budget remaining. Team **pauses feature work** and focuses on database query optimization and API timeouts to reclaim margin. After improvements, achieve 99.6% and restore budget.

---

## Getting Started: Setting Baselines Before Committing

Before locking in SLO targets, collect **2–4 weeks of baseline data**:

1. **Deploy monitoring**: Ensure all SLI metrics are being collected in Prometheus (usually done at app startup).
2. **Record percentiles**: Track P50, P95, P99 latencies; count successes vs. failures for each SLI.
3. **Identify anomalies**: Note any spikes, patterns, or external factors (database maintenance, high load tests).
4. **Set realistic targets**: SLOs should be achievable under normal conditions with 5–10% margin.

   - If baseline shows 96% latency <300ms, don't commit to 99%; commit to 95% to leave safety margin.
   - If baseline shows 99.2% uptime, 99% SLO is realistic; avoid 99.9% unless you've over-provisioned.

---

## Health Checks & Monitoring

Monitor SLI compliance via:

- **Health endpoint** (`GET /api/v1/health`): Returns service status and basic metrics; use for alerting on availability.
- **Prometheus metrics** (`GET /metrics`): Detailed SLI data; scrape every 15 seconds into Prometheus/Grafana for dashboards and SLO calculations.
- **Celery monitoring**: Task counters exposed via `/metrics` for success/failure rates and latencies.
- **Observability**: OpenTelemetry traces (sent to Jaeger) for debugging latency issues and identifying slow endpoints.

---

## SLAs (Service Level Agreements)

For context: An **SLA** is a formal contract with customers, usually featuring penalties (refunds, credits) if targets are missed. Your internal SLOs should be **slightly stricter** than any external SLA to ensure compliance. For example:

- Internal SLO: 99.5% availability.
- External SLA: 99% availability (gives you 0.5% margin to prevent penalty clauses).

This document focuses on **internal SLOs**; any customer-facing SLA requires separate documentation and legal review.

---

## Review & Adjustment Cadence

- **Weekly**: Check actual SLI performance against targets; flag if trending toward SLO violation.
- **Monthly**: Full SLO review; calculate error budget burn; adjust priorities for next month.
- **Quarterly**: Revisit SLO targets; increase thresholds if consistently exceeding targets, lower if repeatedly missing.
- **Annually**: Review SLI definitions; add new SLIs if user needs change (e.g., data export speed, search latency).

---

## Testing the SLO Implementation

### 1. Verify the monitoring stack is running

```bash
docker compose ps prometheus alertmanager
```

Expected: both services show `Up` with ports `9090` and `9093` exposed.

Check readiness:

```bash
curl -sf http://localhost:9090/-/ready && echo PROM_READY
curl -sf http://localhost:9093/-/ready && echo AM_READY
```

### 2. Validate config and rule files

```bash
# Recording rules (39 rules expected)
docker run --rm --entrypoint=promtool \
  -v "$PWD/infra:/etc/prometheus" prom/prometheus:v2.54.1 \
  check rules /etc/prometheus/prometheus_rules.yml

# Alerting rules (19 rules expected)
docker run --rm --entrypoint=promtool \
  -v "$PWD/infra:/etc/prometheus" prom/prometheus:v2.54.1 \
  check rules /etc/prometheus/prometheus_alerts.yml

# Alertmanager config
docker run --rm --entrypoint=amtool \
  -v "$PWD/infra:/etc/alertmanager" prom/alertmanager:v0.27.0 \
  check-config /etc/alertmanager/alertmanager.yml
```

### 3. Start the app and generate traffic

```bash
make api-run        # FastAPI on http://localhost:8000
make worker-run     # Celery worker (separate terminal)
```

Generate successful requests (repeat 20–50 times or use the upload UI at `http://localhost:8000`):

```bash
for i in $(seq 1 30); do curl -s http://localhost:8000/api/v1/health; done
```

Generate error traffic to exercise the error-rate SLIs:

```bash
for i in $(seq 1 30); do
  curl -s -o /dev/null -w "%{http_code}\n" http://localhost:8000/api/v1/does-not-exist
done
```

### 4. Query SLO recording metrics in Prometheus

After generating traffic, confirm recording rules are producing values:

```bash
# Total HTTP request rate
curl -sG 'http://localhost:9090/api/v1/query' \
  --data-urlencode 'query=slo:http_requests:5m'

# HTTP error rate (should be > 0 after error traffic)
curl -sG 'http://localhost:9090/api/v1/query' \
  --data-urlencode 'query=slo:http_error_rate:5m'

# HTTP p95 latency
curl -sG 'http://localhost:9090/api/v1/query' \
  --data-urlencode 'query=slo:http_latency_p95:5m'

# Error budget remaining (requires 30 days of data to be meaningful)
curl -sG 'http://localhost:9090/api/v1/query' \
  --data-urlencode 'query=slo:http_error_budget_remaining:monthly'
```

Or browse `http://localhost:9090` and search for `slo:` in the expression field.

### 5. Verify rule and alert groups are loaded

```bash
curl -s 'http://localhost:9090/api/v1/rules' | grep -E 'slo_recording_rules|slo_alerts|HTTPErrorRateFastBurn'
```

Or open `http://localhost:9090/alerts` in your browser.

### 6. Trigger a test alert quickly

Production alert windows (1 h / 6 h) take too long to validate manually. Add a short-lived test alert group to `infra/prometheus_alerts.yml`:

```yaml
  - name: slo_smoke_test           # REMOVE after testing
    interval: 15s
    rules:
      - alert: SmokeTestHTTPErrors
        expr: |
          sum(rate(http_request_errors_total[2m])) /
          sum(rate(http_requests_total[2m])) > 0
        for: 30s
        labels:
          severity: warning
          slo: smoke_test
        annotations:
          summary: "Smoke test alert — remove after validation"
          description: "Any HTTP error in last 2 minutes triggers this. Value: {{ $value | humanizePercentage }}"
```

Reload Prometheus (no restart needed):

```bash
curl -X POST http://localhost:9090/-/reload
```

Generate a few error requests, then after ~30 seconds check `http://localhost:9090/alerts` — the alert should transition to **Pending** then **Firing**. Check `http://localhost:9093` to confirm Alertmanager received it.

Remove the smoke test group when done and reload again.

### 7. Verify Celery task metrics

After running a task via the upload UI:

```bash
curl -sG 'http://localhost:9090/api/v1/query' \
  --data-urlencode 'query=celery_tasks_started_total'

curl -sG 'http://localhost:9090/api/v1/query' \
  --data-urlencode 'query=slo:celery_success_rate:5m'
```

### 8. Verify webhook delivery metrics

After completing a task that has a `webhook_url` set:

```bash
curl -sG 'http://localhost:9090/api/v1/query' \
  --data-urlencode 'query=webhook_attempts_total'

curl -sG 'http://localhost:9090/api/v1/query' \
  --data-urlencode 'query=slo:webhook_delivery_rate:5m'
```

---

## References

- **SRE Book**: [Google SRE Book](https://sre.google/books/) (Google's foundational SRE practices)
- **Prometheus Metrics**: `/metrics` endpoint (live SLI data)
- **Prometheus UI**: `http://localhost:9090`
- **Alertmanager UI**: `http://localhost:9093`
- **Recording rules**: `infra/prometheus_rules.yml`
- **Alerting rules**: `infra/prometheus_alerts.yml`
- **Alertmanager config**: `infra/alertmanager.yml`
- **Task Status Schema**: See `TaskStatusResponse` in `src/schemas/routes/routes.py` (includes `progress_percentage`, `status` enum)
- **Celery Tasks**: `src/celery_app/tasks/processor.py` (validation, processing workflows)
- **Webhook Service**: `src/services/webhook.py` (delivery logic and retries)
