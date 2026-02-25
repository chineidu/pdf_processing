.PHONY: help
# ===============================
# ============ HELP =============
# ===============================

help:
	@echo "Tool Chat - Available commands:"
	@echo ""
	@echo "📦 Development:"
	@echo "  make install                    - Install all dependencies"
	@echo "  make test                       - Run tests"
	@echo "  make test-verbose               - Run tests with verbose output"
	@echo "  make lint                       - Run linter (ruff)"
	@echo "  make format                     - Format code (ruff)"
	@echo "  make clean-cache                - Clean up cache and temporary files"
	@echo ""
	@echo "🚀 Running the Application:"
	@echo "  make api-run                    - Run FastAPI server on http://localhost:8000"
	@echo "                                    Usage: make api-run WORKERS=1 for single-worker dev"
	@echo "  make api-run-gunicorn           - Run FastAPI server with Gunicorn on http://localhost:8000"
	@echo "                                    Usage: make api-run-gunicorn WORKERS=1 for single-worker dev"
	@echo "  make worker-run                  - Run Celery worker"
	@echo ""
	@echo "🐳 Docker Services:"
	@echo "  make up                         - Start all services"
	@echo "  make down                       - Stop all services"
	@echo "  make restart                    - Restart services"
	@echo "  make logs                       - View logs"
	@echo "  make status                     - Check status"
	@echo "  make health-check               - Verify all services are healthy (RabbitMQ perms, etc)"
	@echo "  make setup                      - Setup from scratch"
	@echo "  make clean-all                  - Clean everything (including volumes)"
	@echo ""
	@echo "🛠 Utilities:"
	@echo "  make check-port                 - Check if a port is in use (default PORT=8000)"
	@echo "                                    Usage: make check-port or make check-port PORT=5000"
	@echo "  make kill-port                 - Kill process using a port (default PORT=8000)"
	@echo "                                    Usage: make kill-port or make kill-port PORT=5000"

# Number of Gunicorn workers. Use WORKERS=1 for local development to keep
# in-memory sessions consistent across requests.
WORKERS ?= 2

.PHONY: install
# ===============================
# =========== INSTALL ===========
# ===============================
install:
	@echo "📦 Installing dependencies..."
	uv sync

.PHONY: api-run api-run-gunicorn worker-run storage-worker-run
# ===============================
# ========== START APP ==========
# ===============================
api-run:
	@echo "🚀 Starting FastAPI server on http://localhost:$(PORT)"
	@PORT=$(PORT) $(MAKE) check-port || { \
		echo; \
		echo -n "❓ Port $(PORT) is in use. Kill and continue? (y/N) "; \
		read -r confirm; \
		if [ "$$confirm" = "y" ] || [ "$$confirm" = "Y" ]; then \
			PORT=$(PORT) $(MAKE) kill-port; \
			PORT=$(PORT) $(MAKE) check-port || { echo "🛑 Port $(PORT) still busy after kill attempt."; exit 1; }; \
		else \
			echo; \
			echo "🛑 Aborted. Free port $(PORT) manually or use a different port."; \
			exit 1; \
		fi; \
	}
	@echo "✅ Port $(PORT) is free. Launching FastAPI..."
	@uv run -m src.api.app --workers $(WORKERS)

api-run-gunicorn:
	@echo "🚀 Starting FastAPI server on http://localhost:$(PORT)"
	@PORT=$(PORT) $(MAKE) check-port || { \
		echo; \
		echo -n "❓ Port $(PORT) is in use. Kill and continue? (y/N) "; \
		read -r confirm; \
		if [ "$$confirm" = "y" ] || [ "$$confirm" = "Y" ]; then \
			PORT=$(PORT) $(MAKE) kill-port; \
			PORT=$(PORT) $(MAKE) check-port || { echo "🛑 Port $(PORT) still busy after kill attempt."; exit 1; }; \
		else \
			echo; \
			echo "🛑 Aborted. Free port $(PORT) manually or use a different port."; \
			exit 1; \
		fi; \
	}
	@echo "✅ Port $(PORT) is free. Launching FastAPI..."
	@uv run -m gunicorn --pythonpath . \
		-k uvicorn.workers.UvicornWorker \
		src.api.app:app \
		-w $(WORKERS) --bind "0.0.0.0:$(PORT)"

worker-run:
	@echo "🚀 Starting Celery worker..."
	@uv run worker.py

storage-worker-run:
	@echo "🚀 Starting storage worker..."
	@uv run -m src.services.ingestion.ingestion_worker

.PHONY: check-port kill-port health-check
# ===============================
# ===== PORT UTILITIES ==========
# ===============================

# Default port is 8000; can be overridden like `make check-port PORT=5000`
PORT ?= 8000

# Check if a port is in use (exit 0 if free, 1 if in use)
# Usage in scripts: `make check-port || echo "port busy"`
check-port:
	@echo "🔍 Checking if port $(PORT) is in use..."
	@if lsof -i :$(PORT) >/dev/null 2>&1; then \
		echo "⚠️  Port $(PORT) is in use"; \
		exit 1; \
	else \
		echo "✅ Port $(PORT) is free"; \
		exit 0; \
	fi

# Kill the process using a port
# Usage:
#   make kill-port           # kills process on default port 8000
#   make kill-port PORT=5000 # kills process on port 5000
kill-port:
	@echo "💀 Killing process using port $(PORT)..."
	@if lsof -i :$(PORT) >/dev/null 2>&1; then \
		PID=$$(lsof -ti :$(PORT)); \
		echo "Killing process $$PID on port $(PORT)"; \
		kill -9 $$PID; \
		sleep 1; \
		if lsof -i :$(PORT) >/dev/null 2>&1; then \
			echo "⚠️  Port $(PORT) still in use after kill attempt."; \
			exit 1; \
		else \
			echo "✅ Port $(PORT) freed."; \
			exit 0; \
		fi; \
	else \
		echo "ℹ️  No process using port $(PORT)."; \
		exit 0; \
	fi

# Health check for services
health-check:
	@echo "🏥 Checking service health..."
	@echo "  RabbitMQ..."
	@if [ -z "$$RABBITMQ_USER" ] || [ -z "$$RABBITMQ_PASSWORD" ]; then \
		echo "    ⚠️  Set RABBITMQ_USER and RABBITMQ_PASSWORD to check RabbitMQ API"; \
	else \
		python3 -c "import os,base64,urllib.request,sys; u=os.environ.get('RABBITMQ_USER'); p=os.environ.get('RABBITMQ_PASSWORD');\nreq=urllib.request.Request('http://localhost:15672/api/overview');\nreq.add_header('Authorization','Basic '+base64.b64encode(f'{u}:{p}'.encode()).decode());\n\ntry:\n    urllib.request.urlopen(req, timeout=5);\n    sys.exit(0)\nexcept Exception:\n    sys.exit(1)" \
			&& echo "    ✅ RabbitMQ API" || echo "    ❌ RabbitMQ API"; \
	fi
	@echo "  PostgreSQL..."
	@docker exec pdf_processing-database-1 pg_isready -U taskflow >/dev/null 2>&1 && echo "    ✅ PostgreSQL" || echo "    ❌ PostgreSQL"
	@echo "  MinIO..."
	@curl -s http://localhost:9000/minio/health/live >/dev/null 2>&1 && echo "    ✅ MinIO" || echo "    ❌ MinIO"
	@echo "  Redis..."
	@docker exec pdf_processing-redis-1 redis-cli ping >/dev/null 2>&1 && echo "    ✅ Redis" || echo "    ❌ Redis"
	@echo ""
	@echo "📊 RabbitMQ Vhosts and Permissions..."
	@if [ -z "$$RABBITMQ_USER" ] || [ -z "$$RABBITMQ_PASSWORD" ]; then \
		echo "  ⚠️  Set RABBITMQ_USER and RABBITMQ_PASSWORD to list vhosts"; \
	else \
		python3 -c "import os,base64,urllib.request,json; u=os.environ.get('RABBITMQ_USER'); p=os.environ.get('RABBITMQ_PASSWORD');\nreq=urllib.request.Request('http://localhost:15672/api/vhosts');\nreq.add_header('Authorization','Basic '+base64.b64encode(f'{u}:{p}'.encode()).decode());\nresp=urllib.request.urlopen(req, timeout=5);\nvhosts=[v['name'] for v in json.load(resp)];\nprint('  Vhosts:', ', '.join(vhosts))"; \
	fi
	@echo ""
	@echo "✅ Health check complete"

.PHONY: test test-verbose
# ===============================
# ============ TESTS ============
# ===============================
test:
	@echo "🧪 Running tests..."
	uv run -m pytest

test-verbose:
	@echo "🧪 Running tests..."
	uv run -m pytest -v


.PHONY: type-check lint format lint-format-all clean-cache
# ===============================
# == LINTING AND TYPE-CHECKING ==
# ===============================
type-check:
	@echo "🔍 Running type checks..."
	uv run ty check src

lint:
	@echo "🔍 Running linter..."
	uv run ruff check .

format:
	@echo "✨ Formatting code..."
	uv run ruff check --fix .
	uv run ruff format .

lint-format-all: test type-check lint format
	@echo "✅ All checks passed!"

clean-cache:
	@echo "🧹 Cleaning up..."
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".ruff_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	@echo "✅ Cleanup complete"


.PHONY: up down build restart logs setup status clean-all
# ===============================
# =========== DOCKER ============
# ===============================
# Start all services
up:
	@echo "🚀 Starting all services..."
	@chmod +x docker/init_databases.sh docker/init_rabbitmq.sh
	docker-compose up -d
	@echo "⏳ Waiting for services to be healthy..."
	@sleep 15
	@echo "✅ All services started! RabbitMQ vhosts auto-initialized."

# Stop all services
down:
	@echo "🛑 Stopping all services..."
	docker-compose down

build:
	@echo "🔨 Building Docker images..."
	docker-compose build

# Restart services
restart: down up

# View logs
logs:
	docker-compose logs -f

# Setup from scratch
setup: clean-all build up
	@echo "✅ Setup complete! Services are running."

# Check status
status:
	docker-compose ps

# Clean everything (including volumes)
clean-all:
	docker-compose down -v --remove-orphans
