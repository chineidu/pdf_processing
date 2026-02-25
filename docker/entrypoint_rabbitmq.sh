#!/bin/bash
# Wrapper entrypoint for RabbitMQ that initializes vhosts/permissions on startup
# This runs BEFORE the normal RabbitMQ startup

set -e

# Start RabbitMQ in the background
echo "🚀 Starting RabbitMQ..."
rabbitmq-server &
RABBITMQ_PID=$!

# Wait for RabbitMQ to be ready
echo "⏳ Waiting for RabbitMQ to be ready..."
max_attempts=30
attempt=1
while [ $attempt -le $max_attempts ]; do
    if rabbitmq-diagnostics check_port_connectivity >/dev/null 2>&1; then
        echo "✅ RabbitMQ is ready"
        break
    fi
    echo "  Attempt $attempt/$max_attempts..."
    sleep 1
    attempt=$((attempt + 1))
done

if [ $attempt -gt $max_attempts ]; then
    echo "❌ RabbitMQ failed to start"
    kill $RABBITMQ_PID 2>/dev/null || true
    exit 1
fi

# Run initialization script if it exists
if [ -f "/docker-entrypoint-initdb.d/init_rabbitmq.sh" ]; then
    echo "🔧 Running RabbitMQ initialization..."
    bash /docker-entrypoint-initdb.d/init_rabbitmq.sh
    if [ $? -ne 0 ]; then
        echo "❌ Initialization failed"
        kill $RABBITMQ_PID 2>/dev/null || true
        exit 1
    fi
fi

echo "✅ RabbitMQ ready with vhosts initialized"

# Keep RabbitMQ running in foreground
wait $RABBITMQ_PID
