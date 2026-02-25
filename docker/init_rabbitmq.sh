#!/bin/sh
# Initialize RabbitMQ vhosts, users, and permissions on first startup
# This script runs inside the rabbitmq-init container via docker-compose

set -eu

echo "🔧 RabbitMQ Initialization Script"

RMQ_HOST="local-rabbitmq"
RMQ_PORT=15672
RMQ_USER="guest"
RMQ_PASS="guest"

# RabbitMQ Management API expects URL-encoded vhost values.
# These map to:
# - ROOT_VHOST_ENCODED='/':                %2F
# - STORAGE_VHOST_PLAIN='storage_events':   storage_events
# - STORAGE_VHOST_ENCODED='/storage_events': %2Fstorage_events
# - CELERY_VHOST_ENCODED='/celery_tasks':    %2Fcelery_tasks
ROOT_VHOST_ENCODED="%2F"
STORAGE_VHOST_PLAIN="storage_events"
STORAGE_VHOST_ENCODED="%2Fstorage_events"
CELERY_VHOST_ENCODED="%2Fcelery_tasks"
TARGET_VHOSTS="$ROOT_VHOST_ENCODED $STORAGE_VHOST_PLAIN $STORAGE_VHOST_ENCODED $CELERY_VHOST_ENCODED"

# Wait for RabbitMQ to be ready (use service name for Docker networking)
echo "⏳ Waiting for RabbitMQ to be ready..."
max_attempts=30
attempt=1
while [ $attempt -le $max_attempts ]; do
    if curl -s -f -u "$RMQ_USER:$RMQ_PASS" "http://$RMQ_HOST:$RMQ_PORT/api/overview" >/dev/null 2>&1; then
        echo "✅ RabbitMQ API is ready"
        break
    fi
    echo "  Attempt $attempt/$max_attempts..."
    sleep 1
    attempt=$((attempt + 1))
done

if [ "$attempt" -gt "$max_attempts" ]; then
    echo "❌ RabbitMQ failed to respond"
    exit 1
fi

# Create required vhosts.
echo "📝 Creating vhosts..."
for vhost in $TARGET_VHOSTS; do
    curl -s -f -u "$RMQ_USER:$RMQ_PASS" -X PUT "http://$RMQ_HOST:$RMQ_PORT/api/vhosts/$vhost" >/dev/null
done

# Set user permissions on all target vhosts.
echo "🔐 Setting permissions..."
for vhost in $TARGET_VHOSTS; do
    curl -s -f -u "$RMQ_USER:$RMQ_PASS" -X PUT "http://$RMQ_HOST:$RMQ_PORT/api/permissions/$vhost/$RMQ_USER" \
        -H "Content-Type: application/json" \
        -d '{"configure":".*","write":".*","read":".*"}' >/dev/null
done

# Validate setup by checking for the decoded vhost name in API output.
echo "✅ Validating setup..."
if ! curl -s -u "$RMQ_USER:$RMQ_PASS" "http://$RMQ_HOST:$RMQ_PORT/api/vhosts" | grep -q '/storage_events'; then
    echo "❌ Failed to create /storage_events vhost"
    exit 1
fi

echo "✅ RabbitMQ initialization complete and validated"
