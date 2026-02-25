#!/bin/bash

# RabbitMQ Virtual Host Initialization Script (For Development Environment)
# This script creates multiple virtual hosts and sets up permissions.
# Run this after starting RabbitMQ services with docker-compose.

set -e  # Exit on any error

# Load environment variables from .env file
if [ -f ".env" ]; then
    echo "📄 Loading environment variables from .env file..."
    set -a
    source .env
    set +a
else
    echo "⚠️  Warning: .env file not found, using default values"
fi

# Configuration (with fallback defaults)
RABBITMQ_CONTAINER="${RABBITMQ_CONTAINER:-local-rabbitmq}"
ADMIN_USER="${RABBITMQ_DEFAULT_USER:-guest}"
ADMIN_PASSWORD="${RABBITMQ_DEFAULT_PASS:-guest}"
RABBITMQ_HOST="${RABBITMQ_HOST:-localhost}"
RABBITMQ_PORT="${RABBITMQ_PORT:-5672}"

# Virtual hosts to create
VHOSTS=("/storage_events" "/celery_tasks")

echo "🐰 Initializing RabbitMQ virtual hosts: ${VHOSTS[*]}"
echo "   Container: $RABBITMQ_CONTAINER"
echo "   Host: $RABBITMQ_HOST:$RABBITMQ_PORT"
echo "   Admin User: $ADMIN_USER"
echo ""

# Function to check if RabbitMQ is ready
wait_for_rabbitmq() {
    echo "⏳ Waiting for RabbitMQ to be ready..."
    local max_attempts=30
    local attempt=1

    while [ $attempt -le $max_attempts ]; do
        if $DOCKER_CMD exec $RABBITMQ_CONTAINER rabbitmq-diagnostics check_port_connectivity > /dev/null 2>&1; then
            echo "✅ RabbitMQ is ready!"
            return 0
        fi

        echo "Attempt $attempt/$max_attempts: RabbitMQ not ready yet..."
        sleep 2
        ((attempt++))
    done

    echo "❌ RabbitMQ failed to start within expected time"
    return 1
}

# Function to create a single virtual host
create_vhost() {
    local vhost="$1"
    echo "🏗️  Creating virtual host: $vhost"

    if $DOCKER_CMD exec $RABBITMQ_CONTAINER rabbitmqctl list_vhosts | grep -q "^$vhost$"; then
        echo "ℹ️  Virtual host $vhost already exists"
        return 0
    fi

    if $DOCKER_CMD exec $RABBITMQ_CONTAINER rabbitmqctl add_vhost "$vhost"; then
        echo "✅ Virtual host $vhost created successfully"
    else
        echo "❌ Failed to create virtual host $vhost"
        return 1
    fi
}

# Function to set permissions on a virtual host
set_permissions() {
    local vhost="$1"
    echo "🔐 Setting permissions for user '$ADMIN_USER' on virtual host: $vhost"

    if $DOCKER_CMD exec $RABBITMQ_CONTAINER rabbitmqctl set_permissions -p "$vhost" "$ADMIN_USER" ".*" ".*" ".*"; then
        echo "✅ Permissions set successfully for user '$ADMIN_USER' on $vhost"
    else
        echo "❌ Failed to set permissions for user '$ADMIN_USER' on $vhost"
        return 1
    fi
}

# Function to verify setup for a virtual host
verify_setup() {
    local vhost="$1"
    echo "🔍 Verifying virtual host setup for $vhost..."

    # Check if vhost exists
    if ! $DOCKER_CMD exec $RABBITMQ_CONTAINER rabbitmqctl list_vhosts | grep -q "^$vhost$"; then
        echo "❌ Virtual host $vhost was not created"
        return 1
    fi

    # Check permissions
    if ! $DOCKER_CMD exec $RABBITMQ_CONTAINER rabbitmqctl list_permissions -p "$vhost" | grep -q "^$ADMIN_USER"; then
        echo "❌ Permissions for user $ADMIN_USER were not set on $vhost"
        return 1
    fi

    echo "✅ Virtual host $vhost setup verified successfully!"
}

# Main execution
main() {
    echo "🚀 Starting RabbitMQ virtual host initialization..."
    echo ""

    # Determine Docker command
    DOCKER_CMD="/usr/local/bin/docker"
    if ! command -v docker >/dev/null 2>&1; then
        if [ -x "$DOCKER_CMD" ]; then
            echo "Using Docker at: $DOCKER_CMD"
        else
            echo "❌ Docker not found in PATH or at $DOCKER_CMD"
            exit 1
        fi
    else
        DOCKER_CMD="docker"
    fi

    # Check if container is running
    if ! $DOCKER_CMD ps --format "table {{.Names}}" | grep -q "^${RABBITMQ_CONTAINER}$"; then
        echo "❌ RabbitMQ container '$RABBITMQ_CONTAINER' is not running"
        echo "   Please start it first with: docker-compose up -d"
        exit 1
    fi

    # Wait for RabbitMQ to be ready
    if ! wait_for_rabbitmq; then
        exit 1
    fi

    # Process each virtual host
    for vhost in "${VHOSTS[@]}"; do
        echo ""
        echo "--- Processing $vhost ---"
        if ! create_vhost "$vhost"; then
            exit 1
        fi
        if ! set_permissions "$vhost"; then
            exit 1
        fi
        if ! verify_setup "$vhost"; then
            exit 1
        fi
    done

    echo ""
    echo "🎉 RabbitMQ virtual hosts initialization completed successfully!"
    echo ""
    echo "📋 Summary:"
    echo "   Virtual Hosts: ${VHOSTS[*]}"
    echo "   Container: $RABBITMQ_CONTAINER"
    echo "   User: $ADMIN_USER"
    echo "   Host: $RABBITMQ_HOST:$RABBITMQ_PORT"
    echo "   Permissions: Configure=.*, Write=.*, Read=.* on each vhost"
    echo ""
    echo "🌐 You can access the management UI at: http://localhost:15672"
    echo "   Login with: $ADMIN_USER / [password hidden]"
}

# Run main function
main "$@"
