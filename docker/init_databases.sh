#!/bin/bash
set -e

# This script creates multiple databases in the PostgreSQL container
# It runs automatically when the container is initialized

# Get API database name from environment variable or use default
API_DB_NAME="${API_DB_NAME:-users_db}"

echo "🔧 Initializing databases..."
echo "  - Main DB: ${POSTGRES_DB}"
echo "  - API DB: ${API_DB_NAME}"

# Check if API database already exists
if psql -U "$POSTGRES_USER" -lqt | cut -d \| -f 1 | grep -qw "$API_DB_NAME"; then
    echo "✓ Database '$API_DB_NAME' already exists"
else
    echo "⚙️  Creating database '$API_DB_NAME'..."
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
		CREATE DATABASE "$API_DB_NAME";
		GRANT ALL PRIVILEGES ON DATABASE "$API_DB_NAME" TO $POSTGRES_USER;
	EOSQL
    echo "✅ Database '$API_DB_NAME' created successfully"
fi

echo "✅ All databases ready: ${POSTGRES_DB}, ${API_DB_NAME}"
