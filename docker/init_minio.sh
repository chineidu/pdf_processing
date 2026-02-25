#!/bin/sh

# Exit immediately if a command exits with a non-zero status
set -e

echo "Starting MinIO initialization..."

# 1. Alias the local MinIO server
# (Ensure MINIO_ROOT_USER and MINIO_ROOT_PASSWORD match your main MinIO container env vars)
mc alias set myminio http://minio:9000 "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}"

# 2. Create the bucket if it doesn't already exist
echo "Creating bucket..."
mc mb myminio/${AWS_S3_BUCKET} --ignore-existing

# 3. Bind the event to the AMQP target '1' (which we will configure via Env Vars)
echo "Binding AMQP notification event to bucket..."
mc event add myminio/${AWS_S3_BUCKET} arn:minio:sqs::1:amqp --event put

echo "MinIO initialization completed successfully!"
