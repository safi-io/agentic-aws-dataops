#!/usr/bin/env bash
set -euo pipefail

BUCKET_NAME="${S3_BUCKET_NAME:-agentic-dataops-local}"
REGION="${AWS_DEFAULT_REGION:-us-east-1}"

awslocal s3api create-bucket --bucket "${BUCKET_NAME}" --region "${REGION}" >/dev/null 2>&1 || true

awslocal s3 cp /opt/localstack/sample_data/clean_orders.csv "s3://${BUCKET_NAME}/bronze/orders.csv" >/dev/null
awslocal s3 cp /opt/localstack/sample_data/clean_orders.csv "s3://${BUCKET_NAME}/bronze/clean_orders.csv" >/dev/null
awslocal s3 cp /opt/localstack/sample_data/unexpected_column.csv "s3://${BUCKET_NAME}/bronze/unexpected_column.csv" >/dev/null
awslocal s3 cp /opt/localstack/sample_data/high_nulls.csv "s3://${BUCKET_NAME}/bronze/high_nulls.csv" >/dev/null

awslocal s3api put-object --bucket "${BUCKET_NAME}" --key silver/.keep --body /dev/null >/dev/null
awslocal s3api put-object --bucket "${BUCKET_NAME}" --key quarantine/.keep --body /dev/null >/dev/null

