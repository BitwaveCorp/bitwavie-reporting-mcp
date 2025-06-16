#!/bin/bash

# Setup environment variables for testing
export PORT=3000
export GOOGLE_CLOUD_PROJECT_ID="bitwavie-dev"
export BIGQUERY_DATASET_ID="crypto_transactions"
export BIGQUERY_TABLE_ID="transactions"
export ANTHROPIC_API_KEY="your-api-key-here"
export USE_ENHANCED_NLQ="true"
export INCLUDE_SQL_IN_RESPONSES="true"
export SCHEMA_REFRESH_INTERVAL_MS="3600000"

# Print confirmation
echo "Environment variables set for testing:"
echo "PORT: $PORT"
echo "GOOGLE_CLOUD_PROJECT_ID: $GOOGLE_CLOUD_PROJECT_ID"
echo "BIGQUERY_DATASET_ID: $BIGQUERY_DATASET_ID"
echo "BIGQUERY_TABLE_ID: $BIGQUERY_TABLE_ID"
echo "USE_ENHANCED_NLQ: $USE_ENHANCED_NLQ"
echo "INCLUDE_SQL_IN_RESPONSES: $INCLUDE_SQL_IN_RESPONSES"
echo "SCHEMA_REFRESH_INTERVAL_MS: $SCHEMA_REFRESH_INTERVAL_MS"
echo "ANTHROPIC_API_KEY: [HIDDEN]"

echo ""
echo "To use these variables, run:"
echo "source ./setup-test-env.sh"
