#!/bin/bash

# Load environment variables from .env file if it exists
if [ -f ".env" ]; then
  echo "Loading environment variables from .env file"
  export $(grep -v '^#' .env | xargs)
fi

# Set environment variables for the MCP HTTP server with defaults
export PORT=${PORT:-8080}
export GOOGLE_CLOUD_PROJECT_ID=${GOOGLE_CLOUD_PROJECT_ID:-"bitwave-customers"}
export BIGQUERY_DATASET_ID=${BIGQUERY_DATASET_ID:-"reporting"}
export BIGQUERY_TABLE_ID=${BIGQUERY_TABLE_ID:-"actions"}
export USE_ENHANCED_NLQ=${USE_ENHANCED_NLQ:-"true"}
export INCLUDE_SQL_IN_RESPONSES=${INCLUDE_SQL_IN_RESPONSES:-"true"}
export SCHEMA_REFRESH_INTERVAL_MS=${SCHEMA_REFRESH_INTERVAL_MS:-"3600000"}

# Start the HTTP server
echo "Starting MCP HTTP Server on port $PORT..."
echo "Project ID: $GOOGLE_CLOUD_PROJECT_ID"
echo "Dataset ID: $BIGQUERY_DATASET_ID"
echo "Table ID: $BIGQUERY_TABLE_ID"

node http-server.js
