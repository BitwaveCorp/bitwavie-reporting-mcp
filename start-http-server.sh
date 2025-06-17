#!/bin/sh

# Load environment variables from .env file if it exists
if [ -f ".env" ]; then
  echo "Loading environment variables from .env file"
  export $(grep -v '^#' .env | xargs 2>/dev/null || true)
fi

# Set environment variables for the MCP HTTP server with defaults
export PORT=${PORT:-8080}
export GOOGLE_CLOUD_PROJECT_ID=${GOOGLE_CLOUD_PROJECT_ID:-"bitwave-customers"}
export BIGQUERY_DATASET_ID=${BIGQUERY_DATASET_ID:-"reporting"}
export BIGQUERY_TABLE_ID=${BIGQUERY_TABLE_ID:-"actions"}
export USE_ENHANCED_NLQ=${USE_ENHANCED_NLQ:-"true"}
export INCLUDE_SQL_IN_RESPONSES=${INCLUDE_SQL_IN_RESPONSES:-"true"}
export SCHEMA_REFRESH_INTERVAL_MS=${SCHEMA_REFRESH_INTERVAL_MS:-"3600000"}

# Print startup information
echo "Starting MCP HTTP Server on port $PORT..."
echo "Project ID: $GOOGLE_CLOUD_PROJECT_ID"
echo "Dataset ID: $BIGQUERY_DATASET_ID"
echo "Table ID: $BIGQUERY_TABLE_ID"
echo "Node version: $(node -v)"
echo "NPM version: $(npm -v)"
echo "Current directory: $(pwd)"
echo "Directory contents: $(ls -la)"

# Make sure the HTTP server file exists
if [ ! -f "http-server.js" ]; then
  echo "ERROR: http-server.js not found!"
  ls -la
  exit 1
fi

# Start the HTTP server with explicit node path
exec node http-server.js
