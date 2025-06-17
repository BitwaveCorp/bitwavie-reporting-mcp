#!/bin/sh

# Print startup information
echo "[SIMPLE-HTTP] Starting simple HTTP server..."
echo "[SIMPLE-HTTP] Node version: $(node -v)"
echo "[SIMPLE-HTTP] NPM version: $(npm -v)"
echo "[SIMPLE-HTTP] Current directory: $(pwd)"
echo "[SIMPLE-HTTP] Directory contents: $(ls -la)"

# Load environment variables from .env file if it exists
if [ -f ".env" ]; then
  echo "[SIMPLE-HTTP] Loading environment variables from .env file"
  set -a
  . ./.env
  set +a
else
  echo "[SIMPLE-HTTP] No .env file found, using environment variables from container"
fi

# Print environment variables (excluding sensitive ones)
echo "[SIMPLE-HTTP] PORT: ${PORT:-8080}"
echo "[SIMPLE-HTTP] NODE_ENV: ${NODE_ENV:-development}"
echo "[SIMPLE-HTTP] GOOGLE_CLOUD_PROJECT_ID: ${GOOGLE_CLOUD_PROJECT_ID:-Not set}"
echo "[SIMPLE-HTTP] BIGQUERY_DATASET_ID: ${BIGQUERY_DATASET_ID:-Not set}"
echo "[SIMPLE-HTTP] BIGQUERY_TABLE_ID: ${BIGQUERY_TABLE_ID:-Not set}"
echo "[SIMPLE-HTTP] USE_ENHANCED_NLQ: ${USE_ENHANCED_NLQ:-false}"
echo "[SIMPLE-HTTP] INCLUDE_SQL_IN_RESPONSES: ${INCLUDE_SQL_IN_RESPONSES:-true}"
echo "[SIMPLE-HTTP] SCHEMA_REFRESH_INTERVAL_MS: ${SCHEMA_REFRESH_INTERVAL_MS:-3600000}"

# Make sure the HTTP server file exists
if [ ! -f "simple-http-server.js" ]; then
  echo "ERROR: simple-http-server.js not found!"
  ls -la
  exit 1
fi

# Start the HTTP server with explicit node path
exec node simple-http-server.js
