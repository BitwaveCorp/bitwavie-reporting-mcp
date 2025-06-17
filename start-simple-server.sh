#!/bin/sh

# Print startup information
echo "[SIMPLE-HTTP] Starting simple HTTP server..."
echo "[SIMPLE-HTTP] Node version: $(node -v)"
echo "[SIMPLE-HTTP] NPM version: $(npm -v)"
echo "[SIMPLE-HTTP] Current directory: $(pwd)"
echo "[SIMPLE-HTTP] Directory contents: $(ls -la)"
echo "[SIMPLE-HTTP] PORT: ${PORT:-8080}"

# Make sure the HTTP server file exists
if [ ! -f "simple-http-server.js" ]; then
  echo "ERROR: simple-http-server.js not found!"
  ls -la
  exit 1
fi

# Start the HTTP server with explicit node path
exec node simple-http-server.js
