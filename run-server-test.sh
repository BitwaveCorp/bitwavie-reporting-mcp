#!/bin/bash

# Script to start the server and run tests
echo "🚀 Starting ReportingMCPServer and running tests..."

# Install required packages for the test script if not already installed
echo "📦 Installing test dependencies..."
npm install --no-save node-fetch chalk

# Make the test script executable
chmod +x ./test-server.js

# Start the server in the background
echo "🌐 Starting server..."
node ./dist/index.js &
SERVER_PID=$!

# Wait for server to start
echo "⏳ Waiting for server to start (5 seconds)..."
sleep 5

# Run the tests
echo "🧪 Running tests..."
./test-server.js

# Capture the test result
TEST_RESULT=$?

# Kill the server
echo "🛑 Stopping server..."
kill $SERVER_PID

# Exit with the test result
exit $TEST_RESULT
