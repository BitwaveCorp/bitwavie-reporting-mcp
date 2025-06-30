# MCP HTTP Server

This is a JSON-RPC over HTTP server implementation for the Bitwave MCP (Reporting MCP Server). It provides an HTTP interface to the MCP server's functionality, allowing for easier testing and integration with HTTP clients.

## Architecture Overview

The simple-http-server.js is a lightweight wrapper around the main Reporting MCP Server. It imports the compiled `ReportingMCPServer` class from './dist/server.js' and delegates most of its functionality to this class. This means:

1. It doesn't reimplement core logic like query execution, error handling, or data processing
2. When the main server code is updated and compiled, the HTTP server automatically benefits from those changes
3. It provides a simplified JSON-RPC interface to the more complex functionality in the main server
4. It maintains its own session storage for connection details, which is used by the main server through a global function

## Features

- Full JSON-RPC 2.0 support
- Implements a subset of the main server's functionality through these methods:
  - `test_connection`: Tests the connection to the MCP server and underlying data sources
  - `tools/list` or `list_tools`: Lists available tools and capabilities
  - `tools/call`: Generic method to call specific tools (currently supports `analyze_actions_data`)
  - `analyze_actions_data`: Direct method to analyze crypto transaction data using the LLM query translator
  - `connection/validate-table-access`: Validates access to BigQuery tables
  - `connection/status`: Returns the current connection status
  - `connectdatasource/update-session`: Updates the connection session with new details
  - `connection/session-details`: Returns the current session connection details
- Session management for connection details with environment variable fallbacks
- Enhanced error handling with LLM-powered explanations for SQL errors
- Comprehensive logging for debugging and monitoring
- Graceful error handling and shutdown

## Enhanced Error Handling

The HTTP server benefits from the main server's enhanced LLM-powered SQL error handling mechanism. When a SQL query fails, the system:

1. Captures the original error from BigQuery
2. Sends the error, original SQL, user query, and context to the LLM (Claude)
3. Receives back:
   - A user-friendly explanation of what went wrong
   - Alternative query suggestions the user could try
   - A corrected SQL query when possible
4. Attempts to execute the corrected SQL
5. Returns enhanced error information to the client, including:
   - The original error message
   - A natural language explanation of the error
   - Alternative query suggestions

This provides a much better user experience when errors occur, helping users understand what went wrong and how to fix it without requiring SQL expertise.

## Usage

### Starting the Server

Use the provided script to start the server:

```bash
./start-http-server.sh
```

This script will:
1. Load environment variables from a `.env` file if it exists
2. Set default values for required environment variables
3. Start the HTTP server on the specified port (default: 8080)

### Environment Variables

The following environment variables can be configured:

- `PORT`: The port to run the HTTP server on (default: 8080)
- `GOOGLE_CLOUD_PROJECT_ID`: Google Cloud project ID for BigQuery
- `BIGQUERY_DATASET_ID`: BigQuery dataset ID
- `BIGQUERY_TABLE_ID`: BigQuery table ID
- `ANTHROPIC_API_KEY`: API key for Anthropic (if used)
- `USE_ENHANCED_NLQ`: Whether to use enhanced natural language queries (true/false)
- `INCLUDE_SQL_IN_RESPONSES`: Whether to include SQL in responses (true/false)
- `SCHEMA_REFRESH_INTERVAL_MS`: Schema refresh interval in milliseconds

### Testing the Server

You can test the server using the provided test script:

```bash
node test-rpc.js
```

This script will:
1. Test the connection to the MCP server
2. Get the list of available tools
3. Test the `analyze_actions_data` method directly
4. Test the `tools/call` method with `analyze_actions_data`

## JSON-RPC API

### Test Connection

```json
{
  "jsonrpc": "2.0",
  "method": "test_connection",
  "id": "test-123"
}
```

### List Tools

```json
{
  "jsonrpc": "2.0",
  "method": "tools/list",
  "id": "test-123"
}
```

### Analyze Actions Data (Direct)

```json
{
  "jsonrpc": "2.0",
  "method": "analyze_actions_data",
  "params": [
    {
      "query": "Show me recent transactions",
      "confirmedMappings": {},
      "previousResponse": {}
    }
  ],
  "id": "test-123"
}
```

### Call Tool (Fallback)

```json
{
  "jsonrpc": "2.0",
  "method": "tools/call",
  "params": {
    "name": "analyze_actions_data",
    "arguments": {
      "query": "Show me recent transactions",
      "confirmedMappings": {},
      "previousResponse": {}
    }
  },
  "id": "test-123"
}
```

## Integration with Frontend

The frontend can use this HTTP server by making standard HTTP requests to the `/rpc` endpoint with JSON-RPC formatted payloads. This allows for easier testing and development without requiring WebSocket connections.

## Logging

The server provides extensive logging to help with debugging:
- Server initialization and configuration
- Request processing for each method
- Detailed information about query execution
- Timing information for performance monitoring
- Error handling and stack traces

All logs are prefixed with `[HTTP-RPC]` for easy filtering.
