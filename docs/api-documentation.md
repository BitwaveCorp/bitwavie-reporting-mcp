# Reporting MCP Server: API Documentation

This document provides detailed information about the API endpoints (MCP tools) available in the Reporting MCP Server.

## MCP Protocol

The Reporting MCP Server implements the Model Context Protocol (MCP), which uses JSON-RPC 2.0 for communication. Clients can interact with the server by sending JSON-RPC requests to the server's standard input and receiving responses from the server's standard output.

### JSON-RPC Format

**Request:**
```json
{
  "jsonrpc": "2.0",
  "id": "<request_id>",
  "method": "<method_name>",
  "params": {
    // Method-specific parameters
  }
}
```

**Response:**
```json
{
  "jsonrpc": "2.0",
  "id": "<request_id>",
  "result": {
    // Method-specific result
  }
}
```

**Error:**
```json
{
  "jsonrpc": "2.0",
  "id": "<request_id>",
  "error": {
    "code": <error_code>,
    "message": "<error_message>",
    "data": {
      // Additional error information
    }
  }
}
```

## Available Methods

### `list_tools`

Lists all available tools provided by the server.

**Request:**
```json
{
  "jsonrpc": "2.0",
  "id": "1",
  "method": "list_tools",
  "params": {}
}
```

**Response:**
```json
{
  "jsonrpc": "2.0",
  "id": "1",
  "result": {
    "tools": [
      {
        "name": "analyze_actions_data",
        "description": "Run analytical queries on actions data using natural language",
        "parameters": {
          "type": "object",
          "properties": {
            "query": {
              "type": "string",
              "description": "Natural language query to analyze the data"
            }
          },
          "required": ["query"]
        }
      },
      {
        "name": "generate_lots_report",
        "description": "Generate a report of lots with their acquisition dates and balances",
        "parameters": {
          "type": "object",
          "properties": {
            "parameters": {
              "type": "object",
              "properties": {
                "runId": {
                  "type": "string",
                  "description": "Run ID to filter data, use 'latest' for the most recent run"
                },
                "asOfDate": {
                  "type": "string",
                  "description": "Date to generate the report for (YYYY-MM-DD)"
                }
              },
              "required": ["runId", "asOfDate"]
            },
            "groupBy": {
              "type": "array",
              "description": "Fields to group the report by",
              "items": {
                "type": "string"
              }
            }
          },
          "required": ["parameters"]
        }
      },
      {
        "name": "generate_valuation_rollforward",
        "description": "Generate a valuation rollforward report",
        "parameters": {
          "type": "object",
          "properties": {
            "parameters": {
              "type": "object",
              "properties": {
                "runId": {
                  "type": "string",
                  "description": "Run ID to filter data, use 'latest' for the most recent run"
                },
                "startDate": {
                  "type": "string",
                  "description": "Start date for the report period (YYYY-MM-DD)"
                },
                "endDate": {
                  "type": "string",
                  "description": "End date for the report period (YYYY-MM-DD)"
                }
              },
              "required": ["runId", "startDate", "endDate"]
            },
            "groupBy": {
              "type": "array",
              "description": "Fields to group the report by",
              "items": {
                "type": "string"
              }
            }
          },
          "required": ["parameters"]
        }
      },
      {
        "name": "generate_inventory_balance",
        "description": "Generate an inventory balance report",
        "parameters": {
          "type": "object",
          "properties": {
            "parameters": {
              "type": "object",
              "properties": {
                "runId": {
                  "type": "string",
                  "description": "Run ID to filter data, use 'latest' for the most recent run"
                },
                "asOfDate": {
                  "type": "string",
                  "description": "Date to generate the report for (YYYY-MM-DD)"
                }
              },
              "required": ["runId", "asOfDate"]
            },
            "groupBy": {
              "type": "array",
              "description": "Fields to group the report by",
              "items": {
                "type": "string"
              }
            }
          },
          "required": ["parameters"]
        }
      },
      {
        "name": "configure_data_source",
        "description": "Configure the data source for the server",
        "parameters": {
          "type": "object",
          "properties": {
            "type": {
              "type": "string",
              "description": "Type of data source (e.g., 'bigquery', 'csv')",
              "enum": ["bigquery", "csv"]
            },
            "config": {
              "type": "object",
              "description": "Configuration for the data source",
              "properties": {
                "projectId": {
                  "type": "string",
                  "description": "Google Cloud project ID (for BigQuery)"
                },
                "datasetId": {
                  "type": "string",
                  "description": "BigQuery dataset ID"
                },
                "tableId": {
                  "type": "string",
                  "description": "BigQuery table ID"
                },
                "keyFilename": {
                  "type": "string",
                  "description": "Path to the service account key file (for BigQuery)"
                }
              }
            }
          },
          "required": ["type", "config"]
        }
      },
      {
        "name": "validate_column_mapping",
        "description": "Validate column mapping for the data source",
        "parameters": {
          "type": "object",
          "properties": {
            "mapping": {
              "type": "object",
              "description": "Mapping of column names to their meanings"
            }
          },
          "required": ["mapping"]
        }
      }
    ]
  }
}
```

### `call_tool`

Calls a specific tool with the provided arguments.

**Request:**
```json
{
  "jsonrpc": "2.0",
  "id": "2",
  "method": "call_tool",
  "params": {
    "name": "<tool_name>",
    "arguments": {
      // Tool-specific arguments
    }
  }
}
```

**Response:**
```json
{
  "jsonrpc": "2.0",
  "id": "2",
  "result": {
    // Tool-specific result
  }
}
```

## Tool Details

### 1. `analyze_actions_data`

Runs analytical queries on actions data using natural language.

**Arguments:**
```json
{
  "query": "What is the total BTC balance?"
}
```

**Result:**
```json
{
  "query": "What is the total BTC balance?",
  "sql": "SELECT asset, SUM(CAST(assetBalance AS FLOAT64)) as total_balance FROM `project.dataset.table` WHERE asset = 'BTC' GROUP BY asset",
  "results": [
    {
      "asset": "BTC",
      "total_balance": 123.45
    }
  ],
  "metadata": {
    "execution_time_ms": 245,
    "row_count": 1
  }
}
```

### 2. `generate_lots_report`

Generates a report of lots with their acquisition dates and balances.

**Arguments:**
```json
{
  "parameters": {
    "runId": "latest",
    "asOfDate": "2025-06-10"
  },
  "groupBy": ["asset", "lotId"]
}
```

**Result:**
```json
{
  "report": {
    "name": "Lots Report",
    "parameters": {
      "runId": "run_20250610_1",
      "asOfDate": "2025-06-10"
    },
    "data": [
      {
        "asset": "BTC",
        "lotId": "lot_12345",
        "acquisitionDate": "2024-01-15T00:00:00Z",
        "balance": 1.5,
        "carryingValue": 45000.00,
        "costBasis": 30000.00
      },
      // Additional records...
    ]
  },
  "metadata": {
    "execution_time_ms": 350,
    "row_count": 25
  }
}
```

### 3. `generate_valuation_rollforward`

Generates a valuation rollforward report.

**Arguments:**
```json
{
  "parameters": {
    "runId": "latest",
    "startDate": "2025-01-01",
    "endDate": "2025-06-10"
  },
  "groupBy": ["asset"]
}
```

**Result:**
```json
{
  "report": {
    "name": "Valuation Rollforward",
    "parameters": {
      "runId": "run_20250610_1",
      "startDate": "2025-01-01",
      "endDate": "2025-06-10"
    },
    "data": [
      {
        "asset": "BTC",
        "beginningBalance": 50000.00,
        "acquisitions": 25000.00,
        "disposals": -15000.00,
        "impairment": -2000.00,
        "revaluationUp": 5000.00,
        "revaluationDown": -1000.00,
        "endingBalance": 62000.00
      },
      // Additional records...
    ]
  },
  "metadata": {
    "execution_time_ms": 420,
    "row_count": 10
  }
}
```

### 4. `generate_inventory_balance`

Generates an inventory balance report.

**Arguments:**
```json
{
  "parameters": {
    "runId": "latest",
    "asOfDate": "2025-06-10"
  },
  "groupBy": ["asset", "inventory"]
}
```

**Result:**
```json
{
  "report": {
    "name": "Inventory Balance",
    "parameters": {
      "runId": "run_20250610_1",
      "asOfDate": "2025-06-10"
    },
    "data": [
      {
        "asset": "BTC",
        "inventory": "Trading",
        "balance": 3.5,
        "carryingValue": 105000.00,
        "costBasis": 30000.00
      },
      {
        "asset": "BTC",
        "inventory": "Treasury",
        "balance": 10.0,
        "carryingValue": 280000.00,
        "costBasis": 28000.00
      },
      // Additional records...
    ]
  },
  "metadata": {
    "execution_time_ms": 380,
    "row_count": 15
  }
}
```

### 5. `configure_data_source`

Configures the data source for the server.

**Arguments:**
```json
{
  "type": "bigquery",
  "config": {
    "projectId": "my-project-id",
    "datasetId": "crypto_accounting",
    "tableId": "gl_actions",
    "keyFilename": "./keys/service-account.json"
  }
}
```

**Result:**
```json
{
  "status": "success",
  "message": "Data source configured successfully",
  "config": {
    "type": "bigquery",
    "projectId": "my-project-id",
    "datasetId": "crypto_accounting",
    "tableId": "gl_actions"
  }
}
```

### 6. `validate_column_mapping`

Validates column mapping for the data source.

**Arguments:**
```json
{
  "mapping": {
    "asset": "cryptocurrency_symbol",
    "assetBalance": "quantity",
    "carryingValue": "book_value",
    "timestamp": "transaction_date"
  }
}
```

**Result:**
```json
{
  "status": "success",
  "valid": true,
  "message": "Column mapping is valid",
  "mapping": {
    "asset": "cryptocurrency_symbol",
    "assetBalance": "quantity",
    "carryingValue": "book_value",
    "timestamp": "transaction_date"
  }
}
```

## Error Codes

| Code    | Description                      |
|---------|----------------------------------|
| -32600  | Invalid Request                  |
| -32601  | Method not found                 |
| -32602  | Invalid params                   |
| -32603  | Internal error                   |
| -32000  | Query parsing error              |
| -32001  | BigQuery execution error         |
| -32002  | Data source configuration error  |
| -32003  | Report generation error          |
| -32004  | Column mapping validation error  |
