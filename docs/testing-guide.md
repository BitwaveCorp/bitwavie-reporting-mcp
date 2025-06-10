# Reporting MCP Server: Testing Guide

This guide provides instructions for testing the Reporting MCP Server's functionality, including BigQuery connectivity, natural language query processing, and report generation.

## Prerequisites

Before testing, ensure you have:

1. Configured the server with the correct environment variables
2. Access to the BigQuery dataset and table
3. Node.js v16 or higher installed

## Test Scripts Overview

The repository includes several test scripts to validate different aspects of the server:

| Script | Purpose |
|--------|---------|
| `test-bigquery.js` | Tests BigQuery connectivity and schema access |
| `test-nl-query.js` | Tests natural language query processing |
| `test-inventory-report.js` | Tests inventory balance report generation |
| `fixed-inventory-test.js` | Tests inventory balance report with corrected SQL syntax |
| `test-derivative-reports.js` | Tests all derivative report types |
| `simple-test.js` | Tests basic MCP JSON-RPC communication |
| `sdk-test.js` | Tests MCP SDK client communication |
| `direct-test.js` | Tests direct JSON-RPC communication |
| `test-server.js` | Runs the MCP server for manual testing |

## Testing BigQuery Connectivity

To test the connection to BigQuery and verify access to the dataset and table:

```bash
node test-bigquery.js
```

Expected output:
```
Testing BigQuery connection...
Using credentials from: ./reporting-mcp/keys/bitwave-solutions-a99267d2687a.json
Project ID: bitwave-solutions
Dataset ID: 0_Bitwavie_MCP
Table ID: 2622d4df5b2a15ec811e_gl_actions

Running test query...
Query results: [{ count: '169324' }]

Getting table metadata...
Table schema has 64 fields.
Sample fields: runId, timestamp, asset, ...

BigQuery connection test completed successfully!
```

## Testing Natural Language Query Processing

To test the natural language query processing capability:

```bash
node test-nl-query.js
```

Expected output:
```
Testing natural language query processing...
Using credentials from: ./reporting-mcp/keys/bitwave-solutions-a99267d2687a.json

=== QUERY 1: What is my BTC balance? ===
Generated SQL: SELECT asset, SUM(CAST(assetBalance AS FLOAT64)) as balance FROM `bitwave-solutions.0_Bitwavie_MCP.2622d4df5b2a15ec811e_gl_actions` WHERE asset = 'BTC' GROUP BY asset
Results: []

=== QUERY 2: What is my total balance for all assets? ===
Generated SQL: SELECT asset, SUM(CAST(assetBalance AS FLOAT64)) as balance FROM `bitwave-solutions.0_Bitwavie_MCP.2622d4df5b2a15ec811e_gl_actions` GROUP BY asset ORDER BY balance DESC
Results: [ { asset: 'CANTON', balance: 2334994561032.97 } ]

=== QUERY 3: Which asset has the most transactions? ===
Generated SQL: SELECT asset, COUNT(*) as transaction_count FROM `bitwave-solutions.0_Bitwavie_MCP.2622d4df5b2a15ec811e_gl_actions` GROUP BY asset ORDER BY transaction_count DESC LIMIT 1
Results: [ { asset: 'CANTON', transaction_count: 169324 } ]

Natural language query test completed successfully!
```

## Testing Derivative Reports

To test all derivative report types (lots, valuation rollforward, and inventory balance):

```bash
node test-derivative-reports.js
```

Expected output:
```
Testing derivative report generation...
Using credentials from: ./reporting-mcp/keys/bitwave-solutions-a99267d2687a.json
Project ID: bitwave-solutions
Dataset ID: 0_Bitwavie_MCP
Table ID: 2622d4df5b2a15ec811e_gl_actions

=== LOTS REPORT ===
Generating lots report with params: {"runId":"latest","asOfDate":"2025-06-10"}
Group by: [ 'asset', 'lotId' ]

Lots report results:
Total records: 10
Record 1: {
  "asset": "CANTON",
  "lotId": "CANTON.#1220e4ac3c6961a17030b52b782bff1efb2a02e2d7c5a4504de3f70927b90d9d1ebe:0-input_validator_reward_amount.0.0",
  "balance": 37222.96827365999,
  "carryingValue": 436.78934549599995,
  "acquisitionDate": {
    "value": "2024-11-18T16:06:45.000Z"
  }
}
...

=== VALUATION ROLLFORWARD REPORT ===
Generating valuation rollforward report with params: {"runId":"latest","asOfDate":"2025-06-10"}
Group by: [ 'asset' ]

Valuation rollforward report results:
Total records: 1
Record 1: {
  "asset": "CANTON",
  "carryingValue": 2962944.985009758,
  "acquisitions": 1194166.509999997,
  "disposals": 245060.64394692975,
  "impairment": null,
  "revaluationUp": null,
  "revaluationDown": null
}

=== INVENTORY BALANCE REPORT ===
Generating inventory balance report with params: {"runId":"latest","asOfDate":"2025-06-10"}
Group by: [ 'asset', 'inventory' ]

Inventory balance report results:
Total records: 1
Record 1: {
  "asset": "CANTON",
  "inventory": null,
  "balance": 2334994561032.97,
  "carryingValue": 2962944.985009758,
  "costBasis": 0.000001268930144187998
}

Derivative report tests completed successfully!
```

## Testing MCP JSON-RPC Communication

### Using Simple Test

To test basic MCP JSON-RPC communication:

```bash
node simple-test.js
```

Expected output (if working correctly):
```
Starting MCP server process...
Sending request: {"jsonrpc":"2.0","id":"1","method":"mcp.list_tools","params":{}}
Response: {
  "jsonrpc": "2.0",
  "id": "1",
  "result": {
    "tools": [
      {
        "name": "analyze_actions_data",
        "description": "Run analytical queries on actions data using natural language",
        "parameters": { ... }
      },
      ...
    ]
  }
}
```

### Using Direct Test

To test direct JSON-RPC communication with different method formats:

```bash
node direct-test.js
```

This script tests various method name formats to identify the correct format expected by the server.

### Using SDK Test

To test communication using the MCP SDK client:

```bash
node sdk-test.js
```

## Troubleshooting Tests

### Common Test Issues

1. **"Method not found" errors**:
   - Try different method name formats (with/without "mcp." prefix)
   - Check the server logs for the expected method names
   - Verify that the server is properly registering the methods

2. **BigQuery query errors**:
   - Ensure table names are properly quoted with backticks
   - Include the project ID in fully qualified table names
   - Check for SQL syntax errors in the queries

3. **Environment variable issues**:
   - Verify that all environment variables are correctly set in the `.env` file
   - Check for typos in environment variable names
   - Ensure the service account key file path is correct

### Debugging Tests

To get more detailed information during testing:

1. Add console.log statements to the test scripts
2. Run the tests with Node.js debugging enabled:
   ```bash
   node --inspect test-script.js
   ```
3. Use Chrome DevTools to debug the tests by navigating to `chrome://inspect`

## Manual Testing

To manually test the server:

1. Start the server in a terminal:
   ```bash
   node test-server.js
   ```

2. In another terminal, send JSON-RPC requests to the server:
   ```bash
   echo '{"jsonrpc":"2.0","id":"1","method":"mcp.list_tools","params":{}}' | node test-server.js
   ```

## Continuous Integration Testing

For automated testing in a CI environment:

1. Create a `.env.test` file with test environment variables
2. Run tests with the test environment:
   ```bash
   NODE_ENV=test node test-bigquery.js
   ```

3. Use a CI service like GitHub Actions to run tests automatically on each commit

## Conclusion

By following this testing guide, you can verify that all components of the Reporting MCP Server are functioning correctly. If you encounter issues, refer to the troubleshooting section or check the server logs for more detailed error information.
