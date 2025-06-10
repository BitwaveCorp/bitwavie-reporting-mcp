# Reporting MCP Server: Technical Documentation

## Architecture Overview

The Reporting MCP Server is built on the Model Context Protocol (MCP) framework and provides analytical capabilities for crypto accounting data. It connects to Google BigQuery to execute queries and generate reports based on user requests.

### Core Components

```
┌─────────────────────────────────────────────────────────────┐
│                    Reporting MCP Server                     │
├─────────────────┬───────────────────────┬──────────────────┤
│  Query Parser   │   BigQuery Client     │ Report Generators│
├─────────────────┴───────────────────────┴──────────────────┤
│                      MCP Server SDK                         │
└─────────────────────────────────────────────────────────────┘
```

#### 1. Query Parser

The `QueryParser` class is responsible for translating natural language queries into SQL. It follows a 5-step process:

1. **UNDERSTAND** - Parse user intent using NLP techniques
2. **MAP** - Identify relevant columns in the database schema
3. **AGGREGATE** - Apply appropriate aggregation functions
4. **FILTER** - Apply filters based on the query constraints
5. **PRESENT** - Format the results for presentation

#### 2. BigQuery Client

The `BigQueryClient` class handles communication with Google BigQuery. It provides methods for:

- Configuring the connection to BigQuery
- Executing SQL queries
- Processing query results
- Error handling and retries

#### 3. Report Generators

The server includes three specialized report generators:

- **LotsReportGenerator**: Generates reports on individual lots with acquisition dates and balances
- **ValuationRollforwardGenerator**: Generates reports on asset acquisitions, disposals, and value changes
- **InventoryBalanceGenerator**: Generates reports on current balances by asset and inventory

## Implementation Details

### Server Initialization

The server is initialized in `server.ts`:

```typescript
const server = new ReportingMCPServer();
server.run().catch(console.error);
```

The `ReportingMCPServer` class sets up the MCP server with the necessary capabilities and handlers:

```typescript
constructor() {
  this.server = new Server(
    {
      name: 'reporting-mcp-server',
      version: '1.0.0',
    },
    {
      capabilities: {
        tools: {},
      },
    }
  );

  // Initialize service components
  this.queryParser = new QueryParser();
  this.bigQueryClient = new BigQueryClient();
  this.lotsReportGen = new LotsReportGenerator(this.bigQueryClient);
  this.rollforwardGen = new ValuationRollforwardGenerator(this.bigQueryClient);
  this.inventoryGen = new InventoryBalanceGenerator(this.bigQueryClient);

  this.setupToolHandlers();
}
```

### Tool Handlers

The server registers handlers for various MCP tools:

```typescript
setupToolHandlers() {
  // Register tool handlers
  this.server.onListTools(async () => {
    return {
      tools: [
        {
          name: 'analyze_actions_data',
          description: 'Run analytical queries on actions data using natural language',
          parameters: { /* ... */ }
        },
        {
          name: 'generate_lots_report',
          description: 'Generate a report of lots with their acquisition dates and balances',
          parameters: { /* ... */ }
        },
        // Additional tools...
      ]
    };
  });

  this.server.onCallTool(async (request) => {
    // Handle tool calls based on the tool name
    switch (request.name) {
      case 'analyze_actions_data':
        return this.handleAnalyticalQuery(request.arguments);
      case 'generate_lots_report':
        return this.handleLotsReport(request.arguments);
      // Additional handlers...
    }
  });
}
```

### Communication Protocol

The server uses the MCP JSON-RPC protocol for communication. It listens on standard input/output (stdio) for JSON-RPC requests and sends responses in the same format.

## Data Flow

1. Client sends a JSON-RPC request to the server
2. Server parses the request and routes it to the appropriate handler
3. Handler executes the requested operation (e.g., runs a query or generates a report)
4. Server sends the response back to the client

## Error Handling

The server implements robust error handling:

- **Input validation**: Validates all input parameters before processing
- **Query errors**: Catches and formats BigQuery errors
- **Connection issues**: Handles connection failures with appropriate retries
- **Resource limits**: Enforces query timeouts and result size limits

## Performance Considerations

- **Query optimization**: The server optimizes SQL queries for performance
- **Result pagination**: Large result sets are paginated to avoid memory issues
- **Caching**: Frequently used queries and schema information are cached
- **Connection pooling**: BigQuery connections are pooled for efficiency
