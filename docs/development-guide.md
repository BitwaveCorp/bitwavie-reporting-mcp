# Reporting MCP Server: Development Guide

This guide provides information for developers working on the Reporting MCP Server codebase, including architecture, code organization, and best practices.

## Project Structure

```
reporting-mcp/
├── src/
│   ├── server.ts                 # Main server entry point
│   ├── bigquery/
│   │   ├── client.ts             # BigQuery client implementation
│   │   └── schema.ts             # BigQuery schema utilities
│   ├── query/
│   │   ├── parser.ts             # Natural language query parser
│   │   └── sql-generator.ts      # SQL generation utilities
│   ├── reports/
│   │   ├── inventory-balance.ts  # Inventory balance report generator
│   │   ├── lots-report.ts        # Lots report generator
│   │   └── valuation-rollforward.ts # Valuation rollforward generator
│   └── utils/
│       ├── config.ts             # Configuration utilities
│       └── logging.ts            # Logging utilities
├── tests/
│   ├── unit/                     # Unit tests
│   └── integration/              # Integration tests
├── keys/                         # Service account keys (gitignored)
└── dist/                         # Compiled JavaScript output
```

## Core Components

### 1. Server Component

The `ReportingMCPServer` class in `server.ts` is the main entry point for the application. It:

- Initializes the MCP server
- Sets up tool handlers for JSON-RPC methods
- Configures service components (BigQuery client, query parser, report generators)
- Handles incoming requests and routes them to the appropriate handlers

```typescript
class ReportingMCPServer {
  private server: Server;
  private queryParser: QueryParser;
  private bigQueryClient: BigQueryClient;
  private lotsReportGen: LotsReportGenerator;
  private rollforwardGen: ValuationRollforwardGenerator;
  private inventoryGen: InventoryBalanceGenerator;

  constructor() {
    // Initialize server and components
  }

  async run() {
    // Start the server
  }

  private setupToolHandlers() {
    // Register tool handlers
  }

  private async handleAnalyticalQuery(args: any) {
    // Handle natural language query
  }

  private async handleLotsReport(args: any) {
    // Generate lots report
  }

  // Additional handlers...
}
```

### 2. BigQuery Integration

The `BigQueryClient` class in `bigquery/client.ts` handles communication with Google BigQuery:

```typescript
class BigQueryClient {
  private client: BigQuery;
  
  constructor() {
    this.client = new BigQuery({
      projectId: process.env.GOOGLE_CLOUD_PROJECT_ID,
      keyFilename: process.env.GOOGLE_APPLICATION_CREDENTIALS
    });
  }

  async executeQuery(query: string) {
    // Execute query and return results
  }

  async getTableSchema() {
    // Get table schema
  }

  // Additional methods...
}
```

### 3. Query Parsing

The `QueryParser` class in `query/parser.ts` translates natural language queries into SQL:

```typescript
class QueryParser {
  private schema: TableSchema;
  
  constructor(schema: TableSchema) {
    this.schema = schema;
  }

  async parseQuery(query: string) {
    // Parse query and generate SQL
  }

  private mapQueryToColumns(query: string) {
    // Map query terms to database columns
  }

  private determineAggregations(query: string) {
    // Determine aggregation functions
  }

  private applyFilters(query: string) {
    // Apply filters based on query constraints
  }

  // Additional methods...
}
```

### 4. Report Generators

The report generator classes in the `reports/` directory generate predefined reports:

```typescript
class ReportGenerator {
  protected bigQueryClient: BigQueryClient;
  
  constructor(bigQueryClient: BigQueryClient) {
    this.bigQueryClient = bigQueryClient;
  }

  protected async executeQuery(query: string) {
    // Execute query and return results
  }
}

class InventoryBalanceGenerator extends ReportGenerator {
  async generate(params: ReportParams, groupBy: string[] = []) {
    // Generate inventory balance report
  }
}

// Additional report generators...
```

## Development Workflow

### Setting Up the Development Environment

1. Clone the repository
2. Install dependencies:
   ```bash
   npm install
   ```
3. Set up environment variables in a `.env` file
4. Create a `keys` directory and add your service account key

### Building the Project

To build the TypeScript code:

```bash
npm run build
```

This compiles the TypeScript code to JavaScript in the `dist` directory.

### Running in Development Mode

To run the server in development mode with hot reloading:

```bash
npm run dev
```

### Running Tests

To run unit tests:

```bash
npm test
```

To run integration tests:

```bash
npm run test:integration
```

## Extending the Server

### Adding a New Report Type

To add a new report type:

1. Create a new report generator class in the `reports/` directory:
   ```typescript
   // src/reports/new-report.ts
   import { ReportGenerator } from './base';
   import { BigQueryClient } from '../bigquery/client';
   import { ReportParams } from '../types';

   export class NewReportGenerator extends ReportGenerator {
     constructor(bigQueryClient: BigQueryClient) {
       super(bigQueryClient);
     }

     async generate(params: ReportParams, groupBy: string[] = []) {
       // Generate SQL query
       const query = this.buildQuery(params, groupBy);
       
       // Execute query
       const results = await this.executeQuery(query);
       
       // Process results
       return this.processResults(results);
     }

     private buildQuery(params: ReportParams, groupBy: string[]) {
       // Build SQL query
     }

     private processResults(results: any[]) {
       // Process query results
     }
   }
   ```

2. Update the server class to include the new report generator:
   ```typescript
   // src/server.ts
   import { NewReportGenerator } from './reports/new-report';

   class ReportingMCPServer {
     private newReportGen: NewReportGenerator;

     constructor() {
       // Initialize other components
       this.newReportGen = new NewReportGenerator(this.bigQueryClient);
     }

     private setupToolHandlers() {
       // Register existing tool handlers
       
       // Add new tool handler
       this.server.onCallTool(async (request) => {
         if (request.name === 'generate_new_report') {
           return this.handleNewReport(request.arguments);
         }
         // Other handlers
       });
     }

     private async handleNewReport(args: any) {
       // Handle new report generation
       return this.newReportGen.generate(args.parameters, args.groupBy);
     }
   }
   ```

### Enhancing the Query Parser

To enhance the query parser with new capabilities:

1. Update the `QueryParser` class:
   ```typescript
   // src/query/parser.ts
   class QueryParser {
     // Existing methods
     
     private handleNewQueryType(query: string) {
       // Handle new query type
     }
   }
   ```

2. Add new mapping rules in the query-to-SQL mapping logic

### Adding New BigQuery Functionality

To add new BigQuery functionality:

1. Update the `BigQueryClient` class:
   ```typescript
   // src/bigquery/client.ts
   class BigQueryClient {
     // Existing methods
     
     async newBigQueryFunction() {
       // Implement new functionality
     }
   }
   ```

## Best Practices

### Code Style

- Follow TypeScript best practices
- Use interfaces for type definitions
- Document public methods with JSDoc comments
- Use async/await for asynchronous operations

### Error Handling

- Use try/catch blocks for error handling
- Provide meaningful error messages
- Log errors with appropriate context
- Return structured error responses

Example:

```typescript
async executeQuery(query: string) {
  try {
    const [rows] = await this.client.query({ query });
    return rows;
  } catch (error) {
    console.error('Error executing BigQuery query:', error);
    throw new Error(`BigQuery error: ${error.message}`);
  }
}
```

### Logging

- Use structured logging
- Include relevant context in log messages
- Use appropriate log levels

Example:

```typescript
import { logger } from '../utils/logging';

async handleAnalyticalQuery(args: any) {
  logger.info('Processing analytical query', { query: args.query });
  
  try {
    const result = await this.queryParser.parseQuery(args.query);
    logger.debug('Query parsed successfully', { sql: result.sql });
    return result;
  } catch (error) {
    logger.error('Error processing analytical query', { error, query: args.query });
    throw error;
  }
}
```

### Testing

- Write unit tests for individual components
- Write integration tests for end-to-end functionality
- Use mocks for external dependencies
- Test edge cases and error conditions

Example:

```typescript
// tests/unit/query-parser.test.ts
describe('QueryParser', () => {
  let queryParser: QueryParser;
  let mockSchema: TableSchema;
  
  beforeEach(() => {
    mockSchema = { /* mock schema */ };
    queryParser = new QueryParser(mockSchema);
  });
  
  it('should parse a simple balance query', async () => {
    const result = await queryParser.parseQuery('What is my BTC balance?');
    expect(result.sql).toContain('SELECT asset, SUM(CAST(assetBalance AS FLOAT64))');
    expect(result.sql).toContain('WHERE asset = \'BTC\'');
  });
  
  // Additional tests...
});
```

## Performance Optimization

### Query Optimization

- Use query parameters to prevent SQL injection
- Limit result sets to avoid memory issues
- Use appropriate indexes in BigQuery tables
- Optimize JOIN operations

### Caching

- Cache frequently used data
- Use memory-efficient caching strategies
- Implement cache invalidation when data changes

Example:

```typescript
class BigQueryClient {
  private schemaCache: Map<string, TableSchema> = new Map();
  
  async getTableSchema() {
    const cacheKey = `${process.env.GOOGLE_CLOUD_PROJECT_ID}.${process.env.BIGQUERY_DATASET_ID}.${process.env.BIGQUERY_TABLE_ID}`;
    
    // Check cache first
    if (this.schemaCache.has(cacheKey)) {
      return this.schemaCache.get(cacheKey);
    }
    
    // Fetch schema from BigQuery
    const schema = await this.fetchSchemaFromBigQuery();
    
    // Cache the result
    this.schemaCache.set(cacheKey, schema);
    
    return schema;
  }
}
```

## Debugging

### Local Debugging

- Use the `--inspect` flag with Node.js
- Add debug logging statements
- Use the Chrome DevTools for debugging

### Production Debugging

- Use structured logging
- Include request IDs in logs
- Monitor error rates and performance metrics

## Deployment

### Containerization

- Use Docker to containerize the application
- Create a Dockerfile:
  ```dockerfile
  FROM node:16-alpine
  
  WORKDIR /app
  
  COPY package*.json ./
  RUN npm ci --only=production
  
  COPY dist/ ./dist/
  
  CMD ["node", "dist/server.js"]
  ```

### Environment Configuration

- Use environment variables for configuration
- Store secrets securely
- Use different configurations for development, testing, and production

### CI/CD

- Set up automated tests in CI/CD pipeline
- Automate deployment process
- Include security scanning in the pipeline
