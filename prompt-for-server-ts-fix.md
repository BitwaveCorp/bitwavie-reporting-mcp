# Prompt for Fixing the Reporting MCP Server Implementation

## Task Description
Fix the TypeScript implementation of the `server.ts` file in the Bitwavie MCP Reporting Server. The server needs to properly integrate with the NLQProcessor, BigQueryClient, and MCP SDK without TypeScript errors.

## Key Requirements

1. Fix all TypeScript errors in the `ReportingMCPServer` class implementation
2. Ensure consistent usage of MCP SDK APIs for handler registration
3. Fix the BigQueryClient integration, particularly using `executeAnalyticalQuery` instead of directly calling the private `executeQuery` method
4. Properly implement session management for NLQ processing
5. Ensure robust error handling and fallback to legacy query parsing when NLQ fails
6. Complete the implementation of all handler methods, especially `handleAnalyzeData` and `handleTestConnection`

## Specific Implementation Details

### Class Structure
- Keep the class name as `ReportingMCPServer`
- Maintain all existing private properties:
  - `server: Server`
  - `queryParser: QueryParser`
  - `bigQueryClient: BigQueryClient`
  - `anthropic: Anthropic | null`
  - `nlqProcessor: NLQProcessor | null`
  - `useEnhancedNLQ: boolean`

### Constructor
- Initialize the `Server` from MCP SDK with proper configuration
- Initialize legacy services: `QueryParser` and `BigQueryClient`
- Initialize Anthropic client if `ANTHROPIC_API_KEY` environment variable is set
- Initialize `NLQProcessor` if enhanced NLQ mode is enabled
- Call `setupHandlers()` and `initializeBigQueryClient()`

### Method: `initializeBigQueryClient`
- Configure BigQueryClient with environment variables
- Initialize NLQProcessor if enabled
- Handle errors properly

### Method: `setupHandlers`
- Use `server.registerHandler` consistently for all handlers
- Use zod schemas for request validation
- Register handlers for:
  - `testConnection`
  - `analyzeData`
- Set up clean shutdown handlers for SIGINT and SIGTERM

### Method: `handleTestConnection`
- Test BigQuery connection
- Return formatted response with connection status
- Include proper error handling

### Method: `handleAnalyzeData`
- Use enhanced NLQ processor if enabled
- Generate session ID based on query hash
- Process query with NLQProcessor
- Fall back to legacy processing if NLQ fails
- Return properly formatted response

### Method: `handleAnalyzeDataLegacy`
- Handle responses to column selection prompts
- Process confirmed mappings
- Execute queries using `bigQueryClient.executeAnalyticalQuery` (NOT the private `executeQuery` method)
- Format and return query results

### Method: `generateSessionId`
- Create a consistent hash of the query for session tracking

### Method: `formatQueryResults`
- Format BigQuery results into the expected response format

### Method: `applyConfirmedMappings`
- Apply user-confirmed column mappings to the parse result

### Method: `formatColumnMappingConfirmation`
- Format column mapping confirmation prompts

### Method: `formatGainLossColumnList` and `formatFullColumnList`
- Format column lists for user selection

### Method: `start`
- Start the MCP server
- Log server startup

### Method: `stop`
- Stop the MCP server
- Clean up resources

## Important Implementation Notes

1. **BigQueryClient Usage**: 
   - Always use `executeAnalyticalQuery(parseResult, parameters)` for executing queries
   - Never directly call the private `executeQuery(sql)` method
   - Pass properly typed `ReportParameters` objects

2. **MCP SDK Handler Registration**:
   - Use `server.registerHandler` consistently
   - Provide proper zod schemas for request validation
   - Follow the MCP SDK pattern for handler registration

3. **NLQProcessor Integration**:
   - Initialize NLQProcessor with proper configuration
   - Use session IDs for query state management
   - Handle errors and fall back to legacy processing

4. **Error Handling**:
   - Use try/catch blocks for all external API calls
   - Log errors with appropriate context
   - Return user-friendly error messages

5. **Session Management**:
   - Use consistent session IDs based on query hash
   - Pass session state between requests
   - Handle session context in NLQProcessor

6. **TypeScript Types**:
   - Use proper type annotations for all methods and parameters
   - Avoid type assertions except where absolutely necessary
   - Ensure compatibility with imported types from other modules

## Example Code Snippets

### Handler Registration
```typescript
private setupHandlers(): void {
  // Test connection handler
  this.server.registerHandler({
    name: 'testConnection',
    description: 'Test connection to BigQuery',
    inputSchema: z.object({}),
    handler: async () => {
      return await this.handleTestConnection();
    },
  });

  // Analyze data handler
  this.server.registerHandler({
    name: 'analyzeData',
    description: 'Analyze data with natural language query',
    inputSchema: z.object({
      query: z.string().optional(),
      confirmedMappings: z.record(z.string()).optional(),
      previousResponse: z.any().optional(),
    }),
    handler: async (args) => {
      return await this.handleAnalyzeData(args);
    },
  });
}
```

### NLQ Processing with Fallback
```typescript
public async handleAnalyzeData(args: any): Promise<any> {
  const { query = '', confirmedMappings, previousResponse } = args;
  
  if (this.useEnhancedNLQ && this.nlqProcessor) {
    try {
      const sessionId = this.generateSessionId(query);
      return await this.nlqProcessor.processQuery(query, sessionId, previousResponse);
    } catch (error) {
      return this.handleAnalyzeDataLegacy(args);
    }
  }
  
  return this.handleAnalyzeDataLegacy(args);
}
```

### BigQuery Execution
```typescript
// Execute the query with BigQueryClient
const parameters: ReportParameters = {
  runId: '*',
  startDate: parseResult.timeRange?.start,
  endDate: parseResult.timeRange?.end,
  asOfDate: parseResult.timeRange?.end || currentDate
};

const queryResult = await this.bigQueryClient.executeAnalyticalQuery(parseResult, parameters);
```

## Final Notes
- Maintain all existing functionality while fixing TypeScript errors
- Ensure clean shutdown and resource cleanup
- Follow best practices for error handling and logging
- Make sure all environment variables are properly handled
