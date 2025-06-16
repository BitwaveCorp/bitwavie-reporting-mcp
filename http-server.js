import express from 'express';
import { ReportingMCPServer } from './dist/server.js';

// Create an Express app
const app = express();
app.use(express.json());

// Log configuration
console.log('[HTTP-RPC] Starting server with configuration:');
console.log(`  Port: ${process.env.PORT || 8080}`);
console.log(`  Environment: ${process.env.NODE_ENV || 'development'}`);
console.log(`  Project ID: ${process.env.GOOGLE_CLOUD_PROJECT_ID || 'Not set'}`);
console.log(`  Dataset ID: ${process.env.BIGQUERY_DATASET_ID || 'Not set'}`);
console.log(`  Table ID: ${process.env.BIGQUERY_TABLE_ID || 'Not set'}`);
console.log(`  Enhanced NLQ: ${process.env.USE_ENHANCED_NLQ === 'true' ? 'Enabled' : 'Disabled'}`);
console.log(`  Include SQL: ${process.env.INCLUDE_SQL_IN_RESPONSES === 'true' ? 'Enabled' : 'Disabled'}`);

// Create the MCP server with configuration
console.log('[HTTP-RPC] Initializing MCP server...');
const mcpServer = new ReportingMCPServer({
  port: process.env.PORT ? parseInt(process.env.PORT) : 3000,
  projectId: process.env.GOOGLE_CLOUD_PROJECT_ID || '',
  datasetId: process.env.BIGQUERY_DATASET_ID || '',
  tableId: process.env.BIGQUERY_TABLE_ID || '',
  anthropicApiKey: process.env.ANTHROPIC_API_KEY || '',
  useEnhancedNLQ: process.env.USE_ENHANCED_NLQ === 'true',
  includeSqlInResponses: process.env.INCLUDE_SQL_IN_RESPONSES === 'true',
  schemaRefreshIntervalMs: process.env.SCHEMA_REFRESH_INTERVAL_MS ? 
    parseInt(process.env.SCHEMA_REFRESH_INTERVAL_MS) : 3600000
});
console.log('[HTTP-RPC] MCP server initialized successfully');

// Health check endpoint
app.get('/', (_req, res) => {
  res.status(200).send('MCP HTTP Server is running');
});

// JSON-RPC endpoint handler function
async function handleRpcRequest(req, res) {
  try {
    const { jsonrpc, method, params, id } = req.body;
    console.log(`[HTTP-RPC] Processing method: ${method}, id: ${id}`);
    
    if (jsonrpc !== '2.0') {
      console.log(`[HTTP-RPC] Invalid JSON-RPC version: ${jsonrpc}`);
      return res.status(400).json({
        jsonrpc: '2.0',
        error: { code: -32600, message: 'Invalid Request: jsonrpc version must be 2.0' },
        id
      });
    }
    
    // Handle different methods
    switch (method) {
      case 'test_connection': {
        console.log('[HTTP-RPC] Processing test_connection request');
        console.log('[HTTP-RPC] Calling mcpServer.handleTestConnection()...');
        try {
          const testResult = await mcpServer.handleTestConnection();
          console.log('[HTTP-RPC] test_connection result:', JSON.stringify(testResult));
          return res.json({
            jsonrpc: '2.0',
            result: testResult.success ? 'MCP server is working' : testResult.message,
            id
          });
        } catch (error) {
          console.error('[HTTP-RPC] Error in test_connection:', error);
          return res.status(500).json({
            jsonrpc: '2.0',
            error: {
              code: -32603,
              message: error instanceof Error ? error.message : 'Error testing connection',
              data: error instanceof Error ? error.stack : undefined
            },
            id
          });
        }
      }
        
      case 'list_tools':
      case 'tools/list': {
        console.log('[HTTP-RPC] Processing tools/list request');
        // Define the tools list with the same structure as in server.ts
        const toolsResponse = {
          jsonrpc: '2.0',
          result: {
            tools: [
              { name: 'analyze_actions_data', description: 'Analyze crypto transaction data' },
              { name: 'test_connection', description: 'Test connection to MCP server' }
            ]
          },
          id
        };
        console.log('[HTTP-RPC] Returning tools list:', JSON.stringify(toolsResponse));
        return res.json(toolsResponse);
      }
        
      case 'tools/call': {
        console.log('[HTTP-RPC] Processing tools/call request');
        if (!params || typeof params !== 'object' || !params.name) {
          console.log('[HTTP-RPC] Invalid params for tools/call:', params);
          return res.status(400).json({
            jsonrpc: '2.0',
            error: { code: -32602, message: 'Invalid params for tools/call' },
            id
          });
        }
        
        const toolName = params.name;
        const toolArgs = params.arguments || {};
        console.log(`[HTTP-RPC] tools/call for tool: ${toolName}`);
        
        if (toolName === 'analyze_actions_data') {
          try {
            console.log('[HTTP-RPC] tools/call: Calling mcpServer.handleAnalyzeData with query:', toolArgs.query);
            console.log('[HTTP-RPC] tools/call: confirmedMappings:', JSON.stringify(toolArgs.confirmedMappings || {}));
            console.log('[HTTP-RPC] tools/call: previousResponse:', JSON.stringify(toolArgs.previousResponse || {}));
            
            const startTime = Date.now();
            const analyzeResult = await mcpServer.handleAnalyzeData({
              query: toolArgs.query,
              confirmedMappings: toolArgs.confirmedMappings,
              previousResponse: toolArgs.previousResponse
            });
            const endTime = Date.now();
            
            console.log(`[HTTP-RPC] tools/call: handleAnalyzeData completed in ${endTime - startTime}ms`);
            console.log('[HTTP-RPC] tools/call: Result structure:', Object.keys(analyzeResult).join(', '));
            console.log('[HTTP-RPC] tools/call: Answer preview:', analyzeResult.answer?.substring(0, 100) + '...');
            
            return res.json({
              jsonrpc: '2.0',
              result: analyzeResult,
              id
            });
          } catch (toolError) {
            console.error('[HTTP-RPC] Error in tools/call for analyze_actions_data:', toolError);
            return res.status(500).json({
              jsonrpc: '2.0',
              error: {
                code: -32603,
                message: toolError instanceof Error ? toolError.message : 'Error processing tool',
                data: toolError instanceof Error ? toolError.stack : undefined
              },
              id
            });
          }
        } else {
          return res.status(400).json({
            jsonrpc: '2.0',
            error: { code: -32601, message: `Tool '${toolName}' not found` },
            id
          });
        }
      }
        
      case 'analyze_actions_data': {
        console.log('[HTTP-RPC] Processing analyze_actions_data request');
        if (!params || !Array.isArray(params) || params.length === 0) {
          console.log('[HTTP-RPC] Invalid params for analyze_actions_data:', params);
          return res.status(400).json({
            jsonrpc: '2.0',
            error: { code: -32602, message: 'Invalid params for analyze_actions_data' },
            id
          });
        }
        
        const requestData = params[0];
        console.log('[HTTP-RPC] analyze_actions_data request data:', JSON.stringify(requestData));
        
        try {
          console.log('[HTTP-RPC] direct: Calling mcpServer.handleAnalyzeData with query:', requestData.query);
          console.log('[HTTP-RPC] direct: confirmedMappings:', JSON.stringify(requestData.confirmedMappings || {}));
          console.log('[HTTP-RPC] direct: previousResponse:', JSON.stringify(requestData.previousResponse || {}));
          
          const startTime = Date.now();
          const analyzeResult = await mcpServer.handleAnalyzeData({
            query: requestData.query,
            confirmedMappings: requestData.confirmedMappings,
            previousResponse: requestData.previousResponse
          });
          const endTime = Date.now();
          
          console.log(`[HTTP-RPC] direct: handleAnalyzeData completed in ${endTime - startTime}ms`);
          console.log('[HTTP-RPC] direct: Result structure:', Object.keys(analyzeResult).join(', '));
          console.log('[HTTP-RPC] direct: Answer preview:', analyzeResult.answer?.substring(0, 100) + '...');
          console.log('[HTTP-RPC] direct: SQL generated:', analyzeResult.sql || 'No SQL generated');
          
          return res.json({
            jsonrpc: '2.0',
            result: analyzeResult,
            id
          });
        } catch (analyzeError) {
          console.error('[HTTP-RPC] Error in analyze_actions_data:', analyzeError);
          return res.status(500).json({
            jsonrpc: '2.0',
            error: {
              code: -32603,
              message: analyzeError instanceof Error ? analyzeError.message : 'Error processing query',
              data: analyzeError instanceof Error ? analyzeError.stack : undefined
            },
            id
          });
        }
      }
        
      default: {
        console.log(`[HTTP-RPC] Method not found: ${method}`);
        return res.status(404).json({
          jsonrpc: '2.0',
          error: { code: -32601, message: `Method '${method}' not found` },
          id
        });
      }
    }
  } catch (error) {
    console.error('[HTTP-RPC] Error processing RPC request:', error);
    return res.status(500).json({
      jsonrpc: '2.0',
      error: {
        code: -32603,
        message: 'Internal error',
        data: error instanceof Error ? error.stack : undefined
      },
      id: req.body?.id || null
    });
  }
}

// Register the RPC endpoint handler
app.post('/rpc', handleRpcRequest);

// Start the server
const port = process.env.PORT || 8080;
const server = app.listen(port, () => {
  console.log(`[HTTP-RPC] MCP HTTP Server running on port ${port}`);
  console.log(`[HTTP-RPC] Server ready to accept JSON-RPC requests at http://localhost:${port}/rpc`);
  console.log(`[HTTP-RPC] Supported methods: test_connection, tools/list, tools/call, analyze_actions_data`);
  console.log(`[HTTP-RPC] Test with: node test-rpc.js`);
});

// Handle server errors
server.on('error', (error) => {
  console.error(`[HTTP-RPC] Server error: ${error.message}`);
  if (error.code === 'EADDRINUSE') {
    console.error(`[HTTP-RPC] Port ${port} is already in use. Please choose a different port.`);
  }
  process.exit(1);
});

// Handle graceful shutdown
process.on('SIGINT', () => {
  console.log('[HTTP-RPC] Shutting down HTTP server');
  server.close(() => {
    console.log('[HTTP-RPC] HTTP server closed');
    process.exit(0);
  });
});

// Handle uncaught exceptions
process.on('uncaughtException', (error) => {
  console.error('[HTTP-RPC] Uncaught exception:', error);
  console.error('[HTTP-RPC] Server will continue running, but may be in an unstable state');
});

console.log('[HTTP-RPC] Server initialization complete');

