// Simple HTTP server for Cloud Run deployment testing with MCP server integration
import express from 'express';
import { ReportingMCPServer } from './dist/server.js';

// Create an Express app
const app = express();
app.use(express.json());

// Log configuration
console.log('[SIMPLE-HTTP] Starting server with configuration:');
console.log(`[SIMPLE-HTTP] Port: ${process.env.PORT || 8080}`);
console.log(`[SIMPLE-HTTP] Environment: ${process.env.NODE_ENV || 'development'}`);
console.log(`[SIMPLE-HTTP] Project ID: ${process.env.GOOGLE_CLOUD_PROJECT_ID || 'Not set'}`);
console.log(`[SIMPLE-HTTP] Dataset ID: ${process.env.BIGQUERY_DATASET_ID || 'Not set'}`);
console.log(`[SIMPLE-HTTP] Table ID: ${process.env.BIGQUERY_TABLE_ID || 'Not set'}`);

// Initialize MCP server
let mcpServer;
try {
  console.log('[SIMPLE-HTTP] Initializing MCP server...');
  mcpServer = new ReportingMCPServer({
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
  console.log('[SIMPLE-HTTP] MCP server initialized successfully');
} catch (error) {
  console.error('[SIMPLE-HTTP] Failed to initialize MCP server:', error);
  console.log('[SIMPLE-HTTP] Continuing with limited functionality');
}

// Log startup information
console.log('[SIMPLE-HTTP] Starting server...');
console.log(`[SIMPLE-HTTP] NODE_ENV: ${process.env.NODE_ENV}`);
console.log(`[SIMPLE-HTTP] PORT: ${process.env.PORT || 8080}`);

// Health check endpoint
app.get('/', (req, res) => {
  console.log('[SIMPLE-HTTP] Health check request received');
  res.status(200).send('MCP HTTP Server is running');
});

// JSON-RPC endpoint with MCP server integration
app.post('/rpc', async (req, res) => {
  console.log('[SIMPLE-HTTP] RPC request received:', JSON.stringify(req.body));
  
  const { jsonrpc, method, params, id } = req.body;
  
  if (jsonrpc !== '2.0') {
    return res.status(400).json({
      jsonrpc: '2.0',
      error: { code: -32600, message: 'Invalid Request: jsonrpc version must be 2.0' },
      id
    });
  }
  
  try {
    console.log(`[SIMPLE-HTTP] Processing method: ${method}`);
    
    // Handle different methods
    switch (method) {
      case 'list_tools':
      case 'tools/list': {
        console.log('[SIMPLE-HTTP] Processing tools/list request');
        
        // Return the list of available tools
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
        
        console.log('[SIMPLE-HTTP] Returning tools list');
        return res.json(toolsResponse);
      }
        
      case 'test_connection': {
        console.log('[SIMPLE-HTTP] Processing test_connection request');
        
        if (mcpServer) {
          try {
            console.log('[SIMPLE-HTTP] Calling mcpServer.handleTestConnection()...');
            const testResult = await mcpServer.handleTestConnection();
            console.log('[SIMPLE-HTTP] test_connection result:', JSON.stringify(testResult));
            
            return res.json({
              jsonrpc: '2.0',
              result: testResult.success ? 'MCP server is working' : testResult.message,
              id
            });
          } catch (error) {
            console.error('[SIMPLE-HTTP] Error in test_connection:', error);
            return res.json({
              jsonrpc: '2.0',
              result: 'MCP server test failed but HTTP server is working',
              id
            });
          }
        } else {
          // Fallback if MCP server is not initialized
          console.log('[SIMPLE-HTTP] MCP server not initialized, using fallback response');
          return res.json({
            jsonrpc: '2.0',
            result: 'HTTP server is working, but MCP server is not initialized',
            id
          });
        }
      }
      
      case 'analyze_actions_data': {
        console.log('[SIMPLE-HTTP] Processing analyze_actions_data request');
        const requestData = Array.isArray(params) ? params[0] : params;
        console.log('[SIMPLE-HTTP] Request data:', JSON.stringify(requestData));
        
        if (mcpServer) {
          try {
            console.log('[SIMPLE-HTTP] Calling mcpServer.handleAnalyzeData...');
            const startTime = Date.now();
            
            const analyzeResult = await mcpServer.handleAnalyzeData({
              query: requestData.query,
              confirmedMappings: requestData.confirmedMappings,
              previousResponse: requestData.previousResponse
            });
            
            const endTime = Date.now();
            console.log(`[SIMPLE-HTTP] handleAnalyzeData completed in ${endTime - startTime}ms`);
            console.log('[SIMPLE-HTTP] Result structure:', Object.keys(analyzeResult).join(', '));
            
            return res.json({
              jsonrpc: '2.0',
              result: analyzeResult,
              id
            });
          } catch (error) {
            console.error('[SIMPLE-HTTP] Error in analyze_actions_data:', error);
            
            // Provide a fallback response
            return res.json({
              jsonrpc: '2.0',
              result: {
                answer: `Error analyzing data: ${error.message}. Query was: ${requestData.query}`,
                sql: "-- Error occurred, no SQL generated",
                metadata: {
                  error: error.message,
                  query: requestData.query
                }
              },
              id
            });
          }
        } else {
          // Fallback if MCP server is not initialized
          console.log('[SIMPLE-HTTP] MCP server not initialized, using fallback response');
          return res.json({
            jsonrpc: '2.0',
            result: {
              answer: `Analysis for query: ${requestData.query} (MCP server not initialized)`,
              sql: "-- MCP server not initialized",
              metadata: {
                query: requestData.query
              }
            },
            id
          });
        }
      }
        
      case 'tools/call': {
        console.log('[SIMPLE-HTTP] Processing tools/call request');
        const { name: toolName, arguments: toolArgs } = params;
        console.log(`[SIMPLE-HTTP] Tool: ${toolName}, Arguments:`, JSON.stringify(toolArgs));
        
        if (toolName === 'analyze_actions_data') {
          if (mcpServer) {
            try {
              console.log('[SIMPLE-HTTP] Calling mcpServer.handleAnalyzeData via tools/call...');
              const startTime = Date.now();
              
              const analyzeResult = await mcpServer.handleAnalyzeData({
                query: toolArgs.query,
                confirmedMappings: toolArgs.confirmedMappings,
                previousResponse: toolArgs.previousResponse
              });
              
              const endTime = Date.now();
              console.log(`[SIMPLE-HTTP] tools/call handleAnalyzeData completed in ${endTime - startTime}ms`);
              
              return res.json({
                jsonrpc: '2.0',
                result: analyzeResult,
                id
              });
            } catch (error) {
              console.error('[SIMPLE-HTTP] Error in tools/call analyze_actions_data:', error);
              
              // Provide a fallback response
              return res.json({
                jsonrpc: '2.0',
                result: {
                  answer: `Error analyzing data: ${error.message}. Query was: ${toolArgs.query}`,
                  sql: "-- Error occurred, no SQL generated",
                  metadata: {
                    error: error.message,
                    query: toolArgs.query
                  }
                },
                id
              });
            }
          } else {
            // Fallback if MCP server is not initialized
            console.log('[SIMPLE-HTTP] MCP server not initialized, using fallback response for tools/call');
            return res.json({
              jsonrpc: '2.0',
              result: {
                answer: `Analysis for query: ${toolArgs.query} (MCP server not initialized)`,
                sql: "-- MCP server not initialized",
                metadata: {
                  query: toolArgs.query
                }
              },
              id
            });
          }
        } else {
          // Unsupported tool
          console.log(`[SIMPLE-HTTP] Unsupported tool: ${toolName}`);
          return res.status(400).json({
            jsonrpc: '2.0',
            error: { code: -32601, message: `Method not found: ${toolName}` },
            id
          });
        }
      }
        
      default: {
        // Simple response for other methods
        console.log(`[SIMPLE-HTTP] Unsupported method: ${method}`);
        return res.status(400).json({
          jsonrpc: '2.0',
          error: { code: -32601, message: `Method not found: ${method}` },
          id
        });
      }
    }
  } catch (error) {
    console.error(`[SIMPLE-HTTP] Error processing RPC request:`, error);
    return res.status(500).json({
      jsonrpc: '2.0',
      error: { code: -32603, message: 'Internal error', data: error.message },
      id
    });
  }
});

// Start the server
const port = process.env.PORT || 8080;
const server = app.listen(port, () => {
  console.log(`[SIMPLE-HTTP] Server running on port ${port}`);
});

// Handle graceful shutdown
process.on('SIGINT', () => {
  console.log('[SIMPLE-HTTP] Shutting down server');
  server.close(() => {
    console.log('[SIMPLE-HTTP] Server closed');
    process.exit(0);
  });
});

// Handle uncaught exceptions
process.on('uncaughtException', (error) => {
  console.error('[SIMPLE-HTTP] Uncaught exception:', error);
});

console.log('[SIMPLE-HTTP] Server initialization complete');
