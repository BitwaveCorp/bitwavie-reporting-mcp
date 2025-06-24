// Simple HTTP server for Cloud Run deployment testing with MCP server integration
import express from 'express';
import cors from 'cors';
import { ReportingMCPServer } from './dist/server.js';

// Create an Express app
const app = express();
app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Session storage for connection details
// Session storage for connection details
const sessionStorage = {
  connectionDetails: null,
  isConnected: false
};

// Function to get session details that can be used by ConnectionManager
global.getSessionConnectionDetails = () => sessionStorage.connectionDetails;

// Function to get the current connection details (from session or environment variables)
function getCurrentConnectionDetails() {
  // Use session storage if available
  if (sessionStorage.isConnected && sessionStorage.connectionDetails) {
    const sessionDetails = {
      projectId: sessionStorage.connectionDetails.projectId,
      datasetId: sessionStorage.connectionDetails.datasetId,
      tableId: sessionStorage.connectionDetails.tableId,
      privateKey: sessionStorage.connectionDetails.privateKey ? '[REDACTED]' : undefined
    };
    
    console.log('[SIMPLE-HTTP] Using session connection details for query:', {
      projectId: sessionDetails.projectId,
      datasetId: sessionDetails.datasetId,
      tableId: sessionDetails.tableId,
      hasPrivateKey: !!sessionDetails.privateKey
    });
    
    return {
      projectId: sessionStorage.connectionDetails.projectId,
      datasetId: sessionStorage.connectionDetails.datasetId,
      tableId: sessionStorage.connectionDetails.tableId,
      privateKey: sessionStorage.connectionDetails.privateKey
    };
  }
  
  // Fall back to environment variables
  const envDetails = {
    projectId: process.env.GOOGLE_CLOUD_PROJECT_ID,
    datasetId: process.env.BIGQUERY_DATASET_ID,
    tableId: process.env.BIGQUERY_TABLE_ID
  };
  
  console.log('[SIMPLE-HTTP] Using environment variables for query:', envDetails);
  
  return envDetails;
}

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
              { name: 'test_connection', description: 'Test connection to MCP server' },
              { name: 'connection/validate-table-access', description: 'Validate BigQuery table access' },
              { name: 'connection/status', description: 'Get current connection status' },
              { name: 'connection/clear', description: 'Clear current connection' }
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
            
            // Get current connection details (from session or environment variables)
            const connectionDetails = getCurrentConnectionDetails();
            
            // Add connection details to the request
            const analyzeResult = await mcpServer.handleAnalyzeData({
              query: requestData.query,
              confirmedMappings: requestData.confirmedMappings,
              previousResponse: requestData.previousResponse,
              connectionDetails: connectionDetails
            });
            
            console.log('[SIMPLE-HTTP] Using connection details for query:', JSON.stringify({
              projectId: connectionDetails.projectId,
              datasetId: connectionDetails.datasetId,
              tableId: connectionDetails.tableId,
              hasPrivateKey: !!connectionDetails.privateKey
            }));
            
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
      
      case 'connection/validate-table-access': {
        console.log('[SIMPLE-HTTP] Processing connection/validate-table-access request');
        console.log('[SIMPLE-HTTP] Request params:', JSON.stringify(params));
        
        // Check if params is an array (as expected from frontend)
        const requestData = Array.isArray(params) ? params[0] : params;
        console.log('[SIMPLE-HTTP] connection/validate-table-access request data:', JSON.stringify(requestData));
        
        if (mcpServer) {
          try {
            console.log('[SIMPLE-HTTP] Calling mcpServer.validateConnection...');
            
            // Extract connection parameters from the request
            const connectionRequest = {
              projectId: requestData.projectId,
              datasetId: requestData.datasetId,
              tableId: requestData.tableId,
              privateKey: requestData.privateKey
            };
            
            // Pass the private key directly for validation
            // The validateTableMapping function will check if it matches what's in the mappings file
            console.log('[SIMPLE-HTTP] Using provided private key for validation');
            
            // No need to modify the private key - pass it as is
            // The validateTableMapping function will handle the comparison
            
            // Call the validateConnection method from the MCP server
            const validationResult = await mcpServer.validateConnection(connectionRequest);
            
            console.log('[SIMPLE-HTTP] validateConnection result:', JSON.stringify(validationResult));
            
            // Store connection details in session storage if validation was successful
            if (validationResult.success) {
              sessionStorage.isConnected = true;
              sessionStorage.connectionDetails = {
                projectId: connectionRequest.projectId,
                datasetId: connectionRequest.datasetId,
                tableId: connectionRequest.tableId,
                privateKey: connectionRequest.privateKey
              };
              console.log('[SIMPLE-HTTP] Connection details stored in session storage');
            } else {
              // Clear session storage if validation failed
              sessionStorage.isConnected = false;
              sessionStorage.connectionDetails = null;
              console.log('[SIMPLE-HTTP] Connection validation failed, session storage cleared');
            }
            
            return res.json({
              jsonrpc: '2.0',
              result: validationResult,
              id
            });
          } catch (error) {
            console.error('[SIMPLE-HTTP] Error in connection/validate-table-access:', error);
            return res.status(500).json({
              jsonrpc: '2.0',
              error: {
                code: -32603,
                message: error instanceof Error ? error.message : 'Error validating connection',
                data: error instanceof Error ? error.stack : undefined
              },
              id
            });
          }
        } else {
          // Fallback if MCP server is not initialized
          console.log('[SIMPLE-HTTP] MCP server not initialized, returning error');
          return res.json({
            jsonrpc: '2.0',
            error: {
              code: -32603,
              message: 'MCP server not initialized, cannot validate connection'
            },
            id
          });
        }
      }
      
      case 'connection/status': {
        console.log('[SIMPLE-HTTP] Processing connection/status request');
        
        // Use session storage for connection status if available
        // Otherwise fall back to environment variables
        let isConnected = sessionStorage.isConnected;
        let connectionDetails = sessionStorage.connectionDetails;
        
        // If no session connection, check if we have environment variables as fallback
        if (!isConnected) {
          const hasEnvVars = !!(process.env.GOOGLE_CLOUD_PROJECT_ID && 
                             process.env.BIGQUERY_DATASET_ID && 
                             process.env.BIGQUERY_TABLE_ID);
          
          // Only use environment variables if we don't have a session connection
          if (hasEnvVars) {
            isConnected = true;
            connectionDetails = {
              projectId: process.env.GOOGLE_CLOUD_PROJECT_ID,
              datasetId: process.env.BIGQUERY_DATASET_ID,
              tableId: process.env.BIGQUERY_TABLE_ID
            };
          }
        }
        
        console.log(`[SIMPLE-HTTP] Connection status: ${isConnected ? 'Connected' : 'Not connected'}`);
        if (isConnected && connectionDetails) {
          console.log(`[SIMPLE-HTTP] Using ${sessionStorage.isConnected ? 'session' : 'environment'} connection details:`, 
            JSON.stringify({
              projectId: connectionDetails.projectId,
              datasetId: connectionDetails.datasetId,
              tableId: connectionDetails.tableId,
              hasPrivateKey: !!connectionDetails.privateKey
            }));
        }
        
        return res.json({
          jsonrpc: '2.0',
          result: {
            success: true,
            isConnected,
            connectionDetails: isConnected ? {
              projectId: connectionDetails.projectId,
              datasetId: connectionDetails.datasetId,
              tableId: connectionDetails.tableId
            } : null
          },
          id
        });
      }
      
      case 'connection/clear': {
        console.log('[SIMPLE-HTTP] Processing connection/clear request');
        
        // Clear session storage
        sessionStorage.isConnected = false;
        sessionStorage.connectionDetails = null;
        
        console.log('[SIMPLE-HTTP] Connection cleared from session storage');
        
        return res.json({
          jsonrpc: '2.0',
          result: {
            success: true,
            message: 'Connection cleared successfully'
          },
          id
        });
      }
      
      case 'connection/session-details': {
        console.log('[SIMPLE-HTTP] Processing connection/session-details request');
        
        // Get connection details from session storage
        const connectionDetails = sessionStorage.connectionDetails;
        
        // Initialize response object
        const sessionDetails = {
          isConnected: sessionStorage.isConnected,
          connectionDetails: connectionDetails ? {
            projectId: connectionDetails.projectId || '',
            datasetId: connectionDetails.datasetId || '',
            tableId: connectionDetails.tableId || '',
            hasPrivateKey: !!connectionDetails.privateKey
          } : null,
          timestamp: new Date().toISOString()
        };
        
        // Use ConnectionManager to get fully qualified table ID if we have connection details
        if (mcpServer && connectionDetails) {
          try {
            // Get ConnectionManager instance from MCP server
            const connectionManager = mcpServer.getConnectionManager();
            
            if (connectionManager) {
              // Log connection details (safely - without private key)
              const safeDetails = connectionManager.logConnectionDetails(connectionDetails);
              
              // Get fully qualified table ID
              const fullyQualifiedTableId = connectionManager.getFullyQualifiedTableId(connectionDetails);
              
              // Add to response
              sessionDetails.fullyQualifiedTableId = fullyQualifiedTableId;
              sessionDetails.connectionSource = connectionDetails && 
                (connectionDetails.projectId || connectionDetails.datasetId || connectionDetails.tableId) ? 
                'session' : 'environment';
              
              console.log('[SIMPLE-HTTP] Got fully qualified table ID:', fullyQualifiedTableId);
            } else {
              console.log('[SIMPLE-HTTP] ConnectionManager not available');
              sessionDetails.error = 'ConnectionManager not available';
            }
          } catch (error) {
            console.error('[SIMPLE-HTTP] Error getting fully qualified table ID:', error);
            sessionDetails.error = `Error getting fully qualified table ID: ${error.message}`;
          }
        } else if (!connectionDetails) {
          sessionDetails.error = 'No connection details available';
        }
        
        console.log('[SIMPLE-HTTP] Returning session details:', JSON.stringify(sessionDetails));
        
        return res.json({
          jsonrpc: '2.0',
          result: sessionDetails,
          id
        });
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
