import express from 'express';
import { ReportingMCPServer } from './server.js';

// Create an Express app
const app = express();
app.use(express.json());

// Create the MCP server with configuration
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

// Health check endpoint
app.get('/', (req, res) => {
  res.status(200).send('MCP Server is running');
});

// JSON-RPC endpoint
app.post('/rpc', async (req, res) => {
  try {
    // Get the JSON-RPC request
    const request = req.body;
    
    // Process the request using the MCP server
    // We'll use the internal processRequest method
    const response = await mcpServer.handleAnalyzeData(request);
    
    // Send the response
    res.json(response);
  } catch (error) {
    console.error('Error processing RPC request:', error);
    res.status(500).json({
      jsonrpc: '2.0',
      error: {
        code: -32603,
        message: 'Internal error'
      },
      id: req.body?.id || null
    });
  }
});

// Start the server
const port = process.env.PORT || 8080;
app.listen(port, () => {
  console.log(`MCP HTTP Server running on port ${port}`);
});
