import express from 'express';
import { ReportingMCPServer } from './server.js';

// Create an Express app
const app = express();
app.use(express.json());

// Create the MCP server
const mcpServer = new ReportingMCPServer();

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
    const response = await mcpServer.processRequest(request);
    
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
