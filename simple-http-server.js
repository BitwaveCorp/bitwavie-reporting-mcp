// Simple HTTP server for Cloud Run deployment testing
import express from 'express';

// Create an Express app
const app = express();
app.use(express.json());

// Log startup information
console.log('[SIMPLE-HTTP] Starting server...');
console.log(`[SIMPLE-HTTP] NODE_ENV: ${process.env.NODE_ENV}`);
console.log(`[SIMPLE-HTTP] PORT: ${process.env.PORT || 8080}`);

// Health check endpoint
app.get('/', (req, res) => {
  console.log('[SIMPLE-HTTP] Health check request received');
  res.status(200).send('MCP HTTP Server is running');
});

// Basic JSON-RPC endpoint
app.post('/rpc', (req, res) => {
  console.log('[SIMPLE-HTTP] RPC request received:', JSON.stringify(req.body));
  
  const { jsonrpc, method, params, id } = req.body;
  
  if (jsonrpc !== '2.0') {
    return res.status(400).json({
      jsonrpc: '2.0',
      error: { code: -32600, message: 'Invalid Request: jsonrpc version must be 2.0' },
      id
    });
  }
  
  // Simple response for all methods
  return res.json({
    jsonrpc: '2.0',
    result: `Received method: ${method}`,
    id
  });
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
