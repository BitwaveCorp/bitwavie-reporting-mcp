#!/usr/bin/env node

import { spawn } from 'child_process';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';
import { setTimeout as sleep } from 'timers/promises';
import readline from 'readline';
import fs from 'fs';
import dotenv from 'dotenv';

// Load environment variables from .env file
dotenv.config();

// ES module equivalent of __dirname
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Colors for console output
const colors = {
  reset: '\x1b[0m',
  bright: '\x1b[1m',
  dim: '\x1b[2m',
  red: '\x1b[31m',
  green: '\x1b[32m',
  yellow: '\x1b[33m',
  blue: '\x1b[34m',
  cyan: '\x1b[36m'
};

// Sample natural language queries to test
const TEST_QUERIES = [
  "Show me Bitcoin transactions from last month",
  "What's the total balance of ETH across all wallets?",
  "How many BTC transactions occurred in January?",
  "Show me the top 5 assets by value"
];

// Start the MCP server process
async function startServer() {
  console.log(`${colors.cyan}Starting MCP server...${colors.reset}`);
  
  // Start the server process
  const serverProcess = spawn('node', ['--enable-source-maps', join(__dirname, 'dist', 'server.js')], {
    cwd: __dirname,
    env: {
      ...process.env,
      NODE_ENV: 'development',
      DEBUG: 'mcp:*'
    }
  });
  
  // Handle server output
  serverProcess.stdout.on('data', (data) => {
    // Server output is for JSON-RPC communication, we don't print it
  });
  
  serverProcess.stderr.on('data', (data) => {
    const output = data.toString().trim();
    if (output.includes('MCP Server running')) {
      console.log(`${colors.green}✓ MCP server started${colors.reset}`);
    } else if (!output.includes('jsonrpc')) {
      console.log(`${colors.dim}[Server] ${output}${colors.reset}`);
    }
  });
  
  // Handle server exit
  serverProcess.on('close', (code) => {
    if (code !== null) {
      console.log(`${colors.yellow}Server process exited with code ${code}${colors.reset}`);
    }
  });
  
  // Wait for server to start
  await sleep(2000);
  return serverProcess;
}

// Send a JSON-RPC request to the server and get the response
async function sendRequest(serverProcess, request) {
  return new Promise((resolve, reject) => {
    // Write the request to the server's stdin
    serverProcess.stdin.write(JSON.stringify(request) + '\n');
    
    // Set up a listener for the response
    const onData = (data) => {
      try {
        const response = JSON.parse(data.toString());
        if (response.id === request.id) {
          serverProcess.stdout.removeListener('data', onData);
          resolve(response);
        }
      } catch (error) {
        // Not JSON or not our response, ignore
      }
    };
    
    // Listen for responses
    serverProcess.stdout.on('data', onData);
    
    // Set a timeout
    setTimeout(() => {
      serverProcess.stdout.removeListener('data', onData);
      reject(new Error('Request timed out'));
    }, 10000);
  });
}

// Test the connection to BigQuery
async function testConnection(serverProcess) {
  console.log(`\n${colors.cyan}Testing connection to BigQuery...${colors.reset}`);
  
  const request = {
    jsonrpc: '2.0',
    id: 'conn-test-' + Date.now(),
    method: 'tools/call',
    params: {
      name: 'test_connection'
    }
  };
  
  try {
    const response = await sendRequest(serverProcess, request);
    
    if (response.result && response.result.content) {
      const content = response.result.content[0].text;
      console.log(`${colors.green}✓ Connection test successful${colors.reset}`);
      console.log(content);
      return true;
    } else {
      console.log(`${colors.red}✗ Connection test failed${colors.reset}`);
      console.log(response);
      return false;
    }
  } catch (error) {
    console.log(`${colors.red}✗ Connection test error: ${error.message}${colors.reset}`);
    return false;
  }
}

// Test natural language query processing
async function testNaturalLanguageQuery(serverProcess, query) {
  console.log(`\n${colors.cyan}Testing query: "${query}"${colors.reset}`);
  
  const request = {
    jsonrpc: '2.0',
    id: 'nlp-test-' + Date.now(),
    method: 'tools/call',
    params: {
      name: 'analyze_actions_data',
      arguments: {
        query: query
      }
    }
  };
  
  try {
    const response = await sendRequest(serverProcess, request);
    
    if (response.result && response.result.content) {
      const content = response.result.content[0].text;
      console.log(`${colors.green}✓ Query processed successfully${colors.reset}`);
      console.log(content);
      return true;
    } else {
      console.log(`${colors.red}✗ Query processing failed${colors.reset}`);
      console.log(response);
      return false;
    }
  } catch (error) {
    console.log(`${colors.red}✗ Query processing error: ${error.message}${colors.reset}`);
    return false;
  }
}

// Interactive mode for testing custom queries
async function interactiveMode(serverProcess) {
  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
  });
  
  console.log(`\n${colors.bright}${colors.cyan}=== Interactive Mode ====${colors.reset}`);
  console.log(`Enter natural language queries or type 'exit' to quit.\n`);
  
  const askQuestion = () => {
    rl.question(`${colors.yellow}Enter query: ${colors.reset}`, async (query) => {
      if (query.toLowerCase() === 'exit') {
        rl.close();
        await cleanupAndExit(serverProcess);
        return;
      }
      
      await testNaturalLanguageQuery(serverProcess, query);
      askQuestion();
    });
  };
  
  askQuestion();
}

// Clean up and exit
async function cleanupAndExit(serverProcess) {
  console.log(`\n${colors.cyan}Shutting down server...${colors.reset}`);
  serverProcess.kill();
  await sleep(1000);
  console.log(`${colors.green}Done.${colors.reset}`);
}

// Run all tests
async function runTests() {
  console.log(`${colors.bright}${colors.cyan}=== MCP Server NLP Integration Test ====${colors.reset}\n`);
  
  // Start the server
  const serverProcess = await startServer();
  
  // Test connection first
  const connectionOk = await testConnection(serverProcess);
  if (!connectionOk) {
    console.log(`${colors.red}Connection test failed, aborting further tests.${colors.reset}`);
    await cleanupAndExit(serverProcess);
    return;
  }
  
  // Run the predefined test queries
  console.log(`\n${colors.cyan}Running predefined test queries...${colors.reset}`);
  for (const query of TEST_QUERIES) {
    await testNaturalLanguageQuery(serverProcess, query);
  }
  
  // Switch to interactive mode
  await interactiveMode(serverProcess);
}

// Handle Ctrl+C
process.on('SIGINT', async () => {
  console.log(`\n${colors.yellow}Interrupted by user${colors.reset}`);
  process.exit(0);
});

// Run the tests
runTests().catch(error => {
  console.error(`${colors.red}Fatal error:${colors.reset}`, error);
  process.exit(1);
});
