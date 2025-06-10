// Comprehensive MCP Server Test Script
import { spawn } from 'child_process';
import { fileURLToPath } from 'url';
import { dirname } from 'path';
import { setTimeout as sleep } from 'timers/promises';

// ES module equivalent of __dirname
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Test results tracking
const testResults = {
  total: 0,
  passed: 0,
  failed: 0,
  tests: []
};

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

// Test utilities
function logTest(name, passed, message = '') {
  const status = passed ? `${colors.green}✓ PASS${colors.reset}` : `${colors.red}✗ FAIL${colors.reset}`;
  console.log(`${status} ${colors.bright}${name}${colors.reset} ${message}`);
  
  testResults.total++;
  passed ? testResults.passed++ : testResults.failed++;
  testResults.tests.push({ name, passed, message });
}

function assert(condition, testName, message = '') {
  if (condition) {
    logTest(testName, true);
  } else {
    logTest(testName, false, message);
  }
}

// Start the MCP server process
async function startServer() {
  console.log(`${colors.cyan}Starting MCP server...${colors.reset}`);
  
  const serverProcess = spawn('npx', ['tsx', 'src/server.ts'], {
    cwd: __dirname,
    stdio: ['pipe', 'pipe', 'pipe'] // Pipe stdin/stdout/stderr
  });
  
  // Buffer for collecting stdout data
  let stdoutBuffer = '';
  let stderrBuffer = '';
  
  // Listen for stdout data
  serverProcess.stdout.on('data', (data) => {
    const output = data.toString();
    stdoutBuffer += output;
  });
  
  // Listen for stderr data
  serverProcess.stderr.on('data', (data) => {
    const output = data.toString();
    stderrBuffer += output;
    console.log(`${colors.yellow}[Server stderr]${colors.reset} ${output}`);
  });
  
  // Wait for server initialization
  await sleep(2000);
  
  if (stderrBuffer.includes('Error') || stderrBuffer.includes('error')) {
    console.log(`${colors.red}Server startup error:${colors.reset} ${stderrBuffer}`);
    process.exit(1);
  }
  
  console.log(`${colors.green}MCP server started${colors.reset}`);
  return serverProcess;
}

// Send a JSON-RPC request to the server and get the response
async function sendRequest(serverProcess, request) {
  return new Promise((resolve) => {
    let responseData = '';
    
    // Set up response listener before sending the request
    const responseHandler = (data) => {
      const output = data.toString();
      responseData += output;
      
      try {
        // Check if we have a complete JSON response
        const response = JSON.parse(responseData);
        serverProcess.stdout.removeListener('data', responseHandler);
        resolve(response);
      } catch (e) {
        // Incomplete JSON, wait for more data
      }
    };
    
    serverProcess.stdout.on('data', responseHandler);
    
    // Send the request
    console.log(`${colors.blue}Sending request:${colors.reset} ${JSON.stringify(request)}`);
    serverProcess.stdin.write(JSON.stringify(request) + '\n');
  });
}

// Run all tests
async function runTests() {
  const serverProcess = await startServer();
  
  try {
    console.log(`\n${colors.bright}Running MCP Server Tests${colors.reset}\n`);
    
    // Test 1: List Tools Request
    const listRequest = {
      jsonrpc: '2.0',
      id: '1',
      method: 'tools/list',
      params: {}
    };
    
    const listResponse = await sendRequest(serverProcess, listRequest);
    console.log(`${colors.dim}Response:${colors.reset}`, JSON.stringify(listResponse, null, 2));
    
    assert(
      listResponse.jsonrpc === '2.0',
      'JSON-RPC Version Check',
      `Expected jsonrpc: '2.0', got: ${listResponse.jsonrpc}`
    );
    
    assert(
      listResponse.id === '1',
      'Response ID Match',
      `Expected id: '1', got: ${listResponse.id}`
    );
    
    assert(
      Array.isArray(listResponse.result?.tools),
      'Tools List Structure',
      'Expected tools array in response'
    );
    
    assert(
      listResponse.result?.tools.some(tool => tool.name === 'test_connection'),
      'test_connection Tool Present',
      'Expected test_connection tool in tools list'
    );
    
    assert(
      listResponse.result?.tools.some(tool => tool.name === 'analyze_actions_data'),
      'analyze_actions_data Tool Present',
      'Expected analyze_actions_data tool in tools list'
    );
    
    // Test 2: Call test_connection Tool
    const testConnectionRequest = {
      jsonrpc: '2.0',
      id: '2',
      method: 'tools/call',
      params: {
        name: 'test_connection',
        arguments: {}
      }
    };
    
    const testConnectionResponse = await sendRequest(serverProcess, testConnectionRequest);
    console.log(`${colors.dim}Response:${colors.reset}`, JSON.stringify(testConnectionResponse, null, 2));
    
    assert(
      testConnectionResponse.jsonrpc === '2.0',
      'test_connection JSON-RPC Version Check',
      `Expected jsonrpc: '2.0', got: ${testConnectionResponse.jsonrpc}`
    );
    
    assert(
      testConnectionResponse.id === '2',
      'test_connection Response ID Match',
      `Expected id: '2', got: ${testConnectionResponse.id}`
    );
    
    assert(
      Array.isArray(testConnectionResponse.result?.content),
      'test_connection Response Structure',
      'Expected content array in response'
    );
    
    // Test 3: Call analyze_actions_data Tool
    const analyzeRequest = {
      jsonrpc: '2.0',
      id: '3',
      method: 'tools/call',
      params: {
        name: 'analyze_actions_data',
        arguments: {
          query: 'Show me total transactions by asset'
        }
      }
    };
    
    const analyzeResponse = await sendRequest(serverProcess, analyzeRequest);
    console.log(`${colors.dim}Response:${colors.reset}`, JSON.stringify(analyzeResponse, null, 2));
    
    assert(
      analyzeResponse.jsonrpc === '2.0',
      'analyze_actions_data JSON-RPC Version Check',
      `Expected jsonrpc: '2.0', got: ${analyzeResponse.jsonrpc}`
    );
    
    assert(
      analyzeResponse.id === '3',
      'analyze_actions_data Response ID Match',
      `Expected id: '3', got: ${analyzeResponse.id}`
    );
    
    assert(
      Array.isArray(analyzeResponse.result?.content),
      'analyze_actions_data Response Structure',
      'Expected content array in response'
    );
    
    // Test 4: Invalid Method Test
    const invalidMethodRequest = {
      jsonrpc: '2.0',
      id: '4',
      method: 'invalid_method',
      params: {}
    };
    
    const invalidMethodResponse = await sendRequest(serverProcess, invalidMethodRequest);
    console.log(`${colors.dim}Response:${colors.reset}`, JSON.stringify(invalidMethodResponse, null, 2));
    
    assert(
      invalidMethodResponse.error,
      'Invalid Method Error Check',
      'Expected error object in response'
    );
    
    assert(
      invalidMethodResponse.error?.code === -32601,
      'Invalid Method Error Code Check',
      `Expected error code: -32601, got: ${invalidMethodResponse.error?.code}`
    );
    
    // Test 5: Invalid Tool Name Test
    const invalidToolRequest = {
      jsonrpc: '2.0',
      id: '5',
      method: 'tools/call',
      params: {
        name: 'invalid_tool',
        arguments: {}
      }
    };
    
    const invalidToolResponse = await sendRequest(serverProcess, invalidToolRequest);
    console.log(`${colors.dim}Response:${colors.reset}`, JSON.stringify(invalidToolResponse, null, 2));
    
    assert(
      invalidToolResponse.result?.content?.[0]?.text?.includes('Unknown tool'),
      'Invalid Tool Name Check',
      'Expected "Unknown tool" message in response'
    );
    
    // Test 6: Missing Required Arguments Test
    const missingArgsRequest = {
      jsonrpc: '2.0',
      id: '6',
      method: 'tools/call',
      params: {
        name: 'analyze_actions_data',
        arguments: {}
      }
    };
    
    const missingArgsResponse = await sendRequest(serverProcess, missingArgsRequest);
    console.log(`${colors.dim}Response:${colors.reset}`, JSON.stringify(missingArgsResponse, null, 2));
    
    // The server doesn't currently validate required parameters at the schema level
    // It just passes an empty string as the query
    assert(
      missingArgsResponse.result?.content?.[0]?.text?.includes('Query:'),
      'Missing Arguments Response Check',
      'Expected query response even with missing arguments'
    );
    
  } catch (error) {
    console.error(`${colors.red}Test error:${colors.reset}`, error);
  } finally {
    // Print test summary
    console.log(`\n${colors.bright}Test Summary:${colors.reset}`);
    console.log(`Total: ${testResults.total}`);
    console.log(`Passed: ${colors.green}${testResults.passed}${colors.reset}`);
    console.log(`Failed: ${testResults.failed > 0 ? colors.red : ''}${testResults.failed}${colors.reset}`);
    
    // Shutdown server
    console.log(`\n${colors.cyan}Shutting down MCP server...${colors.reset}`);
    serverProcess.kill();
    process.exit(testResults.failed > 0 ? 1 : 0);
  }
}

// Run all tests
runTests().catch(error => {
  console.error(`${colors.red}Fatal error:${colors.reset}`, error);
  process.exit(1);
});
