// Comprehensive MCP Server Cloud Run Test Script
import fetch from 'node-fetch';
import { setTimeout as sleep } from 'timers/promises';

// Cloud Run URL
const CLOUD_RUN_URL = 'https://qa-bitwavie-reporting-mcp-390118763134.us-central1.run.app/rpc';

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

// Send a JSON-RPC request to the server and get the response
async function sendRequest(request) {
  console.log(`${colors.blue}Sending request to ${CLOUD_RUN_URL}:${colors.reset}`);
  console.log(JSON.stringify(request, null, 2));
  
  try {
    const response = await fetch(CLOUD_RUN_URL, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(request)
    });
    
    if (!response.ok) {
      throw new Error(`HTTP error! Status: ${response.status}`);
    }
    
    const data = await response.json();
    return data;
  } catch (error) {
    console.error(`${colors.red}Error making RPC request:${colors.reset}`, error);
    throw error;
  }
}

// Run all tests
async function runTests() {
  try {
    console.log(`\n${colors.bright}Running MCP Server Tests on Cloud Run${colors.reset}\n`);
    console.log(`${colors.cyan}Testing server at:${colors.reset} ${CLOUD_RUN_URL}\n`);
    
    // Test 1: List Tools Request
    const listRequest = {
      jsonrpc: '2.0',
      id: '1',
      method: 'tools/list',
      params: {}
    };
    
    const listResponse = await sendRequest(listRequest);
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
    
    const testConnectionResponse = await sendRequest(testConnectionRequest);
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
    
    // Check if the connection was successful
    const connectionText = testConnectionResponse.result?.content[0]?.text || '';
    assert(
      connectionText.includes('Connection Successful'),
      'BigQuery Connection Success',
      'Expected successful BigQuery connection'
    );
    
    // Test 3: Call analyze_actions_data Tool
    const analyzeRequest = {
      jsonrpc: '2.0',
      id: '3',
      method: 'tools/call',
      params: {
        name: 'analyze_actions_data',
        arguments: {
          query: 'Show me the top 10 actions by value'
        }
      }
    };
    
    const analyzeResponse = await sendRequest(analyzeRequest);
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
    
    // Check if the query was received
    const analyzeText = analyzeResponse.result?.content[0]?.text || '';
    assert(
      analyzeText.includes('Query Received'),
      'Query Received Check',
      'Expected query received confirmation'
    );
    
    // Test 4: Error Handling - Invalid Method
    const invalidMethodRequest = {
      jsonrpc: '2.0',
      id: '4',
      method: 'invalid_method',
      params: {}
    };
    
    const invalidMethodResponse = await sendRequest(invalidMethodRequest);
    console.log(`${colors.dim}Response:${colors.reset}`, JSON.stringify(invalidMethodResponse, null, 2));
    
    assert(
      invalidMethodResponse.error?.code === -32601,
      'Invalid Method Error Code',
      `Expected error code -32601, got: ${invalidMethodResponse.error?.code}`
    );
    
    assert(
      invalidMethodResponse.error?.message === 'Method not found',
      'Invalid Method Error Message',
      `Expected 'Method not found', got: ${invalidMethodResponse.error?.message}`
    );
    
    // Test 5: Error Handling - Invalid Tool
    const invalidToolRequest = {
      jsonrpc: '2.0',
      id: '5',
      method: 'tools/call',
      params: {
        name: 'invalid_tool',
        arguments: {}
      }
    };
    
    const invalidToolResponse = await sendRequest(invalidToolRequest);
    console.log(`${colors.dim}Response:${colors.reset}`, JSON.stringify(invalidToolResponse, null, 2));
    
    // Print test summary
    console.log(`\n${colors.bright}Test Summary:${colors.reset}`);
    console.log(`${colors.green}Passed:${colors.reset} ${testResults.passed}/${testResults.total}`);
    
    if (testResults.failed > 0) {
      console.log(`${colors.red}Failed:${colors.reset} ${testResults.failed}/${testResults.total}`);
      
      console.log(`\n${colors.red}Failed Tests:${colors.reset}`);
      testResults.tests
        .filter(test => !test.passed)
        .forEach(test => {
          console.log(`${colors.red}✗${colors.reset} ${test.name}: ${test.message}`);
        });
    } else {
      console.log(`${colors.green}All tests passed!${colors.reset}`);
    }
  } catch (error) {
    console.error(`${colors.red}Fatal error:${colors.reset}`, error);
    process.exit(1);
  }
}

// Run all tests
runTests();
