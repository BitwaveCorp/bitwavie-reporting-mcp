import fetch from 'node-fetch';

// The URL of your deployed MCP server
const MCP_SERVER_URL = 'https://qa-bitwavie-reporting-mcp-390118763134.us-central1.run.app/rpc';

// Function to make a JSON-RPC request
async function makeRpcRequest(method, params = {}) {
  const requestId = Math.floor(Math.random() * 10000);
  
  const requestBody = {
    jsonrpc: '2.0',
    id: requestId,
    method,
    params
  };
  
  console.log(`Sending request to ${MCP_SERVER_URL}:`);
  console.log(JSON.stringify(requestBody, null, 2));
  
  try {
    const response = await fetch(MCP_SERVER_URL, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(requestBody)
    });
    
    if (!response.ok) {
      throw new Error(`HTTP error! Status: ${response.status}`);
    }
    
    const data = await response.json();
    console.log('Response:');
    console.log(JSON.stringify(data, null, 2));
    return data;
  } catch (error) {
    console.error('Error making RPC request:', error);
    throw error;
  }
}

// Test the tools/list method
async function testToolsList() {
  console.log('Testing tools/list method...');
  try {
    await makeRpcRequest('tools/list');
    console.log('tools/list test completed successfully!');
  } catch (error) {
    console.error('tools/list test failed:', error);
  }
}

// Test the test_connection tool
async function testConnection() {
  console.log('\nTesting test_connection tool...');
  try {
    await makeRpcRequest('tools/call', {
      name: 'test_connection',
      arguments: {}
    });
    console.log('test_connection test completed!');
  } catch (error) {
    console.error('test_connection test failed:', error);
  }
}

// Test the analyze_actions_data tool
async function testAnalyzeData() {
  console.log('\nTesting analyze_actions_data tool...');
  try {
    await makeRpcRequest('tools/call', {
      name: 'analyze_actions_data',
      arguments: {
        query: 'Show me the top 10 actions by value'
      }
    });
    console.log('analyze_actions_data test completed!');
  } catch (error) {
    console.error('analyze_actions_data test failed:', error);
  }
}

// Run the tests
async function runTests() {
  await testToolsList();
  await testConnection();
  await testAnalyzeData();
}

runTests();
