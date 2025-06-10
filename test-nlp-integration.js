#!/usr/bin/env node

import fetch from 'node-fetch';

// Configuration
const SERVER_URL = 'https://qa-bitwavie-reporting-mcp-390118763134.us-central1.run.app/rpc';
const TEST_QUERIES = [
  "Show me Bitcoin transactions from last month",
  "What's the total balance of ETH across all wallets?",
  "How many BTC transactions occurred in January?",
  "Show me the top 5 assets by value"
];

async function sendJsonRpcRequest(method, params = {}) {
  const requestId = Date.now();
  const requestBody = {
    jsonrpc: '2.0',
    id: requestId,
    method: method,
    params: params
  };

  console.log(`\n📤 Sending request: ${method}`);
  if (Object.keys(params).length > 0) {
    console.log('   Parameters:', JSON.stringify(params, null, 2));
  }

  try {
    const response = await fetch(SERVER_URL, {
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
    return data;
  } catch (error) {
    console.error(`❌ Error: ${error.message}`);
    return null;
  }
}

async function testConnection() {
  console.log('\n🔍 Testing MCP server connection...');
  const response = await sendJsonRpcRequest('tools/call', {
    name: 'test_connection'
  });

  if (response && response.result) {
    console.log('✅ Connection test successful!');
    console.log(response.result.content[0].text);
    return true;
  } else {
    console.log('❌ Connection test failed!');
    return false;
  }
}

async function testNaturalLanguageQueries() {
  console.log('\n🔍 Testing natural language query processing...');
  
  for (const query of TEST_QUERIES) {
    console.log(`\n📝 Testing query: "${query}"`);
    
    const response = await sendJsonRpcRequest('tools/call', {
      name: 'analyze_actions_data',
      arguments: {
        query: query
      }
    });

    if (response && response.result) {
      console.log('✅ Query processed successfully!');
      console.log(response.result.content[0].text);
    } else {
      console.log('❌ Query processing failed!');
    }
  }
}

async function runTests() {
  console.log('🚀 Starting MCP NLP Integration Tests');
  console.log('====================================');
  
  // First test connection
  const connectionOk = await testConnection();
  if (!connectionOk) {
    console.log('❌ Aborting tests due to connection failure');
    return;
  }
  
  // Then test NLP queries
  await testNaturalLanguageQueries();
  
  console.log('\n====================================');
  console.log('🏁 MCP NLP Integration Tests Complete');
}

// Run the tests
runTests().catch(error => {
  console.error('❌ Unhandled error:', error);
  process.exit(1);
});
