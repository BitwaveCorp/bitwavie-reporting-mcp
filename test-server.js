#!/usr/bin/env node

/**
 * Test script for the ReportingMCPServer
 * This script tests all major functionality of the server including:
 * - Connection testing
 * - Schema retrieval
 * - Basic query processing
 * - Enhanced NLQ processing
 * - Legacy NLQ processing
 * - Confirmation flow
 * - Error handling
 */

import fetch from 'node-fetch';
import chalk from 'chalk';

// Configuration
const SERVER_URL = 'http://localhost:3000';
const TEST_QUERIES = [
  {
    name: 'Simple Query',
    query: 'Show me total sales for last month',
    expectConfirmation: false
  },
  {
    name: 'Ambiguous Query',
    query: 'How is our performance?',
    expectConfirmation: true
  },
  {
    name: 'Complex Query',
    query: 'Compare revenue by product category for Q1 vs Q2',
    expectConfirmation: false
  },
  {
    name: 'Error-inducing Query',
    query: 'Show me data from nonexistent_table',
    expectError: true
  }
];

// Test tracking
let passedTests = 0;
let failedTests = 0;
let skippedTests = 0;

// Helper functions
function logSuccess(message) {
  console.log(chalk.green('✓ ' + message));
  passedTests++;
}

function logFailure(message, error) {
  console.log(chalk.red('✗ ' + message));
  if (error) {
    console.log(chalk.red('  Error: ' + (error.message || JSON.stringify(error))));
  }
  failedTests++;
}

function logSkipped(message) {
  console.log(chalk.yellow('⚠ ' + message));
  skippedTests++;
}

function logInfo(message) {
  console.log(chalk.blue('ℹ ' + message));
}

async function testConnection() {
  try {
    logInfo('Testing connection to server...');
    const response = await fetch(`${SERVER_URL}/api/testConnection`);
    const data = await response.json();
    
    if (data.success) {
      logSuccess('Connection test passed');
      if (data.schema) {
        logSuccess('Schema retrieved successfully');
        logInfo(`Schema contains ${Object.keys(data.schema).length} tables/views`);
      } else {
        logFailure('Schema not retrieved');
      }
    } else {
      logFailure('Connection test failed', { message: data.message });
    }
    return data.success;
  } catch (error) {
    logFailure('Connection test failed - server might not be running', error);
    return false;
  }
}

async function testQuery(queryInfo) {
  try {
    logInfo(`Testing query: "${queryInfo.name}"`);
    const response = await fetch(`${SERVER_URL}/api/analyzeData`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({ query: queryInfo.query })
    });
    
    const data = await response.json();
    
    // Check if we got a response
    if (!data) {
      logFailure(`No response for query: ${queryInfo.name}`);
      return null;
    }
    
    // Check for expected confirmation
    if (queryInfo.expectConfirmation) {
      if (data.needsConfirmation) {
        logSuccess(`Confirmation correctly requested for: ${queryInfo.name}`);
      } else {
        logFailure(`Expected confirmation but didn't get one for: ${queryInfo.name}`);
      }
    }
    
    // Check for expected error
    if (queryInfo.expectError) {
      if (data.error) {
        logSuccess(`Error correctly returned for: ${queryInfo.name}`);
      } else {
        logFailure(`Expected error but didn't get one for: ${queryInfo.name}`);
      }
    } else if (data.error) {
      logFailure(`Unexpected error for: ${queryInfo.name}`, { message: data.error });
    } else {
      logSuccess(`Query executed successfully: ${queryInfo.name}`);
    }
    
    // Check content
    if (data.content && Array.isArray(data.content) && data.content.length > 0) {
      logSuccess(`Response contains content for: ${queryInfo.name}`);
      
      // Check for table results
      const tableContent = data.content.find(item => item.type === 'table' && item.table);
      if (tableContent) {
        logSuccess(`Table results returned for: ${queryInfo.name}`);
        logInfo(`Table has ${tableContent.table.headers.length} columns and ${tableContent.table.rows.length} rows`);
      }
    }
    
    return data;
  } catch (error) {
    logFailure(`Query test failed for: ${queryInfo.name}`, error);
    return null;
  }
}

async function testConfirmationResponse(initialResponse) {
  if (!initialResponse || !initialResponse.needsConfirmation) {
    logSkipped('Skipping confirmation test - no confirmation needed');
    return;
  }
  
  try {
    logInfo('Testing confirmation response...');
    
    // Create a mock confirmation response
    // In a real scenario, the user would select from options
    const mockConfirmedMappings = { 
      "{{column}}": "revenue",
      "{{time_period}}": "last_quarter" 
    };
    
    const response = await fetch(`${SERVER_URL}/api/analyzeData`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        confirmedMappings: mockConfirmedMappings,
        previousResponse: initialResponse
      })
    });
    
    const data = await response.json();
    
    if (data.error) {
      logFailure('Confirmation response failed', { message: data.error });
    } else {
      logSuccess('Confirmation response processed successfully');
      
      if (data.content && Array.isArray(data.content) && data.content.length > 0) {
        logSuccess('Confirmation response contains content');
      }
    }
  } catch (error) {
    logFailure('Confirmation test failed', error);
  }
}

async function runTests() {
  console.log(chalk.bold('🧪 Starting ReportingMCPServer Tests 🧪'));
  console.log('='.repeat(50));
  
  // Test 1: Connection
  const connectionSuccess = await testConnection();
  if (!connectionSuccess) {
    console.log(chalk.red('\n❌ Connection failed - skipping remaining tests'));
    return;
  }
  
  console.log('\n' + '-'.repeat(50));
  
  // Test 2: Query Processing
  let confirmationResponse = null;
  for (const queryInfo of TEST_QUERIES) {
    const response = await testQuery(queryInfo);
    if (response && response.needsConfirmation && !confirmationResponse) {
      confirmationResponse = response;
    }
    console.log('-'.repeat(50));
  }
  
  // Test 3: Confirmation Flow
  if (confirmationResponse) {
    await testConfirmationResponse(confirmationResponse);
  }
  
  // Summary
  console.log('\n' + '='.repeat(50));
  console.log(chalk.bold('📊 Test Summary 📊'));
  console.log(chalk.green(`✓ Passed: ${passedTests}`));
  console.log(chalk.red(`✗ Failed: ${failedTests}`));
  console.log(chalk.yellow(`⚠ Skipped: ${skippedTests}`));
  console.log('='.repeat(50));
  
  if (failedTests === 0) {
    console.log(chalk.bold.green('\n🎉 All tests passed! 🎉'));
  } else {
    console.log(chalk.bold.red(`\n❌ ${failedTests} test(s) failed ❌`));
  }
}

// Run the tests
runTests().catch(error => {
  console.error('Test script error:', error);
  process.exit(1);
});
