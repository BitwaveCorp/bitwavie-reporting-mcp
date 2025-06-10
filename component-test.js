// Component test for the MCP server
import { fileURLToPath } from 'url';
import { dirname } from 'path';
import dotenv from 'dotenv';
import { BigQueryClient } from './reporting-mcp/src/services/bigquery-client.js';
import { QueryParser } from './reporting-mcp/src/services/query-parser.js';
import { InventoryBalanceGenerator } from './reporting-mcp/src/reports/inventory-balance.js';

// ES module equivalent of __dirname
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Load environment variables
dotenv.config();

async function runComponentTest() {
  console.log('Starting component test...');
  
  try {
    // Initialize the BigQuery client
    console.log('Initializing BigQuery client...');
    const bigQueryClient = new BigQueryClient();
    
    // Configure the BigQuery client with environment variables
    await bigQueryClient.configure({
      projectId: process.env.GOOGLE_CLOUD_PROJECT_ID,
      datasetId: process.env.BIGQUERY_DATASET_ID,
      tableId: process.env.BIGQUERY_TABLE_ID,
      keyFilename: process.env.GOOGLE_APPLICATION_CREDENTIALS
    });
    
    console.log('BigQuery client configured successfully.');
    
    // Initialize the query parser
    console.log('\nInitializing query parser...');
    const queryParser = new QueryParser();
    console.log('Query parser initialized.');
    
    // Initialize the inventory balance generator
    console.log('\nInitializing inventory balance generator...');
    const inventoryGen = new InventoryBalanceGenerator(bigQueryClient);
    console.log('Inventory balance generator initialized.');
    
    // Test the BigQuery connection
    console.log('\nTesting BigQuery connection...');
    const testQuery = 'SELECT 1 as test';
    const testResult = await bigQueryClient.executeQuery(testQuery);
    console.log('BigQuery connection test result:', testResult);
    
    // Generate a simple inventory balance report
    console.log('\nGenerating inventory balance report...');
    const reportParams = {
      runId: 'latest',
      asOfDate: '2025-06-10'
    };
    
    const reportData = await inventoryGen.generate(reportParams, ['asset', 'inventory']);
    console.log(`Report generated with ${reportData.length} records.`);
    
    // Display the first few records
    if (reportData.length > 0) {
      console.log('\nSample report data:');
      reportData.slice(0, 3).forEach((record, index) => {
        console.log(`Record ${index + 1}:`, JSON.stringify(record, null, 2));
      });
    }
    
    console.log('\nComponent test completed successfully.');
  } catch (error) {
    console.error('Error during component test:', error);
  }
}

// Install dependencies and run the component test
console.log('Installing required packages...');
import { spawn } from 'child_process';
spawn('npm', ['install', 'dotenv'], { 
  stdio: 'inherit' 
}).on('close', () => {
  runComponentTest().catch(console.error);
});
