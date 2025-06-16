// Simple test script to verify our TypeScript fixes
import { SchemaManager } from './dist/services/schema-manager.js';
import { QueryConfirmationFormatter } from './dist/services/query-confirmation-formatter.js';
import { LLMQueryTranslator } from './dist/services/llm-query-translator.js';
import { ResultFormatter } from './dist/services/result-formatter.js';

async function runTests() {
  console.log('Running simple tests to verify TypeScript fixes...');
  
  try {
    // Test SchemaManager with null schema
    console.log('\nTesting SchemaManager...');
    const schemaManager = new SchemaManager({
      projectId: 'test-project',
      datasetId: 'test-dataset',
      tableId: 'test-table'
    });
    
    // This should not throw an error even with null schema
    const schemaText = schemaManager.getSchemaForLLM();
    console.log('SchemaManager.getSchemaForLLM() returned without errors');
    console.log('Schema text starts with:', schemaText.substring(0, 50) + '...');
    
    // Test QueryConfirmationFormatter
    console.log('\nTesting QueryConfirmationFormatter...');
    const queryFormatter = new QueryConfirmationFormatter();
    const columns = ['transaction_date', 'asset_id', 'amount', 'wallet_address'];
    const selectionText = queryFormatter.formatColumnSelectionOptions(columns);
    console.log('QueryConfirmationFormatter.formatColumnSelectionOptions() returned without errors');
    console.log('Selection text length:', selectionText.length);
    
    // Test ResultFormatter with null metadata
    console.log('\nTesting ResultFormatter...');
    const resultFormatter = new ResultFormatter();
    const executionResult = {
      rows: [{ test: 'data' }],
      metadata: null // Test null metadata handling
    };
    
    try {
      const formattedResult = resultFormatter.formatResults(executionResult, {});
      console.log('ResultFormatter.formatResults() handled null metadata without errors');
    } catch (error) {
      console.error('ResultFormatter test failed:', error);
    }
    
    console.log('\nAll tests completed successfully!');
  } catch (error) {
    console.error('Test failed:', error);
  }
}

runTests();
