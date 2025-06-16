#!/usr/bin/env node

/**
 * Script to identify and fix critical TypeScript errors
 * This focuses on the most important errors that prevent the server from building
 */

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { execSync } from 'child_process';

// Get __dirname equivalent in ES modules
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Main function
async function main() {
  console.log('🔍 Identifying critical TypeScript errors...');
  
  // Fix the http-server.ts issue
  fixHttpServer();
  
  // Fix the server.ts constructor issue
  fixServerConstructor();
  
  console.log('✅ Critical fixes applied. Try building again with npm run build');
}

function fixHttpServer() {
  const httpServerPath = path.join(__dirname, 'src', 'http-server.ts');
  
  if (!fs.existsSync(httpServerPath)) {
    console.log('⚠️ http-server.ts not found, skipping');
    return;
  }
  
  console.log('🔧 Fixing http-server.ts...');
  
  let content = fs.readFileSync(httpServerPath, 'utf8');
  
  // Replace processRequest with handleAnalyzeData
  content = content.replace(
    /mcpServer\.processRequest\(request\)/g,
    'mcpServer.handleAnalyzeData(request)'
  );
  
  fs.writeFileSync(httpServerPath, content);
  console.log('✅ Fixed http-server.ts');
}

function fixServerConstructor() {
  const indexPath = path.join(__dirname, 'src', 'index.ts');
  
  if (!fs.existsSync(indexPath)) {
    console.log('⚠️ index.ts not found, skipping');
    return;
  }
  
  console.log('🔧 Fixing server initialization in index.ts...');
  
  let content = fs.readFileSync(indexPath, 'utf8');
  
  // Check if we're creating a ReportingMCPServer without config
  if (content.includes('new ReportingMCPServer()')) {
    // Add config parameter
    content = content.replace(
      'new ReportingMCPServer()',
      `new ReportingMCPServer({
        port: process.env.PORT ? parseInt(process.env.PORT) : 3000,
        projectId: process.env.GOOGLE_CLOUD_PROJECT_ID || '',
        datasetId: process.env.BIGQUERY_DATASET_ID || '',
        tableId: process.env.BIGQUERY_TABLE_ID || '',
        anthropicApiKey: process.env.ANTHROPIC_API_KEY,
        useEnhancedNLQ: process.env.USE_ENHANCED_NLQ === 'true',
        includeSqlInResponses: process.env.INCLUDE_SQL_IN_RESPONSES === 'true',
        schemaRefreshIntervalMs: process.env.SCHEMA_REFRESH_INTERVAL_MS ? 
          parseInt(process.env.SCHEMA_REFRESH_INTERVAL_MS) : 3600000
      })`
    );
    
    fs.writeFileSync(indexPath, content);
    console.log('✅ Fixed server initialization in index.ts');
  } else {
    console.log('⚠️ Server initialization pattern not found in index.ts');
  }
}

// Run the main function
main().catch(err => {
  console.error('Error:', err);
  process.exit(1);
});
