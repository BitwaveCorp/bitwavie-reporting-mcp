#!/usr/bin/env node

import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import { z } from 'zod';
import { QueryParser } from './services/query-parser.js';
import { BigQueryClient } from './services/bigquery-client.js';
import { BigQueryConfig, ColumnMapping, QueryParseResult, ReportParameters } from './types/actions-report.js';

// Enhanced logging function with timestamps and flow tracking
const logFlow = (stage: string, direction: 'ENTRY' | 'EXIT' | 'ERROR' | 'INFO', message: string, data: any = null) => {
  const timestamp = new Date().toISOString();
  let logPrefix = '???';
  
  switch (direction) {
    case 'ENTRY': logPrefix = '>>>'; break;
    case 'EXIT': logPrefix = '<<<'; break;
    case 'ERROR': logPrefix = '!!!'; break;
    case 'INFO': logPrefix = '---'; break;
  }
  
  const logMessage = `[${timestamp}] ${logPrefix} MCP_${stage}: ${message}`;
  
  if (data) {
    console.error(logMessage, data);
  } else {
    console.error(logMessage);
  }
};

export class ReportingMCPServer {
  private server: Server;
  private queryParser: QueryParser;
  private bigQueryClient: BigQueryClient;

  constructor() {
    this.server = new Server(
      {
        name: 'reporting-mcp-server',
        version: '1.0.0',
      },
      {
        capabilities: {
          tools: {},
        },
      }
    );

    // Initialize services
    this.queryParser = new QueryParser();
    this.bigQueryClient = new BigQueryClient();
    
    this.setupHandlers();
    this.initializeBigQueryClient();
  }
  
  private async initializeBigQueryClient(): Promise<void> {
    try {
      const projectId = process.env.GOOGLE_CLOUD_PROJECT_ID;
      const datasetId = process.env.BIGQUERY_DATASET_ID;
      const tableId = process.env.BIGQUERY_TABLE_ID;
      
      if (!projectId || !datasetId || !tableId) {
        console.error('BigQuery configuration missing. Check environment variables.');
        return;
      }
      
      const config: BigQueryConfig = {
        projectId,
        datasetId,
        tableId,
        // Only add keyFilename if it's defined
        ...(process.env.GOOGLE_APPLICATION_CREDENTIALS ? { keyFilename: process.env.GOOGLE_APPLICATION_CREDENTIALS } : {})
      };
      
      await this.bigQueryClient.configure(config);
    } catch (error) {
      console.error('Failed to initialize BigQuery client:', error);
    }
  }

  private setupHandlers(): void {
    // Define schemas for request handlers
    const listToolsSchema = z.object({
      method: z.literal('tools/list')
    });
    
    const callToolSchema = z.object({
      method: z.literal('tools/call'),
      params: z.object({
        name: z.string(),
        arguments: z.record(z.string(), z.any()).optional()
      })
    });
    
    // Use schemas for request handlers
    this.server.setRequestHandler(listToolsSchema, async (request) => {
      logFlow('TOOLS_LIST', 'ENTRY', 'Received tools/list request', request);
      const response = {
        tools: [
          {
            name: 'test_connection',
            description: 'Test the MCP server connection and BigQuery setup',
            inputSchema: {
              type: 'object',
              properties: {},
            },
          },
          {
            name: 'analyze_actions_data',
            description: 'Analyze Actions Report data using natural language queries',
            inputSchema: {
              type: 'object',
              properties: {
                query: {
                  type: 'string',
                  description: 'Natural language query',
                },
              },
              required: ['query'],
            },
          },
        ],
      };
      
      logFlow('TOOLS_LIST', 'EXIT', 'Sending tools/list response', { toolCount: response.tools.length });
      return response;
    });

    this.server.setRequestHandler(callToolSchema, async (request) => {
      const { name, arguments: args = {} } = request.params;
      
      logFlow('TOOLS_CALL', 'ENTRY', `Received tools/call request for ${name}`, { toolName: name, args });
      
      try {
        switch (name) {
          case 'test_connection':
            return await this.handleTestConnection();
          case 'analyze_actions_data':
            return await this.handleAnalyzeData(args);
          default:
            throw new Error(`Unknown tool: ${name}`);
        }
      } catch (error) {
        return {
          content: [
            {
              type: 'text',
              text: `❌ Error: ${error instanceof Error ? error.message : String(error)}`,
            },
          ],
        };
      }
    });
  }

  private async handleTestConnection(): Promise<any> {
    logFlow('TEST_CONNECTION', 'ENTRY', 'Testing connection to BigQuery');
    try {
      const projectId = process.env.GOOGLE_CLOUD_PROJECT_ID;
      const datasetId = process.env.BIGQUERY_DATASET_ID;
      const tableId = process.env.BIGQUERY_TABLE_ID;

      if (!projectId || !datasetId || !tableId) {
        return {
          content: [
            {
              type: 'text',
              text: '❌ **Configuration Missing**\n\nRequired environment variables:\n- GOOGLE_CLOUD_PROJECT_ID\n- BIGQUERY_DATASET_ID\n- BIGQUERY_TABLE_ID',
            },
          ],
        };
      }

      // Test BigQuery connection
      const { BigQuery } = await import('@google-cloud/bigquery');
      const bigquery = new BigQuery({ projectId });
      
      const dataset = bigquery.dataset(datasetId);
      const table = dataset.table(tableId);
      const [metadata] = await table.getMetadata();

      const response = {
        content: [
          {
            type: 'text',
            text: `✅ **Connection Successful**\n\n**Project:** ${projectId}\n**Dataset:** ${datasetId}\n**Table:** ${tableId}\n**Schema Fields:** ${metadata.schema?.fields?.length || 0}`,
          },
        ],
      };
      
      logFlow('TEST_CONNECTION', 'EXIT', 'BigQuery connection successful', { 
        projectId, 
        datasetId, 
        tableId, 
        fieldCount: metadata.schema?.fields?.length || 0 
      });
      
      return response;
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      
      logFlow('TEST_CONNECTION', 'ERROR', 'BigQuery connection failed', { error: errorMessage });
      
      return {
        content: [
          {
            type: 'text',
            text: `❌ **Connection Failed**\n\nError: ${errorMessage}`,
          },
        ],
      };
    }
  }

  private async handleAnalyzeData(args: any): Promise<any> {
    const { query = '', confirmedMappings } = args;
    
    logFlow('ANALYZE_DATA', 'ENTRY', 'Analyzing data with query', { query, hasMappings: !!confirmedMappings });
    
    try {
      if (!query || query.trim().length === 0) {
        return {
          content: [{
            type: 'text',
            text: '❌ **Error**: Please provide a valid query.'
          }]
        };
      }
      
      // Parse the natural language query
      let parseResult = await this.queryParser.parseQuery(query);
      
      // Apply any confirmed mappings if provided
      if (confirmedMappings) {
        parseResult = this.applyConfirmedMappings(parseResult, confirmedMappings);
      }
      
      // Check if we still need column mapping confirmation
      const needsConfirmation = parseResult.columns.some(col => !col.confirmed);
      if (needsConfirmation) {
        return await this.formatColumnMappingConfirmation(parseResult, query);
      }
      
      // Execute the query
      const currentDate = new Date().toISOString().split('T')[0];
      
      // Create parameters object with type assertion to avoid TypeScript errors
      const parameters = {
        runId: '*' as string
      };
      
      // Add date parameters
      if (parseResult.timeRange?.start) {
        (parameters as any).startDate = parseResult.timeRange.start;
      }
      
      if (parseResult.timeRange?.end) {
        (parameters as any).endDate = parseResult.timeRange.end;
        (parameters as any).asOfDate = parseResult.timeRange.end;
      } else {
        (parameters as any).asOfDate = currentDate;
      }
      
      // Cast the final object to the expected type
      const typedParameters = parameters as ReportParameters;
      
      logFlow('BIGQUERY_EXECUTE', 'ENTRY', 'Executing BigQuery analytical query', { 
        intent: parseResult.intent,
        aggregationType: parseResult.aggregationType,
        columnCount: parseResult.columns.length,
        filterCount: Object.keys(parseResult.filters || {}).length,
        parameters: typedParameters
      });
      
      const queryResult = await this.bigQueryClient.executeAnalyticalQuery(parseResult, typedParameters);
      
      logFlow('BIGQUERY_EXECUTE', 'EXIT', 'BigQuery query execution completed', {
        success: queryResult.success,
        rowCount: queryResult.metadata?.rows_processed || 0,
        executionTime: queryResult.metadata?.execution_time_ms || 0,
        cached: queryResult.metadata?.cached || false
      });
      
      // Format and return the results
      const formattedResults = this.formatQueryResults(queryResult, parseResult, query);
      
      logFlow('ANALYZE_DATA', 'EXIT', 'Analysis completed successfully', {
        contentLength: formattedResults.content?.[0]?.text?.length || 0,
        hasMetadata: !!formattedResults.metadata
      });
      
      return formattedResults;
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      console.error('Error analyzing data:', error);
      
      logFlow('ANALYZE_DATA', 'ERROR', 'Error analyzing data', { error: errorMessage });
      
      return {
        content: [{
          type: 'text',
          text: `❌ **Error Processing Query**\n\n${errorMessage}\n\nPlease try rephrasing your query or check the server logs for more details.`
        }]
      };
    }
  }
  
  private async formatColumnMappingConfirmation(parseResult: QueryParseResult, originalQuery: string): Promise<any> {
    const unconfirmedColumns = parseResult.columns.filter(col => !col.confirmed);
    
    // Get available columns from BigQuery
    let availableColumns: string[] = [];
    try {
      availableColumns = await this.bigQueryClient.getAvailableColumns();
    } catch (error) {
      console.error('Error fetching available columns:', error);
      // Continue with empty array if we can't fetch columns
    }

    let text = `📋 **Column Mapping Confirmation Needed**\n\nFor your query: "${originalQuery}"\n\nI need to confirm the following column mappings:\n\n`;
    
    // Process each unconfirmed column
    const columnMappings = [];
    for (const col of unconfirmedColumns) {
      const userTerm = col.userTerm || 'N/A';
      const currentMappings = col.mappedColumns || [];
      const description = col.description || 'No description available';
      
      // Get similar columns from BigQuery
      const similarColumns = await this.bigQueryClient.findSimilarColumns(userTerm);
      
      // Format the suggestions
      let suggestionsText = '';
      if (similarColumns.length > 0) {
        suggestionsText = `\n   Suggestions: ${similarColumns.join(', ')}`;
      } else if (availableColumns.length > 0) {
        // If no similar columns found, show some available columns
        suggestionsText = `\n   Available columns: ${availableColumns.slice(0, 5).join(', ')}${availableColumns.length > 5 ? '...' : ''}`;
      }
      
      // Add to the text
      text += `${columnMappings.length + 1}. Term: "${userTerm}"\n`;
      text += `   Maps to: ${currentMappings.join(', ') || 'None'}`;
      text += suggestionsText;
      text += `\n   Description: ${description}\n\n`;
      
      columnMappings.push({
        userTerm,
        currentMappings,
        similarColumns,
        description
      });
    }
    
    text += 'Please confirm if these mappings are correct, or reply with the number and the correct column name (e.g., "1 amount" to map the first term to "amount").';
    
    return {
      content: [{
        type: 'text',
        text
      }],
      // Include structured data for easier parsing in the response
      metadata: {
        type: 'column_mapping_confirmation',
        columns: columnMappings
      }
    };
  }

  private applyConfirmedMappings(parseResult: QueryParseResult, confirmedMappings: any): QueryParseResult {
    // Create a deep copy of the parse result
    const result = JSON.parse(JSON.stringify(parseResult));
    
    // Apply the confirmed mappings
    result.columns = result.columns.map((col: any) => {
      if (!col.confirmed && col.userTerm && confirmedMappings[col.userTerm]) {
        return {
          ...col,
          mappedColumns: [confirmedMappings[col.userTerm]],
          confirmed: true
        };
      }
      return col;
    });
    
    return result;
  }

  private formatQueryResults(queryResult: any, parseResult: QueryParseResult, originalQuery: string): any {
    if (!queryResult.success) {
      return {
        content: [{
          type: 'text',
          text: `❌ **Query Error**\n\n${queryResult.error?.message || 'Unknown error'}\n\n${queryResult.error?.suggestions ? 'Suggestions:\n- ' + queryResult.error.suggestions.join('\n- ') : ''}`
        }]
      };
    }
    
    // If no data returned
    if (!queryResult.data || (Array.isArray(queryResult.data) && queryResult.data.length === 0)) {
      return {
        content: [{
          type: 'text',
          text: `📊 **No Data Found**\n\nYour query: "${originalQuery}"\n\nNo matching data was found. Try adjusting your filters or time range.`
        }]
      };
    }
    
    // Format successful results
    let resultText = `📊 **Query Results**\n\nQuery: "${originalQuery}"\n\n`;
    
    // Add summary if available
    if (queryResult.summary) {
      resultText += `${queryResult.summary}\n\n`;
    }
    
    // Format data as a table or list depending on structure
    if (Array.isArray(queryResult.data)) {
      if (queryResult.data.length > 10) {
        resultText += `Showing top 10 of ${queryResult.data.length} results:\n\n`;
      }
      
      // Create table header if data is tabular
      const sampleItem = queryResult.data[0];
      if (sampleItem && typeof sampleItem === 'object') {
        const headers = Object.keys(sampleItem);
        resultText += '| ' + headers.join(' | ') + ' |\n';
        resultText += '| ' + headers.map(() => '---').join(' | ') + ' |\n';
        
        // Add rows (limit to 10)
        const displayData = queryResult.data.slice(0, 10);
        displayData.forEach((item: Record<string, any>) => {
          resultText += '| ' + headers.map(h => item[h]?.toString() || '').join(' | ') + ' |\n';
        });
      } else {
        // Simple list for non-object data
        const displayData = queryResult.data.slice(0, 10);
        displayData.forEach((item: any, index: number) => {
          resultText += `${index + 1}. ${item}\n`;
        });
      }
    } else if (typeof queryResult.data === 'object') {
      // Handle single object result
      Object.entries(queryResult.data).forEach(([key, value]) => {
        resultText += `**${key}**: ${value}\n`;
      });
    } else {
      // Handle scalar result
      resultText += `Result: ${queryResult.data}\n`;
    }
    
    // Add metadata
    resultText += `\n**Execution time**: ${queryResult.metadata.execution_time_ms}ms | `;
    resultText += `**Rows processed**: ${queryResult.metadata.rows_processed} | `;
    resultText += queryResult.metadata.cached ? '**Cached result**' : '**Fresh result**';
    
    return {
      content: [{
        type: 'text',
        text: resultText
      }]
    };
  }

  async run(): Promise<void> {
    console.error('===========================================');
    console.error('🚀 Starting Reporting MCP Server');
    console.error(`Project: ${process.env.GOOGLE_CLOUD_PROJECT_ID || 'Not set'}`);
    console.error(`Dataset: ${process.env.BIGQUERY_DATASET_ID || 'Not set'}`);
    console.error(`Table: ${process.env.BIGQUERY_TABLE_ID || 'Not set'}`);
    console.error('===========================================');
    
    const transport = new StdioServerTransport();
    await this.server.connect(transport);
    console.error('Reporting MCP Server running on stdio');
  }
  
  // Process a JSON-RPC request for HTTP API
  async processRequest(request: any): Promise<any> {
    try {
      // Handle tools/list request
      if (request.method === 'tools/list') {
        return {
          jsonrpc: '2.0',
          id: request.id,
          result: {
            tools: [
              {
                name: 'test_connection',
                description: 'Test the MCP server connection and BigQuery setup',
                inputSchema: {
                  type: 'object',
                  properties: {}
                }
              },
              {
                name: 'analyze_actions_data',
                description: 'Analyze Actions Report data using natural language queries',
                inputSchema: {
                  type: 'object',
                  properties: {
                    query: {
                      type: 'string',
                      description: 'Natural language query'
                    }
                  },
                  required: ['query']
                }
              }
            ]
          }
        };
      }
      
      // Handle tools/call request
      if (request.method === 'tools/call') {
        const toolName = request.params?.name;
        const args = request.params?.arguments || {};
        
        let result;
        if (toolName === 'test_connection') {
          result = await this.handleTestConnection();
        } else if (toolName === 'analyze_actions_data') {
          result = await this.handleAnalyzeData(args);
        } else {
          return {
            jsonrpc: '2.0',
            id: request.id,
            result: {
              content: [{
                type: 'text',
                text: `❌ Error: Unknown tool: ${toolName}`
              }]
            }
          };
        }
        
        return {
          jsonrpc: '2.0',
          id: request.id,
          result
        };
      }
      
      // Handle unknown method
      return {
        jsonrpc: '2.0',
        id: request.id,
        error: {
          code: -32601,
          message: 'Method not found'
        }
      };
    } catch (error) {
      console.error('Error processing request:', error);
      return {
        jsonrpc: '2.0',
        id: request.id,
        error: {
          code: -32603,
          message: 'Internal server error'
        }
      };
    }
  }
}

// Handle graceful shutdown
process.on('SIGINT', () => {
  console.error('Shutting down...');
  process.exit(0);
});

const server = new ReportingMCPServer();
server.run().catch((error) => {
  console.error('Failed to start server:', error);
  process.exit(1);
});