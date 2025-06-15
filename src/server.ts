#!/usr/bin/env node

import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import { z } from 'zod';
import { Anthropic } from '@anthropic-ai/sdk';
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
  private anthropic: Anthropic | null = null;

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
    
    // Initialize Anthropic client if API key is available
    if (process.env.ANTHROPIC_API_KEY) {
      this.anthropic = new Anthropic({
        apiKey: process.env.ANTHROPIC_API_KEY,
      });
      console.log('✅ Anthropic client initialized');
    } else {
      console.warn('⚠️ ANTHROPIC_API_KEY not found in environment variables. Intelligent column mapping will be disabled.');
    }
    
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
      
      // If we don't have confirmed mappings, skip the query parser's column mapping entirely
      if (!confirmedMappings) {
        // Check if this is a gain/loss query
        const lowerQuery = query.toLowerCase();
        const isGainLossQuery = lowerQuery.includes('gain') || lowerQuery.includes('loss') || lowerQuery.includes('profit');
        
        if (isGainLossQuery) {
          logFlow('GAIN_LOSS_QUERY', 'INFO', 'Detected gain/loss query, showing relevant columns', { query });
          return await this.formatGainLossColumnList(query);
        } else {
          // For all other queries without confirmed mappings, show the full column list
          logFlow('COLUMN_LIST', 'INFO', 'No confirmed mappings, showing full column list', { query });
          return await this.formatFullColumnList(query);
        }
      }
      
      // Parse the natural language query
      let parseResult = await this.queryParser.parseQuery(query);
      
      // Apply confirmed mappings
      parseResult = this.applyConfirmedMappings(parseResult, confirmedMappings);
      
      // At this point, all columns should be confirmed since we have confirmedMappings
      
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
  
  /**
   * Uses Claude to intelligently map columns based on natural language understanding
   * @param parseResult The initial parse result with unconfirmed columns
   * @param query The original user query
   * @returns Updated parse result with Claude-confirmed column mappings
   */
  private async useIntelligentColumnMapping(parseResult: QueryParseResult, query: string): Promise<QueryParseResult> {
    if (!this.anthropic) {
      throw new Error('Anthropic client not initialized');
    }

    // Get available columns and their metadata from BigQuery
    const availableColumns = await this.bigQueryClient.getAvailableColumns();
    if (!availableColumns || availableColumns.length === 0) {
      throw new Error('No columns available from BigQuery');
    }

    // Get column metadata for context
    const columnMetadata: Record<string, any> = {};
    for (const column of availableColumns) {
      try {
        const metadata = await this.bigQueryClient.getColumnMetadata(column);
        if (metadata) {
          columnMetadata[column] = metadata;
        }
      } catch (error) {
        console.warn(`Could not get metadata for column ${column}:`, error);
      }
    }

    // Create a system prompt for Claude
    const systemPrompt = `You are an expert data analyst helping map natural language queries to database columns.

Available columns in the BigQuery table:
${availableColumns.map(col => `- ${col}: ${columnMetadata[col]?.description || 'No description available'}`).join('\n')}

Your task is to determine which columns are relevant to the user's query and map them appropriately.
For each column in the query parse result, determine if it should be confirmed and mapped to a specific BigQuery column.
Only confirm columns that are actually needed to answer the query. Don't include columns that aren't relevant.

Respond in JSON format with the following structure:
{
  "columns": [
    {
      "userTerm": "[original term]",
      "mappedColumn": "[bigquery column name]",
      "confirmed": true/false,
      "reasoning": "[brief explanation]"
    },
    ...
  ]
}`;

    // Create a message for Claude with the query and parse result
    const message = `User query: "${query}"

Current parse result columns:
${JSON.stringify(parseResult.columns, null, 2)}

Please map these columns to the appropriate BigQuery columns based on the query intent.`;

    // Call Claude API
    const response = await this.anthropic.messages.create({
      model: 'claude-3-5-sonnet-20240620',  // Updated to latest model
      max_tokens: 1000,
      system: systemPrompt,
      messages: [{ role: 'user', content: message }],
    });

    // Extract and parse the JSON response
    const content = response.content[0]?.type === 'text' ? response.content[0].text : '';
    const jsonMatch = content.match(/\{[\s\S]*\}/); // Extract JSON object from response
    
    if (!jsonMatch) {
      throw new Error('Could not parse Claude response as JSON');
    }
    
    try {
      const mappingResult = JSON.parse(jsonMatch[0]);
      
      // Create a deep copy of the parse result
      const updatedParseResult = JSON.parse(JSON.stringify(parseResult));
      
      // Update columns based on Claude's mappings
      if (mappingResult.columns && Array.isArray(mappingResult.columns)) {
        // Replace the columns with Claude's suggestions
        updatedParseResult.columns = mappingResult.columns.map((mapping: {
          userTerm: string;
          mappedColumn?: string;
          confirmed: boolean;
          reasoning?: string;
        }) => {
          return {
            userTerm: mapping.userTerm,
            mappedColumns: mapping.mappedColumn ? [mapping.mappedColumn] : [],
            confirmed: mapping.confirmed,
            description: mapping.reasoning || 'Mapped by Claude',
            type: columnMetadata[mapping.mappedColumn || '']?.type || 'string'
          };
        });
      }
      
      return updatedParseResult;
    } catch (error) {
      console.error('Error parsing Claude response:', error, content);
      throw new Error('Failed to parse Claude response');
    }
  }

  /**
   * Shows gain/loss specific columns for gain/loss related queries
   * @param originalQuery The original user query
   * @returns Formatted response with gain/loss columns highlighted
   */
  private async formatGainLossColumnList(originalQuery: string): Promise<any> {
    // Get all available columns from BigQuery
    let availableColumns: string[] = [];
    let columnMetadata: Record<string, any> = {};
    
    try {
      availableColumns = await this.bigQueryClient.getAvailableColumns();
      
      // Get metadata for each column
      for (const column of availableColumns) {
        try {
          const metadata = await this.bigQueryClient.getColumnMetadata(column);
          if (metadata) {
            columnMetadata[column] = metadata;
          }
        } catch (error) {
          console.warn(`Could not get metadata for column ${column}:`, error);
        }
      }
    } catch (error) {
      console.error('Error fetching available columns:', error);
      // Continue with empty array if we can't fetch columns
    }
    
    // Filter for gain/loss related columns
    const gainLossColumns = availableColumns.filter(column => {
      const lowerColumn = column.toLowerCase();
      return lowerColumn.includes('gain') || 
             lowerColumn.includes('loss') || 
             lowerColumn.includes('profit') || 
             lowerColumn.includes('pnl');
    });
    
    // Create the response text
    let text = `📋 **Gain/Loss Analysis**\n\nFor your query: "${originalQuery}"\n\n`;
    text += `Here are the gain/loss related columns you might want to use:\n\n`;
    
    // Add gain/loss columns with descriptions
    if (gainLossColumns.length > 0) {
      text += `**Gain/Loss Columns:**\n`;
      for (const column of gainLossColumns) {
        const description = columnMetadata[column]?.description || 'No description available';
        const type = columnMetadata[column]?.type || 'UNKNOWN';
        text += `- \`${column}\`: ${description} (${type})\n`;
      }
      text += '\n';
    } else {
      text += `No specific gain/loss columns found. Here are some financial columns you might want to use:\n\n`;
    }
    
    // Add example queries
    text += `**Example Queries:**\n`;
    text += `- "Sum shortTermGainLoss for all transactions"\n`;
    text += `- "What is my total longTermGainLoss for ETH"\n`;
    text += `- "Show me total gainLoss grouped by asset"\n\n`;
    
    text += `Please rephrase your query to clearly specify which gain/loss column you want to use.`;
    
    return {
      content: [{
        type: 'text',
        text
      }]
    };
  }

  /**
   * Shows a full list of available columns when Claude mapping fails
   * @param originalQuery The original user query
   * @returns Formatted response with all available columns and example
   */
  private async formatFullColumnList(originalQuery: string): Promise<any> {
    // Get all available columns from BigQuery
    let availableColumns: string[] = [];
    let columnMetadata: Record<string, any> = {};
    
    try {
      availableColumns = await this.bigQueryClient.getAvailableColumns();
      
      // Get metadata for each column
      for (const column of availableColumns) {
        try {
          const metadata = await this.bigQueryClient.getColumnMetadata(column);
          if (metadata) {
            columnMetadata[column] = metadata;
          }
        } catch (error) {
          console.warn(`Could not get metadata for column ${column}:`, error);
        }
      }
    } catch (error) {
      console.error('Error fetching available columns:', error);
      // Continue with empty array if we can't fetch columns
    }
    
    // Group columns by category for better organization
    const groupedColumns: Record<string, string[]> = {
      'Time-related': [],
      'Asset-related': [],
      'Transaction-related': [],
      'Financial': [],
      'Other': []
    };
    
    // Categorize columns
    for (const column of availableColumns) {
      const description = columnMetadata[column]?.description || '';
      const type = columnMetadata[column]?.type || 'UNKNOWN';
      
      if (['timestamp', 'date', 'time'].some(term => column.toLowerCase().includes(term))) {
        groupedColumns['Time-related']?.push(column);
      } else if (['asset', 'coin', 'token', 'currency'].some(term => column.toLowerCase().includes(term))) {
        groupedColumns['Asset-related']?.push(column);
      } else if (['action', 'transaction', 'transfer'].some(term => column.toLowerCase().includes(term))) {
        groupedColumns['Transaction-related']?.push(column);
      } else if (['amount', 'value', 'price', 'fee', 'gain', 'loss', 'cost', 'basis'].some(term => column.toLowerCase().includes(term))) {
        groupedColumns['Financial']?.push(column);
      } else {
        groupedColumns['Other']?.push(column);
      }
    }
    
    // Create the response text
    let text = `📋 **Please Specify the Columns You Need**\n\nFor your query: "${originalQuery}"\n\n`;
    text += `I need you to specify which columns you want to use in your query. Here are all available columns grouped by category:\n\n`;
    
    // Add grouped columns to the text
    for (const [category, columns] of Object.entries(groupedColumns)) {
      if (columns.length > 0) {
        text += `**${category} Columns:**\n`;
        for (const column of columns) {
          const description = columnMetadata[column]?.description || 'No description available';
          const type = columnMetadata[column]?.type || 'UNKNOWN';
          text += `- \`${column}\`: ${description} (${type})\n`;
        }
        text += '\n';
      }
    }
    
    // Add example prompt
    text += `**Example Query Format:**\n`;
    text += `"Sum shortTermGainLoss column for all rows where asset column is ETH"\n\n`;
    text += `Please rephrase your query to clearly specify which columns you want to use.`;
    
    return {
      content: [{
        type: 'text',
        text
      }]
    };
  }

  private async formatColumnMappingConfirmation(parseResult: QueryParseResult, originalQuery: string): Promise<any> {
    const unconfirmedColumns = parseResult.columns.filter(col => !col.confirmed);
    
    // If there are no unconfirmed columns, no need for confirmation
    if (unconfirmedColumns.length === 0) {
      return null;
    }
    
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