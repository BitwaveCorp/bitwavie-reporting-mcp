#!/usr/bin/env node

import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import { z } from 'zod';

export class ReportingMCPServer {
  private server: Server;

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

    this.setupHandlers();
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
    this.server.setRequestHandler(listToolsSchema, async () => {
      return {
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
    });

    this.server.setRequestHandler(callToolSchema, async (request) => {
      const { name, arguments: args = {} } = request.params;
      
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

      return {
        content: [
          {
            type: 'text',
            text: `✅ **Connection Successful**\n\n**Project:** ${projectId}\n**Dataset:** ${datasetId}\n**Table:** ${tableId}\n**Schema Fields:** ${metadata.schema?.fields?.length || 0}`,
          },
        ],
      };
    } catch (error) {
      return {
        content: [
          {
            type: 'text',
            text: `❌ **Connection Failed**\n\nError: ${error instanceof Error ? error.message : String(error)}`,
          },
        ],
      };
    }
  }

  private async handleAnalyzeData(args: any): Promise<any> {
    const { query = '' } = args;
    return {
      content: [
        {
          type: 'text',
          text: `📊 **Query Received**\n\nQuery: "${query}"\n\n✅ MCP server is working! Natural language processing implementation coming next.`,
        },
      ],
    };
  }

  async run(): Promise<void> {
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