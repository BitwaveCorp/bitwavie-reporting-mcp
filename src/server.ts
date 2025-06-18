import express from 'express';
import http from 'http';
import { z } from 'zod';
import Anthropic from '@anthropic-ai/sdk';
import { ReportParameters } from './types/actions-report';

// Import service modules 
import { SchemaManager } from './services/schema-manager.js';
import { LLMQueryTranslator } from './services/llm-query-translator.js';
import { QueryConfirmationFormatter } from './services/query-confirmation-formatter.js';
import { QueryExecutor } from './services/query-executor.js';
import { ResultFormatter } from './services/result-formatter.js';

// Import legacy services
import { BigQueryClient } from './services/bigquery-client.js';
import { QueryParser } from './services/query-parser.js';
import { BigQuery } from '@google-cloud/bigquery';

// Session data interface definition
interface SessionData {
  query: string;
  translationResult?: TranslationResult;
  confirmationResponse?: ConfirmationResponse;
  executionResult?: ExecutionResult;
  formattedResult?: FormattedResult;
  timestamp: number;
}

// Utility functions
function logFlow(component: string, level: string, message: string, error?: any): void {
  const timestamp = new Date().toISOString();
  const errorMsg = error ? ` | Error: ${error instanceof Error ? error.message : error}` : '';
  console.log(`[${timestamp}] ${component} ${level}: ${message}${errorMsg}`);
}

function formatError(error: any): string {
  if (error instanceof Error) {
    return error.message;
  }
  if (typeof error === 'string') {
    return error;
  }
  return JSON.stringify(error);
}

function generateSessionId(query: string): string {
  const timestamp = Date.now();
  const queryHash = query.toLowerCase().replace(/\s+/g, '_').substring(0, 20);
  return `session_${timestamp}_${queryHash}`;
}

// Service configuration interfaces
interface SchemaManagerConfig {
  projectId: string;
  datasetId: string;
  tableId: string;
  refreshIntervalMs?: number;
}

interface LLMQueryTranslatorConfig {
  anthropic: Anthropic;
  schemaManager: SchemaManager;
}

interface QueryConfirmationFormatterConfig {
  includeSql?: boolean;
  includeSampleData?: boolean;
  suggestAlternatives?: boolean;
}

interface QueryExecutorConfig {
  projectId: string;
  llmTranslator?: LLMQueryTranslator;
}

interface ResultFormatterConfig {
  maxRows?: number;
  includePerformanceMetrics?: boolean;
  suggestVisualizations?: boolean;
}

interface BigQueryClientConfig {
  projectId: string;
  datasetId: string;
  tableId: string;
}

interface QueryParseResult {
  intent: 'list' | 'filter' | 'aggregation' | 'comparison' | 'trend' | 'balance';
  assets: string[];
  timeRange?: TimeRange;
  filters: FilterCondition[];
  aggregations: Aggregation[];
  groupBy: GroupByClause[];
  orderBy: OrderByClause[];
  columns: ColumnMapping[];
  metadata: QueryMetadata;
  aggregationType?: 'sum' | 'count' | 'avg' | 'min' | 'max';
  limit?: number;
  generatedSql?: string;
}

// Missing type definitions for QueryParseResult
interface TimeRange {
  start?: string;
  end?: string;
  period?: string;
}

interface FilterCondition {
  column: string;
  operator: string;
  value: any;
}

interface Aggregation {
  function: string;
  column: string;
  alias?: string;
}

interface GroupByClause {
  column: string;
}

interface OrderByClause {
  column: string;
  direction: 'ASC' | 'DESC';
}

interface ColumnMapping {
  name: string;
  alias?: string;
}

interface QueryMetadata {
  confidence: number;
  generationTime: number;
}

// Key Interface Definitions from document
interface ReportingServerConfig {
  port?: number;
  projectId: string;
  datasetId: string;
  tableId: string;
  anthropicApiKey?: string;
  useEnhancedNLQ?: boolean;
  includeSqlInResponses?: boolean;
  schemaRefreshIntervalMs?: number;
}

interface QueryComponents {
  filterOperations: { description: string; sqlClause: string };
  aggregationOperations: { description: string; sqlClause: string };
  groupByOperations: { description: string; sqlClause: string };
  orderByOperations: { description: string; sqlClause: string };
  limitOperations: { description: string; sqlClause: string };
}

interface ExecutionMetadata {
  executionTimeMs: number;
  bytesProcessed?: number;
  cacheHit?: boolean;
}

interface FormattedResultContent {
  type: string;
  text?: string;
  table?: {
    headers: string[];
    rows: any[][];
  };
}

interface FormattedResultMetadata {
  executionTimeMs: number;
  bytesProcessed?: number;
  cacheHit?: boolean;
  rowCount: number;
  totalRows: number;
  visualizationHint?: string;
}

interface TranslationResult {
  originalQuery: string;
  interpretedQuery: string;
  sql: string;
  confidence: number;
  components: QueryComponents;
  requiresConfirmation: boolean;
  alternatives?: Array<{ sql: string; description: string }>;
  alternativeInterpretations: string[] | undefined;
  error?: string;
  confirmedMappings?: Record<string, string>;
}

// Import the ExecutionResult type from query-executor.ts
import { ExecutionResult as QueryExecutorResult } from './services/query-executor';

// Define our own ExecutionResult interface that includes both the original properties
// and the additional properties needed by the formatter
interface ExecutionResult {
  success: boolean;
  data?: any[];
  rows?: any[];
  columns?: string[];
  error?: {
    message: string;
    code: string;
    sqlState?: string;
    details?: string;
  };
  metadata: {
    executionTimeMs: number;
    bytesProcessed?: number;
    rowCount?: number;
    retryCount: number;
    originalSql: string;
    finalSql: string;
  };
}

interface FormattedResult {
  content: FormattedResultContent[];
  // Enhanced rawData field for direct JSON access
  rawData?: {
    headers: string[];
    rows: any[];
    displayRows: number;   // Number of rows to display in UI (max 100)
    truncated: boolean;    // Whether the display data was truncated
    exceedsDownloadLimit: boolean; // Whether the data exceeds 5000 row download limit
  };
  // Processing steps to show how the query was interpreted and executed
  processingSteps?: Array<{
    type: string;
    message?: string;
    filters?: { description: string; sqlClause: string };
    aggregations?: { description: string; sqlClause: string };
    groupBy?: { description: string; sqlClause: string };
    orderBy?: { description: string; sqlClause: string };
    limit?: { description: string; sqlClause: string };
  }>;
  metadata: FormattedResultMetadata;
  summary?: string;
}

interface ConfirmationResponse {
  content: Array<{
    type: string;
    text?: string;
  }>;
  needsConfirmation: boolean;
}

// SessionData interface moved to the top of the file

interface AnalyzeDataRequest {
  query?: string;
  confirmedMappings?: Record<string, string>;
  previousResponse?: any;
}

interface AnalyzeDataResponse {
  content?: Array<{
    type: string;
    text?: string;
    table?: {
      headers: string[];
      rows: any[][];
    };
  }>;
  // Enhanced rawData field for direct JSON access
  rawData?: {
    headers: string[];
    rows: any[];
    displayRows: number;   // Number of rows to display in UI (max 100)
    truncated: boolean;    // Whether the display data was truncated
    exceedsDownloadLimit: boolean; // Whether the data exceeds 5000 row download limit
  };
  // Processing steps to show how the query was interpreted and executed
  processingSteps?: Array<{
    type: string;
    message?: string;
    filters?: { description: string; sqlClause: string };
    aggregations?: { description: string; sqlClause: string };
    groupBy?: { description: string; sqlClause: string };
    orderBy?: { description: string; sqlClause: string };
    limit?: { description: string; sqlClause: string };
  }>;
  needsConfirmation?: boolean;
  sql?: string;
  error?: string;
  originalQuery?: string;
  translationResult?: TranslationResult;
}

interface TestConnectionResponse {
  success: boolean;
  message: string;
  schema?: any;
}

export class ReportingMCPServer {
  private server: express.Application;
  private config: ReportingServerConfig;
  private sessions: Map<string, SessionData> = new Map();
  private schemaManager: SchemaManager | null = null;
  private llmQueryTranslator: LLMQueryTranslator | null = null;
  private queryConfirmationFormatter: QueryConfirmationFormatter | null = null;
  private queryExecutor: QueryExecutor | null = null;
  private resultFormatter: ResultFormatter | null = null;
  private bigQueryClient: BigQueryClient | null = null;
  private queryParser: QueryParser | null = null;
  private httpServer: http.Server | null = null;
  private cleanupInterval: NodeJS.Timeout | null = null;
  private sessionMaxAgeMs: number = 30 * 60 * 1000; // 30 minutes default
  
  // Method declarations to fix TypeScript errors
  // Method implementations are provided below

  constructor(config: ReportingServerConfig) {
    // Initialize config with defaults
    this.config = {
      port: config.port || 3000,
      projectId: config.projectId,
      datasetId: config.datasetId,
      tableId: config.tableId,
      anthropicApiKey: config.anthropicApiKey || '',
      useEnhancedNLQ: true, // Always use enhanced NLQ flow
      includeSqlInResponses: config.includeSqlInResponses !== undefined ? config.includeSqlInResponses : false,
      schemaRefreshIntervalMs: config.schemaRefreshIntervalMs || 3600000 // Default: 1 hour
    };
    
    logFlow('SERVER', 'INFO', 'Enhanced NLQ flow is enforced, legacy flow is disabled');

    // Initialize Express server
    this.server = express();

    // Initialize services
    this.initializeServices();

    // Register handlers
    this.registerHandlers();
  }

  private async initializeServices(): Promise<void> {
    try {
      // Initialize SchemaManager
      this.schemaManager = new SchemaManager();
      await this.schemaManager.configure({
        projectId: this.config.projectId,
        datasetId: this.config.datasetId,
        tableId: this.config.tableId,
        refreshIntervalMs: this.config.schemaRefreshIntervalMs || 3600000 // Ensure it's always a number
      });

      // Initialize LLMQueryTranslator if API key is provided
      if (this.config.anthropicApiKey) {
        // No need to create Anthropic instance here, it's created inside LLMQueryTranslator
        this.llmQueryTranslator = new LLMQueryTranslator(
          this.schemaManager,
          this.config.anthropicApiKey
        );
      }

      // Initialize QueryConfirmationFormatter
      this.queryConfirmationFormatter = new QueryConfirmationFormatter({
        includeSQL: this.config.includeSqlInResponses ?? false, // Ensure it's a boolean
        includeSampleData: true,
        suggestAlternatives: true
      });

      // Initialize QueryExecutor
      this.queryExecutor = new QueryExecutor(
        this.config.projectId,
        this.llmQueryTranslator || undefined
      );

      // Initialize ResultFormatter
      this.resultFormatter = new ResultFormatter({
        maxRows: 100,
        includePerformanceMetrics: true,
        suggestVisualizations: true
      });

      // Initialize legacy services
      this.bigQueryClient = new BigQueryClient();
      this.bigQueryClient.configure({
        projectId: this.config.projectId,
        datasetId: this.config.datasetId,
        tableId: this.config.tableId
      });

      this.queryParser = new QueryParser();

      logFlow('SERVER', 'INFO', 'Services initialized successfully');
    } catch (error) {
      logFlow('SERVER', 'ERROR', 'Failed to initialize services:', error);
      throw error;
    }
  }

  private registerHandlers(): void {
    // Set up middleware for parsing JSON requests
    this.server.use(express.json());

    // Register analyze data handler as an Express route
    this.server.post('/api/analyzeData', async (req, res) => {
      try {
        // Validate request using zod schema
        const schema = z.object({
          query: z.string().optional(),
          confirmedMappings: z.record(z.string()).optional(),
          previousResponse: z.any().optional()
        });

        // Parse and validate the request body
        const validatedData = schema.parse(req.body);

        // Process the request
        const result = await this.handleAnalyzeData(validatedData as AnalyzeDataRequest);

        // Send the response
        res.json(result);
      } catch (error) {
        // Handle validation or processing errors
        res.status(400).json({ 
          error: error instanceof Error ? error.message : 'Unknown error' 
        });
      }
    });

    // Register test connection handler as an Express route
    this.server.get('/api/testConnection', async (_req, res) => {
      try {
        // Process the request
        const result = await this.handleTestConnection();

        // Send the response
        res.json(result);
      } catch (error) {
        // Handle errors
        res.status(500).json({ 
          success: false,
          message: error instanceof Error ? error.message : 'Unknown error' 
        });
      }
    });

    // Register JSON-RPC endpoint
    this.server.post('/rpc', (req, res) => {
      console.log('[RPC] Received request:', JSON.stringify(req.body));
      const handleRpcRequest = async () => {
        try {
          const { jsonrpc, method, params, id } = req.body;
          console.log(`[RPC] Processing method: ${method}, id: ${id}`);
          
          // Validate JSON-RPC version
          if (jsonrpc !== '2.0') {
            console.log(`[RPC] Invalid JSON-RPC version: ${jsonrpc}`);
            return res.status(400).json({
              jsonrpc: '2.0',
              error: { code: -32600, message: 'Invalid Request: jsonrpc version must be 2.0' },
              id
            });
          }
          
          // Handle different RPC methods
          console.log(`[RPC] Handling method: ${method}`);
          switch (method) {
            case 'test_connection':
              console.log('[RPC] Processing test_connection request');
              const testResult = await this.handleTestConnection();
              console.log('[RPC] test_connection result:', testResult);
              return res.json({
                jsonrpc: '2.0',
                result: testResult.success ? 'MCP server is working' : testResult.message,
                id
              });
              
            case 'list_tools':
            case 'tools/list':
              console.log('[RPC] Processing tools/list request');
              // Return available tools
              const toolsResponse = {
                jsonrpc: '2.0',
                result: {
                  tools: [
                    { name: 'analyze_actions_data', description: 'Analyze crypto transaction data' },
                    { name: 'test_connection', description: 'Test connection to MCP server' }
                  ]
                },
                id
              };
              console.log('[RPC] Returning tools list:', JSON.stringify(toolsResponse));
              console.log('[RPC] DEBUG: Tools list structure:', JSON.stringify({
                resultType: typeof toolsResponse.result,
                hasTools: !!toolsResponse.result?.tools,
                toolsType: typeof toolsResponse.result?.tools,
                isArray: Array.isArray(toolsResponse.result?.tools),
                toolsLength: toolsResponse.result?.tools?.length || 0
              }));
              return res.json(toolsResponse);
              
            case 'tools/call':
              console.log('[RPC] Processing tools/call request');
              if (!params || typeof params !== 'object' || !params.name) {
                console.log('[RPC] Invalid params for tools/call:', params);
                return res.status(400).json({
                  jsonrpc: '2.0',
                  error: { code: -32602, message: 'Invalid params for tools/call' },
                  id
                });
              }
              
              // Handle different tools
              const toolName = params.name;
              const toolArgs = params.arguments || {};
              
              console.log(`[RPC] tools/call for tool: ${toolName}`);
              
              if (toolName === 'analyze_actions_data') {
                try {
                  console.log('[RPC] Calling handleAnalyzeData with query:', toolArgs.query);
                  const analyzeResult = await this.handleAnalyzeData({
                    query: toolArgs.query,
                    confirmedMappings: toolArgs.confirmedMappings,
                    previousResponse: toolArgs.previousResponse
                  });
                  
                  return res.json({
                    jsonrpc: '2.0',
                    result: analyzeResult,
                    id
                  });
                } catch (toolError) {
                  console.error('[RPC] Error in tools/call for analyze_actions_data:', toolError);
                  return res.status(500).json({
                    jsonrpc: '2.0',
                    error: {
                      code: -32603,
                      message: toolError instanceof Error ? toolError.message : 'Error processing tool',
                      data: toolError instanceof Error ? toolError.stack : undefined
                    },
                    id
                  });
                }
              } else {
                return res.status(400).json({
                  jsonrpc: '2.0',
                  error: { code: -32601, message: `Tool '${toolName}' not found` },
                  id
                });
              }
              
            case 'analyze_actions_data':
              console.log('[RPC] Processing analyze_actions_data request');
              if (!params || !Array.isArray(params) || params.length === 0) {
                console.log('[RPC] Invalid params for analyze_actions_data:', params);
                return res.status(400).json({
                  jsonrpc: '2.0',
                  error: { code: -32602, message: 'Invalid params for analyze_actions_data' },
                  id
                });
              }
              
              const requestData = params[0];
              console.log('[RPC] analyze_actions_data request data:', JSON.stringify(requestData));
              
              try {
                console.log('[RPC] Calling handleAnalyzeData with query:', requestData.query);
                const analyzeResult = await this.handleAnalyzeData({
                  query: requestData.query,
                  confirmedMappings: requestData.confirmedMappings,
                  previousResponse: requestData.previousResponse
                });
                
                console.log('[RPC] handleAnalyzeData result received, length:', 
                  JSON.stringify(analyzeResult).length);
                
                const response = {
                  jsonrpc: '2.0',
                  result: analyzeResult,
                  id
                };
                
                console.log('[RPC] Sending analyze_actions_data response');
                return res.json(response);
              } catch (analyzeError) {
                console.error('[RPC] Error in handleAnalyzeData:', analyzeError);
                return res.status(500).json({
                  jsonrpc: '2.0',
                  error: {
                    code: -32603,
                    message: analyzeError instanceof Error ? analyzeError.message : 'Error processing analyze_actions_data',
                    data: analyzeError instanceof Error ? analyzeError.stack : undefined
                  },
                  id
                });
              }
              
            default:
              console.log(`[RPC] Method not found: ${method}`);
              return res.status(400).json({
                jsonrpc: '2.0',
                error: { code: -32601, message: `Method not found: ${method}` },
                id
              });
          }
        } catch (error) {
          console.error('[RPC] General error in RPC handler:', error);
          return res.status(500).json({
            jsonrpc: '2.0',
            error: {
              code: -32603,
              message: error instanceof Error ? error.message : 'Internal JSON-RPC error',
              data: error instanceof Error ? error.stack : undefined
            },
            id: req.body.id
          });
        }
      };
      
      // Execute the async handler
      console.log('[RPC] Starting async handler execution');
      handleRpcRequest().catch(err => {
        console.error('[RPC] Unhandled error in async handler:', err);
        if (!res.headersSent) {
          res.status(500).json({
            jsonrpc: '2.0',
            error: {
              code: -32603,
              message: err instanceof Error ? err.message : 'Unhandled error in RPC handler',
              data: err instanceof Error ? err.stack : undefined
            },
            id: req.body?.id
          });
        }
      });
    });
  }

  public async start(): Promise<void> {
    try {
      if (!this.schemaManager) {
        throw new Error('SchemaManager not initialized');
      }

      // Refresh schema to ensure connection is working before starting
      await this.schemaManager.refreshSchema();
      logFlow('SERVER', 'INFO', 'Schema refreshed successfully');

      if (!this.httpServer) {
        this.httpServer = this.server.listen(this.config.port, () => {
          logFlow('SERVER', 'INFO', `Server started on port ${this.config.port}`);
        });
        
        // Start session cleanup
        this.startSessionCleanup();
      }
    } catch (error) {
      logFlow('SERVER', 'ERROR', 'Failed to start server:', error);
      throw error;
    }
  }

  public async stop(): Promise<void> {
    try {
      // Close HTTP server if it exists
      if (this.httpServer) {
        return new Promise<void>((resolve, reject) => {
          this.httpServer!.close((err) => {
            if (err) {
              logFlow('SERVER', 'ERROR', 'Error closing HTTP server:', err);
              reject(err);
            } else {
              // Clear sessions on shutdown
              this.sessions.clear();
              
              // Clear cleanup interval if it exists
              if (this.cleanupInterval) {
                clearInterval(this.cleanupInterval);
                this.cleanupInterval = null;
              }
              
              logFlow('SERVER', 'INFO', 'Server stopped successfully');
              resolve();
            }
          });
        });
      }
      logFlow('SERVER', 'INFO', 'ReportingMCPServer stopped');
      return Promise.resolve();
    } catch (error: any) {
      logFlow('SERVER', 'ERROR', 'Failed to stop server:', error);
      throw error;
    }
  }

  private async handleTestConnection(): Promise<TestConnectionResponse> {
    try {
      // Verify schema manager is initialized
      if (!this.schemaManager) {
        throw new Error('Schema manager not initialized');
      }
      
      // Refresh schema to ensure connection is working
      await this.schemaManager.refreshSchema();
      const schema = this.schemaManager.getSchema();
      
      return {
        success: true,
        message: 'Connection successful',
        schema
      };
    } catch (error: any) {
      logFlow('SERVER', 'ERROR', 'Connection test failed:', error);
      
      return {
        success: false,
        message: `Connection failed: ${formatError(error)}`
      };
    }
  }

  public async handleAnalyzeData(request: AnalyzeDataRequest): Promise<AnalyzeDataResponse> {
    try {
      // Validate input
      if (!request.query && !request.previousResponse) {
        return {
          content: [{ type: 'text', text: 'Please provide a query to analyze.' }],
          error: 'No query provided'
        };
      }
      
      // Check if it's a confirmation response - more robust detection
      if (request.confirmedMappings) {
        // If we have confirmed mappings, treat it as a confirmation response
        logFlow('SERVER', 'INFO', 'Detected confirmation response with mappings');
        
        // If previousResponse is missing or incomplete, try to extract from the query context
        if (!request.previousResponse?.translationResult && request.query) {
          logFlow('SERVER', 'INFO', 'Attempting to reconstruct previousResponse from context');
        }
        
        return this.handleConfirmationResponse(request);
      }
      
      // Generate session ID for new queries
      const query = request.query as string;
      const sessionId = generateSessionId(query);
      
      // Create new session
      const sessionData: SessionData = {
        query,
        timestamp: Date.now()
      };
      
      this.sessions.set(sessionId, sessionData);
      logFlow('SERVER', 'INFO', `New session created: ${sessionId} for query: ${query}`);
      
      // Always use enhanced NLQ flow
      if (this.llmQueryTranslator) {
        return this.processEnhancedNLQ(sessionId, query);
      } else {
        // Only if LLM translator is not available, throw an error
        throw new Error('Enhanced NLQ flow is required but LLM translator is not initialized');
      }
    } catch (error: any) {
      logFlow('SERVER', 'ERROR', 'Error in handleAnalyzeData:', error);
      
      // No fallback to legacy processing - enhanced flow is required
      logFlow('SERVER', 'ERROR', 'Enhanced NLQ processing failed and legacy flow is disabled');
      
      // Return user-friendly error messages
      return {
        content: [{ type: 'text', text: `Error processing query: ${formatError(error)}` }],
        error: formatError(error)
      };
    }
  }

  private async processEnhancedNLQ(sessionId: string, query: string): Promise<AnalyzeDataResponse> {
    console.log(`[processEnhancedNLQ] Processing enhanced NLQ for session ${sessionId}, query: ${query}`);
    
    try {
      // Translate the query using LLM
      if (!this.llmQueryTranslator) {
        throw new Error('LLMQueryTranslator not initialized');
      }
      
      const translationResult = await this.llmQueryTranslator.translateQuery(query);
      console.log(`[processEnhancedNLQ] Translation result:`, JSON.stringify(translationResult, null, 2));
      
      // Store the translation result in the session data
      const sessionData = this.sessions.get(sessionId);
      if (sessionData) {
        sessionData.translationResult = translationResult;
        sessionData.query = query;
      }
      
      // Create understanding message based on the interpreted query
      const understandingMessage = `"${translationResult.interpretedQuery}"`;
      
      // Add explanation of how the query was generated
      const queryExplanation = `\n\n**Wavie took the following steps:**\n\n "${translationResult.interpretedQuery}"\n Your request was translated into SQL: \`${translationResult.sql}\`\n The query was executed against the database\n Results were formatted for display`;
      
      // Create processing steps for frontend display
      // Define processing steps with proper typing
      const processingSteps: Array<{
        type: string;
        message?: string;
        filters?: { description: string; sqlClause: string };
        aggregations?: { description: string; sqlClause: string };
        groupBy?: { description: string; sqlClause: string };
        orderBy?: { description: string; sqlClause: string };
        limit?: { description: string; sqlClause: string };
      }> = [
        {
          type: 'query_interpretation',
          message: `I understand your query as: "${translationResult.interpretedQuery}"`
        },
        {
          type: 'sql_generation',
          message: translationResult.sql
        },
        {
          type: 'components',
          ...(translationResult.components.filterOperations && {
            filters: {
              description: translationResult.components.filterOperations.description || 'No filters applied',
              sqlClause: translationResult.components.filterOperations.sqlClause || ''
            }
          }),
          ...(translationResult.components.aggregationOperations && {
            aggregations: {
              description: translationResult.components.aggregationOperations.description || 'No aggregations',
              sqlClause: translationResult.components.aggregationOperations.sqlClause || ''
            }
          }),
          ...(translationResult.components.groupByOperations && {
            groupBy: {
              description: translationResult.components.groupByOperations.description || 'No grouping',
              sqlClause: translationResult.components.groupByOperations.sqlClause || ''
            }
          }),
          ...(translationResult.components.orderByOperations && {
            orderBy: {
              description: translationResult.components.orderByOperations.description || 'No ordering',
              sqlClause: translationResult.components.orderByOperations.sqlClause || ''
            }
          }),
          ...(translationResult.components.limitOperations && {
            limit: {
              description: translationResult.components.limitOperations.description || 'No limit',
              sqlClause: translationResult.components.limitOperations.sqlClause || ''
            }
          })
        }
      ];
      
      console.log('Processing steps created:', JSON.stringify(processingSteps, null, 2));
      
      // Auto-execute the query with the understanding message, explanation, and processing steps
      return this.executeAndFormatQuery(
        sessionId, 
        translationResult.sql, 
        understandingMessage, 
        queryExplanation,
        processingSteps as any // Temporary type assertion to bypass TypeScript error
      );
      
    } catch (error) {
      console.error(`[processEnhancedNLQ] Error processing enhanced NLQ:`, error);
      logFlow('SERVER', 'ERROR', `Enhanced NLQ processing failed for session ${sessionId}:`, error);
      // No fallback to legacy NLQ - we only use enhanced NLQ flow
      return {
        content: [{ type: 'text', text: `Error processing your query: ${formatError(error)}` }],
        error: formatError(error),
        needsConfirmation: false,
        originalQuery: query
      };
    }
  }

  private async processLegacyNLQ(query: string): Promise<AnalyzeDataResponse> {
    // Legacy NLQ flow is completely disabled
    logFlow('SERVER', 'WARN', `Attempt to use disabled legacy NLQ flow: ${query}`);
    
    return {
      content: [{ 
        type: 'text', 
        text: `Legacy NLQ flow is disabled. The system is configured to use enhanced NLQ flow only.` 
      }],
      error: 'Legacy NLQ flow is disabled',
      sql: '',
      originalQuery: query,
      needsConfirmation: false
    };
  }
  
  private async handleConfirmationResponse(request: AnalyzeDataRequest): Promise<AnalyzeDataResponse> {
    try {
      logFlow('SERVER', 'INFO', 'Processing confirmation response');
      
      // Find the session from previousResponse
      let translationResult = request.previousResponse?.translationResult;
      let originalQuery = request.query || '';
      
      // If translationResult is not directly available, try to extract it from data or other properties
      if (!translationResult && request.previousResponse?.data?.translationResult) {
        translationResult = request.previousResponse.data.translationResult;
        logFlow('SERVER', 'INFO', 'Found translationResult in previousResponse.data');
      }
      
      // Try to extract original query from previousResponse
      if (request.previousResponse?.originalQuery) {
        originalQuery = request.previousResponse.originalQuery;
        logFlow('SERVER', 'INFO', `Found originalQuery: ${originalQuery}`);
      } else if (request.previousResponse?.query) {
        originalQuery = request.previousResponse.query;
        logFlow('SERVER', 'INFO', `Using query from previousResponse: ${originalQuery}`);
      }
      
      // Last resort: check if the previousResponse itself might contain the necessary fields
      if (!translationResult && request.previousResponse?.sql) {
        translationResult = {
          sql: request.previousResponse.sql,
          originalQuery: originalQuery
        };
        logFlow('SERVER', 'INFO', 'Created translationResult from previousResponse fields');
      }
      
      if (!translationResult) {
        logFlow('SERVER', 'ERROR', 'Invalid confirmation request - cannot find translation result', {
          query: originalQuery,
          previousResponse: request.previousResponse ? 'present' : 'missing',
          confirmedMappings: request.confirmedMappings ? 'present' : 'missing'
        });
        return {
          content: [{ type: 'text', text: 'Invalid confirmation request - missing translation result. Please try your query again.' }],
          error: 'Missing translation result',
          needsConfirmation: false // Explicitly set to false to prevent confirmation loops
        };
      }
      
      // Log the confirmedMappings for debugging
      logFlow('SERVER', 'INFO', `Received confirmedMappings: ${JSON.stringify(request.confirmedMappings || {})}`);
      
      // Process confirmed mappings
      let sqlToExecute = translationResult.sql;
      
      if (request.confirmedMappings) {
        // Handle confirmed queries, alternative selections, and corrections
        for (const [placeholder, value] of Object.entries(request.confirmedMappings)) {
          if (value) { // Only replace if value is not null or empty
            logFlow('SERVER', 'INFO', `Replacing ${placeholder} with ${value} in SQL`);
            sqlToExecute = sqlToExecute.replace(placeholder, value);
          } else {
            logFlow('SERVER', 'WARN', `Empty value for mapping ${placeholder}, skipping replacement`);
          }
        }
      } else {
        logFlow('SERVER', 'WARN', 'No confirmedMappings provided in confirmation response');
      }
      
      // Generate new session for confirmed query
      const sessionId = generateSessionId(translationResult.originalQuery + '_confirmed');
      const sessionData: SessionData = {
        query: translationResult.originalQuery,
        translationResult: {
          ...translationResult,
          sql: sqlToExecute,
          confirmedMappings: request.confirmedMappings || {} // Store the confirmed mappings
        },
        timestamp: Date.now()
      };
      
      this.sessions.set(sessionId, sessionData);
      
      // Execute confirmed queries and format results
      return this.executeAndFormatQuery(sessionId, sqlToExecute);
    } catch (error) {
      logFlow('SERVER', 'ERROR', 'Error processing confirmation response:', error);
      return {
        content: [{ type: 'text', text: `Error processing confirmation: ${formatError(error)}` }],
        error: formatError(error)
      };
    }
  }

  private async executeAndFormatQuery(
    sessionId: string, 
    sql: string, 
    understandingMessage?: string, 
    queryExplanation?: string,
    processingSteps?: Array<{
      type: string;
      message?: string;
      filters?: { description: string; sqlClause: string };
      aggregations?: { description: string; sqlClause: string };
      groupBy?: { description: string; sqlClause: string };
      orderBy?: { description: string; sqlClause: string };
      limit?: { description: string; sqlClause: string };
    }>
  ): Promise<AnalyzeDataResponse> {
    console.log('DATA CHECKER [BACKEND] - Starting executeAndFormatQuery');
    console.log('DATA CHECKER [BACKEND] - SQL:', sql.substring(0, 500) + (sql.length > 500 ? '...' : ''));
    console.log('DATA CHECKER [BACKEND] - Processing steps:', JSON.stringify(processingSteps, null, 2));
    // Get the session data
    const sessionData = this.sessions.get(sessionId);
    if (!sessionData) {
      throw new Error('Session not found');
    }

    try {
      // Execute the query
      if (!this.queryExecutor) {
        throw new Error('QueryExecutor not initialized');
      }
      
      const executionResult = await this.queryExecutor.executeQuery(sql);
      
      // Store the execution result in session data
      if (executionResult) {
        // Cast the execution result to our extended type
        sessionData.executionResult = executionResult as unknown as ExecutionResult;
      }
      
      if (executionResult.error) {
        return {
          content: [{ type: 'text', text: `Query execution failed: ${executionResult.error.message}` }],
          error: executionResult.error.message,
          sql: this.config.includeSqlInResponses ? sql : '',
          originalQuery: sessionData.query,
          // Explicitly set needsConfirmation to false for error responses after confirmation
          needsConfirmation: false,
          // Only include translationResult if it exists
          ...(sessionData.translationResult && { translationResult: sessionData.translationResult })
        };
      }
      
      // Format the results
      if (!this.resultFormatter) {
        throw new Error('ResultFormatter not initialized');
      }
      
      // Cast to our ExecutionResult interface
      const typedResult = executionResult as unknown as ExecutionResult;
      
      // Ensure we have rows and columns for the formatter
      const formatterInput: ExecutionResult = {
        ...executionResult,
        rows: typedResult.rows || typedResult.data || [],
        columns: typedResult.columns || []
      };
      
      const formattedResult = await this.resultFormatter.formatResults(
        formatterInput,
        sessionData.translationResult
      );
      
      // If we have an understanding message and/or query explanation, prepend them to the content
      let content = formattedResult.content || [];
      
      // Add understanding message if provided
      if (understandingMessage) {
        content = [
          { type: 'text', text: understandingMessage },
          ...content
        ];
      }
      
      // Add query explanation at the end if provided
      if (queryExplanation) {
        content.push({ type: 'text', text: queryExplanation });
      }
      
      // Log the response structure before sending to frontend
      console.log('Response structure:', {
        contentLength: content.length,
        hasRawData: !!formattedResult.rawData
      });
      
      // Log the rawData if available
      if (formattedResult.rawData) {
        console.log('Raw data structure:', {
          headers: formattedResult.rawData.headers,
          totalRows: formattedResult.rawData.rows?.length || 0,
          displayRows: formattedResult.rawData.displayRows,
          truncated: formattedResult.rawData.truncated,
          exceedsDownloadLimit: formattedResult.rawData.exceedsDownloadLimit
        });
        
        // Log a sample of the data (first 2 rows)
        if (formattedResult.rawData.rows?.length) {
          console.log('Data sample:', {
            sampleRows: formattedResult.rawData.rows.slice(0, 2)
          });
        }
      }
      
      // Build the response object
      const response: AnalyzeDataResponse = {
        content: content,
        needsConfirmation: false,
        originalQuery: sessionData.query
      };
      
      // Add rawData if available
      if (formattedResult.rawData) {
        response.rawData = formattedResult.rawData;
      }
      
      // Add rawData if available
      if (formattedResult.rawData) {
        response.rawData = formattedResult.rawData;
      }
      
      // Add processing steps if available
      if (processingSteps?.length) {
        response.processingSteps = processingSteps;
        console.log('Using provided processingSteps:', JSON.stringify(processingSteps, null, 2));
      } else if (formattedResult.processingSteps?.length) {
        response.processingSteps = formattedResult.processingSteps;
        console.log('Using processingSteps from formattedResult');
      } else if (queryExplanation) {
        // Create default processing steps from query explanation if none provided
        response.processingSteps = [{
          type: 'query_explanation',
          message: queryExplanation
        }];
        console.log('Created default processingSteps from queryExplanation');
      }
      
      // Add SQL if configured
      if (this.config.includeSqlInResponses) {
        response.sql = sql;
      }
      
      // Log the response structure for debugging
      console.log('Response structure:', {
        hasRawData: !!response.rawData,
        rawDataRowCount: response.rawData?.rows?.length || 0,
        hasProcessingSteps: !!response.processingSteps,
        processingStepsCount: response.processingSteps?.length || 0,
        hasSql: !!response.sql
      });
      
      // Log a sample of the raw data if it exists
      if (response.rawData?.rows?.length) {
        console.log('Raw data sample (first 2 rows):', 
          response.rawData.rows.slice(0, 2));
      }
      
      // Log the final response structure
      console.log('Final response structure:', {
        hasRawData: !!response.rawData,
        contentLength: response.content?.length || 0,
        processingSteps: response.processingSteps?.length || 0,
        hasSql: !!response.sql
      });
      
      return response;
    } catch (error) {
      logFlow('SERVER', 'ERROR', 'Error in executeAndFormatQuery', error);
      
      const errorResponse: AnalyzeDataResponse = {
        content: [{ type: 'text', text: `Error executing query: ${formatError(error)}` }],
        needsConfirmation: false
      };
      
      // Only include these fields if they have values
      if (this.config.includeSqlInResponses) {
        errorResponse.sql = sql;
      }
      
      if (sessionData?.query) {
        errorResponse.originalQuery = sessionData.query;
      }
      
      return errorResponse;
    }
  }
  
  /**
   * Start the session cleanup interval
   */
  private startSessionCleanup(): void {
    const cleanupIntervalMs = 60 * 60 * 1000; // 1 hour
    this.cleanupInterval = setInterval(() => {
      this.cleanupOldSessions();
    }, cleanupIntervalMs);
    logFlow('SERVER', 'INFO', 'Session cleanup started');
  }
  
  private cleanupOldSessions(): void {
    const now = Date.now();
    let cleanedCount = 0;
    
    try {
      for (const [sessionId, sessionData] of this.sessions.entries()) {
        const sessionAge = now - sessionData.timestamp;
        if (sessionAge > this.sessionMaxAgeMs) {
          this.sessions.delete(sessionId);
          cleanedCount++;
        }
      }
      
      if (cleanedCount > 0) {
        logFlow('SERVER', 'INFO', `Cleaned up ${cleanedCount} expired sessions`);
      }
    } catch (error) {
      logFlow('SERVER', 'ERROR', 'Error during session cleanup', error);
    }
  }
}