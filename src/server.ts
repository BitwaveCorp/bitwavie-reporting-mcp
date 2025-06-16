import express from 'express';
import http from 'http';
import { z } from 'zod';
import Anthropic from '@anthropic-ai/sdk';
import { ReportParameters } from './types/actions-report';

// Import modular services
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
  needsConfirmation?: boolean;
  sql?: string;
  error?: string;
  originalQuery?: string;
  translationResult?: any;
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
      useEnhancedNLQ: config.useEnhancedNLQ !== undefined ? config.useEnhancedNLQ : false,
      includeSqlInResponses: config.includeSqlInResponses !== undefined ? config.includeSqlInResponses : false,
      schemaRefreshIntervalMs: config.schemaRefreshIntervalMs || 3600000 // Default: 1 hour
    };

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
              return res.json(toolsResponse);
              
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

      // Start HTTP server
      this.httpServer = this.server.listen(this.config.port, () => {
        logFlow('SERVER', 'INFO', `Server started on port ${this.config.port}`);
      });
      
      // Start session cleanup
      if (this.cleanupInterval === null) {
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
      
      // Check if it's a confirmation response
      if (request.previousResponse?.needsConfirmation && request.confirmedMappings) {
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
      
      // Route to enhanced or legacy flow based on config
      if (this.config.useEnhancedNLQ && this.llmQueryTranslator) {
        return this.processEnhancedNLQ(sessionId, query);
      } else {
        return this.processLegacyNLQ(query);
      }
    } catch (error: any) {
      logFlow('SERVER', 'ERROR', 'Error in handleAnalyzeData:', error);
      
      // Provide fallback to legacy processing when enhanced flow fails
      if (this.config.useEnhancedNLQ && request.query) {
        logFlow('SERVER', 'WARN', 'Attempting fallback to legacy processing');
        try {
          return this.processLegacyNLQ(request.query);
        } catch (fallbackError: any) {
          logFlow('SERVER', 'ERROR', 'Fallback also failed:', fallbackError);
        }
      }
      
      // Return user-friendly error messages
      return {
        content: [{ type: 'text', text: `Error processing query: ${formatError(error)}` }],
        error: formatError(error)
      };
    }
  }

  private async processEnhancedNLQ(sessionId: string, query: string): Promise<AnalyzeDataResponse> {
    try {
      logFlow('SERVER', 'INFO', `Processing enhanced NLQ for session: ${sessionId}`);
      
      // Check if LLM translator is initialized
      if (!this.llmQueryTranslator) {
        throw new Error('LLM query translator not initialized');
      }
      
      // Translate query using LLM
      const translationResult = await this.llmQueryTranslator.translateQuery(query);
      
      // Update session with translation result
      const sessionData = this.sessions.get(sessionId);
      if (!sessionData) {
        throw new Error(`Session ${sessionId} not found`);
      }
      
      sessionData.translationResult = translationResult;
      this.sessions.set(sessionId, sessionData);
      
      // Check if translation failed or has errors
      if (!translationResult) {
        logFlow('SERVER', 'WARN', 'Translation failed, falling back to legacy');
        return this.processLegacyNLQ(query);
      }
      
      // Check confidence threshold
      if (translationResult.confidence < 0.7) {
        // Check if confirmation formatter is initialized
        if (!this.queryConfirmationFormatter) {
          throw new Error('Query confirmation formatter not initialized');
        }
        
        // Request confirmation if needed
        const confirmationResponse = this.queryConfirmationFormatter.formatConfirmation(translationResult);
        sessionData.confirmationResponse = confirmationResponse;
        this.sessions.set(sessionId, sessionData);
        
        return {
          content: confirmationResponse.content,
          needsConfirmation: true,
          sql: this.config.includeSqlInResponses ? translationResult.sql : '',
          originalQuery: sessionData.query
        };
      }
      
      // If confidence is high, execute directly
      return this.executeAndFormatQuery(sessionId, translationResult.sql);
    } catch (error) {
      logFlow('SERVER', 'ERROR', `Enhanced NLQ processing failed for session ${sessionId}:`, error);
      // Fallback to legacy processing
      return this.processLegacyNLQ(query);
    }
  }

  private async processLegacyNLQ(query: string): Promise<AnalyzeDataResponse> {
    try {
      logFlow('SERVER', 'INFO', `Processing legacy NLQ: ${query}`);
      
      if (!this.queryParser) {
        throw new Error('QueryParser not initialized');
      }
      
      // Parse using legacy QueryParser
      const parsedQuery = this.queryParser.parseQuery(query);
      
      if (!parsedQuery) {
        return {
          content: [{ type: 'text', text: `Unable to parse query` }],
          error: 'Query parsing failed',
          sql: '',
          originalQuery: query
        };
      }
      
      if (!this.bigQueryClient) {
        throw new Error('BigQueryClient not initialized');
      }
      
      // Execute SQL using BigQueryClient
      // Create parameters object with required fields
      const parameters: ReportParameters = {
        runId: `legacy-nlq-${Date.now()}`
        // Optional fields are not explicitly set
      };
      const result = await this.bigQueryClient.executeAnalyticalQuery(parsedQuery, parameters);
      
      // Type assertion for QueryResult to access rows and columns
      interface QueryResultWithData {
        rows?: any[];
        columns?: string[];
        metadata?: {
          execution_time_ms: number;
          rows_processed: number;
          cached: boolean;
          columns_used: string[];
          generatedSql?: string;
        };
        generatedSql?: string;
      }
      
      // Cast result to the expected structure
      const typedResult = result as unknown as QueryResultWithData;
      const rows = Array.isArray(typedResult.rows) ? typedResult.rows : [];
      const columns = Array.isArray(typedResult.columns) ? typedResult.columns : [];
      
      // Format results in compatible format
      const formattedResult: FormattedResult = {
        content: [
          {
            type: 'text',
            text: `Query executed successfully. Found ${rows.length} results.`
          },
          {
            type: 'table',
            table: {
              headers: columns,
              rows: rows
            }
          }
        ],
        metadata: {
          executionTimeMs: result.metadata?.execution_time_ms || 0,
          bytesProcessed: result.metadata?.rows_processed,
          cacheHit: result.metadata?.cached || false,
          rowCount: rows.length,
          totalRows: rows.length
        }
      };
      
      // For legacy parser, we need to extract SQL if available
      let sqlToInclude = '';
      if (this.config.includeSqlInResponses) {
        // Try to get SQL from metadata or any other available source
        if (parsedQuery && parsedQuery.metadata && typeof parsedQuery.metadata === 'object') {
          // Check if there's a SQL property in metadata
          const metadata = parsedQuery.metadata as Record<string, any>;
          // Use type assertion to access potential generatedSql property
          const extendedMetadata = metadata as { generatedSql?: string; sql?: string };
          const extendedParsedQuery = parsedQuery as unknown as { generatedSql?: string };
          
          if (extendedMetadata.generatedSql) {
            sqlToInclude = extendedMetadata.generatedSql;
          } else if (extendedMetadata.sql) {
            sqlToInclude = extendedMetadata.sql;
          } else if (extendedParsedQuery.generatedSql) {
            sqlToInclude = extendedParsedQuery.generatedSql;
          }
        }
      }
      
      return {
        content: formattedResult.content,
        sql: sqlToInclude,
        originalQuery: query
      };
    } catch (error) {
      logFlow('SERVER', 'ERROR', 'Legacy NLQ processing failed:', error);
      // Handle error case
      let sqlToInclude = '';
      
      return {
        content: [{ type: 'text', text: `Error executing query: ${formatError(error)}` }],
        error: formatError(error),
        sql: sqlToInclude,
        originalQuery: query
      };
    }
  }
  
  private async handleConfirmationResponse(request: AnalyzeDataRequest): Promise<AnalyzeDataResponse> {
    try {
      logFlow('SERVER', 'INFO', 'Processing confirmation response');
      
      // Find the session from previousResponse
      const translationResult = request.previousResponse.translationResult;
      if (!translationResult) {
        return {
          content: [{ type: 'text', text: 'Invalid confirmation request - missing translation result.' }],
          error: 'Missing translation result'
        };
      }
      
      // Process confirmed mappings
      let sqlToExecute = translationResult.sql;
      
      if (request.confirmedMappings) {
        // Handle confirmed queries, alternative selections, and corrections
        for (const [placeholder, value] of Object.entries(request.confirmedMappings)) {
          sqlToExecute = sqlToExecute.replace(placeholder, value);
        }
      }
      
      // Generate new session for confirmed query
      const sessionId = generateSessionId(translationResult.originalQuery + '_confirmed');
      const sessionData: SessionData = {
        query: translationResult.originalQuery,
        translationResult: {
          ...translationResult,
          sql: sqlToExecute
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

  private async executeAndFormatQuery(sessionId: string, sql: string): Promise<AnalyzeDataResponse> {
    const sessionData = this.sessions.get(sessionId);
    
    if (!sessionData) {
      return {
        content: [{ type: 'text', text: 'Session expired or not found' }],
        error: 'Session not found',
        sql: this.config.includeSqlInResponses ? sql : '',
        originalQuery: ''
      };
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
          originalQuery: sessionData.query
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
      
      const formattedResult = this.resultFormatter.formatResults(
        formatterInput,
        sessionData.translationResult
      );
      
      return {
        content: formattedResult.content,
        sql: this.config.includeSqlInResponses ? sql : '',
        originalQuery: sessionData.query
      };
    } catch (error) {
      logFlow('SERVER', 'ERROR', 'Error executing query', error);
      
      return {
        content: [{ type: 'text', text: `Error executing query: ${formatError(error)}` }],
        error: formatError(error),
        sql: this.config.includeSqlInResponses ? sql : '',
        originalQuery: sessionData.query || ''
      };
    }
  }

  // Session management helper methods
  private startSessionCleanup(): void {
    const cleanupInterval = 60 * 60 * 1000; // 1 hour
    this.cleanupInterval = setInterval(() => {
      this.cleanupOldSessions();
    }, cleanupInterval);
    logFlow('SERVER', 'INFO', 'Session cleanup started');
  }
  
  private cleanupOldSessions(): void {
    const now = Date.now();
    let cleanedCount = 0;
    
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
  }
}