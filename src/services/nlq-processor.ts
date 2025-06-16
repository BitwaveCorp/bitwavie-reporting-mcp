/**
 * Natural Language Query Processor
 * 
 * Integrates all the services for processing natural language queries.
 * This is the main entry point for the refactored natural language query flow.
 */

import { logFlow } from '../utils/logging.js';
import { SchemaManager } from './schema-manager.js';
import { LLMQueryTranslator, TranslationResult } from './llm-query-translator.js';
import { QueryConfirmationFormatter } from './query-confirmation-formatter.js';
import { QueryExecutor, ExecutionResult } from './query-executor.js';
import { ResultFormatter } from './result-formatter.js';

// Types
export interface NLQProcessorConfig {
  projectId: string;
  datasetId: string;
  tableId: string;
  anthropicApiKey: string;
  schemaRefreshIntervalMs?: number;
  maxRetries?: number;
  includeSQL?: boolean;
}

export interface ProcessingState {
  originalQuery: string;
  translationResult?: TranslationResult;
  executionResult?: ExecutionResult;
  confirmedByUser: boolean;
  retryCount: number;
  previousQueries?: string[];
}

export class NLQProcessor {
  private schemaManager: SchemaManager;
  private llmTranslator: LLMQueryTranslator;
  private confirmationFormatter: QueryConfirmationFormatter;
  private queryExecutor: QueryExecutor;
  private resultFormatter: ResultFormatter;
  private config: NLQProcessorConfig;
  
  // Processing state
  private processingStates: Map<string, ProcessingState> = new Map();
  
  constructor(config: NLQProcessorConfig) {
    this.config = config;
    
    // Initialize schema manager
    this.schemaManager = new SchemaManager();
    
    // Initialize LLM translator
    this.llmTranslator = new LLMQueryTranslator(
      this.schemaManager,
      config.anthropicApiKey,
      { maxRetries: config.maxRetries || 2 }
    );
    
    // Initialize confirmation formatter
    this.confirmationFormatter = new QueryConfirmationFormatter({
      includeSQL: config.includeSQL || false,
      suggestAlternatives: true
    });
    
    // Initialize query executor
    this.queryExecutor = new QueryExecutor(
      config.projectId,
      this.llmTranslator,
      { maxRetries: config.maxRetries || 2 }
    );
    
    // Initialize result formatter
    this.resultFormatter = new ResultFormatter({
      includePerformanceMetrics: true,
      suggestVisualizations: true
    });
    
    logFlow('NLQ_PROCESSOR', 'INFO', 'Natural Language Query Processor initialized', {
      projectId: config.projectId,
      datasetId: config.datasetId,
      tableId: config.tableId,
      includeSQL: config.includeSQL
    });
  }
  
  /**
   * Initialize the processor
   * Configures the schema manager and performs initial schema fetch
   */
  public async initialize(): Promise<void> {
    logFlow('NLQ_PROCESSOR', 'ENTRY', 'Initializing NLQ Processor');
    
    try {
      // Configure schema manager
      await this.schemaManager.configure({
        projectId: this.config.projectId,
        datasetId: this.config.datasetId,
        tableId: this.config.tableId,
        ...(this.config.schemaRefreshIntervalMs !== undefined && {
          refreshIntervalMs: this.config.schemaRefreshIntervalMs
        })
      });
      
      logFlow('NLQ_PROCESSOR', 'EXIT', 'NLQ Processor initialized successfully');
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      logFlow('NLQ_PROCESSOR', 'ERROR', 'Failed to initialize NLQ Processor', { error: errorMessage });
      throw new Error(`Failed to initialize NLQ Processor: ${errorMessage}`);
    }
  }
  
  /**
   * Process a natural language query
   * @param query The natural language query
   * @param sessionId Optional session ID for maintaining state
   * @param previousResponse Optional previous response for context
   * @returns Response with content and confirmation status
   */
  public async processQuery(
    query: string,
    sessionId: string = 'default',
    previousResponse?: any
  ): Promise<any> {
    logFlow('NLQ_PROCESSOR', 'ENTRY', 'Processing query', {
      query,
      sessionId,
      hasPreviousResponse: !!previousResponse
    });
    
    try {
      // Get or create processing state for this session
      let state = this.processingStates.get(sessionId);
      
      if (!state) {
        state = {
          originalQuery: query,
          confirmedByUser: false,
          retryCount: 0,
          previousQueries: []
        };
        this.processingStates.set(sessionId, state);
      }
      
      // Check if this is a confirmation response
      if (previousResponse && previousResponse.needsConfirmation) {
        return await this.handleConfirmationResponse(query, sessionId, state);
      }
      
      // This is a new query
      state.originalQuery = query;
      state.confirmedByUser = false;
      state.retryCount = 0;
      
      // Add to previous queries
      if (!state.previousQueries) {
        state.previousQueries = [];
      }
      state.previousQueries.push(query);
      
      // Translate the query
      const translationResult = await this.llmTranslator.translateQuery(query);
      state.translationResult = translationResult;
      
      // If translation requires confirmation, format and return confirmation
      if (translationResult.requiresConfirmation) {
        const confirmationResponse = this.confirmationFormatter.formatConfirmation(translationResult);
        
        logFlow('NLQ_PROCESSOR', 'EXIT', 'Query requires confirmation', {
          needsConfirmation: true
        });
        
        return confirmationResponse;
      }
      
      // If no confirmation required, execute the query
      return await this.executeQueryAndFormatResults(state, sessionId);
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      logFlow('NLQ_PROCESSOR', 'ERROR', 'Error processing query', { error: errorMessage });
      
      // Format error response
      return this.confirmationFormatter.formatErrorResponse(
        query,
        errorMessage
      );
    }
  }
  
  /**
   * Handle a confirmation response from the user
   * @param response The user's response
   * @param sessionId The session ID
   * @param state The current processing state
   * @returns Response with content or confirmation
   */
  private async handleConfirmationResponse(
    response: string,
    sessionId: string,
    state: ProcessingState
  ): Promise<any> {
    logFlow('NLQ_PROCESSOR', 'ENTRY', 'Handling confirmation response', {
      response,
      sessionId
    });
    
    // Check if the response is a confirmation
    const lowerResponse = response.toLowerCase().trim();
    const isConfirmation = lowerResponse === 'yes' || 
                          lowerResponse === 'correct' ||
                          lowerResponse === 'confirm' ||
                          lowerResponse === 'ok' ||
                          lowerResponse === 'looks good';
    
    if (isConfirmation) {
      // User confirmed the query interpretation
      state.confirmedByUser = true;
      
      // Execute the query and format results
      return await this.executeQueryAndFormatResults(state, sessionId);
    } else {
      // User provided a modification or new query
      // Treat this as a new query
      state.originalQuery = response;
      state.confirmedByUser = false;
      state.retryCount = 0;
      
      // Add to previous queries
      if (!state.previousQueries) {
        state.previousQueries = [];
      }
      state.previousQueries.push(response);
      
      // Translate the new query
      const translationResult = await this.llmTranslator.translateQuery(response);
      state.translationResult = translationResult;
      
      // Format confirmation
      const confirmationResponse = this.confirmationFormatter.formatConfirmation(translationResult);
      
      logFlow('NLQ_PROCESSOR', 'EXIT', 'Modified query requires confirmation', {
        needsConfirmation: true
      });
      
      return confirmationResponse;
    }
  }
  
  /**
   * Execute the query and format the results
   * @param state The current processing state
   * @param sessionId The session ID
   * @returns Formatted results
   */
  private async executeQueryAndFormatResults(
    state: ProcessingState,
    sessionId: string
  ): Promise<any> {
    logFlow('NLQ_PROCESSOR', 'ENTRY', 'Executing query and formatting results', {
      sessionId,
      confirmedByUser: state.confirmedByUser
    });
    
    if (!state.translationResult) {
      throw new Error('No translation result available');
    }
    
    try {
      // Execute the query
      const executionResult = await this.queryExecutor.executeQuery(
        state.translationResult.sql
      );
      
      state.executionResult = executionResult;
      
      // If execution failed and we haven't reached max retries, try to correct
      if (!executionResult.success && state.retryCount < (this.config.maxRetries || 2)) {
        state.retryCount++;
        
        logFlow('NLQ_PROCESSOR', 'INFO', 'Query execution failed, attempting correction', {
          error: executionResult.error?.message,
          retryCount: state.retryCount
        });
        
        // If we have an error message, try to correct the SQL
        if (executionResult.error?.message && this.llmTranslator) {
          const correctedSql = await this.llmTranslator.correctSQLError(
            state.translationResult.sql,
            executionResult.error.message
          );
          
          if (correctedSql) {
            // Update the translation result with corrected SQL
            state.translationResult.sql = correctedSql;
            
            // Try executing again
            return await this.executeQueryAndFormatResults(state, sessionId);
          }
        }
      }
      
      // Format the results
      const formattedResult = this.resultFormatter.formatResults(
        executionResult,
        state.translationResult
      );
      
      logFlow('NLQ_PROCESSOR', 'EXIT', 'Query execution and formatting completed', {
        success: executionResult.success,
        rowCount: formattedResult.metadata.rowCount
      });
      
      // Return the formatted result
      return {
        content: formattedResult.content,
        needsConfirmation: false,
        metadata: formattedResult.metadata
      };
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      logFlow('NLQ_PROCESSOR', 'ERROR', 'Error executing query', { error: errorMessage });
      
      // Format error response
      return this.confirmationFormatter.formatErrorResponse(
        state.originalQuery,
        errorMessage,
        state.translationResult.sql
      );
    }
  }
  
  /**
   * Get available columns for selection
   * @returns Array of column names
   */
  public async getAvailableColumns(): Promise<string[]> {
    return this.schemaManager.getColumnNames();
  }
  
  /**
   * Format column selection options
   * @param query The original query
   * @param message Optional message to include
   * @returns Formatted column selection response
   */
  public async formatColumnSelectionOptions(
    query: string,
    message?: string
  ): Promise<any> {
    const columns = await this.getAvailableColumns();
    return this.confirmationFormatter.formatColumnSelectionOptions(columns, query, message);
  }
  
  /**
   * Clean up resources when the processor is no longer needed
   */
  public dispose(): void {
    this.schemaManager.dispose();
    this.processingStates.clear();
    
    logFlow('NLQ_PROCESSOR', 'INFO', 'NLQ Processor disposed');
  }
}
