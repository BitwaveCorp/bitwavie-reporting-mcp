/**
 * LLM Query Translator Service
 * 
 * Uses Claude to translate natural language queries into SQL.
 * Handles both filter operations (population definition) and
 * aggregation/selection operations (results generation).
 */

import Anthropic from '@anthropic-ai/sdk';
import { logFlow } from '../utils/logging.js';
import { SchemaManager } from './schema-manager.js';

// Types
export interface TranslationConfig {
  maxRetries?: number; // Default: 2
  temperatureFilter?: number; // Default: 0.2
  temperatureAggregation?: number; // Default: 0.2
}

export interface QueryComponents {
  filterOperations: {
    description: string;
    sqlClause: string;
  };
  aggregationOperations: {
    description: string;
    sqlClause: string;
  };
  groupByOperations: {
    description: string;
    sqlClause: string;
  };
  orderByOperations: {
    description: string;
    sqlClause: string;
  };
  limitOperations: {
    description: string;
    sqlClause: string;
  };
}

export interface TranslationResult {
  originalQuery: string;
  interpretedQuery: string;
  sql: string;
  components: QueryComponents;
  requiresConfirmation: boolean;
  confidence: number; // 0-1
  alternativeInterpretations: string[] | undefined;
}

export class LLMQueryTranslator {
  private anthropic: Anthropic | null = null;
  private schemaManager: SchemaManager;
  private config: TranslationConfig = {
    maxRetries: 2,
    temperatureFilter: 0.2,
    temperatureAggregation: 0.2
  };
  
  constructor(schemaManager: SchemaManager, anthropicApiKey: string, config?: TranslationConfig) {
    this.schemaManager = schemaManager;
    
    // Initialize Anthropic client
    this.anthropic = new Anthropic({
      apiKey: anthropicApiKey
    });
    
    // Apply custom config if provided
    if (config) {
      this.config = { ...this.config, ...config };
    }
    
    logFlow('LLM_TRANSLATOR', 'INFO', 'LLM Query Translator initialized', {
      maxRetries: this.config.maxRetries,
      temperatureFilter: this.config.temperatureFilter,
      temperatureAggregation: this.config.temperatureAggregation
    });
  }
  
  /**
   * Helper method to safely extract text from an Anthropic response
   * @param response The response from Anthropic API
   * @returns The text content from the response
   * @throws Error if the response format is invalid
   */
  private getResponseText(response: any): string {
    if (!response || !response.content || response.content.length === 0) {
      throw new Error('Invalid response format: missing content');
    }
    
    const contentBlock = response.content[0];
    if (!contentBlock || !('text' in contentBlock)) {
      throw new Error('Invalid response format: content block does not contain text');
    }
    
    return contentBlock.text;
  }
  
  /**
   * Translate a natural language query into SQL
   * @param query The natural language query
   * @param previousContext Optional previous conversation context
   * @returns Translation result with SQL and components
   */
  public async translateQuery(
    query: string, 
    previousContext?: string
  ): Promise<TranslationResult> {
    if (!this.anthropic) {
      throw new Error('Anthropic client not initialized');
    }
    
    logFlow('LLM_TRANSLATOR', 'ENTRY', 'Translating query', { query, hasPreviousContext: !!previousContext });
    
    try {
      // First, analyze the query to understand filter operations (population definition)
      const filterComponents = await this.analyzeFilterOperations(query);
      
      // Then, analyze for aggregation/selection operations (results generation)
      const aggregationComponents = await this.analyzeAggregationOperations(query, filterComponents);
      
      // Generate the complete SQL query
      const sqlResult = await this.generateSQL(query, filterComponents, aggregationComponents);
      
      // Determine if confirmation is required based on confidence
      // Using a very high threshold (0.99) to ensure most queries require confirmation
      // This can be gradually reduced over time as the system improves
      const requiresConfirmation = sqlResult.confidence < 0.99;
      
      // Construct the final translation result
      const result: TranslationResult = {
        originalQuery: query,
        interpretedQuery: sqlResult.interpretedQuery,
        sql: sqlResult.sql,
        components: {
          filterOperations: {
            description: filterComponents.description,
            sqlClause: filterComponents.sqlClause
          },
          aggregationOperations: {
            description: aggregationComponents.aggregationDescription,
            sqlClause: aggregationComponents.aggregationClause
          },
          groupByOperations: {
            description: aggregationComponents.groupByDescription,
            sqlClause: aggregationComponents.groupByClause
          },
          orderByOperations: {
            description: aggregationComponents.orderByDescription,
            sqlClause: aggregationComponents.orderByClause
          },
          limitOperations: {
            description: aggregationComponents.limitDescription,
            sqlClause: aggregationComponents.limitClause
          }
        },
        requiresConfirmation,
        confidence: sqlResult.confidence,
        alternativeInterpretations: sqlResult.alternativeInterpretations
      };
      
      logFlow('LLM_TRANSLATOR', 'EXIT', 'Query translation completed', {
        confidence: result.confidence,
        requiresConfirmation: result.requiresConfirmation,
        sqlLength: result.sql.length
      });
      
      return result;
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      logFlow('LLM_TRANSLATOR', 'ERROR', 'Error translating query', { error: errorMessage });
      throw new Error(`Failed to translate query: ${errorMessage}`);
    }
  }
  
  /**
   * Analyze the filter operations (population definition) in a query
   * @param query The natural language query
   * @returns Filter components with description and SQL clause
   */
  private async analyzeFilterOperations(query: string): Promise<{
    description: string;
    sqlClause: string;
    confidence: number;
  }> {
    if (!this.anthropic) {
      throw new Error('Anthropic client not initialized');
    }
    
    logFlow('LLM_TRANSLATOR', 'ENTRY', 'Analyzing filter operations', { query });
    
    // Get schema information for the prompt
    const schemaInfo = this.schemaManager.getSchemaForLLM();
    
    // Create the prompt for filter operations
    const filterPrompt = `You are an expert SQL translator specializing in BigQuery. Your task is to analyze a natural language query and extract the filter operations (WHERE clause) that define the population of data to analyze.

${schemaInfo}

For the following query: "${query}"

Identify the filter conditions that define WHICH data should be included in the analysis. Consider:
1. Comparison operations (=, !=, >, >=, <, <=)
2. Range operations (BETWEEN, NOT BETWEEN)
3. Text matching (LIKE, NOT LIKE, STARTS_WITH, ENDS_WITH)
4. Set operations (IN, NOT IN)
5. Null handling (IS NULL, IS NOT NULL)
6. Date/time filters (specific dates, relative dates like "last 30 days")
7. Complex logic with AND/OR relationships and proper parenthetical grouping

Respond in JSON format with the following structure:
{
  "description": "A clear description of the filter conditions in plain English",
  "sqlClause": "The SQL WHERE clause that implements these filters (without the word WHERE)",
  "confidence": A number between 0 and 1 indicating your confidence in this interpretation
}

If there are no explicit filters in the query, use a default filter that includes all data (e.g., "1=1").`;

    try {
      // Call Claude to analyze filter operations
      const response = await this.anthropic.messages.create({
        model: 'claude-3-opus-20240229',
        max_tokens: 1000,
        temperature: this.config.temperatureFilter || 0.2,
        system: filterPrompt,
        messages: [
          { role: 'user', content: query }
        ]
      });
      
      // Parse the response
      const content = this.getResponseText(response);
      const jsonMatch = content.match(/\{[\s\S]*\}/);
      
      if (!jsonMatch) {
        throw new Error('Failed to parse filter operations response');
      }
      
      const filterComponents = JSON.parse(jsonMatch[0]);
      
      logFlow('LLM_TRANSLATOR', 'EXIT', 'Filter operations analysis completed', {
        description: filterComponents.description,
        sqlClauseLength: filterComponents.sqlClause.length,
        confidence: filterComponents.confidence
      });
      
      return filterComponents;
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      logFlow('LLM_TRANSLATOR', 'ERROR', 'Error analyzing filter operations', { error: errorMessage });
      
      // Provide a default response in case of error
      return {
        description: "Include all data (no filters specified)",
        sqlClause: "1=1",
        confidence: 0.5
      };
    }
  }
  
  /**
   * Analyze the aggregation/selection operations in a query
   * @param query The natural language query
   * @param filterComponents The filter components from previous analysis
   * @returns Aggregation components with descriptions and SQL clauses
   */
  private async analyzeAggregationOperations(
    query: string,
    filterComponents: { description: string; sqlClause: string; confidence: number; }
  ): Promise<{
    aggregationDescription: string;
    aggregationClause: string;
    groupByDescription: string;
    groupByClause: string;
    orderByDescription: string;
    orderByClause: string;
    limitDescription: string;
    limitClause: string;
    confidence: number;
  }> {
    if (!this.anthropic) {
      throw new Error('Anthropic client not initialized');
    }
    
    logFlow('LLM_TRANSLATOR', 'ENTRY', 'Analyzing aggregation operations', { 
      query,
      filterDescription: filterComponents.description
    });
    
    // Get schema information for the prompt
    const schemaInfo = this.schemaManager.getSchemaForLLM();
    const aggregatableColumns = this.schemaManager.getAggregatableColumns();
    
    // Create the prompt for aggregation operations
    const aggregationPrompt = `You are an expert SQL translator specializing in BigQuery. Your task is to analyze a natural language query and extract the aggregation and selection operations that define what results to generate.

${schemaInfo}

Aggregatable columns: ${aggregatableColumns.join(', ')}

For the following query: "${query}"

I've already determined the filter conditions:
${filterComponents.description}

Now, identify the aggregation and selection operations that define WHAT results to show. Consider:
1. Aggregation functions (SUM, COUNT, AVG, MIN, MAX, etc.)
2. Grouping dimensions (GROUP BY)
3. Sorting criteria (ORDER BY with ASC/DESC)
4. Result limits (LIMIT)
5. Window functions (ROW_NUMBER, RANK, etc.)

Respond in JSON format with the following structure:
{
  "aggregationDescription": "A clear description of the aggregation/selection in plain English",
  "aggregationClause": "The SQL SELECT clause that implements these operations",
  "groupByDescription": "Description of grouping dimensions, if any",
  "groupByClause": "The SQL GROUP BY clause without the words GROUP BY, or empty string if none",
  "orderByDescription": "Description of sorting criteria, if any",
  "orderByClause": "The SQL ORDER BY clause without the words ORDER BY, or empty string if none",
  "limitDescription": "Description of result limits, if any",
  "limitClause": "The SQL LIMIT clause without the word LIMIT, or empty string if none",
  "confidence": A number between 0 and 1 indicating your confidence in this interpretation
}

If the query doesn't specify any aggregation, default to selecting all columns.`;

    try {
      // Call Claude to analyze aggregation operations
      const response = await this.anthropic.messages.create({
        model: 'claude-3-opus-20240229',
        max_tokens: 1000,
        temperature: this.config.temperatureAggregation || 0.2,
        system: aggregationPrompt,
        messages: [
          { role: 'user', content: query }
        ]
      });
      
      // Parse the response
      const content = this.getResponseText(response);
      const jsonMatch = content.match(/\{[\s\S]*\}/);
      
      if (!jsonMatch) {
        throw new Error('Failed to parse aggregation operations response');
      }
      
      const aggregationComponents = JSON.parse(jsonMatch[0]);
      
      logFlow('LLM_TRANSLATOR', 'EXIT', 'Aggregation operations analysis completed', {
        aggregationDescription: aggregationComponents.aggregationDescription,
        hasGroupBy: !!aggregationComponents.groupByClause,
        hasOrderBy: !!aggregationComponents.orderByClause,
        hasLimit: !!aggregationComponents.limitClause,
        confidence: aggregationComponents.confidence
      });
      
      return aggregationComponents;
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      logFlow('LLM_TRANSLATOR', 'ERROR', 'Error analyzing aggregation operations', { error: errorMessage });
      
      // Provide a default response in case of error
      return {
        aggregationDescription: "Show all columns",
        aggregationClause: "*",
        groupByDescription: "",
        groupByClause: "",
        orderByDescription: "",
        orderByClause: "",
        limitDescription: "Limit to 100 results",
        limitClause: "100",
        confidence: 0.5
      };
    }
  }
  
  /**
   * Generate the complete SQL query
   * @param query The original natural language query
   * @param filterComponents The filter components
   * @param aggregationComponents The aggregation components
   * @returns The complete SQL query and metadata
   */
  private async generateSQL(
    query: string,
    filterComponents: { description: string; sqlClause: string; confidence: number; },
    aggregationComponents: {
      aggregationDescription: string;
      aggregationClause: string;
      groupByDescription: string;
      groupByClause: string;
      orderByDescription: string;
      orderByClause: string;
      limitDescription: string;
      limitClause: string;
      confidence: number;
    }
  ): Promise<{
    sql: string;
    interpretedQuery: string;
    confidence: number;
    alternativeInterpretations?: string[];
  }> {
    if (!this.anthropic) {
      throw new Error('Anthropic client not initialized');
    }
    
    logFlow('LLM_TRANSLATOR', 'ENTRY', 'Generating SQL', { 
      query,
      filterConfidence: filterComponents.confidence,
      aggregationConfidence: aggregationComponents.confidence
    });
    
    // Get table information
    const tableId = this.schemaManager.getSchema()?.columns[0]?.name 
      ? `\`${process.env.GOOGLE_CLOUD_PROJECT_ID}.${process.env.BIGQUERY_DATASET_ID}.${process.env.BIGQUERY_TABLE_ID}\``
      : 'actions'; // Default fallback
    
    // Create the SQL query
    let sql = `SELECT ${aggregationComponents.aggregationClause}\nFROM ${tableId}`;
    
    // Add WHERE clause if there are filters
    if (filterComponents.sqlClause && filterComponents.sqlClause !== '1=1') {
      sql += `\nWHERE ${filterComponents.sqlClause}`;
    }
    
    // Add GROUP BY clause if specified
    if (aggregationComponents.groupByClause) {
      sql += `\nGROUP BY ${aggregationComponents.groupByClause}`;
    }
    
    // Add ORDER BY clause if specified
    if (aggregationComponents.orderByClause) {
      sql += `\nORDER BY ${aggregationComponents.orderByClause}`;
    }
    
    // Add LIMIT clause if specified
    if (aggregationComponents.limitClause) {
      sql += `\nLIMIT ${aggregationComponents.limitClause}`;
    }
    
    // Create an interpreted query description
    let interpretedQuery = "I understand you want to ";
    
    if (filterComponents.description) {
      interpretedQuery += `find data where ${filterComponents.description}`;
    } else {
      interpretedQuery += "analyze all data";
    }
    
    if (aggregationComponents.aggregationDescription) {
      interpretedQuery += ` and ${aggregationComponents.aggregationDescription}`;
    }
    
    if (aggregationComponents.groupByDescription) {
      interpretedQuery += `, grouped by ${aggregationComponents.groupByDescription}`;
    }
    
    if (aggregationComponents.orderByDescription) {
      interpretedQuery += `, sorted by ${aggregationComponents.orderByDescription}`;
    }
    
    if (aggregationComponents.limitDescription) {
      interpretedQuery += `, ${aggregationComponents.limitDescription}`;
    }
    
    // Calculate overall confidence
    const confidence = (filterComponents.confidence + aggregationComponents.confidence) / 2;
    
    logFlow('LLM_TRANSLATOR', 'EXIT', 'SQL generation completed', {
      sqlLength: sql.length,
      confidence
    });
    
    return {
      sql,
      interpretedQuery,
      confidence
    };
  }
  
  /**
   * Handle SQL execution errors by attempting to correct the SQL
   * @param sql The original SQL that caused an error
   * @param errorMessage The error message from BigQuery
   * @returns Corrected SQL or null if correction failed
   */
  public async correctSQLError(
    sql: string, 
    errorMessage: string
  ): Promise<string | null> {
    if (!this.anthropic) {
      throw new Error('Anthropic client not initialized');
    }
    
    logFlow('LLM_TRANSLATOR', 'ENTRY', 'Attempting to correct SQL error', { 
      sqlLength: sql.length,
      errorMessage
    });
    
    // Create the prompt for SQL correction
    const correctionPrompt = `You are an expert SQL debugger specializing in BigQuery. Your task is to fix a SQL query that has produced an error.

Original SQL query:
\`\`\`sql
${sql}
\`\`\`

Error message:
${errorMessage}

Please analyze the error and provide a corrected SQL query that addresses the issue. Focus only on fixing the specific error while maintaining the original query's intent.

Respond with ONLY the corrected SQL query, nothing else.`;

    try {
      // Call Claude to correct the SQL
      const response = await this.anthropic.messages.create({
        model: 'claude-3-opus-20240229',
        max_tokens: 1000,
        temperature: 0.1, // Low temperature for more precise corrections
        system: correctionPrompt,
        messages: [
          { role: 'user', content: "Please fix this SQL query" }
        ]
      });
      
      // Extract the corrected SQL
      const content = this.getResponseText(response);
      const sqlMatch = content.match(/```sql\s*([\s\S]*?)\s*```/) || content.match(/`([\s\S]*?)`/) || [null, content.trim()];
      
      if (!sqlMatch || !sqlMatch[1]) {
        throw new Error('Failed to extract corrected SQL');
      }
      
      const correctedSql = sqlMatch[1].trim();
      
      logFlow('LLM_TRANSLATOR', 'EXIT', 'SQL correction completed', {
        originalLength: sql.length,
        correctedLength: correctedSql.length
      });
      
      return correctedSql;
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      logFlow('LLM_TRANSLATOR', 'ERROR', 'Error correcting SQL', { error: errorMessage });
      return null;
    }
  }
}
