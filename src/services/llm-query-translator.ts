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
import { ReportRegistry } from './report-registry.js';
import { ConnectionManager } from './connection-manager.js';

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
  isReportQuery?: boolean;
  reportType?: string | undefined;
  reportParameters?: Record<string, any> | undefined;
  processingSteps?: Array<{step: string, description: string}> | undefined;
  prompt?: string | undefined; // The original prompt sent to the LLM
}

export class LLMQueryTranslator {
  private anthropic: Anthropic | null = null;
  private schemaManager: SchemaManager;
  private reportRegistry: ReportRegistry;
  private config: TranslationConfig = {
    maxRetries: 2,
    temperatureFilter: 0.2,
    temperatureAggregation: 0.2
  };
  
  constructor(schemaManager: SchemaManager, anthropicApiKey: string, reportRegistry: ReportRegistry, config?: TranslationConfig, anthropicProjectId?: string) {
    this.schemaManager = schemaManager;
    this.reportRegistry = reportRegistry;
    
    // Initialize Anthropic client
    this.anthropic = new Anthropic({
      apiKey: anthropicApiKey,
      defaultHeaders: {
        'anthropic-project': anthropicProjectId || 'bitwave-solutions'
      }
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
   * Detect if a query is requesting a predefined report and extract parameters
   * 
   * @param query The natural language query
   * @returns Detection result with report type and parameters if it's a report query
   */
  async detectReportQuery(query: string, reportContext?: Array<{
    id: string;
    name: string;
    description: string;
    requiredParameters: Array<{name: string, description: string, type: string, required: boolean}>;
  }>): Promise<{
    isReportQuery: boolean;
    reportType?: string;
    reportParameters?: Record<string, any>;
    missingRequiredParameters?: string[];
    confidence: number;
    suggestedReports?: Array<{name: string, confidence: number}>;
  }> {
    try {
      if (!this.anthropic) {
        throw new Error('Anthropic client not initialized');
      }

      // Use provided report context or get from registry
      let availableReports;
      let reportsInfo;
      
      if (reportContext) {
        // Use the provided report context
        availableReports = reportContext;
        
        // Format the provided reports for the prompt
        reportsInfo = reportContext.map(report => {
          // Separate required and optional parameters
          const requiredParams = report.requiredParameters.filter(param => param.required);
          const optionalParams = report.requiredParameters.filter(param => !param.required);
          
          const requiredParamInfo = requiredParams
            .map(param => `${param.name} (${param.type}): ${param.description}`)
            .join('\n   ');
            
          const optionalParamInfo = optionalParams
            .map(param => `${param.name} (${param.type}): ${param.description}`)
            .join('\n   ');
            
          return `- ${report.name} (ID: ${report.id}): ${report.description}
   Required parameters:\n   ${requiredParamInfo || 'None'}\n   Optional parameters:\n   ${optionalParamInfo || 'None'}`;
        }).join('\n\n');
      } else {
        // Get all available reports from registry
        availableReports = this.reportRegistry.getAllReports();
        
        if (availableReports.length === 0) {
          return { isReportQuery: false, confidence: 0 };
        }
        
        // Format reports for the prompt
        reportsInfo = availableReports.map((report: any) => {
          return `- ${report.name}: ${report.description}\n   Keywords: ${report.keywords?.join(', ') || 'None'}\n   Required parameters: ${report.requiredParameters?.join(', ') || 'None'}\n   Optional parameters: ${report.optionalParameters?.join(', ') || 'None'}`;
        }).join('\n\n');
      }
      
      logFlow('LLM_TRANSLATOR', 'INFO', 'Detecting if query is for a predefined report', { query });
      
      const response = await this.anthropic.messages.create({
        model: 'claude-3-opus-20240229',
        max_tokens: 1000,
        temperature: 0.2,
        messages: [
          {
            role: 'user',
            content: `You are an expert in natural language understanding for financial reporting systems. Your task is to determine if the user's query is requesting one of our predefined reports, and if so, extract the parameters needed for that report.

Available predefined reports:
${reportsInfo}

User query: "${query}"

IMPORTANT INSTRUCTIONS:
1. Identify which report the user is requesting based on the available reports.
2. CRITICAL: If the query contains parameters (like startDate, endDate, asOfDate, runId), use these to help identify which report is being requested. For example, if the query mentions "startDate" and "endDate", it likely refers to the valuation-rollforward report.
3. Extract any parameters mentioned in the query, even if they're expressed in natural language.
4. For date parameters:
   - Convert natural language date references (e.g., "January", "last quarter", "Q1", "this year") to proper date formats (YYYY-MM-DD).
   - When a range is mentioned (e.g., "January through March"), extract both start and end dates.
   - Use the current year if no year is specified.
5. Match parameters to the required parameters for the identified report.
6. If the report requires parameters that weren't provided, note them as missing.
7. Pay special attention to the parameter patterns - each report has specific parameter requirements that can help identify it.

Analyze the query and respond in JSON format with the following structure:
{
  "isReportQuery": boolean, // true if the query is requesting one of the predefined reports
  "reportType": string, // the ID of the requested report, or null if not a report query
  "confidence": number, // 0.0-1.0 indicating confidence in the detection
  "parameters": { // extracted parameters for the report
    // Include all parameters you can extract from the query
    // Convert natural language dates to YYYY-MM-DD format
    // Use appropriate data types (string, number, boolean, array)
  },
  "missingRequiredParameters": [ // required parameters that weren't provided in the query
    "paramName1",
    "paramName2"
  ],
  "suggestedReports": [ // only include if isReportQuery is false but query is related to available reports
    {
      "name": string, // name of a suggested report
      "confidence": number // 0.0-1.0 indicating relevance
    }
  ]
}

Only return valid JSON. Do not include any explanations or additional text outside the JSON structure.`
          }
        ]
      });
      
      const responseText = this.getResponseText(response);
      
      try {
        const result = JSON.parse(responseText);
        
        logFlow('LLM_TRANSLATOR', 'INFO', 'Report detection result', { 
          isReportQuery: result.isReportQuery,
          reportType: result.reportType,
          confidence: result.confidence,
          parameters: result.parameters,
          suggestedReports: result.suggestedReports
        });
        
        return {
          isReportQuery: result.isReportQuery,
          reportType: result.reportType || undefined,
          reportParameters: result.parameters || {},
          missingRequiredParameters: result.missingRequiredParameters || [],
          confidence: result.confidence,
          suggestedReports: result.suggestedReports
        };
      } catch (error) {
        logFlow('LLM_TRANSLATOR', 'ERROR', 'Failed to parse report detection response', { error, responseText });
        return { isReportQuery: false, confidence: 0 };
      }
    } catch (error) {
      logFlow('LLM_TRANSLATOR', 'ERROR', 'Error detecting report query', { error });
      return { isReportQuery: false, confidence: 0 };
    }
  }
  
  /**
   * Translate a natural language query into SQL
   * @param query The natural language query
   * @param previousContext Optional previous conversation context
   * @returns Translation result with SQL and components
   */
  public async translateQuery(
    query: string, 
    previousContext?: string,
    connectionDetails?: { projectId?: string, datasetId?: string, tableId?: string, privateKey?: string, schemaType?: string }
  ): Promise<TranslationResult> {
    if (!this.anthropic) {
      throw new Error('Anthropic client not initialized');
    }
    
    logFlow('LLM_TRANSLATOR', 'ENTRY', 'Translating query', { 
      query, 
      hasPreviousContext: !!previousContext,
      hasConnectionDetails: !!connectionDetails,
      hasSchemaType: !!connectionDetails?.schemaType,
      schemaType: connectionDetails?.schemaType || 'Not provided',
      connectionSource: connectionDetails ? 'session' : 'environment'
    });

    
    
    try {
      // Configure the SchemaManager with the schema type if provided
      if (connectionDetails?.schemaType) {
        logFlow('LLM_TRANSLATOR', 'INFO', 'Configuring SchemaManager with schema type in translateQuery', {
          schemaType: connectionDetails.schemaType
        });
        
        // Configure the SchemaManager with the connection details including schema type
        await this.schemaManager.configure({
          projectId: connectionDetails.projectId || '',
          datasetId: connectionDetails.datasetId || '',
          tableId: connectionDetails.tableId || '',
          schemaType: connectionDetails.schemaType
        }, { session: { connectionDetails } });
      }
      
      // Step 0: Check if this is a report query
      const reportDetection = await this.detectReportQuery(query);
      
      // If this is a high-confidence report query, return early with report info
      if (reportDetection.isReportQuery && reportDetection.confidence > 0.8 && reportDetection.reportType) {
        const processingSteps = [
          {
            step: 'Report Detection',
            description: `Detected request for predefined report: ${reportDetection.reportType}`
          },
          {
            step: 'Parameter Extraction',
            description: `Extracted parameters: ${JSON.stringify(reportDetection.reportParameters)}`
          }
        ];
        
        return {
          originalQuery: query,
          interpretedQuery: `Generate ${reportDetection.reportType} with parameters: ${JSON.stringify(reportDetection.reportParameters)}`,
          sql: '', // No SQL for report queries, will be generated by the report generator
          components: {
            filterOperations: { description: '', sqlClause: '' },
            aggregationOperations: { description: '', sqlClause: '' },
            groupByOperations: { description: '', sqlClause: '' },
            orderByOperations: { description: '', sqlClause: '' },
            limitOperations: { description: '', sqlClause: '' }
          },
          requiresConfirmation: false,
          confidence: reportDetection.confidence,
          alternativeInterpretations: undefined,
          isReportQuery: true,
          reportType: reportDetection.reportType,
          reportParameters: reportDetection.reportParameters,
          processingSteps
        };
      }
      
      // Step 1: Analyze filter operations (population definition)
      const filterComponents = await this.analyzeFilterOperations(query);
      
      // Step 2: Analyze aggregation/selection operations (results generation)
      const aggregationComponents = await this.analyzeAggregationOperations(query, filterComponents);
      
      // Step 3: Generate SQL from the components
      const sqlResult = await this.generateSQL(query, filterComponents, aggregationComponents, connectionDetails);
      
      // Determine if confirmation is required based on confidence
      const requiresConfirmation = sqlResult.confidence < 0.7;
      
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
        alternativeInterpretations: sqlResult.alternativeInterpretations,
        prompt: sqlResult.prompt, // Include the prompt in the result
        processingSteps: [
          { step: 'Query Analysis', description: 'Analyzing natural language query' },
          { step: 'Filter Operations', description: filterComponents.description },
          { step: 'Aggregation Operations', description: aggregationComponents.aggregationDescription },
          { step: 'SQL Generation', description: 'Generating optimized SQL query' }
        ]
      };
      
      // If we have suggested reports, include them in the result
      if (reportDetection.suggestedReports && reportDetection.suggestedReports.length > 0) {
        const highConfidenceSuggestion = reportDetection.suggestedReports.find(r => r.confidence > 0.7);
        if (highConfidenceSuggestion) {
          result.processingSteps?.push({
            step: 'Report Suggestion',
            description: `You might be interested in the ${highConfidenceSuggestion.name} report which is related to your query.`
          });
        }
      }
      
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
  
  // Log the prompt for debugging
  console.log('PROMPT_SENT_TO_LLM:\n', filterPrompt);

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
  
  // Log the prompt for debugging
  console.log('PROMPT_SENT_TO_LLM:\n', aggregationPrompt);

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
    },
    connectionDetails?: { projectId?: string, datasetId?: string, tableId?: string, privateKey?: string, schemaType?: string }
  ): Promise<{
    sql: string;
    interpretedQuery: string;
    confidence: number;
    alternativeInterpretations?: string[];
    prompt?: string; // Store the prompt used for SQL generation
  }> {
    if (!this.anthropic) {
      throw new Error('Anthropic client not initialized');
    }
    
    logFlow('LLM_TRANSLATOR', 'ENTRY', 'Generating SQL', { 
      query,
      filterConfidence: filterComponents.confidence,
      aggregationConfidence: aggregationComponents.confidence
    });
    
    // Get table information using ConnectionManager
    const connectionManager = ConnectionManager.getInstance();
    
    // Configure the SchemaManager with the schema type if provided
    if (connectionDetails?.schemaType) {
      logFlow('LLM_TRANSLATOR', 'INFO', 'Configuring SchemaManager with schema type', {
        schemaType: connectionDetails.schemaType
      });
      
      // Configure the SchemaManager with the connection details including schema type
      await this.schemaManager.configure({
        projectId: connectionDetails.projectId || '',
        datasetId: connectionDetails.datasetId || '',
        tableId: connectionDetails.tableId || '',
        schemaType: connectionDetails.schemaType
      }, { session: { connectionDetails } });
    }
    
    const columnName = this.schemaManager.getSchema()?.columns[0]?.name;
    
    // Ensure connectionDetails is properly initialized if provided
    if (connectionDetails) {
      // Make sure none of the properties are undefined
      connectionDetails = {
        projectId: connectionDetails.projectId || '',
        datasetId: connectionDetails.datasetId || '',
        tableId: connectionDetails.tableId || '',
        privateKey: connectionDetails.privateKey || '',
        schemaType: connectionDetails.schemaType || ''
      };
    }
    
    // Log connection details (safely - without private key)
    const safeDetails = connectionManager.logConnectionDetails(connectionDetails);
    
    // Get fully qualified table ID from ConnectionManager
    logFlow('WALKTHROUGH_SHOWTABLE4', 'INFO', 'Show connection details', {
      connectionDetails: safeDetails,
      hasProjectId: !!connectionDetails?.projectId,
      hasDatasetId: !!connectionDetails?.datasetId,
      hasTableId: !!connectionDetails?.tableId,
      hasSchemaType: !!connectionDetails?.schemaType,
      schemaType: connectionDetails?.schemaType || 'Not provided',
      source: connectionDetails && (connectionDetails.projectId || connectionDetails.datasetId || connectionDetails.tableId) ? 'session' : 'environment'
    });

    const tableId = columnName 
      ? connectionManager.getFullyQualifiedTableId(connectionDetails)
      : 'actions'; // Default fallback
    
    logFlow('WALKTHROUGH_SHOWTABLE5', 'INFO', 'Getting fully qualified table ID from connection details', {
      tableId
    });
    
    // Validate and ensure we have a non-empty aggregation clause
    let selectClause = aggregationComponents.aggregationClause;
    
    // Final safety check - never allow an empty SELECT clause
    if (!selectClause || selectClause.trim() === '') {
      logFlow('LLM_TRANSLATOR', 'INFO', 'Empty SELECT clause detected, using fallback', {
        originalQuery: query
      });
      selectClause = '*';
    }
    
    // Create the SQL query with enhanced safety checks
    // Double-check that selectClause is not empty before using it
    if (!selectClause || selectClause.trim() === '') {
      logFlow('LLM_TRANSLATOR', 'INFO', 'Empty SELECT clause detected after initial check, using * as fallback', {
        originalQuery: query
      });
      selectClause = '*';
    }
    
    let sql = `SELECT ${selectClause}\nFROM ${tableId}`;
    
    // Final validation - if somehow we still have an empty SELECT clause, log and fix it
    if (sql.match(/SELECT\s+FROM/i)) {
      logFlow('LLM_TRANSLATOR', 'ERROR', 'Critical: Empty SELECT clause still detected in final SQL', {
        originalQuery: query,
        generatedSql: sql
      });
      sql = sql.replace(/SELECT\s+FROM/i, 'SELECT * FROM');
    }
    
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
    
    // Create a prompt that summarizes the SQL generation process
    // This will be useful for error correction
    const prompt = `
# SQL Generation Summary

## Original Query
${query}

## Filter Components
${filterComponents.description}
SQL Clause: ${filterComponents.sqlClause}

## Aggregation Components
${aggregationComponents.aggregationDescription}
SQL Clause: ${aggregationComponents.aggregationClause}

## Group By
${aggregationComponents.groupByDescription}
SQL Clause: ${aggregationComponents.groupByClause}

## Order By
${aggregationComponents.orderByDescription}
SQL Clause: ${aggregationComponents.orderByClause}

## Limit
${aggregationComponents.limitDescription}
SQL Clause: ${aggregationComponents.limitClause}

## Generated SQL
${sql}
`;
    
    logFlow('LLM_TRANSLATOR', 'EXIT', 'SQL generation completed', {
      sqlLength: sql.length,
      confidence,
      hasPrompt: !!prompt
    });
    
    return {
      sql,
      interpretedQuery,
      confidence,
      prompt
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

  /**
   * Corrects SQL errors with user-friendly explanation and suggestions
   * 
   * @param originalQuery Original query context sent to LLM
   * @param failedSQL Failed SQL query
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

/**
 * Corrects SQL errors with user-friendly explanation and suggestions
 * 
 * @param originalQuery Original query context sent to LLM
 * @param failedSQL Failed SQL query
 * @param errorMessage Error message from BigQuery
 * @param userPrompt User's original natural language prompt
 * @returns Object with explanation, suggestions, and corrected SQL
 */
public async correctSQLWithExplanation(
  originalQuery: string, 
  failedSQL: string, 
  errorMessage: string, 
  userPrompt: string
): Promise<{
  explanation: string;
  suggestions: string[];
  sql: string; // Changed property name from correctedSQL to sql
}> {
  if (!this.anthropic) {
    throw new Error('Anthropic client not initialized');
  }
  
  logFlow('LLM_TRANSLATOR', 'ENTRY', 'Attempting to correct SQL with explanation', { 
    sqlLength: failedSQL.length,
    errorMessage,
    userPrompt
  });
  
  try {
    const prompt = `Hey LLM, I am feeding you an error message and you have 2 objectives:

FIRST: Your objective is to provide an explanation for the user of this error message in natural language coupled with 1 or 2 suggested prompts that they could send that may bypass this error. Remember that the suggested prompts should be similar to their original prompt entry which in this case was [${userPrompt}]

SECOND: Your objective is to execute one of those alternatives and provide the SQL for it as well.

PRIOR CONTEXT:
(A) ${originalQuery}
(B) User Entered: ${userPrompt}

ERROR DETAILS:
Failed SQL: ${failedSQL}
Error Message: ${errorMessage}

Please respond in the following JSON format:
{
  "explanation": "Clear explanation of the error in user-friendly terms",
  "suggestions": ["Alternative prompt 1", "Alternative prompt 2"],
  "sql": "SELECT ... FROM ... WHERE ..."
}`;

    // Log the prompt being sent to the LLM
    console.log('PROMPT_SENT_TO_LLM_FOR_ERROR_CORRECTION:', prompt);

    // Call Claude to get explanation and correction
    const response = await this.anthropic.messages.create({
      model: 'claude-3-opus-20240229',
      max_tokens: 1000,
      temperature: 0.2,
      system: "You are an expert SQL assistant specializing in BigQuery syntax.",
      messages: [{ role: "user", content: prompt }]
    });

    const content = this.getResponseText(response);
    
    try {
      // Try to parse the JSON response
      const result = JSON.parse(content);
      
      logFlow('LLM_TRANSLATOR', 'EXIT', 'SQL correction with explanation completed', {
        hasExplanation: !!result.explanation,
        suggestionCount: result.suggestions?.length || 0,
        hasCorrectedSQL: !!result.sql
      });
      
      return {
        explanation: result.explanation || `The error occurred because: ${errorMessage}`,
        suggestions: Array.isArray(result.suggestions) ? result.suggestions : [`Try rephrasing your query: "${userPrompt}"`],
        sql: result.sql || ""
      };
    } catch (parseError) {
      logFlow('LLM_TRANSLATOR', 'INFO', 'Error parsing LLM response as JSON', { 
        error: parseError instanceof Error ? parseError.message : String(parseError),
        rawResponse: content
      });
      
      return {
        explanation: `The error occurred because: ${errorMessage}`,
        suggestions: [`Try rephrasing your query: "${userPrompt}"`],
        sql: ""
      };
    }
  } catch (error) {
    const errorMsg = error instanceof Error ? error.message : String(error);
    logFlow('LLM_TRANSLATOR', 'ERROR', 'Error in correctSQLWithExplanation', { error: errorMsg });
    
    return {
      explanation: `I couldn't analyze the error. The original error was: ${errorMessage}`,
      suggestions: [`Try rephrasing your query: "${userPrompt}"`],
      sql: ""
    };
  }
}
}
