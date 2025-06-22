/**
 * BigQuery Client Service - Database Connection and Query Execution
 * 
 * Handles:
 * - BigQuery connection configuration
 * - SQL query generation from parsed queries
 * - Query execution with proper error handling
 * - CSV fallback support
 * - Query optimization and caching
 */

import { BigQuery, Dataset, Table } from '@google-cloud/bigquery';
import * as fs from 'fs/promises';
import * as path from 'path';
import { 
  BigQueryConfig, 
  ActionRecord, 
  QueryParseResult, 
  QueryResult,
  ReportParameters 
} from '../types/actions-report.js';

// Connection details interface for session-based connections
interface ConnectionDetails {
  projectId: string;
  datasetId: string;
  tableId: string;
}

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
  
  const logMessage = `[${timestamp}] ${logPrefix} BIGQUERY_${stage}: ${message}`;
  
  if (data) {
    console.error(logMessage, data);
  } else {
    console.error(logMessage);
  }
};

export class BigQueryClient {
  private bigquery: BigQuery | null = null;
  private config: BigQueryConfig | null = null;
  private dataset: Dataset | null = null;
  private table: Table | null = null;
  private queryCache: Map<string, { data: any; timestamp: number }> = new Map();
  private readonly CACHE_TTL = 5 * 60 * 1000; // 5 minutes
  
  // Column metadata caching
  private availableColumns: string[] = [];
  private columnsFetched: boolean = false;
  private tableSchema: any = null;
  private schemaFetched: boolean = false;
  
  // Predefined column descriptions for enhanced semantic understanding
  private predefinedColumnMetadata: Record<string, any> = {
    // Time-related columns
    'timestamp': { 
      description: 'Date and time when the transaction occurred', 
      type: 'TIMESTAMP',
      aggregatable: false
    },
    
    // Asset identification columns
    'asset': { 
      description: 'Cryptocurrency symbol/ticker (e.g., BTC, ETH, SOL)', 
      type: 'STRING',
      aggregatable: false
    },
    'assetName': { 
      description: 'Full name of the cryptocurrency (e.g., Bitcoin, Ethereum, Solana)', 
      type: 'STRING',
      aggregatable: false
    },
    
    // Transaction type columns
    'action': { 
      description: 'Type of transaction (buy, sell, transfer, stake, etc.)', 
      type: 'STRING',
      aggregatable: false
    },
    'transactionType': { 
      description: 'Category of transaction (trade, transfer, income, etc.)', 
      type: 'STRING',
      aggregatable: false
    },
    
    // Quantity columns
    'amount': { 
      description: 'Quantity of cryptocurrency in the transaction', 
      type: 'NUMERIC',
      aggregatable: true
    },
    'balance': { 
      description: 'Current balance of the cryptocurrency', 
      type: 'NUMERIC',
      aggregatable: true
    },
    
    // Financial columns
    'price': { 
      description: 'Price per unit of the cryptocurrency at transaction time', 
      type: 'NUMERIC',
      aggregatable: true
    },
    'value': { 
      description: 'Total value of the transaction in fiat currency', 
      type: 'NUMERIC',
      aggregatable: true
    },
    'fee': { 
      description: 'Transaction fee paid', 
      type: 'NUMERIC',
      aggregatable: true
    },
    
    // Gain/Loss columns
    'shortTermGainLoss': { 
      description: 'Realized gain or loss for assets held less than a year', 
      type: 'NUMERIC',
      aggregatable: true
    },
    'longTermGainLoss': { 
      description: 'Realized gain or loss for assets held more than a year', 
      type: 'NUMERIC',
      aggregatable: true
    },
    'undatedGainLoss': { 
      description: 'Gain or loss where the holding period is unknown', 
      type: 'NUMERIC',
      aggregatable: true
    },
    'totalGainLoss': { 
      description: 'Total realized gain or loss across all holding periods', 
      type: 'NUMERIC',
      aggregatable: true
    },
    'unrealizedGainLoss': { 
      description: 'Potential gain or loss for assets still held', 
      type: 'NUMERIC',
      aggregatable: true
    },
    
    // Cost basis columns
    'costBasisAcquired': { 
      description: 'Cost basis of assets acquired in the transaction', 
      type: 'NUMERIC',
      aggregatable: true
    },
    'costBasisRelieved': { 
      description: 'Cost basis of assets disposed in the transaction', 
      type: 'NUMERIC',
      aggregatable: true
    },
    'carryingValue': { 
      description: 'Current carrying value of the assets', 
      type: 'NUMERIC',
      aggregatable: true
    },
    'fairMarketValueDisposed': { 
      description: 'Fair market value of assets at time of disposal', 
      type: 'NUMERIC',
      aggregatable: true
    },
    
    // Other columns
    'assetUnitAdj': { 
      description: 'Adjustment to asset units', 
      type: 'NUMERIC',
      aggregatable: true
    },
    'wallet': { 
      description: 'Wallet address or identifier', 
      type: 'STRING',
      aggregatable: false
    },
    'exchange': { 
      description: 'Exchange or platform where the transaction occurred', 
      type: 'STRING',
      aggregatable: false
    }
  }
  
  constructor() {
    // Initialize empty - configuration happens via configure()
  }
  
  // ========================================================================
  // COLUMN METADATA
  // ========================================================================
  
  /**
   * Fetches and caches the available columns from the BigQuery table
   */
  /**
   * Fetches and caches the available columns from the BigQuery table schema
   * @returns Array of column names available in the table
   */
  public async getAvailableColumns(): Promise<string[]> {
    // Return cached columns if available
    if (this.columnsFetched && this.availableColumns.length > 0) {
      return this.availableColumns;
    }
    
    // If we already have the schema, use it instead of making another API call
    if (this.schemaFetched && this.tableSchema?.fields) {
      logFlow('GET_COLUMNS', 'INFO', 'Using cached schema to get columns');
      this.availableColumns = this.tableSchema.fields.map((field: any) => field.name);
      this.columnsFetched = true;
      return this.availableColumns;
    }
    
    // Otherwise fetch the schema if needed
    if (!this.schemaFetched) {
      await this.fetchTableSchema();
      
      // If schema fetching was successful, use it
      if (this.schemaFetched && this.tableSchema?.fields) {
        this.availableColumns = this.tableSchema.fields.map((field: any) => field.name);
        this.columnsFetched = true;
        return this.availableColumns;
      }
    }
    
    // Fallback to direct metadata fetch if schema is still not available
    if (!this.table) {
      throw new Error('BigQuery table not initialized. Call configure() first.');
    }
    
    try {
      logFlow('GET_COLUMNS', 'ENTRY', 'Fetching columns directly from table metadata');
      const [metadata] = await this.table.getMetadata();
      this.availableColumns = metadata.schema.fields.map((field: any) => field.name);
      this.columnsFetched = true;
      logFlow('GET_COLUMNS', 'EXIT', `Retrieved ${this.availableColumns.length} columns`);
      return this.availableColumns;
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      logFlow('GET_COLUMNS', 'ERROR', 'Error fetching table columns', { error: errorMessage });
      console.error('Error fetching table columns:', error);
      return [];
    }
  }
  
  /**
   * Finds similar columns in the table based on the input string
   * @param input The input string to find similar columns for
   * @param limit Maximum number of suggestions to return
   */
  public async findSimilarColumns(input: string, limit: number = 3): Promise<string[]> {
    const columns = await this.getAvailableColumns();
    if (columns.length === 0) return [];
    
    // Simple similarity scoring based on string inclusion and position
    const scoredColumns = columns.map(column => {
      const lowerColumn = column.toLowerCase();
      const lowerInput = input.toLowerCase();
      
      // Score based on:
      // 1. Exact match (highest score)
      if (lowerColumn === lowerInput) return { column, score: 100 };
      
      // 2. Starts with input
      if (lowerColumn.startsWith(lowerInput)) return { column, score: 80 };
      
      // 3. Contains input
      if (lowerColumn.includes(lowerInput)) return { column, score: 60 };
      
      // 4. Words in common
      const columnWords = new Set(lowerColumn.split(/[^a-z0-9]+/).filter(Boolean));
      const inputWords = new Set(lowerInput.split(/[^a-z0-9]+/).filter(Boolean));
      const commonWords = [...inputWords].filter(word => columnWords.has(word)).length;
      
      return { column, score: commonWords * 10 };
    });
    
    // Sort by score and return top N
    return scoredColumns
      .sort((a, b) => b.score - a.score)
      .slice(0, limit)
      .filter(item => item.score > 0)
      .map(item => item.column);
  }
  /**
   * Fetches and caches the table schema from BigQuery
   * This provides actual column metadata from the database
   */
  private async fetchTableSchema(): Promise<void> {
    if (this.schemaFetched && this.tableSchema) {
      return;
    }
    
    if (!this.table) {
      throw new Error('BigQuery table not initialized. Call configure() first.');
    }
    
    try {
      logFlow('FETCH_SCHEMA', 'ENTRY', 'Fetching table schema from BigQuery');
      const [metadata] = await this.table.getMetadata();
      this.tableSchema = metadata.schema;
      this.schemaFetched = true;
      logFlow('FETCH_SCHEMA', 'EXIT', 'Table schema fetched successfully', { 
        fieldCount: this.tableSchema?.fields?.length || 0 
      });
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      logFlow('FETCH_SCHEMA', 'ERROR', 'Error fetching table schema', { error: errorMessage });
      console.error('Error fetching table schema:', error);
    }
  }

  // ========================================================================
  // CONFIGURATION
  // ========================================================================
  
  async configure(config: BigQueryConfig): Promise<void> {
    logFlow('CONFIGURE', 'ENTRY', 'Configuring BigQuery client', { 
      projectId: config.projectId,
      datasetId: config.datasetId,
      tableId: config.tableId,
      hasKeyFile: !!config.keyFilename,
      hasCredentials: !!config.credentials
    });
    
    this.config = config;
    this.columnsFetched = false; // Reset columns cache on reconfigure
    
    try {
      // Initialize BigQuery client
      const options: any = {
        projectId: config.projectId,
      };

      if (config.keyFilename) {
        options.keyFilename = config.keyFilename;
      } else if (config.credentials) {
        options.credentials = config.credentials;
      }

      this.bigquery = new BigQuery(options);
      this.dataset = this.bigquery.dataset(config.datasetId);
      this.table = this.dataset.table(config.tableId);

      // Test connection
      await this.testConnection();
      
      // Fetch and cache the table schema for dynamic column metadata
      await this.fetchTableSchema();
      
      logFlow('CONFIGURE', 'EXIT', `BigQuery connected: ${config.projectId}.${config.datasetId}.${config.tableId}`);
      console.log(`✅ BigQuery connected: ${config.projectId}.${config.datasetId}.${config.tableId}`);
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      logFlow('CONFIGURE', 'ERROR', 'Failed to configure BigQuery', { error: errorMessage });
      throw new Error(`Failed to configure BigQuery: ${errorMessage}`);
    }
  }

  private async testConnection(): Promise<void> {
    if (!this.table) {
      logFlow('TEST_CONNECTION', 'ERROR', 'BigQuery table not configured');
      throw new Error('BigQuery table not configured');
    }

    logFlow('TEST_CONNECTION', 'ENTRY', 'Testing BigQuery connection');
    try {
      const [metadata] = await this.table.getMetadata();
      const fieldCount = metadata.schema?.fields?.length || 0;
      logFlow('TEST_CONNECTION', 'EXIT', `Table schema verified: ${fieldCount} fields`, { fieldCount });
      console.log(`Table schema verified: ${fieldCount} fields`);
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      logFlow('TEST_CONNECTION', 'ERROR', 'BigQuery connection test failed', { error: errorMessage });
      throw new Error(`BigQuery connection test failed: ${errorMessage}`);
    }
  }

  // ========================================================================
  // QUERY EXECUTION - Main Entry Points
  // ========================================================================

  /**
   * Execute analytical query from parsed natural language
   */
  async executeAnalyticalQuery(
    parseResult: QueryParseResult, 
    parameters: ReportParameters
  ): Promise<QueryResult> {
    const startTime = Date.now();
    logFlow('EXECUTE_QUERY', 'ENTRY', 'Executing analytical query', { 
      intent: parseResult.intent,
      aggregationType: parseResult.aggregationType,
      columnCount: parseResult.columns.length,
      filterCount: Object.keys(parseResult.filters || {}).length
    });
    
    try {
      // Generate SQL from parsed query
      const sql = this.generateAnalyticalSQL(parseResult, parameters);
      logFlow('SQL_GENERATION', 'INFO', 'Generated SQL query', { sql });
      
      // Execute query
      logFlow('SQL_EXECUTE', 'ENTRY', 'Executing SQL query against BigQuery');
      const results = await this.executeQuery(sql);
      logFlow('SQL_EXECUTE', 'EXIT', 'SQL query execution completed', { rowCount: results.length });
      
      // Cache results
      this.setCache(this.getCacheKey(sql, parameters), results);

      // Format results based on query intent
      const formattedData = this.formatAnalyticalResults(results, parseResult);

      const executionTime = Date.now() - startTime;
      logFlow('EXECUTE_QUERY', 'EXIT', 'Query execution completed successfully', { 
        rowCount: results.length,
        executionTime: `${executionTime}ms`,
        cached: false
      });
      
      return {
        success: true,
        data: formattedData,
        summary: this.generateResultSummary(formattedData, parseResult),
        metadata: {
          rows_processed: results.length,
          execution_time_ms: executionTime,
          cached: false,
          columns_used: parseResult.columns
            .map(col => col.mappedColumns || [])
            .flat()
            .filter(Boolean) as string[]
        }
      };

    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      const executionTime = Date.now() - startTime;
      
      logFlow('EXECUTE_QUERY', 'ERROR', 'Query execution failed', { 
        error: errorMessage,
        executionTime: `${executionTime}ms`
      });
      
      return {
        success: false,
        error: {
          type: 'COMPUTATION_ERROR',
          message: errorMessage,
          suggestions: this.getErrorSuggestions(error)
        },
        metadata: {
          rows_processed: 0,
          execution_time_ms: executionTime,
          cached: false,
          columns_used: []
        }
      };
    }
  }

  /**
   * Execute predefined report queries
   */
  async executeReport(reportId: string, parameters: ReportParameters = {}): Promise<QueryResult> {
    // Check if connection details are provided in parameters
    if (parameters.connectionDetails) {
      // Temporarily reconfigure the client with session connection details
      await this.reconfigureWithConnectionDetails(parameters.connectionDetails);
    }
    logFlow('REPORT_QUERY', 'ENTRY', 'Executing report query', { parameters });
    
    try {
      // Get the SQL for the report
      const reportSql = await this.getReportSql(reportId);
      
      if (!reportSql) {
        throw new Error(`Report SQL not found for report ID: ${reportId}`);
      }
      
      // PARAMETER_REVIEW 6: Log parameters at the start of executeReportQuery
      console.log('PARAMETER_REVIEW 6 - BigQueryClient.executeReportQuery:', {
        parameters,
        hasAsOfDate: parameters.asOfDate ? 'YES' : 'NO',
        asOfDateValue: parameters.asOfDate,
        sqlContainsAsOfDate: reportSql.includes('@asOfDate') ? 'YES' : 'NO'
      });
      
      // Log all available parameters for debugging
      console.log('PARAMETER_CHECKER - Available parameters:', {
        runId: parameters.runId,
        orgId: parameters.orgId,
        asOfDate: parameters.asOfDate,
        asOfSEC: parameters.asOfSEC,
        startDate: parameters.startDate,
        endDate: parameters.endDate
      });
      
      // Check for parameter placeholders in SQL
      const placeholders = reportSql.match(/@[a-zA-Z0-9_]+/g) || [];
      console.log('PARAMETER_CHECKER - SQL parameter placeholders:', placeholders);
      
      const parameterizedSQL = this.replaceParameters(reportSql, parameters);
      logFlow('SQL_GENERATION', 'INFO', 'Generated parameterized SQL', { sql: parameterizedSQL });
    
      const results = await this.executeQuery(parameterizedSQL);
      logFlow('REPORT_QUERY', 'EXIT', 'Report query execution completed', { rowCount: results.length });
      
      return {
        success: true,
        data: results,
        metadata: {
          rows_processed: results.length,
          execution_time_ms: 0,
          cached: false,
          columns_used: []
        }
      };
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      logFlow('REPORT_QUERY', 'ERROR', 'Report query execution failed', { error: errorMessage });
      
      return {
        success: false,
        error: { type: 'DATA_ERROR', message: errorMessage },
        data: [],
        metadata: {
          rows_processed: 0,
          execution_time_ms: 0,
          cached: false,
          columns_used: []
        }
      };
    }
  }

  // ========================================================================
  // SQL GENERATION - Convert Parsed Queries to BigQuery SQL
  // ========================================================================

  private generateAnalyticalSQL(parseResult: QueryParseResult, parameters: ReportParameters): string {
    const { intent, aggregationType, columns, filters } = parseResult;
    
    // Build SELECT clause
    const selectColumns = this.buildSelectClause(columns, aggregationType || 'sum');
    
    // Build FROM clause
    const fromClause = this.buildFromClause();
    
    // Build WHERE clause
    const whereClause = this.buildWhereClause(filters, parameters);
    
    // Build GROUP BY clause (if needed)
    const groupByClause = this.buildGroupByClause(parseResult);
    
    // Build ORDER BY clause
    const orderByClause = this.buildOrderByClause(parseResult);

    const sql = `
      ${selectColumns}
      ${fromClause}
      ${whereClause}
      ${groupByClause}
      ${orderByClause}
    `.trim().replace(/\s+/g, ' ');

    console.log('Generated SQL:', sql);
    return sql;
  }

  private buildSelectClause(columns: any[], aggregationType: string | undefined): string {
    const selectParts: string[] = [];
    
    columns.forEach(columnMapping => {
      (columnMapping.mappedColumns || []).forEach((column: string) => {
        const metadata = this.getColumnMetadata(column);
        
        if (metadata?.aggregatable && aggregationType !== 'count') {
          // Apply aggregation function
          switch (aggregationType) {
            case 'sum':
              selectParts.push(`SUM(COALESCE(CAST(${column} AS BIGNUMERIC), 0)) as ${column}_sum`);
              break;
            case 'avg':
              selectParts.push(`AVG(COALESCE(CAST(${column} AS BIGNUMERIC), 0)) as ${column}_avg`);
              break;
            case 'max':
              selectParts.push(`MAX(COALESCE(CAST(${column} AS BIGNUMERIC), 0)) as ${column}_max`);
              break;
            case 'min':
              selectParts.push(`MIN(COALESCE(CAST(${column} AS BIGNUMERIC), 0)) as ${column}_min`);
              break;
          }
        } else if (aggregationType === 'count') {
          selectParts.push(`COUNT(*) as transaction_count`);
        } else {
          // Non-aggregated column
          selectParts.push(column);
        }
      });
    });

    // Always include grouping columns for aggregations
    if (selectParts.some(part => part.includes('SUM(') || part.includes('COUNT(') || part.includes('AVG('))) {
      if (!selectParts.some(part => part === 'asset')) {
        selectParts.unshift('asset');
      }
    }

    return `SELECT ${selectParts.join(', ')}`;
  }

  private buildFromClause(): string {
    if (!this.config) {
      throw new Error('BigQuery not configured');
    }
    
    return `FROM \`${this.config.projectId}.${this.config.datasetId}.${this.config.tableId}\``;
  }

  private buildWhereClause(filters: Record<string, any>, parameters: ReportParameters): string {
    const conditions: string[] = [];

    // Required parameters
    if (parameters.runId) {
      conditions.push(`runId = '${parameters.runId}'`);
    }
    
    if (parameters.orgId) {
      conditions.push(`orgId = '${parameters.orgId}'`);
    }

    // Asset filters
    if (filters.assets && filters.assets.length > 0) {
      const assetList = filters.assets.map((asset: string) => `'${asset}'`).join(', ');
      conditions.push(`asset IN (${assetList})`);
    }

    // Date filters
    if (filters.startDate) {
      const startTimestamp = Math.floor(new Date(filters.startDate).getTime() / 1000);
      conditions.push(`timestampSEC >= ${startTimestamp}`);
    }
    
    if (filters.endDate) {
      const endTimestamp = Math.floor(new Date(filters.endDate + ' 23:59:59').getTime() / 1000);
      conditions.push(`timestampSEC <= ${endTimestamp}`);
    }

    // Wallet filters
    if (filters.includeWallets && filters.includeWallets.length > 0) {
      const walletList = filters.includeWallets.map((wallet: string) => `'${wallet}'`).join(', ');
      conditions.push(`wallet IN (${walletList})`);
    }
    
    if (filters.excludeWallets && filters.excludeWallets.length > 0) {
      const walletList = filters.excludeWallets.map((wallet: string) => `'${wallet}'`).join(', ');
      conditions.push(`(wallet IS NULL OR wallet NOT IN (${walletList}))`);
    }

    // Action filters
    if (filters.actions && filters.actions.length > 0) {
      const actionList = filters.actions.map((action: string) => `'${action}'`).join(', ');
      conditions.push(`action IN (${actionList})`);
    }

    // Status filters
    if (filters.status && filters.status.length > 0) {
      const statusList = filters.status.map((status: string) => `'${status}'`).join(', ');
      conditions.push(`status IN (${statusList})`);
    }

    return conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';
  }

  private buildGroupByClause(parseResult: QueryParseResult): string {
    const { aggregationType, columns } = parseResult;
    
    // Only add GROUP BY for aggregation queries
    if (aggregationType === 'count' || 
        columns.some(col => (col.mappedColumns || []).some((mcol: string) => this.getColumnMetadata(mcol)?.aggregatable))) {
      
      const groupColumns: string[] = ['asset']; // Always group by asset for financial queries
      
      // Add additional non-aggregatable columns to GROUP BY
      columns.forEach(columnMapping => {
        (columnMapping.mappedColumns || []).forEach((column: string) => {
          const metadata = this.getColumnMetadata(column);
          if (!metadata?.aggregatable && !groupColumns.includes(column)) {
            groupColumns.push(column);
          }
        });
      });

      return groupColumns.length > 0 ? `GROUP BY ${groupColumns.join(', ')}` : '';
    }

    return '';
  }

  private buildOrderByClause(parseResult: QueryParseResult): string {
    const { intent, aggregationType, groupBy, orderBy, columns } = parseResult;
    
    // If explicit ORDER BY is specified in the query, use it
    if (orderBy && orderBy.length > 0) {
      return `ORDER BY ${orderBy.map(o => o.column).join(', ')}`;
    }
    
    // For aggregation queries, order by the aggregated value
    if (aggregationType === 'sum' || aggregationType === 'count') {
      // Order by the aggregated values (descending for totals)
      return 'ORDER BY 2 DESC'; // Second column is usually the aggregated value
    }
    
    // For GROUP BY queries, only order by columns that are in the GROUP BY clause
    if (groupBy && groupBy.length > 0 && groupBy[0]) {
      // This avoids the "ORDER BY expression references column which is neither grouped nor aggregated" error
      return `ORDER BY ${groupBy[0].column} ASC`;
    }
    
    // For non-aggregated queries, try to find appropriate columns to order by
    // First check if we have timestamp-like columns
    const timeColumns = columns.filter(col => 
      col.name.includes('time') || 
      col.name.includes('date') || 
      col.name.includes('timestamp')
    );
    
    if (timeColumns.length > 0 && timeColumns[0]) {
      // Use the first time-related column for ordering
      return `ORDER BY ${timeColumns[0].name} DESC`;
    }
    
    // If we have an 'asset' column, use it as primary sort
    const assetColumn = columns.find(col => col.name === 'asset');
    if (assetColumn) {
      return 'ORDER BY asset ASC';
    }
    
    // Last resort: order by the first column
    if (columns.length > 0 && columns[0]) {
      return `ORDER BY ${columns[0].name} ASC`;
    }
    
    // Fallback if we somehow have no columns
    return '';
  }

  // ========================================================================
  // QUERY EXECUTION AND UTILITIES
  // ========================================================================

  /**
   * Execute a SQL query against BigQuery
   */
  /**
   * Reconfigures the BigQuery client with session-based connection details
   * @param connectionDetails The connection details from the user session
   */
  /**
   * Get the SQL for a report by ID
   * @param reportId The report ID
   * @returns The SQL for the report
   */
  private async getReportSql(reportId: string): Promise<string | null> {
    // This is a placeholder - in a real implementation, you would fetch the SQL from a database or file
    // For now, we'll return a simple SQL statement for testing
    return `SELECT * FROM \`${this.config?.projectId}.${this.config?.datasetId}.${this.config?.tableId}\` LIMIT 10`;
  }
  
  /**
   * Reconfigures the BigQuery client with session-based connection details
   * @param connectionDetails The connection details from the user session
   */
  private async reconfigureWithConnectionDetails(connectionDetails: ConnectionDetails): Promise<void> {
    logFlow('RECONFIGURE', 'ENTRY', 'Reconfiguring BigQuery client with session connection details', { 
      projectId: connectionDetails.projectId,
      datasetId: connectionDetails.datasetId,
      tableId: connectionDetails.tableId
    });
    
    try {
      // Save the original configuration to restore later if needed
      const originalConfig = this.config;
      
      // Create a new configuration with the session connection details
      const sessionConfig: BigQueryConfig = {
        projectId: connectionDetails.projectId,
        datasetId: connectionDetails.datasetId,
        tableId: connectionDetails.tableId,
        // Keep the same authentication method
        credentials: originalConfig?.credentials
      };
      
      // Only add keyFilename if it exists in the original config
      if (originalConfig?.keyFilename) {
        sessionConfig.keyFilename = originalConfig.keyFilename;
      }
      
      // Configure the client with the new details
      await this.configure(sessionConfig);
      
      logFlow('RECONFIGURE', 'EXIT', 'Successfully reconfigured BigQuery client with session connection details');
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      logFlow('RECONFIGURE', 'ERROR', 'Failed to reconfigure BigQuery client', { error: errorMessage });
      throw new Error(`Failed to reconfigure BigQuery client: ${errorMessage}`);
    }
  }
  
  private async executeQuery(sql: string): Promise<any[]> {
    if (!this.bigquery) {
      logFlow('EXECUTE_QUERY', 'ERROR', 'BigQuery client not initialized');
      throw new Error('BigQuery client not initialized');
    }

    // PARAMETER_REVIEW 8: Log SQL in executeQuery
    console.log('PARAMETER_REVIEW 8 - BigQueryClient.executeQuery:', {
      sqlContainsAsOfDate: sql.includes('@asOfDate') ? 'YES' : 'NO',
      sqlPreview: sql.substring(0, 300) + (sql.length > 300 ? '...' : '')
    });

    logFlow('EXECUTE_QUERY', 'ENTRY', 'Executing SQL query', { 
      sqlLength: sql.length,
      sqlPreview: sql.substring(0, 100) + (sql.length > 100 ? '...' : '')
    });
    
    const startTime = Date.now();
    try {
      // Execute the query
      const [rows] = await this.bigquery.query({
        query: sql,
        location: 'US',  // Adjust based on your dataset location
      });

      const executionTime = Date.now() - startTime;
      logFlow('EXECUTE_QUERY', 'EXIT', 'Query execution completed', { 
        rowCount: rows.length, 
        executionTime: `${executionTime}ms` 
      });
      return rows;
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      const executionTime = Date.now() - startTime;
      
      logFlow('EXECUTE_QUERY', 'ERROR', 'Query execution failed', { 
        error: errorMessage,
        executionTime: `${executionTime}ms`
      });
      console.error('BigQuery query execution error:', error);
      throw new Error(`Query execution failed: ${errorMessage}`);
    }
  }

  private replaceParameters(sql: string, parameters: ReportParameters): string {
    // PARAMETER_REVIEW 7: Log parameters in replaceParameters
    console.log('PARAMETER_REVIEW 7 - BigQueryClient.replaceParameters:', {
      parameters,
      hasAsOfDate: parameters.asOfDate ? 'YES' : 'NO',
      asOfDateValue: parameters.asOfDate,
      sqlContainsAsOfDate: sql.includes('@asOfDate') ? 'YES' : 'NO'
    });
    
    // Log all available parameters for debugging
    console.log('PARAMETER_CHECKER - Available parameters:', {
      runId: parameters.runId,
      orgId: parameters.orgId,
      asOfDate: parameters.asOfDate,
      asOfSEC: parameters.asOfSEC,
      startDate: parameters.startDate,
      endDate: parameters.endDate
    });
    
    // Check for parameter placeholders in SQL
    const placeholders = sql.match(/@[a-zA-Z0-9_]+/g) || [];
    console.log('PARAMETER_CHECKER - SQL parameter placeholders:', placeholders);
    
    let parameterizedSQL = sql;
    
    // Replace parameter placeholders
    if (parameters.runId) {
      parameterizedSQL = parameterizedSQL.replace(/@runId/g, `'${parameters.runId}'`);
    }
    
    if (parameters.orgId) {
      parameterizedSQL = parameterizedSQL.replace(/@orgId/g, `'${parameters.orgId}'`);
    }
    
    if (parameters.asOfSEC) {
      parameterizedSQL = parameterizedSQL.replace(/@asOfSEC/g, parameters.asOfSEC.toString());
    } 
    
    if (parameters.asOfDate) {
      // Replace @asOfDate directly in the SQL
      parameterizedSQL = parameterizedSQL.replace(/@asOfDate/g, `'${parameters.asOfDate}'`);
      
      // Also handle @asOfSEC if it's used in the query but not provided in parameters
      if (!parameters.asOfSEC && parameterizedSQL.includes('@asOfSEC')) {
        const asOfSEC = Math.floor(new Date(parameters.asOfDate + ' 23:59:59').getTime() / 1000);
        parameterizedSQL = parameterizedSQL.replace(/@asOfSEC/g, asOfSEC.toString());
        console.log(`Converted asOfDate ${parameters.asOfDate} to asOfSEC ${asOfSEC}`);
      }
    }
    
    if (parameters.startDate) {
      parameterizedSQL = parameterizedSQL.replace(/@startDate/g, `'${parameters.startDate}'`);
    }
    
    if (parameters.endDate) {
      parameterizedSQL = parameterizedSQL.replace(/@endDate/g, `'${parameters.endDate}'`);
    }

    // Replace table reference placeholder
    if (this.config) {
      const tableRef = `${this.config.projectId}.${this.config.datasetId}.${this.config.tableId}`;
      parameterizedSQL = parameterizedSQL.replace(/\{ACTIONS_REPORT_TABLE\}/g, tableRef);
    }

    return parameterizedSQL;
  }

  // ========================================================================
  // RESULT FORMATTING AND PRESENTATION
  // ========================================================================

  private formatAnalyticalResults(rawResults: any[], parseResult: QueryParseResult): any {
    const { intent, aggregationType, assets } = parseResult;

    if (intent === 'aggregation') {
      return this.formatAggregationResults(rawResults, parseResult);
    } else if (intent === 'comparison') {
      return this.formatComparisonResults(rawResults, parseResult);
    } else {
      return {
        intent: intent,
        results: rawResults,
        count: rawResults.length
      };
    }
  }

  private formatAggregationResults(results: any[], parseResult: QueryParseResult): any {
    const { assets, aggregationType } = parseResult;
    
    const summary = {
      total_assets: results.length,
      aggregation_type: aggregationType,
      breakdown: {} as Record<string, any>,
      grand_total: 0
    };

    // Process each asset result
    results.forEach(row => {
      const asset = row.asset;
      const breakdown: any = {};
      
      // Extract aggregated values
      Object.keys(row).forEach(key => {
        if (key.endsWith('_sum') || key.endsWith('_avg') || key.endsWith('_max') || key.endsWith('_min')) {
          const value = parseFloat(row[key]) || 0;
          breakdown[key] = value;
          
          if (key.includes('gainloss') || key.includes('GainLoss')) {
            summary.grand_total += value;
          }
        } else if (key === 'transaction_count') {
          breakdown.transaction_count = parseInt(row[key]) || 0;
        }
      });

      summary.breakdown[asset] = breakdown;
    });

    return {
      summary,
      detailed_results: results,
      query_info: {
        assets_queried: assets,
        aggregation_type: aggregationType
      }
    };
  }

  private formatComparisonResults(results: any[], parseResult: QueryParseResult): any {
    return {
      comparison_type: 'asset_comparison',
      results: results,
      assets_compared: parseResult.assets,
      total_records: results.length
    };
  }

  private generateResultSummary(formattedData: any, parseResult: QueryParseResult): string {
    const { intent, assets, aggregationType } = parseResult;
    
    if (intent === 'aggregation' && formattedData.summary) {
      const summary = formattedData.summary;
      let summaryText = `📊 **Analysis Results**\n\n`;
      
      if (assets && assets.length > 0) {
        summaryText += `**Assets Analyzed:** ${assets.join(', ')}\n`;
      }
      
      if (aggregationType) {
        summaryText += `**Aggregation Type:** ${aggregationType.toUpperCase()}\n`;
      }
      summaryText += `**Total Assets:** ${summary.total_assets}\n\n`;
      
      if (summary.grand_total !== 0) {
        summaryText += `**Grand Total:** $${summary.grand_total.toLocaleString(undefined, { minimumFractionDigits: 2 })}\n\n`;
      }
      
      // Asset breakdown
      summaryText += `**Breakdown by Asset:**\n`;
      Object.entries(summary.breakdown).forEach(([asset, data]: [string, any]) => {
        summaryText += `\n**${asset}:**\n`;
        Object.entries(data).forEach(([key, value]: [string, any]) => {
          if (typeof value === 'number') {
            if (key.includes('count')) {
              summaryText += `- ${key}: ${value.toLocaleString()}\n`;
            } else {
              summaryText += `- ${key}: $${value.toLocaleString(undefined, { minimumFractionDigits: 2 })}\n`;
            }
          }
        });
      });
      
      return summaryText;
    }
    
    return `Analysis completed successfully. ${formattedData.results?.length || 0} records processed.`;
  }

  // ========================================================================
  // CACHING AND OPTIMIZATION
  // ========================================================================

  private getCacheKey(sql: string, parameters: ReportParameters): string {
    return `${sql}_${JSON.stringify(parameters)}`;
  }

  private getFromCache(key: string): any | null {
    const cached = this.queryCache.get(key);
    if (cached && (Date.now() - cached.timestamp) < this.CACHE_TTL) {
        return cached.data;
    }
    
    if (cached) {
      this.queryCache.delete(key); // Remove expired cache
    }
    
    return null;
  }

  private setCache(key: string, data: any): void {
    this.queryCache.set(key, {
      data,
      timestamp: Date.now()
    });
  }

  // ========================================================================
  // UTILITY METHODS
  // ========================================================================

  /**
   * Gets metadata for a specific column, combining dynamic schema information with predefined metadata
   * @param column The column name to get metadata for
   * @returns Column metadata including type, description, and aggregation capabilities
   */
  public getColumnMetadata(column: string): any {
    // First check if we have predefined metadata for this column
    const predefinedMetadata = this.predefinedColumnMetadata[column];
    
    // If we have schema information, try to get column metadata from there
    let schemaMetadata: any = null;
    if (this.tableSchema?.fields) {
      const field = this.tableSchema.fields.find((f: any) => f.name.toLowerCase() === column.toLowerCase());
      if (field) {
        schemaMetadata = {
          type: field.type,
          description: field.description || null,
          mode: field.mode
        };
      }
    }
    
    // Combine metadata, with predefined taking precedence for description and aggregation info
    // but schema providing the actual type if available
    const result: any = {
      type: (schemaMetadata?.type || predefinedMetadata?.type || 'STRING').toUpperCase(),
      description: predefinedMetadata?.description || schemaMetadata?.description || `${column} column`,
      aggregatable: predefinedMetadata?.aggregatable ?? this.isLikelyAggregatable(column, schemaMetadata?.type)
    };
    
    // Add mode information if available from schema
    if (schemaMetadata?.mode) {
      result.mode = schemaMetadata.mode;
    }
    
    logFlow('GET_COLUMN_METADATA', 'INFO', `Metadata for column: ${column}`, { 
      fromSchema: !!schemaMetadata, 
      fromPredefined: !!predefinedMetadata,
      result
    });
    
    return result;
  }
  
  /**
   * Helper method to determine if a column is likely aggregatable based on its name and type
   * @param column Column name
   * @param type Column data type if known
   * @returns Boolean indicating if column is likely aggregatable
   */
  private isLikelyAggregatable(column: string, type?: string): boolean {
    // Financial and numeric columns are typically aggregatable
    const financialColumns = [
      'shortTermGainLoss', 'longTermGainLoss', 'undatedGainLoss', 'totalGainLoss', 'unrealizedGainLoss',
      'costBasisAcquired', 'costBasisRelieved', 'carryingValue', 'fairMarketValueDisposed', 
      'assetUnitAdj', 'amount', 'balance', 'price', 'value', 'fee'
    ];
    
    // Check if column name is in our known financial columns list
    if (financialColumns.includes(column)) {
      return true;
    }
    
    // Check if column type is numeric
    if (type && ['INTEGER', 'FLOAT', 'NUMERIC', 'BIGNUMERIC'].includes(type.toUpperCase())) {
      return true;
    }
    
    // Check if column name contains keywords suggesting it's numeric
    const numericKeywords = ['amount', 'count', 'total', 'sum', 'avg', 'balance', 'price', 'value', 'fee', 'gain', 'loss'];
    if (numericKeywords.some(keyword => column.toLowerCase().includes(keyword))) {
      return true;
    }
    
    return false;
  }

  private getErrorSuggestions(error: any): string[] {
    const suggestions: string[] = [];
    
    if (error.message?.includes('permission')) {
      suggestions.push('Check BigQuery IAM permissions');
      suggestions.push('Verify service account has BigQuery Data Viewer role');
    }
    
    if (error.message?.includes('not found')) {
      suggestions.push('Verify table name and dataset configuration');
      suggestions.push('Check if the specified runId exists');
    }
    
    if (error.message?.includes('timeout')) {
      suggestions.push('Try reducing the date range');
      suggestions.push('Add more specific filters to reduce data volume');
    }
    
    return suggestions;
  }

  // ========================================================================
  // CSV FALLBACK SUPPORT
  // ========================================================================

  async loadFromCSV(filePath: string): Promise<ActionRecord[]> {
    try {
      const csvContent = await fs.readFile(filePath, 'utf-8');
      // Basic CSV parsing (in production, use a proper CSV library like papaparse)
      const lines = csvContent.split('\n');
      const headers = lines[0]?.split(',') || [];
      const records: ActionRecord[] = [];
      
      for (let i = 1; i < lines.length; i++) {
        if (lines[i]?.trim()) {
          const values = lines[i]?.split(',') || [];
          const record: any = {};
          
          headers.forEach((header, index) => {
            record[header.trim()] = values[index]?.trim();
          });
          
          records.push(record as ActionRecord);
        }
      }
      
      console.log(`✅ Loaded ${records.length} records from CSV: ${filePath}`);
      return records;
    } catch (error) {
      throw new Error(`Failed to load CSV: ${error instanceof Error ? error.message : String(error)}`);
    }
  }
}