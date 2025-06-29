/**
 * Monthly Activity Report Generator
 * 
 * Generates a report showing total activity by month and type for canton transactions.
 * Aggregates transaction data by month, operation, asset, and other key dimensions.
 * Provides summary statistics on transaction counts and total amounts.
 */

import { BigQueryClient } from '../services/bigquery-client.js';
import { QueryExecutor } from '../services/query-executor.js';
import { ConnectionManager } from '../services/connection-manager.js';
import { logFlow } from '../utils/logging.js';
import { 
  MonthlyActivityRecord,
  ReportParameters,
  FieldMetadata 
} from '../types/canton-transaction-report.js';

/**
 * Monthly Activity Report Generator
 */
export class MonthlyActivityReportGenerator {
  private queryExecutor: QueryExecutor;
  private connectionManager: ConnectionManager;
  
  // Field metadata for natural language query mapping
  private static readonly FIELD_METADATA: FieldMetadata[] = [
    {
      column: 'year_month',
      description: 'Year and month of the transactions in YYYY-MM format',
      type: 'string',
      category: 'temporal',
      aliases: ['month', 'period', 'time period', 'monthly'],
      common_queries: ['transactions by month', 'monthly activity', 'period breakdown'],
      aggregatable: true,
      filterable: true
    },
    {
      column: 'operation',
      description: 'Type of operation performed (e.g., buy, sell, transfer)',
      type: 'string',
      category: 'operational',
      aliases: ['transaction type', 'action', 'activity type'],
      common_queries: ['buys', 'sells', 'transfers', 'staking rewards'],
      aggregatable: true,
      filterable: true
    },
    {
      column: 'assetTicker',
      description: 'Ticker symbol of the asset involved in the transaction',
      type: 'string',
      category: 'identifier',
      aliases: ['asset', 'token', 'coin', 'cryptocurrency'],
      common_queries: ['bitcoin transactions', 'ethereum activity', 'by asset'],
      aggregatable: true,
      filterable: true
    },
    {
      column: 'totalAssetAmount',
      description: 'Total amount of the asset involved in transactions',
      type: 'number',
      category: 'financial',
      aliases: ['amount', 'total', 'volume', 'quantity'],
      common_queries: ['largest transactions', 'total volume', 'amount by month'],
      aggregatable: true,
      filterable: true
    },
    {
      column: 'totaltxncount',
      description: 'Total number of transactions',
      type: 'number',
      category: 'operational',
      aliases: ['count', 'transactions', 'frequency', 'number of transactions'],
      common_queries: ['transaction count', 'busiest month', 'activity frequency'],
      aggregatable: true,
      filterable: true
    },
    {
      column: 'fromAddress',
      description: 'Source address for the transaction',
      type: 'string',
      category: 'address',
      aliases: ['source', 'sender', 'from'],
      common_queries: ['transactions from address', 'source of funds'],
      aggregatable: true,
      filterable: true
    },
    {
      column: 'toAddress',
      description: 'Destination address for the transaction',
      type: 'string',
      category: 'address',
      aliases: ['destination', 'recipient', 'to'],
      common_queries: ['transactions to address', 'destination of funds'],
      aggregatable: true,
      filterable: true
    },
    {
      column: 'feeType',
      description: 'Type of fee associated with the transaction',
      type: 'string',
      category: 'fee',
      aliases: ['fee', 'fee category', 'cost type'],
      common_queries: ['gas fees', 'transaction costs', 'fee breakdown'],
      aggregatable: true,
      filterable: true
    },
    {
      column: 'rewardType',
      description: 'Type of reward received (for reward transactions)',
      type: 'string',
      category: 'reward',
      aliases: ['reward', 'reward category', 'earnings type'],
      common_queries: ['staking rewards', 'mining rewards', 'interest earned'],
      aggregatable: true,
      filterable: true
    },
    {
      column: 'rewardFeeType',
      description: 'Type of fee associated with rewards',
      type: 'string',
      category: 'reward',
      aliases: ['reward fee', 'reward cost', 'earnings fee'],
      common_queries: ['reward fees', 'staking costs', 'reward deductions'],
      aggregatable: true,
      filterable: true
    }
  ];
  
  /**
   * Create a new Monthly Activity Report Generator
   * @param bigQueryClient BigQuery client for executing queries
   */
  constructor(private bigQueryClient: BigQueryClient) {
    // Initialize ConnectionManager to get connection details from session
    this.connectionManager = ConnectionManager.getInstance();
    
    // Get project ID from ConnectionManager with fallbacks
    const projectId = this.connectionManager.getProjectId() || process.env.GOOGLE_CLOUD_PROJECT_ID || 'bitwave-solutions';
    console.log(`MonthlyActivityReportGenerator: Initializing QueryExecutor with project ID: ${projectId}`);
    this.queryExecutor = new QueryExecutor(projectId);
  }
  
  /**
   * Build SQL for the Monthly Activity Report
   * @param params Report parameters
   * @param filters Additional filters
   * @returns SQL query string
   */
  buildMonthlyActivityReportSQL(params: ReportParameters, filters: any = {}): string {
    // Get connection details
    const connectionManager = ConnectionManager.getInstance();
    const connectionDetails = connectionManager.getSessionConnectionDetails();
    
    // Get project, dataset, and table information
    const projectId = connectionDetails?.projectId || process.env.GOOGLE_CLOUD_PROJECT_ID || '';
    const datasetId = connectionDetails?.datasetId || process.env.BIGQUERY_DATASET_ID || '';
    const tableId = connectionDetails?.tableId || process.env.BIGQUERY_TABLE_ID || '';
    
    // Validate required connection information
    if (!projectId || !datasetId || !tableId) {
      throw new Error('Missing required connection information: projectId, datasetId, or tableId');
    }
    
    // Build the fully qualified table name
    const fullyQualifiedTableName = `\`${projectId}.${datasetId}.${tableId}\``;
    
    // Build the SQL query
    let sql = `
      SELECT
        FORMAT_DATE('%Y-%m', DATE(dateTime)) AS year_month,
        operation,
        assetTicker,
        fromAddress,
        toAddress,
        feeType,
        rewardFeeType,
        rewardType,
        SUM(assetAmount) AS totalAssetAmount,
        COUNT(parenttransactionId) as totaltxncount
      FROM ${fullyQualifiedTableName} AS details
      WHERE walletId = @walletId
        AND TIMESTAMP(dateTime) >= TIMESTAMP(@startDate)
        AND TIMESTAMP(dateTime) < TIMESTAMP(DATE_ADD(@endDate, INTERVAL 1 DAY))
    `;
    
    // Add asset filter if specified
    if (filters.assets && filters.assets.length > 0) {
      const assetList = filters.assets.map((asset: string) => `'${asset}'`).join(', ');
      sql += `\n      AND assetTicker IN (${assetList})`;
    }
    
    // Add operation filter if specified
    if (filters.operations && filters.operations.length > 0) {
      const operationList = filters.operations.map((op: string) => `'${op}'`).join(', ');
      sql += `\n      AND operation IN (${operationList})`;
    }
    
    // Add group by and order by clauses
    sql += `
      GROUP BY
        year_month,
        operation,
        assetTicker,
        fromAddress,
        toAddress,
        feeType,
        rewardFeeType,
        rewardType
      ORDER BY year_month, operation, assetTicker
    `;
    
    // Add limit if specified
    if (params.limit) {
      sql += `\n      LIMIT ${params.limit}`;
    }
    
    return sql;
  }
  
  /**
   * Transform query results into report format
   * @param rows Query result rows
   * @returns Transformed results
   */
  transformResults(rows: any[]): MonthlyActivityRecord[] {
    // Transform raw query results into strongly typed records
    return rows.map(row => ({
      year_month: row.year_month,
      operation: row.operation,
      assetTicker: row.assetTicker,
      fromAddress: row.fromAddress,
      toAddress: row.toAddress,
      feeType: row.feeType,
      rewardFeeType: row.rewardFeeType,
      rewardType: row.rewardType,
      totalAssetAmount: Number(row.totalAssetAmount) || 0,
      totaltxncount: Number(row.totaltxncount) || 0
    }));
  }
  
  /**
   * Generate summary statistics for the report
   * @param results Transformed results
   * @returns Summary statistics
   */
  generateSummary(results: any[]): any {
    // Calculate summary statistics
    const summary = {
      totalMonths: new Set(results.map(row => row.year_month)).size,
      totalOperations: new Set(results.map(row => row.operation)).size,
      totalAssets: new Set(results.map(row => row.assetTicker)).size,
      totalTransactionCount: results.reduce((sum, row) => sum + (row.totaltxncount || 0), 0),
      totalAssetAmount: results.reduce((sum, row) => sum + (row.totalAssetAmount || 0), 0)
    };
    
    return summary;
  }
  
  /**
   * Generate the Monthly Activity Report
   * @param parameters Report parameters
   * @returns Report data
   */
  async generateReport(parameters: Record<string, any>): Promise<{
    data: any[];
    columns: string[];
    executionTime: number;
    bytesProcessed: number;
    sql: string;
    metadata?: any;
  }> {
    try {
      const startTime = Date.now();
      
      // Extract and validate parameters
      const reportParams: ReportParameters = {
        walletId: parameters.walletId,
        startDate: parameters.startDate,
        endDate: parameters.endDate || 'CURRENT_DATE()',
        limit: parameters.limit || 5000 // Default to 5000 rows if not specified
      };
      
      // Log parameters for debugging
      console.log('MonthlyActivityReportGenerator.generateReport parameters:', {
        originalParameters: parameters,
        reportParams,
        hasWalletId: parameters.walletId ? 'YES' : 'NO',
        walletIdValue: parameters.walletId,
        hasStartDate: parameters.startDate ? 'YES' : 'NO',
        startDateValue: parameters.startDate,
        hasEndDate: parameters.endDate ? 'YES' : 'NO',
        endDateValue: parameters.endDate,
        hasLimit: parameters.limit ? 'YES' : 'NO',
        limitValue: parameters.limit || 5000
      });
      
      // Validate required parameters
      if (!reportParams.walletId) {
        throw new Error('Wallet ID is required for Monthly Activity Report');
      }
      
      if (!reportParams.startDate) {
        throw new Error('Start date is required for Monthly Activity Report');
      }
      
      // Extract filters
      const filters: any = {};
      if (parameters.assets) {
        filters.assets = Array.isArray(parameters.assets) 
          ? parameters.assets 
          : parameters.assets.split(',').map((a: string) => a.trim());
      }
      
      if (parameters.operations) {
        filters.operations = Array.isArray(parameters.operations) 
          ? parameters.operations 
          : parameters.operations.split(',').map((op: string) => op.trim());
      }
      
      // Generate SQL
      const sql = this.buildMonthlyActivityReportSQL(reportParams, filters);
      
      // Execute query
      if (!this.queryExecutor) {
        throw new Error('QueryExecutor not initialized');
      }
      
      // Log parameters before query execution
      console.log('MonthlyActivityReportGenerator before executeQuery:', {
        hasParameters: true,
        parameters: reportParams,
        hasWalletId: reportParams.walletId ? 'YES' : 'NO',
        walletIdValue: reportParams.walletId,
        hasStartDate: reportParams.startDate ? 'YES' : 'NO',
        startDateValue: reportParams.startDate,
        hasEndDate: reportParams.endDate ? 'YES' : 'NO',
        endDateValue: reportParams.endDate
      });
      
      // Pass the parameters to the query executor
      const executionResult = await this.queryExecutor.executeQuery(sql, reportParams);
      
      if (!executionResult.success || !executionResult.data) {
        throw new Error(`Query execution failed: ${executionResult.error?.message || 'Unknown error'}`);
      }
      
      const rows = executionResult.data;
      
      // Transform results
      const results = this.transformResults(rows);
      
      // Generate summary statistics
      const summary = this.generateSummary(results);
      
      // Define columns based on the SQL query results
      const columns = [
        'year_month',
        'operation',
        'assetTicker',
        'fromAddress',
        'toAddress',
        'feeType',
        'rewardFeeType',
        'rewardType',
        'totalAssetAmount',
        'totaltxncount'
      ];
      
      const executionTime = Date.now() - startTime;
      
      return {
        data: results,
        columns,
        executionTime,
        bytesProcessed: executionResult.metadata.bytesProcessed || 0,
        sql,
        metadata: {
          summary,
          totalRecords: results.length,
          period: {
            startDate: reportParams.startDate,
            endDate: reportParams.endDate,
            walletId: reportParams.walletId
          }
        }
      };
    } catch (error) {
      console.error('Error generating Monthly Activity Report:', error);
      throw error;
    }
  }
}
