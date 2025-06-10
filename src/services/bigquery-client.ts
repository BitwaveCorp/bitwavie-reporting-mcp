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

export class BigQueryClient {
  private bigquery: BigQuery | null = null;
  private config: BigQueryConfig | null = null;
  private dataset: Dataset | null = null;
  private table: Table | null = null;
  private queryCache: Map<string, { data: any; timestamp: number }> = new Map();
  private readonly CACHE_TTL = 5 * 60 * 1000; // 5 minutes

  constructor() {
    // Initialize empty - configuration happens via configure()
  }

  // ========================================================================
  // CONFIGURATION
  // ========================================================================
  
  async configure(config: BigQueryConfig): Promise<void> {
    this.config = config;
    
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
      
      console.log(`✅ BigQuery connected: ${config.projectId}.${config.datasetId}.${config.tableId}`);
    } catch (error) {
      throw new Error(`Failed to configure BigQuery: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  private async testConnection(): Promise<void> {
    if (!this.table) {
      throw new Error('BigQuery table not configured');
    }

    try {
      const [metadata] = await this.table.getMetadata();
      console.log(`Table schema verified: ${metadata.schema?.fields?.length || 0} fields`);
    } catch (error) {
      throw new Error(`BigQuery connection test failed: ${error instanceof Error ? error.message : String(error)}`);
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
    
    try {
      // Generate SQL from parsed query
      const sql = this.generateAnalyticalSQL(parseResult, parameters);
      
      // Check cache
      const cacheKey = this.getCacheKey(sql, parameters);
      const cached = this.getFromCache(cacheKey);
      if (cached) {
        return {
          success: true,
          data: cached,
          metadata: {
            rows_processed: cached.length,
            execution_time_ms: Date.now() - startTime,
            cached: true,
            columns_used: parseResult.columns.map(col => col.mappedColumns).flat()
          }
        };
      }

      // Execute query
      const results = await this.executeQuery(sql);
      
      // Cache results
      this.setCache(cacheKey, results);

      // Format results based on query intent
      const formattedData = this.formatAnalyticalResults(results, parseResult);

      return {
        success: true,
        data: formattedData,
        summary: this.generateResultSummary(formattedData, parseResult),
        metadata: {
          rows_processed: results.length,
          execution_time_ms: Date.now() - startTime,
          cached: false,
          columns_used: parseResult.columns.map(col => col.mappedColumns).flat()
        }
      };

    } catch (error) {
      return {
        success: false,
        error: {
          type: 'COMPUTATION_ERROR',
          message: error instanceof Error ? error.message : String(error),
          suggestions: this.getErrorSuggestions(error)
        },
        metadata: {
          rows_processed: 0,
          execution_time_ms: Date.now() - startTime,
          cached: false,
          columns_used: []
        }
      };
    }
  }

  /**
   * Execute predefined report queries
   */
  async executeReportQuery(sql: string, parameters: ReportParameters): Promise<any[]> {
    const parameterizedSQL = this.replaceParameters(sql, parameters);
    return await this.executeQuery(parameterizedSQL);
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
      columnMapping.mappedColumns.forEach((column: string) => {
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
        columns.some(col => col.mappedColumns.some((mcol: string) => this.getColumnMetadata(mcol)?.aggregatable))) {
      
      const groupColumns: string[] = ['asset']; // Always group by asset for financial queries
      
      // Add additional non-aggregatable columns to GROUP BY
      columns.forEach(columnMapping => {
        columnMapping.mappedColumns.forEach((column: string) => {
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
    const { intent, aggregationType } = parseResult;
    
    if (aggregationType === 'sum' || aggregationType === 'count') {
      // Order by the aggregated values (descending for totals)
      return 'ORDER BY 2 DESC'; // Second column is usually the aggregated value
    }
    
    // Default ordering
    return 'ORDER BY asset ASC, timestampSEC DESC';
  }

  // ========================================================================
  // QUERY EXECUTION AND UTILITIES
  // ========================================================================

  private async executeQuery(sql: string): Promise<any[]> {
    if (!this.bigquery) {
      throw new Error('BigQuery client not initialized');
    }

    try {
      const [job] = await this.bigquery.createQueryJob({
        query: sql,
        location: 'US', // Adjust based on your dataset location
        jobTimeoutMs: 60000, // 60 second timeout
      });

      console.log(`Query job created: ${job.id}`);
      
      const [rows] = await job.getQueryResults();
      console.log(`Query returned ${rows.length} rows`);
      
      return rows;
    } catch (error) {
      console.error('BigQuery execution error:', error);
      throw error;
    }
  }

  private replaceParameters(sql: string, parameters: ReportParameters): string {
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
    } else if (parameters.asOfDate) {
      const asOfSEC = Math.floor(new Date(parameters.asOfDate + ' 23:59:59').getTime() / 1000);
      parameterizedSQL = parameterizedSQL.replace(/@asOfSEC/g, asOfSEC.toString());
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

  private getColumnMetadata(column: string): any {
    // This would reference the ACTIONS_REPORT_METADATA
    // For now, returning basic metadata
    const financialColumns = [
      'shortTermGainLoss', 'longTermGainLoss', 'undatedGainLoss',
      'costBasisAcquired', 'costBasisRelieved', 'carryingValue',
      'fairMarketValueDisposed', 'assetUnitAdj'
    ];
    
    return {
      aggregatable: financialColumns.includes(column)
    };
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