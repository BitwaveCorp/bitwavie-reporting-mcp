/**
 * Lots Report Generator - Derivative Report #1
 * 
 * Generates lot-level inventory positions with:
 * - Asset information (asset, assetId) 
 * - Lot details (lotId, lotAcquisitionTimestampSEC)
 * - Unit calculations (unitsAcquired, unitsDisposed, qty)
 * - Cost basis tracking (costBasisAcquired, costBasisRelieved, costBasis)
 * - Valuation calculations (carryingValue, adjustedToValue)
 * - Impairment and revaluation adjustments
 */

import { BigQueryClient } from '../services/bigquery-client.js';
import { QueryExecutor } from '../services/query-executor.js';
import { 
  LotsReportRecord, 
  ReportParameters, 
  FieldMetadata 
} from '../types/actions-report.js';

export class LotsReportGenerator {
  private bigQueryClient: BigQueryClient;
  private queryExecutor: QueryExecutor;

  // Field metadata for natural language query mapping
  private static readonly FIELD_METADATA: FieldMetadata[] = [
    {
      column: 'lotId',
      description: 'Unique identifier for each lot/batch of assets acquired',
      type: 'string',
      category: 'identifier',
      aliases: ['lot', 'batch', 'lot id'],
      common_queries: ['lot performance', 'specific lot', 'lot details'],
      aggregatable: false,
      filterable: true
    },
    {
      column: 'asset',
      description: 'Asset symbol/ticker (e.g., "BTC", "ETH")',
      type: 'string',
      category: 'asset',
      aliases: ['coin', 'token', 'cryptocurrency', 'symbol', 'ticker'],
      common_queries: ['BTC lots', 'ETH lots', 'asset breakdown'],
      aggregatable: false,
      filterable: true
    },
    {
      column: 'qty',
      description: 'Current remaining quantity in lot (unitsAcquired - unitsDisposed)',
      type: 'number',
      category: 'asset',
      aliases: ['quantity', 'remaining', 'balance', 'units left'],
      common_queries: ['lot sizes', 'remaining quantity', 'current holdings'],
      aggregatable: true,
      filterable: true
    },
    {
      column: 'costBasis',
      description: 'Current remaining cost basis (costBasisAcquired - costBasisRelieved)',
      type: 'number',
      category: 'financial',
      aliases: ['cost', 'basis', 'cost basis', 'investment'],
      common_queries: ['lot performance', 'cost basis', 'investment tracking'],
      aggregatable: true,
      filterable: true
    },
    {
      column: 'carryingValue',
      description: 'Current book value (cost basis - impairments + reversals)',
      type: 'number',
      category: 'financial',
      aliases: ['book value', 'carrying amount', 'net value', 'current value'],
      common_queries: ['lot performance', 'book value', 'carrying value'],
      aggregatable: true,
      filterable: true
    },
    {
      column: 'adjustedToValue',
      description: 'Fair value adjusted carrying value',
      type: 'number',
      category: 'financial',
      aliases: ['fair value', 'adjusted value', 'market value'],
      common_queries: ['fair value', 'adjusted value', 'market valuation'],
      aggregatable: true,
      filterable: true
    },
    {
      column: 'timestampSEC',
      description: 'Unix timestamp when this lot was originally acquired',
      type: 'number',
      category: 'temporal',
      aliases: ['acquisition date', 'purchase date', 'acquired when', 'timestamp'],
      common_queries: ['oldest lots', 'newest lots', 'acquisition timing'],
      aggregatable: false,
      filterable: true
    },
    {
      column: 'impairmentExpense',
      description: 'Impairment losses recorded against this lot',
      type: 'number',
      category: 'financial',
      aliases: ['impairment', 'losses', 'writedown'],
      common_queries: ['impaired lots', 'impairment losses', 'writedowns'],
      aggregatable: true,
      filterable: true
    }
  ];

  constructor(bigQueryClient: BigQueryClient) {
    this.bigQueryClient = bigQueryClient;
    
    // Use project ID from environment variable, with fallback to hardcoded value
    const projectId = process.env.GOOGLE_CLOUD_PROJECT_ID || 'bitwave-solutions';
    console.log(`LotsReportGenerator: Initializing QueryExecutor with project ID: ${projectId}`);
    this.queryExecutor = new QueryExecutor(projectId);
  }

  // ========================================================================
  // MAIN REPORT GENERATION
  // ========================================================================

  async generate(
    parameters: ReportParameters, 
    filters?: {
      assets?: string[];
      minQty?: number;
      maxAge?: number; // days
      onlyImpaired?: boolean;
    }
  ): Promise<LotsReportRecord[]> {
    
    console.log('🔄 Generating Lots Report...', { parameters, filters });
    
    // Apply default values for missing parameters
    if (!parameters.asOfDate && !parameters.asOfSEC) {
      parameters.asOfDate = '2050-12-31';
      console.log('LotsReportGenerator: Using default asOfDate: 2050-12-31');
    }
    
    try {
      // Build the SQL query
      const sql = this.buildLotsReportSQL(parameters, filters);
      
      // Execute the query
      const rawResults = await this.bigQueryClient.executeReportQuery(sql, parameters);
      
      // Transform and validate results
      const lotsRecords = this.transformResults(rawResults);
      
      console.log(`✅ Lots Report generated: ${lotsRecords.length} lots`);
      
      return lotsRecords;
      
    } catch (error) {
      console.error('❌ Lots Report generation failed:', error);
      throw new Error(`Lots Report generation failed: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  // ========================================================================
  // SQL QUERY BUILDING
  // ========================================================================

  private buildLotsReportSQL(
    parameters: ReportParameters, 
    filters?: any
  ): string {
    
    const whereConditions = this.buildWhereConditions(parameters, filters);
    const havingConditions = this.buildHavingConditions(filters);
    
    // Get table reference from environment variables with fallbacks
    const projectId = process.env.GOOGLE_CLOUD_PROJECT_ID || 'bitwave-solutions';
    const datasetId = process.env.BIGQUERY_DATASET_ID || '0_Bitwavie_MCP';
    const tableId = process.env.BIGQUERY_TABLE_ID || '2622d4df5b2a15ec811e_gl_actions';
    const fullTablePath = `${projectId}.${datasetId}.${tableId}`;
    
    console.log(`LotsReportGenerator: Using table: ${fullTablePath}`);

    return `
      WITH actions AS (
        SELECT
          runId, lotId, lotAcquisitionTimestampSEC, asset, assetId, action, status, inventory,
          IFNULL(CAST(assetUnitAdj AS BIGNUMERIC), 0) as assetUnitAdj,
          IF(IFNULL(CAST(assetUnitAdj AS BIGNUMERIC), 0) > 0, IFNULL(CAST(assetUnitAdj AS BIGNUMERIC), 0), 0) as unitsAcquired,
          IF(IFNULL(CAST(assetUnitAdj AS BIGNUMERIC), 0) > 0, 0, ABS(IFNULL(CAST(assetUnitAdj AS BIGNUMERIC), 0))) as unitsDisposed,
          IFNULL(CAST(costBasisAcquired AS BIGNUMERIC), 0) as costBasisAcquired,
          IFNULL(CAST(originalCostBasisDisposed AS BIGNUMERIC), 0) AS costBasisRelieved,
          IFNULL(CAST(impairmentExpense AS BIGNUMERIC), 0) AS impairmentExpense,
          IFNULL(CAST(impairmentReversal AS BIGNUMERIC), 0) AS impairmentReversal,
          IFNULL(CAST(revaluationAdjustmentUpward AS BIGNUMERIC), 0) AS revaluationAdjustmentUpward,
          IFNULL(CAST(revaluationAdjustmentDownward AS BIGNUMERIC), 0) AS revaluationAdjustmentDownward,
          IFNULL(CAST(impairmentExpenseDisposed AS BIGNUMERIC), 0) AS impairmentExpenseDisposed,
          txnId, eventId
        FROM \`${fullTablePath}\`
        ${whereConditions}
      ),
      lot_to_txn as (
        SELECT txnId, lotId 
        FROM actions
        WHERE LOWER(actions.action) = 'buy'
        GROUP BY txnId, lotId
      )
      SELECT
        actions.lotId,
        ltt.txnId,
        asset,
        assetId,
        lotAcquisitionTimestampSEC as timestampSEC,
        SUM(unitsAcquired) as unitsAcquired,
        SUM(unitsDisposed) as unitsDisposed,
        SUM(assetUnitAdj) as qty,
        SUM(costBasisAcquired) as costBasisAcquired,
        SUM(costBasisRelieved) as costBasisRelieved,
        SUM(impairmentExpense) AS impairmentExpense,
        SUM(impairmentReversal) AS impairmentReversal,
        SUM(revaluationAdjustmentUpward) AS revaluationAdjustmentUpward,
        SUM(revaluationAdjustmentDownward) AS revaluationAdjustmentDownward,
        (SUM(costBasisAcquired) - SUM(costBasisRelieved)) AS costBasis,
        (SUM(costBasisAcquired) - SUM(costBasisRelieved) - SUM(impairmentExpense) + SUM(impairmentReversal) + SUM(impairmentExpenseDisposed)) AS carryingValue,
        (SUM(costBasisAcquired) - SUM(costBasisRelieved) - SUM(impairmentExpense) + SUM(impairmentReversal) + SUM(revaluationAdjustmentUpward) - SUM(revaluationAdjustmentDownward) + SUM(impairmentExpenseDisposed)) AS adjustedToValue
      FROM actions
      LEFT JOIN lot_to_txn ltt ON ltt.lotId = actions.lotId
      GROUP BY lotId, ltt.txnId, lotAcquisitionTimestampSEC, asset, assetId
      ${havingConditions}
      -- Use lotAcquisitionTimestampSEC which is included in the GROUP BY clause
      ORDER BY lotAcquisitionTimestampSEC DESC, lotId DESC
    `.trim();
  }

  private buildWhereConditions(parameters: ReportParameters, filters?: any): string {
    const conditions: string[] = [];

    // Only add runId condition if it's provided in parameters
    if (parameters.runId) {
      conditions.push(`runId = @runId`);
    }
    
    if (parameters.orgId) {
      conditions.push(`orgId = @orgId`);
    }

    // As-of date filter
    if (parameters.asOfSEC) {
      conditions.push(`timestampSEC <= @asOfSEC`);
    } else if (parameters.asOfDate) {
      // Convert asOfDate to end of day (11:59:59 PM UTC) in Unix timestamp format
      conditions.push(`timestampSEC <= UNIX_SECONDS(TIMESTAMP(DATE(@asOfDate) || ' 23:59:59'))`);
      console.log(`LotsReportGenerator: Using asOfDate filter for end of day: ${parameters.asOfDate}`);
    }

    // Asset filters
    if (filters?.assets && filters.assets.length > 0) {
      const assetList = filters.assets.map((asset: string) => `'${asset}'`).join(', ');
      conditions.push(`asset IN (${assetList})`);
    }

    // Age filter (for lots older than X days)
    if (filters?.maxAge) {
      const maxAgeTimestamp = Math.floor(Date.now() / 1000) - (filters.maxAge * 24 * 60 * 60);
      conditions.push(`lotAcquisitionTimestampSEC >= ${maxAgeTimestamp}`);
    }

    return conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';
  }

  private buildHavingConditions(filters?: any): string {
    const conditions: string[] = [];

    // Default: only include lots with remaining quantity
    conditions.push('SUM(assetUnitAdj) > 0');

    // Minimum quantity filter
    if (filters?.minQty && filters.minQty > 0) {
      conditions.push(`SUM(assetUnitAdj) >= ${filters.minQty}`);
    }

    // Only impaired lots filter
    if (filters?.onlyImpaired) {
      conditions.push('SUM(impairmentExpense) > 0');
    }

    return conditions.length > 0 ? `HAVING ${conditions.join(' AND ')}` : '';
  }

  // ========================================================================
  // RESULT TRANSFORMATION AND VALIDATION
  // ========================================================================

  private transformResults(rawResults: any[]): LotsReportRecord[] {
    return rawResults.map(row => {
      // Convert BigQuery numeric types to JavaScript numbers
      const record: LotsReportRecord = {
        lotId: row.lotId || '',
        txnId: row.txnId,
        asset: row.asset || '',
        assetId: row.assetId || '',
        timestampSEC: parseInt(row.timestampSEC) || 0,
        unitsAcquired: this.parseNumeric(row.unitsAcquired),
        unitsDisposed: this.parseNumeric(row.unitsDisposed),
        qty: this.parseNumeric(row.qty),
        costBasisAcquired: this.parseNumeric(row.costBasisAcquired),
        costBasisRelieved: this.parseNumeric(row.costBasisRelieved),
        costBasis: this.parseNumeric(row.costBasis),
        impairmentExpense: this.parseNumeric(row.impairmentExpense),
        impairmentReversal: this.parseNumeric(row.impairmentReversal),
        revaluationAdjustmentUpward: this.parseNumeric(row.revaluationAdjustmentUpward),
        revaluationAdjustmentDownward: this.parseNumeric(row.revaluationAdjustmentDownward),
        carryingValue: this.parseNumeric(row.carryingValue),
        adjustedToValue: this.parseNumeric(row.adjustedToValue)
      };

      // Validate critical fields
      this.validateLotRecord(record);

      return record;
    });
  }

  private parseNumeric(value: any): number {
    if (value === null || value === undefined || value === '') {
      return 0;
    }
    
    const parsed = parseFloat(value.toString());
    return isNaN(parsed) ? 0 : parsed;
  }

  private validateLotRecord(record: LotsReportRecord): void {
    // Basic validation rules
    if (!record.lotId) {
      throw new Error('Invalid lot record: missing lotId');
    }
    
    if (!record.asset) {
      throw new Error(`Invalid lot record ${record.lotId}: missing asset`);
    }
    
    if (record.qty < 0) {
      console.warn(`Warning: Lot ${record.lotId} has negative quantity: ${record.qty}`);
    }
    
    if (record.costBasis < 0) {
      console.warn(`Warning: Lot ${record.lotId} has negative cost basis: ${record.costBasis}`);
    }
  }

  // ========================================================================
  // ANALYSIS AND REPORTING UTILITIES
  // ========================================================================

  /**
   * Generate summary statistics for the lots report
   */
  generateSummary(lots: LotsReportRecord[]): {
    totalLots: number;
    assetBreakdown: Record<string, any>;
    totalPortfolioValue: number;
    totalCostBasis: number;
    totalUnrealizedGL: number;
    averageLotAge: number;
    impairedLots: number;
  } {
    const summary = {
      totalLots: lots.length,
      assetBreakdown: {} as Record<string, any>,
      totalPortfolioValue: 0,
      totalCostBasis: 0,
      totalUnrealizedGL: 0,
      averageLotAge: 0,
      impairedLots: 0
    };

    const currentTimestamp = Math.floor(Date.now() / 1000);
    let totalAge = 0;

    lots.forEach(lot => {
      // Portfolio totals
      summary.totalPortfolioValue += lot.carryingValue;
      summary.totalCostBasis += lot.costBasis;
      summary.totalUnrealizedGL += (lot.adjustedToValue - lot.costBasis);

      // Age calculation
      const ageInDays = (currentTimestamp - lot.timestampSEC) / (24 * 60 * 60);
      totalAge += ageInDays;

      // Impaired lots
      if (lot.impairmentExpense > 0) {
        summary.impairedLots++;
      }

      // Asset breakdown
      if (!summary.assetBreakdown[lot.asset]) {
        summary.assetBreakdown[lot.asset] = {
          lotCount: 0,
          totalQty: 0,
          totalCostBasis: 0,
          totalCarryingValue: 0,
          avgLotSize: 0
        };
      }

      const assetData = summary.assetBreakdown[lot.asset];
      assetData.lotCount++;
      assetData.totalQty += lot.qty;
      assetData.totalCostBasis += lot.costBasis;
      assetData.totalCarryingValue += lot.carryingValue;
      assetData.avgLotSize = assetData.totalQty / assetData.lotCount;
    });

    summary.averageLotAge = lots.length > 0 ? totalAge / lots.length : 0;

    return summary;
  }

  /**
   * Filter lots based on natural language criteria
   */
  filterLots(lots: LotsReportRecord[], criteria: {
    assets?: string[];
    minValue?: number;
    maxAge?: number;
    onlyImpaired?: boolean;
  }): LotsReportRecord[] {
    return lots.filter(lot => {
      // Asset filter
      if (criteria.assets && criteria.assets.length > 0) {
        if (!criteria.assets.includes(lot.asset)) {
          return false;
        }
      }

      // Minimum value filter
      if (criteria.minValue && lot.carryingValue < criteria.minValue) {
        return false;
      }

      // Age filter (days)
      if (criteria.maxAge) {
        const currentTimestamp = Math.floor(Date.now() / 1000);
        const ageInDays = (currentTimestamp - lot.timestampSEC) / (24 * 60 * 60);
        if (ageInDays > criteria.maxAge) {
          return false;
        }
      }

      // Impaired lots only
      if (criteria.onlyImpaired && lot.impairmentExpense <= 0) {
        return false;
      }

      return true;
    });
  }

  /**
   * Sort lots by various criteria
   */
  sortLots(lots: LotsReportRecord[], sortBy: 'age' | 'value' | 'qty' | 'asset', ascending: boolean = false): LotsReportRecord[] {
    return [...lots].sort((a, b) => {
      let comparison = 0;

      switch (sortBy) {
        case 'age':
          comparison = a.timestampSEC - b.timestampSEC;
          break;
        case 'value':
          comparison = a.carryingValue - b.carryingValue;
          break;
        case 'qty':
          comparison = a.qty - b.qty;
          break;
        case 'asset':
          comparison = a.asset.localeCompare(b.asset);
          break;
      }

      return ascending ? comparison : -comparison;
    });
  }

  // ========================================================================
  // FIELD METADATA ACCESS
  // ========================================================================

  static getFieldMetadata(): FieldMetadata[] {
    return LotsReportGenerator.FIELD_METADATA;
  }

  static getColumnsByCategory(category: string): string[] {
    return LotsReportGenerator.FIELD_METADATA
      .filter(field => field.category === category)
      .map(field => field.column);
  }

  static getColumnAliases(column: string): string[] {
    const field = LotsReportGenerator.FIELD_METADATA.find(f => f.column === column);
    return field ? field.aliases : [];
  }

  // ========================================================================
  // CSV EXPORT SUPPORT
  // ========================================================================

  exportToCSV(lots: LotsReportRecord[]): string {
    const headers = [
      'lotId', 'txnId', 'asset', 'assetId', 'acquisitionDate', 
      'unitsAcquired', 'unitsDisposed', 'qty',
      'costBasisAcquired', 'costBasisRelieved', 'costBasis',
      'impairmentExpense', 'impairmentReversal',
      'revaluationAdjUpward', 'revaluationAdjDownward',
      'carryingValue', 'adjustedToValue'
    ];

    const csvRows = [headers.join(',')];

    lots.forEach(lot => {
      const acquisitionDate = new Date(lot.timestampSEC * 1000).toISOString().split('T')[0];
      
      const row = [
        lot.lotId,
        lot.txnId || '',
        lot.asset,
        lot.assetId,
        acquisitionDate,
        lot.unitsAcquired.toFixed(8),
        lot.unitsDisposed.toFixed(8),
        lot.qty.toFixed(8),
        lot.costBasisAcquired.toFixed(2),
        lot.costBasisRelieved.toFixed(2),
        lot.costBasis.toFixed(2),
        lot.impairmentExpense.toFixed(2),
        lot.impairmentReversal.toFixed(2),
        lot.revaluationAdjustmentUpward.toFixed(2),
        lot.revaluationAdjustmentDownward.toFixed(2),
        lot.carryingValue.toFixed(2),
        lot.adjustedToValue.toFixed(2)
      ];

      csvRows.push(row.join(','));
    });

    return csvRows.join('\n');
  }

  /**
   * Generate the Lots Report based on provided parameters
   * 
   * This is the standardized interface method used by the ReportRegistry
   * 
   * @param parameters Report parameters extracted from natural language query
   * @returns Report generation result with data, columns, execution time, and SQL
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
        asOfDate: parameters.asOfDate || 'CURRENT_DATE()'
      };
      
      // Extract filters
      const filters: any = {};
      if (parameters.assets) {
        filters.assets = Array.isArray(parameters.assets) 
          ? parameters.assets 
          : parameters.assets.split(',').map((a: string) => a.trim());
      }
      
      // Handle includeDisposed parameter
      filters.includeDisposed = parameters.includeDisposed === true || 
                              parameters.includeDisposed === 'true' || 
                              false;
      
      // Generate SQL
      const sql = this.buildLotsReportSQL(reportParams, filters);
      
      // Execute query
      if (!this.queryExecutor) {
        throw new Error('QueryExecutor not initialized');
      }
      
      const executionResult = await this.queryExecutor.executeQuery(sql);
      
      if (!executionResult.success || !executionResult.data) {
        throw new Error(`Query execution failed: ${executionResult.error?.message || 'Unknown error'}`);
      }
      
      const rows = executionResult.data;
      
      // Transform results
      const results = this.transformResults(rows);
      
      // Generate summary statistics
      const summary = this.generateSummary(results);
      
      // Define columns based on the results
      const columns = [
        'lotId',
        'asset',
        'inventory',
        'acquisitionDate',
        'qty',
        'costBasis',
        'carryingValue',
        'adjustedToValue',
        'impairmentExpense',
        'daysHeld',
        'isLongTerm'
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
          asOfDate: reportParams.asOfDate
        }
      };
    } catch (error) {
      console.error('Error generating Lots Report:', error);
      throw error;
    }
  }
}