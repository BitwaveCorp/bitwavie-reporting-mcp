/**
 * Inventory Balance Report Generator - Derivative Report #3
 * 
 * Generates point-in-time snapshot of current inventory positions:
 * - Current quantity held (qty)
 * - Net cost basis (costBasisAcquired - costBasisRelieved)
 * - Carrying value (cost basis adjusted for impairments/reversals)
 * - Various adjustment components (impairment, fair value, revaluation)
 * - Groupable by inventory/subsidiary
 */

import { BigQueryClient } from '../services/bigquery-client.js';
import { QueryExecutor } from '../services/query-executor.js';
import { 
  InventoryBalanceRecord, 
  ReportParameters, 
  FieldMetadata 
} from '../types/actions-report.js';

export class InventoryBalanceGenerator {
  private bigQueryClient: BigQueryClient;
  private queryExecutor: QueryExecutor;

  // Field metadata for natural language query mapping
  private static readonly FIELD_METADATA: FieldMetadata[] = [
    {
      column: 'asset',
      description: 'Asset symbol/ticker (e.g., "BTC", "ETH")',
      type: 'string',
      category: 'asset',
      aliases: ['coin', 'token', 'cryptocurrency', 'symbol', 'ticker'],
      common_queries: ['BTC balance', 'ETH balance', 'asset breakdown'],
      aggregatable: false,
      filterable: true
    },
    {
      column: 'inventory',
      description: 'Inventory/account classification',
      type: 'string',
      category: 'classification',
      aliases: ['account', 'classification', 'category'],
      common_queries: ['inventory positions', 'account breakdown'],
      aggregatable: false,
      filterable: true
    },
    {
      column: 'subsidiaryId',
      description: 'Subsidiary identifier (if included)',
      type: 'string',
      category: 'classification',
      aliases: ['subsidiary', 'entity', 'legal entity'],
      common_queries: ['subsidiary holdings', 'entity breakdown'],
      aggregatable: false,
      filterable: true
    },
    {
      column: 'qty',
      description: 'Current total quantity held across all lots',
      type: 'number',
      category: 'asset',
      aliases: ['quantity', 'balance', 'units', 'holdings'],
      common_queries: ['current holdings', 'quantity held', 'asset balance'],
      aggregatable: true,
      filterable: true
    },
    {
      column: 'costBasis',
      description: 'Net cost basis (costBasisAcquired - costBasisRelieved)',
      type: 'number',
      category: 'financial',
      aliases: ['cost', 'basis', 'cost basis', 'investment cost'],
      common_queries: ['cost basis summary', 'total invested', 'cost breakdown'],
      aggregatable: true,
      filterable: true
    },
    {
      column: 'carryingValue',
      description: 'Current book value (cost basis adjusted for impairments/reversals)',
      type: 'number',
      category: 'financial',
      aliases: ['book value', 'carrying amount', 'net value', 'portfolio value'],
      common_queries: ['total portfolio value', 'book value', 'current value'],
      aggregatable: true,
      filterable: true
    },
    {
      column: 'costBasisAcquired',
      description: 'Total USD cost basis of all acquisitions',
      type: 'number',
      category: 'financial',
      aliases: ['total acquired', 'purchase total', 'acquisition cost'],
      common_queries: ['total purchases', 'acquisition summary'],
      aggregatable: true,
      filterable: true
    },
    {
      column: 'costBasisRelieved',
      description: 'Total USD cost basis of all disposals',
      type: 'number',
      category: 'financial',
      aliases: ['total disposed', 'sale total', 'disposal cost'],
      common_queries: ['total sales', 'disposal summary'],
      aggregatable: true,
      filterable: true
    },
    {
      column: 'impairmentExpense',
      description: 'Net impairment losses in inventory',
      type: 'number',
      category: 'financial',
      aliases: ['impairment', 'writedown', 'losses'],
      common_queries: ['impaired positions', 'impairment losses'],
      aggregatable: true,
      filterable: true
    }
  ];

  constructor(bigQueryClient: BigQueryClient) {
    this.bigQueryClient = bigQueryClient;
    
    // Use project ID from environment variable, with fallback to hardcoded value
    const projectId = process.env.GOOGLE_CLOUD_PROJECT_ID || 'bitwave-solutions';
    console.log(`InventoryBalanceGenerator: Initializing QueryExecutor with project ID: ${projectId}`);
    this.queryExecutor = new QueryExecutor(projectId);
  }

  // ========================================================================
  // MAIN REPORT GENERATION
  // ========================================================================

  async generate(
    parameters: ReportParameters, 
    groupBy?: ('asset' | 'inventory' | 'subsidiary')[],
    filters?: {
      assets?: string[];
      inventories?: string[];
      subsidiaries?: string[];
      minValue?: number;
      excludeZeroBalances?: boolean;
    }
  ): Promise<InventoryBalanceRecord[]> {
    
    console.log('🔄 Generating Inventory Balance Report...', { parameters, groupBy, filters });
    
    // Apply default values for missing parameters
    if (!parameters.asOfDate && !parameters.asOfSEC) {
      parameters.asOfDate = '2050-12-31';
      console.log('InventoryBalanceGenerator: Using default asOfDate: 2050-12-31');
    }
    
    try {
      // Build the SQL query
      const sql = this.buildInventoryBalanceSQL(parameters, groupBy, filters);
      
      // Execute the query
      const queryResult = await this.bigQueryClient.executeReport(sql, parameters);
      
      // Extract data from QueryResult
      const rawResults = queryResult.data || [];
      
      // Transform and validate results
      const inventoryRecords = this.transformResults(rawResults);
      
      console.log(`✅ Inventory Balance Report generated: ${inventoryRecords.length} records`);
      
      return inventoryRecords;
      
    } catch (error) {
      console.error('❌ Inventory Balance Report generation failed:', error);
      throw new Error(`Inventory Balance Report generation failed: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  // ========================================================================
  // SQL QUERY BUILDING
  // ========================================================================

  private buildInventoryBalanceSQL(
    parameters: ReportParameters, 
    groupBy?: ('asset' | 'inventory' | 'subsidiary')[],
    filters?: any
  ): string {
    
    const groupByColumns = this.buildGroupByClause(groupBy);
    const whereConditions = this.buildWhereConditions(parameters, filters);
    const havingConditions = this.buildHavingConditions(filters);
    
    // Get table reference from environment variables with fallbacks
    const projectId = process.env.GOOGLE_CLOUD_PROJECT_ID || 'bitwave-solutions';
    const datasetId = process.env.BIGQUERY_DATASET_ID || '0_Bitwavie_MCP';
    const tableId = process.env.BIGQUERY_TABLE_ID || '2622d4df5b2a15ec811e_gl_actions';
    const fullTablePath = `${projectId}.${datasetId}.${tableId}`;
    
    console.log(`InventoryBalanceGenerator: Using table: ${fullTablePath}`);

    return `
      WITH deduplicated_actions AS (
        SELECT AS VALUE ANY_VALUE(t)
        FROM \`${fullTablePath}\` AS t
        ${whereConditions}
        GROUP BY t.eventId, t.lotId, t.inventory
      ),
      actions AS (
        SELECT
          asset, 
          assetId, 
          inventory,
          COALESCE(subsidiaryId, 'DEFAULT') as subsidiaryId,
          IFNULL(CAST(assetUnitAdj AS BIGNUMERIC), 0) as qty,
          IFNULL(CAST(costBasisAcquired AS BIGNUMERIC), 0) as costBasisAcquired,
          IFNULL(CAST(originalCostBasisDisposed AS BIGNUMERIC), 0) AS costBasisRelieved,
          IFNULL(CAST(impairmentExpense AS BIGNUMERIC), 0) AS impairmentExpense,
          IFNULL(CAST(impairmentReversal AS BIGNUMERIC), 0) AS impairmentExpenseReversal,
          IFNULL(CAST(fairValueAdjustmentUpward AS BIGNUMERIC), 0) AS fairValueAdjustmentUpward,
          IFNULL(CAST(fairValueAdjustmentDownward AS BIGNUMERIC), 0) AS fairValueAdjustmentDownward,
          IFNULL(CAST(revaluationAdjustmentUpward AS BIGNUMERIC), 0) AS revaluationAdjustmentUpward,
          IFNULL(CAST(revaluationAdjustmentDownward AS BIGNUMERIC), 0) AS revaluationAdjustmentDownward,
          IFNULL(CAST(impairmentExpenseDisposed AS BIGNUMERIC), 0) AS impairmentExpenseDisposed
        FROM deduplicated_actions
      )
      SELECT
        ${groupByColumns.select}
        SUM(qty) as qty,
        SUM(costBasisAcquired) as costBasisAcquired,
        SUM(costBasisRelieved) AS costBasisRelieved,
        SUM(impairmentExpense) - SUM(impairmentExpenseDisposed) AS impairmentExpense,
        SUM(impairmentExpenseReversal) AS impairmentExpenseReversal,
        SUM(fairValueAdjustmentUpward) AS fairValueAdjustmentUpward,
        SUM(fairValueAdjustmentDownward) AS fairValueAdjustmentDownward,
        SUM(revaluationAdjustmentUpward) AS revaluationAdjustmentUpward,
        SUM(revaluationAdjustmentDownward) AS revaluationAdjustmentDownward,
        SUM(impairmentExpenseDisposed) AS impairmentExpenseDisposed,
        (SUM(costBasisAcquired) - SUM(costBasisRelieved)) AS costBasis,
        (SUM(costBasisAcquired) - SUM(costBasisRelieved) - SUM(impairmentExpense) + SUM(impairmentExpenseReversal) + SUM(impairmentExpenseDisposed)) AS carryingValue
      FROM actions
      ${groupByColumns.groupBy}
      ${havingConditions}
      ${groupByColumns.orderBy}
      ${parameters.limit ? `LIMIT ${parameters.limit}` : ''}
    `.trim();
  }

  private buildGroupByClause(groupBy?: ('asset' | 'inventory' | 'subsidiary')[]): {
    select: string;
    groupBy: string;
    orderBy: string;
  } {
    // Default grouping includes asset, assetId, and inventory
    const defaultColumns = ['asset', 'assetId', 'inventory'];
    let columns = [...defaultColumns];

    // Add additional grouping dimensions if specified
    if (groupBy && groupBy.length > 0) {
      groupBy.forEach(dimension => {
        switch (dimension) {
          case 'subsidiary':
            if (!columns.includes('subsidiaryId')) {
              columns.push('subsidiaryId');
            }
            break;
          case 'asset':
            // Already included in default
            break;
          case 'inventory':
            // Already included in default
            break;
        }
      });
    }

    return {
      select: columns.join(', ') + ',',
      groupBy: columns.length > 0 ? `GROUP BY ${columns.join(', ')}` : '',
      orderBy: 'ORDER BY asset ASC, inventory ASC'
    };
  }

  private buildWhereConditions(parameters: ReportParameters, filters?: any): string {
    const conditions: string[] = [];

    // Only add runId condition if it's provided in parameters
    if (parameters.runId) {
      conditions.push('t.runId = @runId');
    } else {
      console.log('InventoryBalanceGenerator: No runId provided, skipping runId filter');
    }
    
    if (parameters.orgId) {
      conditions.push('t.orgId = @orgId');
    }

    // As-of date filter
    if (parameters.asOfSEC) {
      conditions.push('t.timestampSEC <= @asOfSEC');
      console.log(`InventoryBalanceGenerator: Using asOfSEC filter: ${parameters.asOfSEC}`);
    } else if (parameters.asOfDate) {
      // Use asOfDate directly in the SQL query
      conditions.push("t.timestampSEC <= UNIX_SECONDS(TIMESTAMP(DATE(@asOfDate) || ' 23:59:59'))");
      console.log(`InventoryBalanceGenerator: Using asOfDate filter for end of day: ${parameters.asOfDate}`);
    }

    // Asset filters
    if (filters?.assets && filters.assets.length > 0) {
      const assetList = filters.assets.map((asset: string) => `'${asset}'`).join(', ');
      conditions.push(`t.asset IN (${assetList})`);
    }

    // Inventory filters
    if (filters?.inventories && filters.inventories.length > 0) {
      const inventoryList = filters.inventories.map((inv: string) => `'${inv}'`).join(', ');
      conditions.push(`t.inventory IN (${inventoryList})`);
    }

    // Subsidiary filters
    if (filters?.subsidiaries && filters.subsidiaries.length > 0) {
      const subsidList = filters.subsidiaries.map((sub: string) => `'${sub}'`).join(', ');
      conditions.push(`t.subsidiaryId IN (${subsidList})`);
    }

    return conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';
  }

  private buildHavingConditions(filters?: any): string {
    const conditions: string[] = [];

    // Exclude zero balances by default unless specified
    if (filters?.excludeZeroBalances !== false) {
      conditions.push('SUM(qty) != 0 OR (SUM(costBasisAcquired) - SUM(costBasisRelieved)) != 0');
    }

    // Minimum value filter
    if (filters?.minValue && filters.minValue > 0) {
      conditions.push(`ABS((SUM(costBasisAcquired) - SUM(costBasisRelieved) - SUM(impairmentExpense) + SUM(impairmentExpenseReversal) + SUM(impairmentExpenseDisposed))) >= ${filters.minValue}`);
    }

    return conditions.length > 0 ? `HAVING ${conditions.join(' AND ')}` : '';
  }

  // ========================================================================
  // RESULT TRANSFORMATION AND VALIDATION
  // ========================================================================

  private transformResults(rawResults: any[]): InventoryBalanceRecord[] {
    return rawResults.map(row => {
      const record: InventoryBalanceRecord = {
        asset: row.asset || '',
        assetId: row.assetId || '',
        inventory: row.inventory || '',
        subsidiaryId: row.subsidiaryId === 'DEFAULT' ? undefined : row.subsidiaryId,
        qty: this.parseNumeric(row.qty),
        costBasisAcquired: this.parseNumeric(row.costBasisAcquired),
        costBasisRelieved: this.parseNumeric(row.costBasisRelieved),
        costBasis: this.parseNumeric(row.costBasis),
        impairmentExpense: this.parseNumeric(row.impairmentExpense),
        impairmentExpenseReversal: this.parseNumeric(row.impairmentExpenseReversal),
        fairValueAdjustmentUpward: this.parseNumeric(row.fairValueAdjustmentUpward),
        fairValueAdjustmentDownward: this.parseNumeric(row.fairValueAdjustmentDownward),
        revaluationAdjustmentUpward: this.parseNumeric(row.revaluationAdjustmentUpward),
        revaluationAdjustmentDownward: this.parseNumeric(row.revaluationAdjustmentDownward),
        impairmentExpenseDisposed: this.parseNumeric(row.impairmentExpenseDisposed),
        carryingValue: this.parseNumeric(row.carryingValue)
      };

      // Validate the record
      this.validateInventoryRecord(record);

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

  private validateInventoryRecord(record: InventoryBalanceRecord): void {
    // Basic validation
    if (!record.asset) {
      throw new Error('Invalid inventory record: missing asset');
    }
    
    if (!record.inventory) {
      console.warn(`Warning: Inventory record for ${record.asset} missing inventory classification`);
    }

    // Mathematical validation
    const calculatedCostBasis = record.costBasisAcquired - record.costBasisRelieved;
    const tolerance = 0.01; // $0.01 tolerance
    
    if (Math.abs(record.costBasis - calculatedCostBasis) > tolerance) {
      console.warn(`Warning: Cost basis calculation mismatch for ${record.asset}/${record.inventory}: ` +
        `calculated ${calculatedCostBasis.toFixed(2)} vs recorded ${record.costBasis.toFixed(2)}`);
    }

    // Reasonableness checks
    if (record.costBasisAcquired < 0) {
      console.warn(`Warning: Negative cost basis acquired for ${record.asset}/${record.inventory}: ${record.costBasisAcquired}`);
    }
    
    if (record.costBasisRelieved < 0) {
      console.warn(`Warning: Negative cost basis relieved for ${record.asset}/${record.inventory}: ${record.costBasisRelieved}`);
    }
  }

  // ========================================================================
  // ANALYSIS AND REPORTING UTILITIES
  // ========================================================================

  /**
   * Generate summary statistics for the inventory balance report
   */
  generateSummary(inventoryRecords: InventoryBalanceRecord[]): {
    totalRecords: number;
    portfolioSummary: {
      totalPortfolioValue: number;
      totalCostBasis: number;
      totalUnrealizedGL: number;
      totalImpairments: number;
    };
    assetBreakdown: Record<string, {
      totalQty: number;
      totalValue: number;
      totalCostBasis: number;
      inventoryCount: number;
      percentOfPortfolio: number;
    }>;
    inventoryBreakdown: Record<string, {
      assetCount: number;
      totalValue: number;
      percentOfPortfolio: number;
    }>;
    subsidiaryBreakdown?: Record<string, {
      assetCount: number;
      totalValue: number;
      percentOfPortfolio: number;
    }>;
  } {
    const summary = {
      totalRecords: inventoryRecords.length,
      portfolioSummary: {
        totalPortfolioValue: 0,
        totalCostBasis: 0,
        totalUnrealizedGL: 0,
        totalImpairments: 0
      },
      assetBreakdown: {} as Record<string, any>,
      inventoryBreakdown: {} as Record<string, any>,
      subsidiaryBreakdown: {} as Record<string, any>
    };

    // Calculate portfolio totals
    inventoryRecords.forEach(record => {
      summary.portfolioSummary.totalPortfolioValue += record.carryingValue;
      summary.portfolioSummary.totalCostBasis += record.costBasis;
      summary.portfolioSummary.totalImpairments += record.impairmentExpense;
      
      // Unrealized G/L calculation (simplified)
      const unrealizedGL = record.fairValueAdjustmentUpward - record.fairValueAdjustmentDownward + 
                          record.revaluationAdjustmentUpward - record.revaluationAdjustmentDownward;
      summary.portfolioSummary.totalUnrealizedGL += unrealizedGL;

      // Asset breakdown
      if (!summary.assetBreakdown[record.asset]) {
        summary.assetBreakdown[record.asset] = {
          totalQty: 0,
          totalValue: 0,
          totalCostBasis: 0,
          inventoryCount: 0,
          percentOfPortfolio: 0
        };
      }
      const assetData = summary.assetBreakdown[record.asset];
      assetData.totalQty += record.qty;
      assetData.totalValue += record.carryingValue;
      assetData.totalCostBasis += record.costBasis;
      assetData.inventoryCount++;

      // Inventory breakdown
      if (!summary.inventoryBreakdown[record.inventory]) {
        summary.inventoryBreakdown[record.inventory] = {
          assetCount: 0,
          totalValue: 0,
          percentOfPortfolio: 0
        };
      }
      const invData = summary.inventoryBreakdown[record.inventory];
      invData.assetCount++;
      invData.totalValue += record.carryingValue;

      // Subsidiary breakdown (if applicable)
      if (record.subsidiaryId) {
        if (!summary.subsidiaryBreakdown[record.subsidiaryId]) {
          summary.subsidiaryBreakdown[record.subsidiaryId] = {
            assetCount: 0,
            totalValue: 0,
            percentOfPortfolio: 0
          };
        }
        const subData = summary.subsidiaryBreakdown[record.subsidiaryId];
        subData.assetCount++;
        subData.totalValue += record.carryingValue;
      }
    });

    // Calculate percentages
    Object.values(summary.assetBreakdown).forEach((assetData: any) => {
      assetData.percentOfPortfolio = summary.portfolioSummary.totalPortfolioValue > 0 ? 
        (assetData.totalValue / summary.portfolioSummary.totalPortfolioValue) * 100 : 0;
    });

    Object.values(summary.inventoryBreakdown).forEach((invData: any) => {
      invData.percentOfPortfolio = summary.portfolioSummary.totalPortfolioValue > 0 ? 
        (invData.totalValue / summary.portfolioSummary.totalPortfolioValue) * 100 : 0;
    });

    Object.values(summary.subsidiaryBreakdown).forEach((subData: any) => {
      subData.percentOfPortfolio = summary.portfolioSummary.totalPortfolioValue > 0 ? 
        (subData.totalValue / summary.portfolioSummary.totalPortfolioValue) * 100 : 0;
    });

    return summary;
  }

  /**
   * Format inventory balance data for presentation
   */
  formatForPresentation(records: InventoryBalanceRecord[]): string {
    let output = '💰 **Inventory Balance Report**\n\n';
    
    const summary = this.generateSummary(records);
    
    output += `**Portfolio Summary:**\n`;
    output += `• Total Portfolio Value: $${summary.portfolioSummary.totalPortfolioValue.toLocaleString(undefined, { minimumFractionDigits: 2 })}\n`;
    output += `• Total Cost Basis: $${summary.portfolioSummary.totalCostBasis.toLocaleString(undefined, { minimumFractionDigits: 2 })}\n`;
    
    if (summary.portfolioSummary.totalUnrealizedGL !== 0) {
      output += `• Total Unrealized G/L: $${summary.portfolioSummary.totalUnrealizedGL.toLocaleString(undefined, { minimumFractionDigits: 2 })}\n`;
    }
    
    if (summary.portfolioSummary.totalImpairments !== 0) {
      output += `• Total Impairments: $${summary.portfolioSummary.totalImpairments.toLocaleString(undefined, { minimumFractionDigits: 2 })}\n`;
    }
    
    output += `• Total Records: ${summary.totalRecords}\n\n`;

    // Asset breakdown
    output += `**Asset Breakdown:**\n`;
    Object.entries(summary.assetBreakdown)
      .sort(([,a], [,b]) => (b as any).totalValue - (a as any).totalValue)
      .forEach(([asset, data]: [string, any]) => {
        output += `• **${asset}**: $${data.totalValue.toLocaleString(undefined, { minimumFractionDigits: 2 })} `;
        output += `(${data.percentOfPortfolio.toFixed(1)}%) - `;
        output += `${data.totalQty.toLocaleString(undefined, { minimumFractionDigits: 8 })} units`;
        if (data.inventoryCount > 1) {
          output += ` across ${data.inventoryCount} inventories`;
        }
        output += '\n';
      });

    // Inventory breakdown (if multiple inventories)
    const inventoryCount = Object.keys(summary.inventoryBreakdown).length;
    if (inventoryCount > 1) {
      output += `\n**Inventory Breakdown:**\n`;
      Object.entries(summary.inventoryBreakdown)
        .sort(([,a], [,b]) => (b as any).totalValue - (a as any).totalValue)
        .forEach(([inventory, data]: [string, any]) => {
          output += `• **${inventory}**: $${data.totalValue.toLocaleString(undefined, { minimumFractionDigits: 2 })} `;
          output += `(${data.percentOfPortfolio.toFixed(1)}%) - ${data.assetCount} assets\n`;
        });
    }

    // Subsidiary breakdown (if applicable)
    if (summary.subsidiaryBreakdown) {
      const subsidiaryCount = Object.keys(summary.subsidiaryBreakdown).length;
      if (subsidiaryCount > 1) {
        output += `\n**Subsidiary Breakdown:**\n`;
        Object.entries(summary.subsidiaryBreakdown)
          .sort(([,a], [,b]) => (b as any).totalValue - (a as any).totalValue)
          .forEach(([subsidiary, data]: [string, any]) => {
            output += `• **${subsidiary}**: $${data.totalValue.toLocaleString(undefined, { minimumFractionDigits: 2 })} `;
            output += `(${data.percentOfPortfolio.toFixed(1)}%) - ${data.assetCount} assets\n`;
          });
      }
    }

    return output;
  }

  /**
   * Filter inventory records based on criteria
   */
  filterRecords(records: InventoryBalanceRecord[], criteria: {
    assets?: string[];
    inventories?: string[];
    subsidiaries?: string[];
    minValue?: number;
    maxValue?: number;
    hasImpairments?: boolean;
    positiveBalanceOnly?: boolean;
  }): InventoryBalanceRecord[] {
    return records.filter(record => {
      // Asset filter
      if (criteria.assets && criteria.assets.length > 0) {
        if (!criteria.assets.includes(record.asset)) {
          return false;
        }
      }

      // Inventory filter
      if (criteria.inventories && criteria.inventories.length > 0) {
        if (!criteria.inventories.includes(record.inventory)) {
          return false;
        }
      }

      // Subsidiary filter
      if (criteria.subsidiaries && criteria.subsidiaries.length > 0) {
        if (!record.subsidiaryId || !criteria.subsidiaries.includes(record.subsidiaryId)) {
          return false;
        }
      }

      // Value filters
      if (criteria.minValue && Math.abs(record.carryingValue) < criteria.minValue) {
        return false;
      }
      
      if (criteria.maxValue && Math.abs(record.carryingValue) > criteria.maxValue) {
        return false;
      }

      // Impairment filter
      if (criteria.hasImpairments !== undefined) {
        const hasImpairments = record.impairmentExpense > 0;
        if (criteria.hasImpairments !== hasImpairments) {
          return false;
        }
      }

      // Positive balance filter
      if (criteria.positiveBalanceOnly && record.carryingValue <= 0) {
        return false;
      }

      return true;
    });
  }

  /**
   * Sort inventory records by various criteria
   */
  sortRecords(records: InventoryBalanceRecord[], sortBy: 'asset' | 'value' | 'qty' | 'costBasis' | 'inventory', ascending: boolean = false): InventoryBalanceRecord[] {
    return [...records].sort((a, b) => {
      let comparison = 0;

      switch (sortBy) {
        case 'asset':
          comparison = a.asset.localeCompare(b.asset);
          break;
        case 'value':
          comparison = a.carryingValue - b.carryingValue;
          break;
        case 'qty':
          comparison = a.qty - b.qty;
          break;
        case 'costBasis':
          comparison = a.costBasis - b.costBasis;
          break;
        case 'inventory':
          comparison = a.inventory.localeCompare(b.inventory);
          break;
      }

      return ascending ? comparison : -comparison;
    });
  }

  // ========================================================================
  // FIELD METADATA ACCESS
  // ========================================================================

  static getFieldMetadata(): FieldMetadata[] {
    return InventoryBalanceGenerator.FIELD_METADATA;
  }

  static getColumnsByCategory(category: string): string[] {
    return InventoryBalanceGenerator.FIELD_METADATA
      .filter(field => field.category === category)
      .map(field => field.column);
  }

  static getColumnAliases(column: string): string[] {
    const field = InventoryBalanceGenerator.FIELD_METADATA.find(f => f.column === column);
    return field ? field.aliases : [];
  }

  // ========================================================================
  // CSV EXPORT SUPPORT
  // ========================================================================

  exportToCSV(records: InventoryBalanceRecord[]): string {
    const headers = [
      'asset', 'assetId', 'inventory', 'subsidiaryId',
      'qty', 'costBasisAcquired', 'costBasisRelieved', 'costBasis',
      'impairmentExpense', 'impairmentReversal',
      'fairValueAdjUpward', 'fairValueAdjDownward',
      'revaluationAdjUpward', 'revaluationAdjDownward',
      'impairmentDisposed', 'carryingValue'
    ];

    const csvRows = [headers.join(',')];

    records.forEach(record => {
      const row = [
        record.asset,
        record.assetId,
        record.inventory,
        record.subsidiaryId || '',
        record.qty.toFixed(8),
        record.costBasisAcquired.toFixed(2),
        record.costBasisRelieved.toFixed(2),
        record.costBasis.toFixed(2),
        record.impairmentExpense.toFixed(2),
        record.impairmentExpenseReversal.toFixed(2),
        record.fairValueAdjustmentUpward.toFixed(2),
        record.fairValueAdjustmentDownward.toFixed(2),
        record.revaluationAdjustmentUpward.toFixed(2),
        record.revaluationAdjustmentDownward.toFixed(2),
        record.impairmentExpenseDisposed.toFixed(2),
        record.carryingValue.toFixed(2)
      ];

      csvRows.push(row.join(','));
    });

    return csvRows.join('\n');
  }

  // ========================================================================
  // RECONCILIATION UTILITIES
  // ========================================================================

  /**
   * Reconcile inventory balance totals with other reports
   */
  /**
   * Reconcile inventory balance totals with other reports
   */
  reconcileWithLotsReport(inventoryRecords: InventoryBalanceRecord[], lotsData: any[]): {
    matches: boolean;
    discrepancies: Array<{
      asset: string;
      inventory: string;
      inventoryValue: number;
      lotsValue: number;
      difference: number;
    }>;
    summary: {
      totalInventoryValue: number;
      totalLotsValue: number;
      totalDifference: number;
    };
  } {
    const tolerance = 0.01; // $0.01 tolerance
    const discrepancies: any[] = [];
    
    // Group lots data by asset and inventory for comparison
    const lotsGrouped = new Map<string, number>();
    lotsData.forEach((lot: any) => {
      const key = `${lot.asset}|${lot.inventory || 'DEFAULT'}`;
      const currentValue = lotsGrouped.get(key) || 0;
      lotsGrouped.set(key, currentValue + (lot.carryingValue || 0));
    });

    // Calculate totals for summary
    const totalInventoryValue = inventoryRecords.reduce((sum, record) => sum + record.carryingValue, 0);
    const totalLotsValue = Array.from(lotsGrouped.values()).reduce((sum, value) => sum + value, 0);

    // Compare each inventory record with lots data
    inventoryRecords.forEach(record => {
      const key = `${record.asset}|${record.inventory}`;
      const lotsValue = lotsGrouped.get(key) || 0;
      const difference = record.carryingValue - lotsValue;

      if (Math.abs(difference) > tolerance) {
        discrepancies.push({
          asset: record.asset,
          inventory: record.inventory,
          inventoryValue: record.carryingValue,
          lotsValue: lotsValue,
          difference: difference
        });
      }
    });

    return {
      matches: discrepancies.length === 0,
      discrepancies,
      summary: {
        totalInventoryValue,
        totalLotsValue,
        totalDifference: totalInventoryValue - totalLotsValue
      }
    };
  }

  /**
   * Generate variance analysis between periods
   */
  generateVarianceAnalysis(currentRecords: InventoryBalanceRecord[], priorRecords: InventoryBalanceRecord[]): {
    newPositions: InventoryBalanceRecord[];
    closedPositions: InventoryBalanceRecord[];
    changedPositions: Array<{
      current: InventoryBalanceRecord;
      prior: InventoryBalanceRecord;
      valueChange: number;
      qtyChange: number;
      percentChange: number;
    }>;
    summary: {
      totalNewValue: number;
      totalClosedValue: number;
      totalNetChange: number;
    };
  } {
    // Create maps for easy lookup
    const currentMap = new Map<string, InventoryBalanceRecord>();
    const priorMap = new Map<string, InventoryBalanceRecord>();

    currentRecords.forEach(record => {
      const key = `${record.asset}|${record.inventory}|${record.subsidiaryId || 'DEFAULT'}`;
      currentMap.set(key, record);
    });

    priorRecords.forEach(record => {
      const key = `${record.asset}|${record.inventory}|${record.subsidiaryId || 'DEFAULT'}`;
      priorMap.set(key, record);
    });

    const newPositions: InventoryBalanceRecord[] = [];
    const closedPositions: InventoryBalanceRecord[] = [];
    const changedPositions: any[] = [];

    // Find new positions (in current but not in prior)
    currentMap.forEach((current, key) => {
      if (!priorMap.has(key)) {
        newPositions.push(current);
      }
    });

    // Find closed positions (in prior but not in current)
    priorMap.forEach((prior, key) => {
      if (!currentMap.has(key)) {
        closedPositions.push(prior);
      }
    });

    // Find changed positions
    currentMap.forEach((current, key) => {
      const prior = priorMap.get(key);
      if (prior) {
        const valueChange = current.carryingValue - prior.carryingValue;
        const qtyChange = current.qty - prior.qty;
        const percentChange = prior.carryingValue !== 0 ? (valueChange / Math.abs(prior.carryingValue)) * 100 : 0;

        if (Math.abs(valueChange) > 0.01 || Math.abs(qtyChange) > 0.00000001) {
          changedPositions.push({
            current,
            prior,
            valueChange,
            qtyChange,
            percentChange
          });
        }
      }
    });

    const summary = {
      totalNewValue: newPositions.reduce((sum, pos) => sum + pos.carryingValue, 0),
      totalClosedValue: closedPositions.reduce((sum, pos) => sum + pos.carryingValue, 0),
      totalNetChange: currentRecords.reduce((sum, pos) => sum + pos.carryingValue, 0) - 
                     priorRecords.reduce((sum, pos) => sum + pos.carryingValue, 0)
    };

    return {
      newPositions,
      closedPositions,
      changedPositions,
      summary
    };
  }

  /**
   * Calculate concentration risk metrics
   */
  calculateConcentrationRisk(records: InventoryBalanceRecord[]): {
    assetConcentration: {
      top5Assets: Array<{ asset: string; value: number; percentage: number }>;
      herfindahlIndex: number;
      concentrationRisk: 'LOW' | 'MEDIUM' | 'HIGH';
    };
    inventoryConcentration: {
      top5Inventories: Array<{ inventory: string; value: number; percentage: number }>;
      inventoryDiversification: number;
    };
  } {

    const totalValue = records.reduce((sum, record) => sum + Math.abs(record.carryingValue), 0);

    // Asset concentration analysis
    const assetTotals = new Map<string, number>();
    records.forEach(record => {
      const current = assetTotals.get(record.asset) || 0;
      assetTotals.set(record.asset, current + Math.abs(record.carryingValue));
    });

    const assetEntries = Array.from(assetTotals.entries())
      .map(([asset, value]) => ({ 
        asset, 
        value, 
        percentage: totalValue > 0 ? (value / totalValue) * 100 : 0 
      }))
      .sort((a, b) => b.value - a.value);

    // Herfindahl-Hirschman Index for asset concentration
    const herfindahlIndex = assetEntries.reduce((sum, entry) => {
      return sum + Math.pow(entry.percentage, 2);
    }, 0);

    let concentrationRisk: 'LOW' | 'MEDIUM' | 'HIGH' = 'LOW';
    if (herfindahlIndex > 2500) concentrationRisk = 'HIGH';
    else if (herfindahlIndex > 1500) concentrationRisk = 'MEDIUM';

    // Inventory concentration analysis
    const inventoryTotals = new Map<string, number>();
    records.forEach(record => {
      const current = inventoryTotals.get(record.inventory) || 0;
      inventoryTotals.set(record.inventory, current + Math.abs(record.carryingValue));
    });

    const inventoryEntries = Array.from(inventoryTotals.entries())
      .map(([inventory, value]) => ({ 
        inventory, 
        value, 
        percentage: totalValue > 0 ? (value / totalValue) * 100 : 0 
      }))
      .sort((a, b) => b.value - a.value);

    return {
      assetConcentration: {
        top5Assets: assetEntries.slice(0, 5),
        herfindahlIndex,
        concentrationRisk
      },
      inventoryConcentration: {
        top5Inventories: inventoryEntries.slice(0, 5),
        inventoryDiversification: inventoryEntries.length
      }
    };
  }

  /**
   * Generate the Inventory Balance Report based on provided parameters
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
        asOfDate: parameters.asOfDate || 'CURRENT_DATE()',
        limit: parameters.limit || 5000 // Default to 5000 rows if not specified
      };
      
      // PARAMETER_REVIEW 2: Log parameters in generateReport
      console.log('PARAMETER_REVIEW 2 - InventoryBalanceGenerator.generateReport:', {
        originalParameters: parameters,
        reportParams,
        hasAsOfDate: parameters.asOfDate ? 'YES' : 'NO',
        asOfDateValue: parameters.asOfDate,
        hasLimit: parameters.limit ? 'YES' : 'NO',
        limitValue: parameters.limit || 5000
      });
      
      // Extract filters
      const filters: any = {};
      if (parameters.assets) {
        filters.assets = Array.isArray(parameters.assets) 
          ? parameters.assets 
          : parameters.assets.split(',').map((a: string) => a.trim());
      }
      
      if (parameters.inventory) {
        filters.inventories = Array.isArray(parameters.inventory) 
          ? parameters.inventory 
          : parameters.inventory.split(',').map((i: string) => i.trim());
      }
      
      // Default grouping
      const groupBy = ['asset', 'inventory'];
      if (parameters.includeSubsidiary) {
        groupBy.push('subsidiary');
      }
      
      // Generate SQL
      const sql = this.buildInventoryBalanceSQL(reportParams, groupBy as any, filters);
      
      // Execute query
      if (!this.queryExecutor) {
        throw new Error('QueryExecutor not initialized');
      }
      
      // PARAMETER_REVIEW 2.1: Log parameters before query execution
      console.log('PARAMETER_REVIEW 2.1 - InventoryBalanceGenerator before executeQuery:', {
        hasParameters: true,
        parameters: reportParams,
        hasAsOfDate: reportParams.asOfDate ? 'YES' : 'NO',
        asOfDateValue: reportParams.asOfDate
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
      
      // Define columns based on the results
      const columns = [
        'asset',
        'inventory',
        'qty',
        'costBasis',
        'carryingValue',
        'impairmentExpense'
      ];
      
      if (groupBy.includes('subsidiary')) {
        columns.splice(2, 0, 'subsidiaryId');
      }
      
      const executionTime = Date.now() - startTime;
      
      return {
        data: results,
        columns,
        executionTime,
        bytesProcessed: executionResult.metadata.bytesProcessed || 0,
        sql,
        metadata: {
          summary,
          totalRecords: results.length
        }
      };
    } catch (error) {
      console.error('Error generating Inventory Balance Report:', error);
      throw error;
    }
  }
}