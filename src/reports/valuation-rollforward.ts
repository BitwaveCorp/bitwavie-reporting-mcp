/**
 * Valuation Rollforward Report Generator - Derivative Report #2
 * 
 * Generates period-based rollforward movements with EXACT ORDER:
 * - Starting Cost Basis
 * + Cost Basis Acquired
 * – Cost Basis Disposed  
 * = Ending Cost Basis
 * - Starting Impairment/Adjustments
 * + Impairment Expense
 * – Impairment Disposed/Reversal
 * = Ending Impairment
 * = Ending Carrying Value
 * - Unrealized Adjustments (Fair Value, Revaluation)
 * = Ending Market Value
 * - Period Realized Gains/Losses (Short-term, Long-term, Undated)
 */

import { BigQueryClient } from '../services/bigquery-client.js';
import { QueryExecutor } from '../services/query-executor.js';
import { ConnectionManager } from '../services/connection-manager.js';
import { 
  ValuationRollforwardRecord, 
  ReportParameters, 
  FieldMetadata 
} from '../types/actions-report.js';

export class ValuationRollforwardGenerator {
  private bigQueryClient: BigQueryClient;
  private queryExecutor: QueryExecutor;
  private connectionManager: ConnectionManager;

  // Field metadata for natural language query mapping
  private static readonly FIELD_METADATA: FieldMetadata[] = [
    {
      column: 'asset',
      description: 'Asset symbol/ticker being tracked',
      type: 'string',
      category: 'asset',
      aliases: ['coin', 'token', 'cryptocurrency', 'symbol'],
      common_queries: ['BTC rollforward', 'ETH rollforward', 'asset breakdown'],
      aggregatable: false,
      filterable: true
    },
    {
      column: 'starting_cost_basis',
      description: 'USD cost basis at beginning of period',
      type: 'number',
      category: 'financial',
      aliases: ['starting cost', 'initial cost basis', 'beginning cost'],
      common_queries: ['cost basis movement', 'starting position'],
      aggregatable: true,
      filterable: true
    },
    {
      column: 'cost_basis_acquired',
      description: 'USD cost basis of new acquisitions during period',
      type: 'number',
      category: 'financial',
      aliases: ['acquisitions', 'purchases', 'new cost basis', 'bought'],
      common_queries: ['cost basis movement', 'period acquisitions', 'new purchases'],
      aggregatable: true,
      filterable: true
    },
    {
      column: 'cost_basis_disposed',
      description: 'USD cost basis of disposals during period',
      type: 'number',
      category: 'financial',
      aliases: ['disposals', 'sales', 'disposed cost', 'sold'],
      common_queries: ['cost basis movement', 'period disposals', 'sales activity'],
      aggregatable: true,
      filterable: true
    },
    {
      column: 'ending_cost_basis',
      description: 'USD cost basis at end of period (starting + acquired - disposed)',
      type: 'number',
      category: 'financial',
      aliases: ['ending cost', 'final cost basis', 'period end cost'],
      common_queries: ['cost basis movement', 'ending position'],
      aggregatable: true,
      filterable: true
    },
    {
      column: 'period_shortterm_gainloss',
      description: 'Short-term realized gains/losses during period',
      type: 'number',
      category: 'financial',
      aliases: ['short term gains', 'stcg', 'short term performance'],
      common_queries: ['period performance', 'short term gains', 'tax analysis'],
      aggregatable: true,
      filterable: true
    },
    {
      column: 'period_longterm_gainloss',
      description: 'Long-term realized gains/losses during period',
      type: 'number',
      category: 'financial',
      aliases: ['long term gains', 'ltcg', 'long term performance'],
      common_queries: ['period performance', 'long term gains', 'tax analysis'],
      aggregatable: true,
      filterable: true
    },
    {
      column: 'ending_unrealized',
      description: 'Unrealized gain/loss at end of period',
      type: 'number',
      category: 'financial',
      aliases: ['unrealized gains', 'paper gains', 'mark to market'],
      common_queries: ['unrealized gains', 'paper gains', 'market performance'],
      aggregatable: true,
      filterable: true
    },
    {
      column: 'ending_carrying_value',
      description: 'Book value at period end (cost basis - impairments)',
      type: 'number',
      category: 'financial',
      aliases: ['book value', 'carrying amount', 'net book value'],
      common_queries: ['book value', 'carrying value', 'accounting value'],
      aggregatable: true,
      filterable: true
    },
    {
      column: 'ending_market_value',
      description: 'Fair value at end of period',
      type: 'number',
      category: 'financial',
      aliases: ['market value', 'fair value', 'current value'],
      common_queries: ['market value', 'fair value', 'current valuation'],
      aggregatable: true,
      filterable: true
    },
    {
      column: 'impairment_expense',
      description: 'New impairment losses during period',
      type: 'number',
      category: 'financial',
      aliases: ['impairment', 'writedown', 'impairment losses'],
      common_queries: ['impairment activity', 'writedowns', 'impairment expense'],
      aggregatable: true,
      filterable: true
    },
    {
      column: 'original_subsidiary',
      description: 'Subsidiary where lot was originally acquired',
      type: 'string',
      category: 'classification',
      aliases: ['subsidiary', 'entity', 'legal entity'],
      common_queries: ['subsidiary breakdown', 'entity analysis'],
      aggregatable: false,
      filterable: true
    }
  ];

  constructor(bigQueryClient: BigQueryClient) {
    this.bigQueryClient = bigQueryClient;
    
    // Initialize ConnectionManager to get connection details from session
    this.connectionManager = ConnectionManager.getInstance();
    
    // Get project ID from ConnectionManager with fallbacks
    const projectId = this.connectionManager.getProjectId() || process.env.GOOGLE_CLOUD_PROJECT_ID || 'bitwave-solutions';
    console.log(`ValuationRollforwardGenerator: Initializing QueryExecutor with project ID: ${projectId}`);
    this.queryExecutor = new QueryExecutor(projectId);
  }

  // ========================================================================
  // MAIN REPORT GENERATION
  // ========================================================================

  async generate(
    parameters: ReportParameters, 
    groupBy?: ('asset' | 'subsidiary' | 'inventory' | 'wallet')[],
    filters?: {
      assets?: string[];
      subsidiaries?: string[];
      minValue?: number;
    }
  ): Promise<ValuationRollforwardRecord[]> {
    
    console.log('🔄 Generating Valuation Rollforward Report...', { parameters, groupBy, filters });
    
    // Apply default values for missing parameters
    if (!parameters.startDate) {
      parameters.startDate = '1970-01-01';
      console.log('ValuationRollforwardGenerator: Using default startDate: 1970-01-01');
    }
    
    if (!parameters.endDate) {
      parameters.endDate = '2050-12-31';
      console.log('ValuationRollforwardGenerator: Using default endDate: 2050-12-31');
    }
    
    try {
      // Build the SQL query
      const sql = this.buildRollforwardSQL(parameters, groupBy, filters);
      
      // Execute the query
      const queryResult = await this.bigQueryClient.executeReport(sql, parameters);
      
      // Extract data from QueryResult
      const rawResults = queryResult.data || [];
      
      // Transform and validate results
      const rollforwardRecords = this.transformResults(rawResults);
      
      console.log(`✅ Valuation Rollforward generated: ${rollforwardRecords.length} records`);
      
      return rollforwardRecords;
      
    } catch (error) {
      console.error('❌ Valuation Rollforward generation failed:', error);
      throw new Error(`Valuation Rollforward generation failed: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  // ========================================================================
  // SQL QUERY BUILDING - Complex Multi-CTE Structure
  // ========================================================================

  private buildRollforwardSQL(
    parameters: ReportParameters, 
    groupBy?: ('asset' | 'subsidiary' | 'inventory' | 'wallet')[],
    filters?: any
  ): string {
    
    const groupByColumns = this.buildGroupByColumns(groupBy);
    const whereConditions = this.buildWhereConditions(parameters, filters);
    
    // Get table reference from ConnectionManager with fallbacks
    const projectId = this.connectionManager.getProjectId() || process.env.GOOGLE_CLOUD_PROJECT_ID || 'bitwave-solutions';
    const datasetId = this.connectionManager.getDatasetId() || process.env.BIGQUERY_DATASET_ID || '0_Bitwavie_MCP';
    const tableId = this.connectionManager.getTableId() || process.env.BIGQUERY_TABLE_ID || '2622d4df5b2a15ec811e_gl_actions';
    const fullTablePath = `${projectId}.${datasetId}.${tableId}`;
    
    console.log(`ValuationRollforwardGenerator: Using table: ${fullTablePath} (Project: ${projectId}, Dataset: ${datasetId}, Table: ${tableId})`);
    console.log(`ValuationRollforwardGenerator: Connection details source: ${this.connectionManager.logConnectionDetails()}`);

    // Check if runId is provided in parameters
    const runIdFilter = parameters.runId ? 'runId = @runId' : '1=1'; // Use 1=1 as a no-op filter if runId not provided
    
    console.log(`ValuationRollforwardGenerator: Using runId filter: ${runIdFilter}`);
    
    return `
      WITH isAvgCost AS (
        SELECT (COUNTIF(undatedGainLoss IS NOT NULL) > 0 OR COUNTIF(lotId IS NULL) > 0) AS isAvgCost
        FROM \`${fullTablePath}\`
        WHERE ${runIdFilter} 
          AND timestampSEC <= UNIX_SECONDS(TIMESTAMP(DATE(@endDate))) 
          AND action = 'sell' 
          AND status = 'complete'
      ),
      prepared_gainloss_table AS (
        SELECT gla.*, 
          CASE WHEN isc.isAvgCost THEN inventory ELSE gla.lotID END AS definedkey,
          COALESCE(subsidiaryId, 'DEFAULT') as original_subsidiary,
          COALESCE(inventory, 'DEFAULT') as original_inventory,
          COALESCE(wallet, 'DEFAULT') as original_wallet
        FROM \`${fullTablePath}\` gla
        CROSS JOIN isAvgCost isc
        WHERE ${runIdFilter} ${filters ? this.buildAssetFilter(filters) : ''}
      ),
      startingbalance AS (
        -- Calculate starting balances before period
        SELECT 
          asset,
          ${groupByColumns.select}
          SUM(COALESCE(CAST(costBasisAcquired AS bignumeric), 0)) - SUM(COALESCE(CAST(originalCostBasisDisposed AS bignumeric), 0)) AS starting_cost_basis,
          SUM(COALESCE(CAST(impairmentExpense AS bignumeric), 0)) - SUM(COALESCE(CAST(impairmentExpenseDisposed AS bignumeric), 0)) AS starting_impairment_in_inventory,
          SUM(COALESCE(CAST(fairValueAdjustmentUpward AS bignumeric), 0)) - SUM(COALESCE(CAST(fairValueAdjustmentDownward AS bignumeric), 0)) AS starting_unrealized
        FROM prepared_gainloss_table
        WHERE timestampSEC < UNIX_SECONDS(TIMESTAMP(DATE(@startDate)))
        GROUP BY asset${groupByColumns.groupBy}
      ),
      increases AS (
        -- Calculate period acquisitions and adjustments
        SELECT 
          asset,
          ${groupByColumns.select}
          SUM(COALESCE(CAST(costBasisAcquired AS bignumeric), 0)) AS cost_basis_acquired,
          SUM(COALESCE(CAST(impairmentExpense AS bignumeric), 0)) AS impairment_expense,
          SUM(COALESCE(CAST(fairValueAdjustmentUpward AS bignumeric), 0)) AS gaap_fair_value_adjust_up,
          SUM(COALESCE(CAST(revaluationAdjustmentUpward AS bignumeric), 0)) AS IFRS_revaluation_adjust_up
        FROM prepared_gainloss_table  
        WHERE timestampSEC >= UNIX_SECONDS(TIMESTAMP(DATE(@startDate))) 
          AND timestampSEC <= UNIX_SECONDS(TIMESTAMP(DATE(@endDate)))
        GROUP BY asset${groupByColumns.groupBy}
      ),
      decreases AS (
        -- Calculate period dispositions and realized gains
        SELECT 
          asset,
          ${groupByColumns.select}
          SUM(COALESCE(CAST(originalCostBasisDisposed AS bignumeric), 0)) AS cost_basis_disposed,
          SUM(COALESCE(CAST(impairmentExpenseDisposed AS bignumeric), 0)) AS impairment_disposed,
          SUM(COALESCE(CAST(impairmentReversal AS bignumeric), 0)) AS impairment_reversal,
          SUM(COALESCE(CAST(fairValueAdjustmentDownward AS bignumeric), 0)) AS gaap_fair_value_adjust_down,
          SUM(COALESCE(CAST(revaluationAdjustmentDownward AS bignumeric), 0)) AS IFRS_revaluation_adjust_down,
          SUM(COALESCE(CAST(shortTermGainLoss AS bignumeric), 0)) AS period_shortterm_gainloss,
          SUM(COALESCE(CAST(LongTermGainLoss AS bignumeric), 0)) AS period_longterm_gainloss,
          SUM(COALESCE(CAST(undatedGainLoss AS bignumeric), 0)) AS period_undated_gainloss
        FROM prepared_gainloss_table
        WHERE timestampSEC >= UNIX_SECONDS(TIMESTAMP(DATE(@startDate))) 
          AND timestampSEC <= UNIX_SECONDS(TIMESTAMP(DATE(@endDate)))
        GROUP BY asset${groupByColumns.groupBy}
      ),
      endingbalance AS (
        -- Calculate ending balances after period
        SELECT 
          asset,
          ${groupByColumns.select}
          SUM(COALESCE(CAST(fairValueAdjustmentUpward AS bignumeric), 0)) - SUM(COALESCE(CAST(fairValueAdjustmentDownward AS bignumeric), 0)) AS ending_unrealized
        FROM prepared_gainloss_table
        WHERE timestampSEC <= UNIX_SECONDS(TIMESTAMP(DATE(@endDate)))
        GROUP BY asset${groupByColumns.groupBy}
      )
      SELECT 
        COALESCE(sb.asset, inc.asset, dec.asset, eb.asset) as asset,
        ${this.buildCoalesceGroupBy(groupByColumns)}
        
        -- Cost Basis Movement (EXACT ORDER)
        COALESCE(sb.starting_cost_basis, 0) AS starting_cost_basis,
        COALESCE(inc.cost_basis_acquired, 0) AS cost_basis_acquired,
        COALESCE(dec.cost_basis_disposed, 0) AS cost_basis_disposed,
        (COALESCE(sb.starting_cost_basis, 0) + COALESCE(inc.cost_basis_acquired, 0) - COALESCE(dec.cost_basis_disposed, 0)) AS ending_cost_basis,
        
        -- Impairment Movement
        COALESCE(sb.starting_impairment_in_inventory, 0) AS starting_impairment_in_inventory,
        COALESCE(inc.impairment_expense, 0) AS impairment_expense,
        COALESCE(dec.impairment_disposed, 0) AS impairment_disposed,
        COALESCE(dec.impairment_reversal, 0) AS impairment_reversal,
        (COALESCE(sb.starting_impairment_in_inventory, 0) + COALESCE(inc.impairment_expense, 0) - COALESCE(dec.impairment_disposed, 0) - COALESCE(dec.impairment_reversal, 0)) AS ending_impairment_in_inventory,
        
        -- Carrying Value (Cost Basis - Impairments)
        ((COALESCE(sb.starting_cost_basis, 0) + COALESCE(inc.cost_basis_acquired, 0) - COALESCE(dec.cost_basis_disposed, 0)) - 
         (COALESCE(sb.starting_impairment_in_inventory, 0) + COALESCE(inc.impairment_expense, 0) - COALESCE(dec.impairment_disposed, 0) - COALESCE(dec.impairment_reversal, 0))) AS ending_carrying_value,
        
        -- Unrealized Adjustments
        COALESCE(sb.starting_unrealized, 0) AS starting_unrealized,
        COALESCE(inc.gaap_fair_value_adjust_up, 0) AS gaap_fair_value_adjust_up,
        COALESCE(dec.gaap_fair_value_adjust_down, 0) AS gaap_fair_value_adjust_down,
        COALESCE(inc.IFRS_revaluation_adjust_up, 0) AS IFRS_revaluation_adjust_up,
        COALESCE(dec.IFRS_revaluation_adjust_down, 0) AS IFRS_revaluation_adjust_down,
        COALESCE(eb.ending_unrealized, 0) AS ending_unrealized,
        
        -- Market Value (Carrying Value + Unrealized Adjustments)
        (((COALESCE(sb.starting_cost_basis, 0) + COALESCE(inc.cost_basis_acquired, 0) - COALESCE(dec.cost_basis_disposed, 0)) - 
          (COALESCE(sb.starting_impairment_in_inventory, 0) + COALESCE(inc.impairment_expense, 0) - COALESCE(dec.impairment_disposed, 0) - COALESCE(dec.impairment_reversal, 0))) + 
         COALESCE(eb.ending_unrealized, 0)) AS ending_market_value,
        
        -- Period Realized Gains/Losses
        COALESCE(dec.period_shortterm_gainloss, 0) AS period_shortterm_gainloss,
        COALESCE(dec.period_longterm_gainloss, 0) AS period_longterm_gainloss,
        COALESCE(dec.period_undated_gainloss, 0) AS period_undated_gainloss
        
      FROM startingbalance sb
      FULL OUTER JOIN increases inc ON sb.asset = inc.asset ${groupByColumns.joins}
      FULL OUTER JOIN decreases dec ON COALESCE(sb.asset, inc.asset) = dec.asset ${groupByColumns.joins}
      FULL OUTER JOIN endingbalance eb ON COALESCE(sb.asset, inc.asset, dec.asset) = eb.asset ${groupByColumns.joins}
      WHERE COALESCE(sb.asset, inc.asset, dec.asset, eb.asset) IS NOT NULL
      ORDER BY asset ASC${groupByColumns.orderBy}
      ${parameters.limit ? `LIMIT ${parameters.limit}` : ''}
    `.trim();
  }

  private buildGroupByColumns(groupBy?: ('asset' | 'subsidiary' | 'inventory' | 'wallet')[]): {
    select: string;
    groupBy: string;
    joins: string;
    orderBy: string;
  } {
    if (!groupBy || groupBy.length === 0) {
      return {
        select: '',
        groupBy: '',
        joins: '',
        orderBy: ''
      };
    }

    const columns: string[] = [];
    const joins: string[] = [];
    const orderBy: string[] = [];

    groupBy.forEach(dimension => {
      switch (dimension) {
        case 'subsidiary':
          columns.push('original_subsidiary');
          joins.push('AND sb.original_subsidiary = inc.original_subsidiary');
          orderBy.push('original_subsidiary ASC');
          break;
        case 'inventory':
          columns.push('original_inventory');
          joins.push('AND sb.original_inventory = inc.original_inventory');
          orderBy.push('original_inventory ASC');
          break;
        case 'wallet':
          columns.push('original_wallet');
          joins.push('AND sb.original_wallet = inc.original_wallet');
          orderBy.push('original_wallet ASC');
          break;
      }
    });

    return {
      select: columns.length > 0 ? columns.join(', ') + ',' : '',
      groupBy: columns.length > 0 ? ', ' + columns.join(', ') : '',
      joins: joins.length > 0 ? ' ' + joins.join(' ') : '',
      orderBy: orderBy.length > 0 ? ', ' + orderBy.join(', ') : ''
    };
  }

  private buildCoalesceGroupBy(groupByColumns: any): string {
    if (!groupByColumns.select) return '';

    const columns = groupByColumns.select.replace(/,$/, '').split(', ');
    return columns.map((col: string) => 
      `COALESCE(sb.${col}, inc.${col}, dec.${col}, eb.${col}) as ${col},`
    ).join('\n        ');
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

    return conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';
  }

  private buildAssetFilter(filters: any): string {
    if (filters.assets && filters.assets.length > 0) {
      const assetList = filters.assets.map((asset: string) => `'${asset}'`).join(', ');
      return ` AND asset IN (${assetList})`;
    }
    return '';
  }

  // ========================================================================
  // RESULT TRANSFORMATION AND VALIDATION
  // ========================================================================

  private transformResults(rawResults: any[]): ValuationRollforwardRecord[] {
    return rawResults.map(row => {
      const record: ValuationRollforwardRecord = {
        asset: row.asset || '',
        original_subsidiary: row.original_subsidiary,
        original_inventory: row.original_inventory,
        original_wallet: row.original_wallet,
        
        // Cost Basis Movement
        starting_cost_basis: this.parseNumeric(row.starting_cost_basis),
        cost_basis_acquired: this.parseNumeric(row.cost_basis_acquired),
        cost_basis_disposed: this.parseNumeric(row.cost_basis_disposed),
        ending_cost_basis: this.parseNumeric(row.ending_cost_basis),
        
        // Impairment Movement
        starting_impairment_in_inventory: this.parseNumeric(row.starting_impairment_in_inventory),
        impairment_expense: this.parseNumeric(row.impairment_expense),
        impairment_disposed: this.parseNumeric(row.impairment_disposed),
        impairment_reversal: this.parseNumeric(row.impairment_reversal),
        ending_impairment_in_inventory: this.parseNumeric(row.ending_impairment_in_inventory),
        
        // Carrying Value
        ending_carrying_value: this.parseNumeric(row.ending_carrying_value),
        
        // Unrealized Adjustments
        starting_unrealized: this.parseNumeric(row.starting_unrealized),
        gaap_fair_value_adjust_up: this.parseNumeric(row.gaap_fair_value_adjust_up),
        gaap_fair_value_adjust_down: this.parseNumeric(row.gaap_fair_value_adjust_down),
        IFRS_revaluation_adjust_up: this.parseNumeric(row.IFRS_revaluation_adjust_up),
        IFRS_revaluation_adjust_down: this.parseNumeric(row.IFRS_revaluation_adjust_down),
        ending_unrealized: this.parseNumeric(row.ending_unrealized),
        
        // Market Value
        ending_market_value: this.parseNumeric(row.ending_market_value),
        
        // Period Realized Gains/Losses
        period_shortterm_gainloss: this.parseNumeric(row.period_shortterm_gainloss),
        period_longterm_gainloss: this.parseNumeric(row.period_longterm_gainloss),
        period_undated_gainloss: this.parseNumeric(row.period_undated_gainloss)
      };

      // Validate the rollforward math
      this.validateRollforwardRecord(record);

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

  private validateRollforwardRecord(record: ValuationRollforwardRecord): void {
    // Validate cost basis rollforward math
    const calculatedEndingCostBasis = record.starting_cost_basis + record.cost_basis_acquired - record.cost_basis_disposed;
    const tolerance = 0.01; // $0.01 tolerance for floating point precision
    
    if (Math.abs(record.ending_cost_basis - calculatedEndingCostBasis) > tolerance) {
      console.warn(`Warning: Cost basis rollforward math mismatch for ${record.asset}: ` +
        `calculated ${calculatedEndingCostBasis.toFixed(2)} vs recorded ${record.ending_cost_basis.toFixed(2)}`);
    }

    // Validate impairment rollforward math
    const calculatedEndingImpairment = record.starting_impairment_in_inventory + record.impairment_expense - record.impairment_disposed - record.impairment_reversal;
    
    if (Math.abs(record.ending_impairment_in_inventory - calculatedEndingImpairment) > tolerance) {
      console.warn(`Warning: Impairment rollforward math mismatch for ${record.asset}: ` +
        `calculated ${calculatedEndingImpairment.toFixed(2)} vs recorded ${record.ending_impairment_in_inventory.toFixed(2)}`);
    }

    // Basic reasonableness checks
    if (record.cost_basis_acquired < 0) {
      console.warn(`Warning: Negative cost basis acquired for ${record.asset}: ${record.cost_basis_acquired}`);
    }
    
    if (record.cost_basis_disposed < 0) {
      console.warn(`Warning: Negative cost basis disposed for ${record.asset}: ${record.cost_basis_disposed}`);
    }
  }

  // ========================================================================
  // ANALYSIS AND REPORTING UTILITIES
  // ========================================================================

  /**
   * Generate summary statistics for the rollforward report
   */
  generateSummary(rollforwards: ValuationRollforwardRecord[]): {
    totalAssets: number;
    periodActivity: {
      totalAcquisitions: number;
      totalDisposals: number;
      netCostBasisChange: number;
      totalRealizedGainLoss: number;
      shortTermGainLoss: number;
      longTermGainLoss: number;
    };
    portfolioMovement: {
      startingPortfolioValue: number;
      endingPortfolioValue: number;
      portfolioChange: number;
      percentageChange: number;
    };
    impairmentActivity: {
      totalImpairmentExpense: number;
      totalImpairmentReversal: number;
      netImpairmentChange: number;
    };
  } {
    const summary = {
      totalAssets: rollforwards.length,
      periodActivity: {
        totalAcquisitions: 0,
        totalDisposals: 0,
        netCostBasisChange: 0,
        totalRealizedGainLoss: 0,
        shortTermGainLoss: 0,
        longTermGainLoss: 0
      },
      portfolioMovement: {
        startingPortfolioValue: 0,
        endingPortfolioValue: 0,
        portfolioChange: 0,
        percentageChange: 0
      },
      impairmentActivity: {
        totalImpairmentExpense: 0,
        totalImpairmentReversal: 0,
        netImpairmentChange: 0
      }
    };

    rollforwards.forEach(record => {
      // Period activity
      summary.periodActivity.totalAcquisitions += record.cost_basis_acquired;
      summary.periodActivity.totalDisposals += record.cost_basis_disposed;
      summary.periodActivity.shortTermGainLoss += record.period_shortterm_gainloss;
      summary.periodActivity.longTermGainLoss += record.period_longterm_gainloss;
      summary.periodActivity.totalRealizedGainLoss += (record.period_shortterm_gainloss + record.period_longterm_gainloss + record.period_undated_gainloss);

      // Portfolio movement
      const startingValue = record.starting_cost_basis - record.starting_impairment_in_inventory;
      summary.portfolioMovement.startingPortfolioValue += startingValue;
      summary.portfolioMovement.endingPortfolioValue += record.ending_carrying_value;

      // Impairment activity
      summary.impairmentActivity.totalImpairmentExpense += record.impairment_expense;
      summary.impairmentActivity.totalImpairmentReversal += record.impairment_reversal;
    });

    // Calculate derived metrics
    summary.periodActivity.netCostBasisChange = summary.periodActivity.totalAcquisitions - summary.periodActivity.totalDisposals;
    summary.portfolioMovement.portfolioChange = summary.portfolioMovement.endingPortfolioValue - summary.portfolioMovement.startingPortfolioValue;
    summary.portfolioMovement.percentageChange = summary.portfolioMovement.startingPortfolioValue > 0 ? 
      (summary.portfolioMovement.portfolioChange / summary.portfolioMovement.startingPortfolioValue) * 100 : 0;
    summary.impairmentActivity.netImpairmentChange = summary.impairmentActivity.totalImpairmentExpense - summary.impairmentActivity.totalImpairmentReversal;

    return summary;
  }

  /**
   * Format rollforward data for presentation
   */
  formatForPresentation(records: ValuationRollforwardRecord[]): string {
    let output = '📈 **Valuation Rollforward Report**\n\n';
    
    const summary = this.generateSummary(records);
    
    output += `**Portfolio Summary:**\n`;
    output += `• Starting Portfolio Value: $${summary.portfolioMovement.startingPortfolioValue.toLocaleString(undefined, { minimumFractionDigits: 2 })}\n`;
    output += `• Ending Portfolio Value: $${summary.portfolioMovement.endingPortfolioValue.toLocaleString(undefined, { minimumFractionDigits: 2 })}\n`;
    output += `• Portfolio Change: $${summary.portfolioMovement.portfolioChange.toLocaleString(undefined, { minimumFractionDigits: 2 })} (${summary.portfolioMovement.percentageChange.toFixed(2)}%)\n\n`;

    output += `**Period Activity:**\n`;
    output += `• Total Acquisitions: $${summary.periodActivity.totalAcquisitions.toLocaleString(undefined, { minimumFractionDigits: 2 })}\n`;
    output += `• Total Disposals: $${summary.periodActivity.totalDisposals.toLocaleString(undefined, { minimumFractionDigits: 2 })}\n`;
    output += `• Net Cost Basis Change: $${summary.periodActivity.netCostBasisChange.toLocaleString(undefined, { minimumFractionDigits: 2 })}\n`;
    output += `• Total Realized Gain/Loss: $${summary.periodActivity.totalRealizedGainLoss.toLocaleString(undefined, { minimumFractionDigits: 2 })}\n`;
    output += `  - Short-term: ${summary.periodActivity.shortTermGainLoss.toLocaleString(undefined, { minimumFractionDigits: 2 })}\n`;
    output += `  - Long-term: ${summary.periodActivity.longTermGainLoss.toLocaleString(undefined, { minimumFractionDigits: 2 })}\n\n`;

    if (summary.impairmentActivity.totalImpairmentExpense > 0 || summary.impairmentActivity.totalImpairmentReversal > 0) {
      output += `**Impairment Activity:**\n`;
      output += `• Impairment Expense: ${summary.impairmentActivity.totalImpairmentExpense.toLocaleString(undefined, { minimumFractionDigits: 2 })}\n`;
      output += `• Impairment Reversal: ${summary.impairmentActivity.totalImpairmentReversal.toLocaleString(undefined, { minimumFractionDigits: 2 })}\n`;
      output += `• Net Impairment Change: ${summary.impairmentActivity.netImpairmentChange.toLocaleString(undefined, { minimumFractionDigits: 2 })}\n\n`;
    }

    output += `**Asset Breakdown (${records.length} assets):**\n`;
    records.forEach(record => {
      const totalGainLoss = record.period_shortterm_gainloss + record.period_longterm_gainloss + record.period_undated_gainloss;
      output += `\n**${record.asset}:**\n`;
      output += `• Cost Basis: ${record.starting_cost_basis.toLocaleString(undefined, { minimumFractionDigits: 2 })} → ${record.ending_cost_basis.toLocaleString(undefined, { minimumFractionDigits: 2 })}\n`;
      output += `• Carrying Value: ${record.ending_carrying_value.toLocaleString(undefined, { minimumFractionDigits: 2 })}\n`;
      if (record.ending_market_value !== record.ending_carrying_value) {
        output += `• Market Value: ${record.ending_market_value.toLocaleString(undefined, { minimumFractionDigits: 2 })}\n`;
      }
      if (totalGainLoss !== 0) {
        output += `• Period Realized G/L: ${totalGainLoss.toLocaleString(undefined, { minimumFractionDigits: 2 })}\n`;
      }
    });

    return output;
  }

  // ========================================================================
  // FIELD METADATA ACCESS
  // ========================================================================

  static getFieldMetadata(): FieldMetadata[] {
    return ValuationRollforwardGenerator.FIELD_METADATA;
  }

  static getColumnsByCategory(category: string): string[] {
    return ValuationRollforwardGenerator.FIELD_METADATA
      .filter(field => field.category === category)
      .map(field => field.column);
  }

  static getColumnAliases(column: string): string[] {
    const field = ValuationRollforwardGenerator.FIELD_METADATA.find(f => f.column === column);
    return field ? field.aliases : [];
  }

  // ========================================================================
  // CSV EXPORT SUPPORT
  // ========================================================================

  exportToCSV(rollforwards: ValuationRollforwardRecord[]): string {
    const headers = [
      'asset', 'original_subsidiary', 'original_inventory', 'original_wallet',
      'starting_cost_basis', 'cost_basis_acquired', 'cost_basis_disposed', 'ending_cost_basis',
      'starting_impairment', 'impairment_expense', 'impairment_disposed', 'impairment_reversal', 'ending_impairment',
      'ending_carrying_value',
      'starting_unrealized', 'gaap_fv_adjust_up', 'gaap_fv_adjust_down', 'ifrs_reval_up', 'ifrs_reval_down', 'ending_unrealized',
      'ending_market_value',
      'period_stcg', 'period_ltcg', 'period_undated_gl'
    ];

    const csvRows = [headers.join(',')];

    rollforwards.forEach(record => {
      const row = [
        record.asset,
        record.original_subsidiary || '',
        record.original_inventory || '',
        record.original_wallet || '',
        record.starting_cost_basis.toFixed(2),
        record.cost_basis_acquired.toFixed(2),
        record.cost_basis_disposed.toFixed(2),
        record.ending_cost_basis.toFixed(2),
        record.starting_impairment_in_inventory.toFixed(2),
        record.impairment_expense.toFixed(2),
        record.impairment_disposed.toFixed(2),
        record.impairment_reversal.toFixed(2),
        record.ending_impairment_in_inventory.toFixed(2),
        record.ending_carrying_value.toFixed(2),
        record.starting_unrealized.toFixed(2),
        record.gaap_fair_value_adjust_up.toFixed(2),
        record.gaap_fair_value_adjust_down.toFixed(2),
        record.IFRS_revaluation_adjust_up.toFixed(2),
        record.IFRS_revaluation_adjust_down.toFixed(2),
        record.ending_unrealized.toFixed(2),
        record.ending_market_value.toFixed(2),
        record.period_shortterm_gainloss.toFixed(2),
        record.period_longterm_gainloss.toFixed(2),
        record.period_undated_gainloss.toFixed(2)
      ];

      csvRows.push(row.join(','));
    });

    return csvRows.join('\n');
  }

  // ========================================================================
  // ADVANCED ANALYSIS METHODS
  // ========================================================================

  /**
   * Calculate portfolio performance metrics
   */
  calculatePerformanceMetrics(rollforwards: ValuationRollforwardRecord[]): {
    totalReturn: number;
    realizedReturn: number;
    unrealizedReturn: number;
    costBasisIRR: number; // Simplified IRR approximation
    assetPerformance: Record<string, {
      totalReturn: number;
      realizedGainLoss: number;
      unrealizedGainLoss: number;
      percentOfPortfolio: number;
    }>;
  } {
    const summary = this.generateSummary(rollforwards);
    
    const totalStartingValue = summary.portfolioMovement.startingPortfolioValue;
    const totalEndingValue = summary.portfolioMovement.endingPortfolioValue;
    const totalRealizedGL = summary.periodActivity.totalRealizedGainLoss;
    
    // Calculate total unrealized G/L
    const totalUnrealizedGL = rollforwards.reduce((sum, record) => {
      return sum + (record.ending_unrealized - record.starting_unrealized);
    }, 0);

    const performance = {
      totalReturn: totalStartingValue > 0 ? ((totalEndingValue - totalStartingValue) / totalStartingValue) * 100 : 0,
      realizedReturn: totalStartingValue > 0 ? (totalRealizedGL / totalStartingValue) * 100 : 0,
      unrealizedReturn: totalStartingValue > 0 ? (totalUnrealizedGL / totalStartingValue) * 100 : 0,
      costBasisIRR: 0, // Simplified - would need more complex calculation
      assetPerformance: {} as Record<string, any>
    };

    // Calculate per-asset performance
    rollforwards.forEach(record => {
      const startingValue = record.starting_cost_basis - record.starting_impairment_in_inventory;
      const endingValue = record.ending_carrying_value;
      const realizedGL = record.period_shortterm_gainloss + record.period_longterm_gainloss + record.period_undated_gainloss;
      const unrealizedGL = record.ending_unrealized - record.starting_unrealized;
      
      performance.assetPerformance[record.asset] = {
        totalReturn: startingValue > 0 ? ((endingValue - startingValue) / startingValue) * 100 : 0,
        realizedGainLoss: realizedGL,
        unrealizedGainLoss: unrealizedGL,
        percentOfPortfolio: totalEndingValue > 0 ? (endingValue / totalEndingValue) * 100 : 0
      };
    });

    return performance;
  }

  /**
   * Identify significant movements and outliers
   */
  identifySignificantMovements(rollforwards: ValuationRollforwardRecord[], thresholds: {
    costBasisChangeThreshold?: number;
    gainLossThreshold?: number;
    impairmentThreshold?: number;
  } = {}): {
    largestAcquisitions: ValuationRollforwardRecord[];
    largestDisposals: ValuationRollforwardRecord[];
    largestGains: ValuationRollforwardRecord[];
    largestLosses: ValuationRollforwardRecord[];
    significantImpairments: ValuationRollforwardRecord[];
  } {
    const {
      costBasisChangeThreshold = 10000, // $10K
      gainLossThreshold = 5000, // $5K
      impairmentThreshold = 1000 // $1K
    } = thresholds;

    return {
      largestAcquisitions: rollforwards
        .filter(r => r.cost_basis_acquired > costBasisChangeThreshold)
        .sort((a, b) => b.cost_basis_acquired - a.cost_basis_acquired)
        .slice(0, 10),
      
      largestDisposals: rollforwards
        .filter(r => r.cost_basis_disposed > costBasisChangeThreshold)
        .sort((a, b) => b.cost_basis_disposed - a.cost_basis_disposed)
        .slice(0, 10),
      
      largestGains: rollforwards
        .filter(r => (r.period_shortterm_gainloss + r.period_longterm_gainloss + r.period_undated_gainloss) > gainLossThreshold)
        .sort((a, b) => (b.period_shortterm_gainloss + b.period_longterm_gainloss + b.period_undated_gainloss) - 
                       (a.period_shortterm_gainloss + a.period_longterm_gainloss + a.period_undated_gainloss))
        .slice(0, 10),
      
      largestLosses: rollforwards
        .filter(r => (r.period_shortterm_gainloss + r.period_longterm_gainloss + r.period_undated_gainloss) < -gainLossThreshold)
        .sort((a, b) => (a.period_shortterm_gainloss + a.period_longterm_gainloss + a.period_undated_gainloss) - 
                       (b.period_shortterm_gainloss + b.period_longterm_gainloss + b.period_undated_gainloss))
        .slice(0, 10),
      
      significantImpairments: rollforwards
        .filter(r => r.impairment_expense > impairmentThreshold)
        .sort((a, b) => b.impairment_expense - a.impairment_expense)
        .slice(0, 10)
    };
  }

  /**
   * Generate the Valuation Rollforward Report based on provided parameters
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
        startDate: parameters.startDate,
        endDate: parameters.endDate || 'CURRENT_DATE()',
        limit: parameters.limit || 5000 // Default to 5000 rows if not specified
      };
      
      // PARAMETER_REVIEW 2: Log parameters in generateReport
      console.log('PARAMETER_REVIEW 2 - ValuationRollforwardGenerator.generateReport:', {
        originalParameters: parameters,
        reportParams,
        hasStartDate: parameters.startDate ? 'YES' : 'NO',
        startDateValue: parameters.startDate,
        hasEndDate: parameters.endDate ? 'YES' : 'NO',
        endDateValue: parameters.endDate,
        hasLimit: parameters.limit ? 'YES' : 'NO',
        limitValue: parameters.limit || 5000
      });
      
      // Validate required parameters
      if (!reportParams.startDate) {
        throw new Error('Start date is required for Valuation Rollforward Report');
      }
      
      // Extract filters
      const filters: any = {};
      if (parameters.assets) {
        filters.assets = Array.isArray(parameters.assets) 
          ? parameters.assets 
          : parameters.assets.split(',').map((a: string) => a.trim());
      }
      
      // Default grouping
      const groupBy = ['asset'];
      if (parameters.includeSubsidiary) {
        groupBy.push('subsidiary');
      }
      
      // Generate SQL
      const sql = this.buildRollforwardSQL(reportParams, groupBy as any, filters);
      
      // Execute query
      if (!this.queryExecutor) {
        throw new Error('QueryExecutor not initialized');
      }
      
      // PARAMETER_REVIEW 2.1: Log parameters before query execution
      console.log('PARAMETER_REVIEW 2.1 - ValuationRollforwardGenerator before executeQuery:', {
        hasParameters: true,
        parameters: reportParams,
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
      
      // Calculate performance metrics
      const performanceMetrics = this.calculatePerformanceMetrics(results);
      
      // Identify significant movements
      const significantMovements = this.identifySignificantMovements(results);
      
      // Define columns based on the results - match all columns in the SQL query
      const columns = [
        'asset',
        'starting_cost_basis',
        'cost_basis_acquired',
        'cost_basis_disposed',
        'ending_cost_basis',
        'starting_impairment_in_inventory',
        'impairment_expense',
        'impairment_disposed',
        'impairment_reversal',
        'ending_impairment_in_inventory',
        'ending_carrying_value',
        'starting_unrealized',
        'gaap_fair_value_adjust_up',
        'gaap_fair_value_adjust_down',
        'IFRS_revaluation_adjust_up',
        'IFRS_revaluation_adjust_down',
        'ending_unrealized',
        'ending_market_value',
        'period_shortterm_gainloss',
        'period_longterm_gainloss',
        'period_undated_gainloss'
      ];
      
      if (groupBy.includes('subsidiary')) {
        columns.splice(1, 0, 'subsidiary');
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
          performanceMetrics,
          significantMovements,
          totalRecords: results.length,
          period: {
            startDate: reportParams.startDate,
            endDate: reportParams.endDate
          }
        }
      };
    } catch (error) {
      console.error('Error generating Valuation Rollforward Report:', error);
      throw error;
    }
  }
}