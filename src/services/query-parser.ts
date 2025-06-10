/**
 * Query Parser Service - Natural Language to SQL Translation
 * 
 * Implements the 5-Step Process:
 * 1. UNDERSTAND - Parse what the user is asking for
 * 2. MAP - Identify relevant columns (with user confirmation)
 * 3. AGGREGATE - Determine aggregation functions
 * 4. FILTER - Apply exclusions/inclusions
 * 5. PRESENT - Structure for SQL generation
 */

import { 
  QueryParseResult, 
  ColumnMapping, 
  FieldMetadata, 
  ACTIONS_REPORT_METADATA 
} from '../types/actions-report.js';

export class QueryParser {
  private fieldMetadata: Map<string, FieldMetadata>;
  private aliasToColumn: Map<string, string>;

  constructor() {
    this.fieldMetadata = new Map();
    this.aliasToColumn = new Map();
    this.initializeMetadata();
  }

  private initializeMetadata(): void {
    // Build lookup maps for fast column resolution
    ACTIONS_REPORT_METADATA.forEach(field => {
      this.fieldMetadata.set(field.column, field);
      
      // Add aliases mapping back to main column
      field.aliases.forEach(alias => {
        this.aliasToColumn.set(alias.toLowerCase(), field.column);
      });
      
      // Add the column name itself as an alias
      this.aliasToColumn.set(field.column.toLowerCase(), field.column);
    });
  }

  // ========================================================================
  // MAIN PARSING ENTRY POINT
  // ========================================================================
  async parseQuery(query: string, reportType: string = 'actions'): Promise<QueryParseResult> {
    const normalizedQuery = query.toLowerCase().trim();

    // STEP 1: UNDERSTAND - Parse user intent and extract components
    const intent = this.determineIntent(normalizedQuery);
    const assets = this.extractAssets(normalizedQuery);
    const timeRange = this.extractTimeRange(normalizedQuery);
    const wallets = this.extractWallets(normalizedQuery);
    const aggregationType = this.determineAggregationType(normalizedQuery);

    // STEP 2: MAP - Identify relevant columns
    const columns = this.mapToColumns(normalizedQuery, intent);

    // STEP 3-4: AGGREGATE & FILTER - Determine filters and aggregations
    const filters = this.buildFilters(normalizedQuery, assets, timeRange, wallets);

    return {
      intent,
      assets: assets || [],
      timeRange: timeRange || {},
      wallets: wallets || {},
      aggregationType: aggregationType || 'sum',
      columns,
      filters
    };
  }

  // ========================================================================
  // STEP 1: UNDERSTAND - Intent and Component Extraction
  // ========================================================================
  private determineIntent(query: string): 'aggregation' | 'filter' | 'comparison' | 'trend' {
    const aggregationKeywords = [
      'total', 'sum', 'amount', 'count', 'average', 'avg', 'max', 'min',
      'gain', 'loss', 'profit', 'performance', 'value'
    ];
    
    const comparisonKeywords = [
      'vs', 'versus', 'compare', 'comparison', 'against', 'between'
    ];
    
    const trendKeywords = [
      'over time', 'trend', 'growth', 'change', 'movement', 'progression'
    ];

    if (comparisonKeywords.some(keyword => query.includes(keyword))) {
      return 'comparison';
    }
    
    if (trendKeywords.some(keyword => query.includes(keyword))) {
      return 'trend';
    }
    
    if (aggregationKeywords.some(keyword => query.includes(keyword))) {
      return 'aggregation';
    }

    return 'filter';
  }

  private extractAssets(query: string): string[] {
    const assets: string[] = [];
    
    // Common crypto asset patterns
    const cryptoAssets = [
      'btc', 'bitcoin', 'eth', 'ethereum', 'usdt', 'tether', 'usdc', 'bnb', 'binance coin',
      'xrp', 'ripple', 'ada', 'cardano', 'sol', 'solana', 'doge', 'dogecoin', 'avax', 'avalanche',
      'matic', 'polygon', 'dot', 'polkadot', 'uni', 'uniswap', 'link', 'chainlink', 'ltc', 'litecoin'
    ];

    // Asset mapping for common variations
    const assetMappings: Record<string, string> = {
      'bitcoin': 'BTC',
      'btc': 'BTC',
      'ethereum': 'ETH',
      'eth': 'ETH',
      'tether': 'USDT',
      'usdt': 'USDT',
      'usdc': 'USDC',
      'ripple': 'XRP',
      'xrp': 'XRP',
      'cardano': 'ADA',
      'ada': 'ADA',
      'solana': 'SOL',
      'sol': 'SOL',
      'dogecoin': 'DOGE',
      'doge': 'DOGE'
    };

    cryptoAssets.forEach(asset => {
      if (query.includes(asset)) {
        const mappedAsset = assetMappings[asset] || asset.toUpperCase();
        if (!assets.includes(mappedAsset)) {
          assets.push(mappedAsset);
        }
      }
    });

    // Look for explicit asset patterns like "BTC", "ETH" (3-4 char uppercase)
    const explicitAssetPattern = /\b[A-Z]{2,5}\b/g;
    const matches = query.toUpperCase().match(explicitAssetPattern);
    if (matches) {
      matches.forEach(match => {
        if (!assets.includes(match) && match.length >= 2 && match.length <= 5) {
          assets.push(match);
        }
      });
    }

    return assets;
  }

  private extractTimeRange(query: string): { start?: string; end?: string } | undefined {
    const timeRange: { start?: string; end?: string } = {};

    // Month patterns
    const monthPatterns = [
      { pattern: /january|jan\b/i, month: '01' },
      { pattern: /february|feb\b/i, month: '02' },
      { pattern: /march|mar\b/i, month: '03' },
      { pattern: /april|apr\b/i, month: '04' },
      { pattern: /may/i, month: '05' },
      { pattern: /june|jun\b/i, month: '06' },
      { pattern: /july|jul\b/i, month: '07' },
      { pattern: /august|aug\b/i, month: '08' },
      { pattern: /september|sep\b/i, month: '09' },
      { pattern: /october|oct\b/i, month: '10' },
      { pattern: /november|nov\b/i, month: '11' },
      { pattern: /december|dec\b/i, month: '12' }
    ];

    // Year extraction
    const yearMatch = query.match(/\b(20\d{2})\b/);
    const year = yearMatch ? yearMatch[1] : new Date().getFullYear().toString();

    // Month extraction
    let month: string | undefined;
    for (const { pattern, month: monthNum } of monthPatterns) {
      if (pattern.test(query)) {
        month = monthNum;
        break;
      }
    }

    if (month) {
      timeRange.start = `${year}-${month}-01`;
      // Calculate last day of month
      const lastDay = new Date(parseInt(year || '0'), parseInt(month || '0'), 0).getDate();
      timeRange.end = `${year}-${month}-${lastDay.toString().padStart(2, '0')}`;
    }

    // Specific date patterns (YYYY-MM-DD)
    const datePattern = /\b(\d{4})-(\d{2})-(\d{2})\b/g;
    const dateMatches = query.match(datePattern);
    if (dateMatches && dateMatches.length >= 1) {
      timeRange.start = dateMatches[0];
      if (dateMatches.length >= 2) {
        if (timeRange && dateMatches[1]) {
          timeRange.end = dateMatches[1];
        }
      }
    }

    // Relative time patterns
    if (query.includes('last month')) {
      const lastMonth = new Date();
      lastMonth.setMonth(lastMonth.getMonth() - 1);
      const year = lastMonth.getFullYear();
      const month = (lastMonth.getMonth() + 1).toString().padStart(2, '0');
      timeRange.start = `${year}-${month}-01`;
      const lastDay = new Date(year, lastMonth.getMonth() + 1, 0).getDate();
      timeRange.end = `${year}-${month}-${lastDay.toString().padStart(2, '0')}`;
    }

    if (query.includes('this year')) {
      const currentYear = new Date().getFullYear();
      timeRange.start = `${currentYear}-01-01`;
      timeRange.end = `${currentYear}-12-31`;
    }

    return Object.keys(timeRange).length > 0 ? timeRange : undefined;
  }

  private extractWallets(query: string): { include?: string[]; exclude?: string[] } | undefined {
    const wallets: { include?: string[]; exclude?: string[] } = {};

    // Exclusion patterns
    const excludePatterns = [
      /excluding\s+(\w+(?:\s+\w+)*?)(?:\s+wallet)?/gi,
      /except\s+(\w+(?:\s+\w+)*?)(?:\s+wallet)?/gi,
      /without\s+(\w+(?:\s+\w+)*?)(?:\s+wallet)?/gi
    ];

    excludePatterns.forEach(pattern => {
      const matches = [...query.matchAll(pattern)];
      matches.forEach(match => {
        if (!wallets.exclude) wallets.exclude = [];
        const walletName = match[1]?.trim() || '';
        if (!wallets.exclude.includes(walletName)) {
          wallets.exclude.push(walletName);
        }
      });
    });

    // Inclusion patterns (from wallet, in wallet, wallet X)
    const includePatterns = [
      /from\s+(\w+(?:\s+\w+)*?)(?:\s+wallet)?/gi,
      /in\s+(\w+(?:\s+\w+)*?)(?:\s+wallet)/gi,
      /(\w+)\s+wallet/gi
    ];

    includePatterns.forEach(pattern => {
      const matches = [...query.matchAll(pattern)];
      matches.forEach(match => {
        const walletName = match[1]?.trim() || '';
        // Skip if this wallet is already in exclude list or is a common word
        if (wallets.exclude?.includes(walletName) || ['my', 'the', 'a', 'an'].includes(walletName.toLowerCase())) {
          return;
        }
        if (!wallets.include) wallets.include = [];
        if (!wallets.include.includes(walletName)) {
          wallets.include.push(walletName);
        }
      });
    });

    return Object.keys(wallets).length > 0 ? wallets : undefined;
  }

  private determineAggregationType(query: string): 'sum' | 'count' | 'avg' | 'min' | 'max' {
    if (query.includes('count') || query.includes('number of')) return 'count';
    if (query.includes('average') || query.includes('avg')) return 'avg';
    if (query.includes('maximum') || query.includes('max') || query.includes('highest')) return 'max';
    if (query.includes('minimum') || query.includes('min') || query.includes('lowest')) return 'min';
    
    // Default to sum for financial queries
    return 'sum';
  }

  // ========================================================================
  // STEP 2: MAP - Column Identification and Mapping
  // ========================================================================
  private mapToColumns(query: string, intent: string): ColumnMapping[] {
    const mappings: ColumnMapping[] = [];

    // Financial data mapping
    if (this.containsFinancialTerms(query)) {
      mappings.push(...this.mapFinancialColumns(query));
    }

    // Asset data mapping
    if (query.includes('quantity') || query.includes('amount') || query.includes('units')) {
      mappings.push({
        userTerm: 'quantity',
        mappedColumns: ['assetUnitAdj'],
        description: 'Asset quantity/units',
        confirmed: false
      });
    }

    // Wallet/account mapping
    if (query.includes('wallet') || query.includes('account')) {
      mappings.push({
        userTerm: 'wallet',
        mappedColumns: ['wallet'],
        description: 'Wallet/account identifier',
        confirmed: false
      });
    }

    // Asset mapping
    if (query.includes('asset') || query.includes('coin') || query.includes('token')) {
      mappings.push({
        userTerm: 'asset',
        mappedColumns: ['asset'],
        description: 'Asset symbol/ticker',
        confirmed: false
      });
    }

    // Time mapping
    if (query.includes('date') || query.includes('time') || query.includes('when')) {
      mappings.push({
        userTerm: 'date',
        mappedColumns: ['timestamp', 'timestampSEC'],
        description: 'Transaction timestamp',
        confirmed: false
      });
    }

    // If no specific mappings found, provide defaults based on intent
    if (mappings.length === 0) {
      mappings.push(...this.getDefaultMappings(intent));
    }

    return mappings;
  }

  private containsFinancialTerms(query: string): boolean {
    const financialTerms = [
      'gain', 'loss', 'profit', 'cost basis', 'value', 'price', 'amount',
      'total', 'sum', 'worth', 'valuation', 'carrying value', 'fair market value'
    ];
    return financialTerms.some(term => query.includes(term));
  }

  private mapFinancialColumns(query: string): ColumnMapping[] {
    const mappings: ColumnMapping[] = [];

    // Gain/Loss mapping
    if (query.includes('gain') || query.includes('loss') || query.includes('profit')) {
      mappings.push({
        userTerm: 'gain/loss',
        mappedColumns: ['shortTermGainLoss', 'longTermGainLoss', 'undatedGainLoss'],
        description: 'Capital gains and losses (short-term, long-term, undated)',
        confirmed: false
      });
    }

    // Cost basis mapping
    if (query.includes('cost basis') || query.includes('cost')) {
      mappings.push({
        userTerm: 'cost basis',
        mappedColumns: ['costBasisAcquired', 'costBasisRelieved'],
        description: 'Cost basis acquired and disposed',
        confirmed: false
      });
    }

    // Value mapping
    if (query.includes('value') || query.includes('worth')) {
      mappings.push({
        userTerm: 'value',
        mappedColumns: ['carryingValue', 'fairMarketValueDisposed'],
        description: 'Carrying value and fair market value',
        confirmed: false
      });
    }

    return mappings;
  }

  private getDefaultMappings(intent: string): ColumnMapping[] {
    switch (intent) {
      case 'aggregation':
        return [
          {
            userTerm: 'financial totals',
            mappedColumns: ['shortTermGainLoss', 'longTermGainLoss', 'undatedGainLoss'],
            description: 'Default financial aggregation columns',
            confirmed: false
          }
        ];
      
      case 'filter':
        return [
          {
            userTerm: 'transaction data',
            mappedColumns: ['asset', 'action', 'wallet', 'timestamp'],
            description: 'Basic transaction filtering columns',
            confirmed: false
          }
        ];
      
      default:
        return [
          {
            userTerm: 'general analysis',
            mappedColumns: ['asset', 'assetUnitAdj', 'carryingValue'],
            description: 'General purpose analysis columns',
            confirmed: false
          }
        ];
    }
  }

  // ========================================================================
  // STEPS 3-4: AGGREGATE & FILTER - Build Filters and Aggregations
  // ========================================================================
  private buildFilters(
    query: string, 
    assets?: string[], 
    timeRange?: { start?: string; end?: string }, 
    wallets?: { include?: string[]; exclude?: string[] }
  ): Record<string, any> {
    const filters: Record<string, any> = {};

    // Asset filters
    if (assets && assets.length > 0) {
      filters.assets = assets;
    }

    // Time range filters
    if (timeRange) {
      if (timeRange.start) filters.startDate = timeRange.start;
      if (timeRange.end) filters.endDate = timeRange.end;
    }

    // Wallet filters
    if (wallets) {
      if (wallets.include && wallets.include.length > 0) {
        filters.includeWallets = wallets.include;
      }
      if (wallets.exclude && wallets.exclude.length > 0) {
        filters.excludeWallets = wallets.exclude;
      }
    }

    // Action filters
    if (query.includes('buy') || query.includes('purchase')) {
      filters.actions = ['buy'];
    } else if (query.includes('sell') || query.includes('sale')) {
      filters.actions = ['sell'];
    } else if (query.includes('transfer')) {
      filters.actions = ['transfer'];
    }

    // Status filters
    if (query.includes('complete')) {
      filters.status = ['complete'];
    }

    return filters;
  }

  // ========================================================================
  // UTILITY METHODS
  // ========================================================================
  
  /**
   * Resolve user terms to actual column names using aliases
   */
  resolveColumnName(userTerm: string): string | undefined {
    return this.aliasToColumn.get(userTerm.toLowerCase());
  }

  /**
   * Get field metadata for a column
   */
  getFieldMetadata(column: string): FieldMetadata | undefined {
    return this.fieldMetadata.get(column);
  }

  /**
   * Get all available columns for a category
   */
  getColumnsByCategory(category: string): string[] {
    return Array.from(this.fieldMetadata.values())
      .filter(field => field.category === category)
      .map(field => field.column);
  }

  /**
   * Suggest columns based on partial user input
   */
  suggestColumns(partialTerm: string): string[] {
    const term = partialTerm.toLowerCase();
    const suggestions: string[] = [];

    // Check direct column names
    Array.from(this.fieldMetadata.keys()).forEach(column => {
      if (column.toLowerCase().includes(term)) {
        suggestions.push(column);
      }
    });

    // Check aliases
    Array.from(this.aliasToColumn.entries()).forEach(([alias, column]) => {
      if (alias.includes(term) && !suggestions.includes(column)) {
        suggestions.push(column);
      }
    });

    // Check common queries
    Array.from(this.fieldMetadata.values()).forEach(field => {
      field.common_queries.forEach(query => {
        if (query.toLowerCase().includes(term) && !suggestions.includes(field.column)) {
          suggestions.push(field.column);
        }
      });
    });

    return suggestions.slice(0, 10); // Limit to top 10 suggestions
  }
}