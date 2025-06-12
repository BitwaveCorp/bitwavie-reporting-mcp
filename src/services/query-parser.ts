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
  TimeRange,
  FilterCondition,
  FilterOperator,
  Aggregation,
  GroupByClause,
  OrderByClause,
  QueryMetadata
} from '../types/actions-report';

// Common patterns for parsing
const DATE_PATTERNS = {
  RELATIVE: /(last|next|this)\s+(\d+)?\s*(day|week|month|quarter|year)s?/i,
  ABSOLUTE: /(january|february|march|april|may|june|july|august|september|october|november|december|\d{1,2}[\/\-]\d{1,2}[\/\-]?\d{2,4})/i,
  RANGE: /(from|between)\s+(.+?)\s+(to|and)\s+(.+)/i
};

const AGGREGATION_KEYWORDS = {
  sum: ['total', 'sum', 'amount', 'value'],
  count: ['count', 'number of', 'how many', 'total number of'],
  avg: ['average', 'mean', 'avg'],
  min: ['minimum', 'least', 'lowest', 'min'],
  max: ['maximum', 'most', 'highest', 'max']
};

const LOGICAL_OPERATORS = ['and', 'or', 'not'] as const;

const COMPARISON_OPERATORS: Record<string, FilterOperator> = {
  '=': '=',
  '!=': '!=',
  '<>': '!=',
  '>': '>',
  '<': '<',
  '>=': '>=',
  '<=': '<=',
  'in': 'IN',
  'not in': 'NOT IN',
  'like': 'LIKE',
  'not like': 'NOT LIKE',
  'is': '=',
  'is not': '!=',
  'between': 'BETWEEN'
} as const;

export class QueryParser {
  private fieldMetadata: Map<string, FieldMetadata>;
  private aliasToColumn: Map<string, string>;
  private columnToMetadata: Map<string, FieldMetadata>;
  private defaultMappings: Record<string, ColumnMapping[]>;

  constructor() {
    this.fieldMetadata = new Map();
    this.aliasToColumn = new Map();
    this.columnToMetadata = new Map();
    this.defaultMappings = {
      list: [
        { name: 'timestamp', type: 'timestamp', displayName: 'Timestamp', description: 'Transaction timestamp' },
        { name: 'asset', type: 'string', displayName: 'Asset', description: 'Cryptocurrency symbol' },
        { name: 'action', type: 'string', displayName: 'Action', description: 'Transaction type' },
        { name: 'amount', type: 'number', displayName: 'Amount', description: 'Transaction amount' }
      ],
      filter: [
        { name: 'timestamp', type: 'timestamp', displayName: 'Timestamp', description: 'Transaction timestamp' },
        { name: 'asset', type: 'string', displayName: 'Asset', description: 'Cryptocurrency symbol' },
        { name: 'action', type: 'string', displayName: 'Action', description: 'Transaction type' },
        { name: 'amount', type: 'number', displayName: 'Amount', description: 'Transaction amount' }
      ],
      aggregation: [
        { name: 'count', type: 'number', displayName: 'Count', description: 'Number of records', isAggregation: true },
        { name: 'sum_amount', type: 'number', displayName: 'Total Amount', description: 'Sum of amount', isAggregation: true }
      ]
    };
    this.initializeMetadata();
  }

  /**
   * Extracts cryptocurrency assets mentioned in the query
   * @param query The natural language query
   * @returns Array of identified asset symbols (e.g., ['BTC', 'ETH'])
   */
  private extractAssets(query: string): string[] {
    if (!query) return [];
    
    // Common cryptocurrency symbols and names
    const cryptoAssets = [
      { symbol: 'BTC', names: ['bitcoin', 'btc'] },
      { symbol: 'ETH', names: ['ethereum', 'ether', 'eth'] },
      { symbol: 'USDT', names: ['tether', 'usdt'] },
      { symbol: 'USDC', names: ['usd coin', 'usdc'] },
      { symbol: 'BNB', names: ['binance coin', 'bnb'] },
      { symbol: 'XRP', names: ['ripple', 'xrp'] },
      { symbol: 'SOL', names: ['solana', 'sol'] },
      { symbol: 'ADA', names: ['cardano', 'ada'] },
      { symbol: 'DOGE', names: ['dogecoin', 'doge'] },
      { symbol: 'DOT', names: ['polkadot', 'dot'] },
    ];

    const foundAssets = new Set<string>();
    const lowerQuery = query.toLowerCase();

    // Check for exact symbol matches (case-insensitive)
    for (const asset of cryptoAssets) {
      const symbolPattern = new RegExp(`\\b${asset.symbol}\\b`, 'i');
      if (symbolPattern.test(query)) {
        foundAssets.add(asset.symbol);
      }
    }

    // Check for full name matches
    for (const asset of cryptoAssets) {
      for (const name of asset.names) {
        const namePattern = new RegExp(`\\b${name}\\b`, 'i');
        if (namePattern.test(lowerQuery)) {
          foundAssets.add(asset.symbol);
          break; // No need to check other names for this asset
        }
      }
    }

    return Array.from(foundAssets);
  }

  /**
   * Extracts filter conditions from the query
   * @param query The natural language query
   * @param assets Array of assets already identified in the query
   * @param timeRange Time range if already identified
   * @returns Array of filter conditions
   */
  private extractFilters(
    query: string, 
    assets: string[] = [], 
    timeRange?: TimeRange
  ): FilterCondition[] {
    const filters: FilterCondition[] = [];
    if (!query || typeof query !== 'string') {
      return filters;
    }

    // Add asset filters if assets were identified
    if (assets && Array.isArray(assets) && assets.length > 0) {
      filters.push({
        column: 'asset',
        operator: 'IN',
        value: assets.filter((asset): asset is string => typeof asset === 'string'),
        logicalOperator: 'AND' as const
      });
    }

    // Add time range filter if available
    if (timeRange) {
      if (timeRange.startDate) {
        filters.push({
          column: 'timestamp',
          operator: '>=',
          value: timeRange.startDate,
          logicalOperator: 'AND' as const
        });
      }
      if (timeRange.endDate) {
        filters.push({
          column: 'timestamp',
          operator: '<=',
          value: timeRange.endDate,
          logicalOperator: 'AND' as const
        });
      }
    }

    // Extract other filter conditions using regex patterns
    interface FilterPattern {
      pattern: RegExp;
      column: string;
      operator: FilterOperator;
      valueMapper?: (match: RegExpMatchArray) => unknown;
    }

    const filterPatterns: FilterPattern[] = [
      // Amount greater than
      {
        pattern: /(amount|value|quantity)\s*(>|greater than|above|more than)\s*(\d+(\.\d+)?)/i,
        column: 'amount',
        operator: '>',
        valueMapper: (match) => match[3] ? parseFloat(match[3]) : 0
      },
      // Amount less than
      {
        pattern: /(amount|value|quantity)\s*(<|less than|below|under)\s*(\d+(\.\d+)?)/i,
        column: 'amount',
        operator: '<',
        valueMapper: (match) => match[3] ? parseFloat(match[3]) : 0
      },
      // Status filters
      {
        pattern: /(status|state)\s*(=|is\s+)?(pending|completed|failed|cancelled|processing)/i,
        column: 'status',
        operator: '=',
        valueMapper: (match) => match[3] ? match[3].toLowerCase() : 'unknown'
      },
      // Action type filters
      {
        pattern: /(action|type|txn)\s*(=|is\s+)?(buy|sell|transfer|deposit|withdrawal|trade|swap)/i,
        column: 'action',
        operator: '=',
        valueMapper: (match) => match[3] ? match[3].toLowerCase() : 'unknown'
      },
      // Contains text
      {
        pattern: /(description|note|memo|comment)\s*(contains?|has|with)\s*["']?([^\s"']+)["']?/i,
        column: 'description',
        operator: 'LIKE',
        valueMapper: (match) => `%${match[3]}%`
      }
    ];

    for (const { pattern, column, operator, valueMapper } of filterPatterns) {
      const match = query.match(pattern);
      if (match) {
        const value = valueMapper ? valueMapper(match) : match[3];
        if (value !== undefined) {
          filters.push({
            column,
            operator,
            value,
            logicalOperator: 'AND' as const
          });
        }
      }
    }

    // If no filters were found but we have a query, add a basic text search
    if (filters.length === 0 && query.trim().length > 0) {
      filters.push({
        column: 'description',
        operator: 'LIKE',
        value: `%${query.trim()}%`,
        logicalOperator: 'AND' as const
      });
    }

    // Ensure the first filter doesn't have a logical operator
    if (filters.length > 0 && filters[0]) {
      const firstFilter = { ...filters[0] };
      delete (firstFilter as Partial<FilterCondition>).logicalOperator;
      filters[0] = firstFilter;
    }

    return filters;
  }

  /**
   * Extracts aggregation functions from the query
   * @param query The natural language query
   * @returns Array of aggregation specifications
   */
  private extractAggregations(query: string): Aggregation[] {
    const aggregations: Aggregation[] = [];
    if (!query) return aggregations;

    // Common aggregation patterns
    const aggregationPatterns: Array<{
      pattern: RegExp;
      type: 'sum' | 'avg' | 'count' | 'min' | 'max';
      column: string;
      alias?: string;
    }> = [
      // Sum patterns
      {
        pattern: /(sum|total|add up|add|calculate sum of|calculate total of)\s+(?:the\s+)?(?:value of\s+)?(\w+)(?:\s+as\s+([\w_]+))?/i,
        type: 'sum',
        column: '$2',
        alias: '$3'
      },
      // Average patterns
      {
        pattern: /(average|avg|mean|calculate average of|calculate mean of)\s+(?:the\s+)?(?:value of\s+)?(\w+)(?:\s+as\s+([\w_]+))?/i,
        type: 'avg',
        column: '$2',
        alias: '$3'
      },
      // Count patterns
      {
        pattern: /(count|number of|how many|calculate count of)\s+(?:the\s+)?(?:number of\s+)?(\w+)(?:\s+as\s+([\w_]+))?/i,
        type: 'count',
        column: '$2',
        alias: '$3'
      },
      // Min patterns
      {
        pattern: /(minimum|min|lowest|smallest|calculate minimum of)\s+(?:the\s+)?(?:value of\s+)?(\w+)(?:\s+as\s+([\w_]+))?/i,
        type: 'min',
        column: '$2',
        alias: '$3'
      },
      // Max patterns
      {
        pattern: /(maximum|max|highest|largest|calculate maximum of)\s+(?:the\s+)?(?:value of\s+)?(\w+)(?:\s+as\s+([\w_]+))?/i,
        type: 'max',
        column: '$2',
        alias: '$3'
      }
    ];

    for (const { pattern, type, column: columnPattern, alias: aliasPattern } of aggregationPatterns) {
      const match = query.match(pattern);
      if (match) {
        // Extract column and alias using the captured groups
        const column = match[2] || '';
        const alias = match[3] || `${type}_${column}`.toLowerCase();
        
        // Only add if we have a valid column
        if (column) {
          aggregations.push({
            function: type,
            column,
            alias
          });
        }
      }
    }

    // If no explicit aggregations found, look for implicit ones
    if (aggregations.length === 0) {
      // Check for common aggregation keywords
      const implicitAggPatterns: Array<{
        keywords: string[];
        type: 'sum' | 'avg' | 'count' | 'min' | 'max';
        column: string;
      }> = [
        { keywords: ['total', 'sum', 'add up'], type: 'sum', column: 'amount' },
        { keywords: ['average', 'mean', 'avg'], type: 'avg', column: 'amount' },
        { keywords: ['count', 'how many', 'number of'], type: 'count', column: '*' },
        { keywords: ['minimum', 'min', 'lowest'], type: 'min', column: 'amount' },
        { keywords: ['maximum', 'max', 'highest'], type: 'max', column: 'amount' },
      ];

      for (const { keywords, type, column } of implicitAggPatterns) {
        if (keywords.some(keyword => query.toLowerCase().includes(keyword))) {
          aggregations.push({
            function: type,
            column,
            alias: `${type}_${column}`.toLowerCase()
          });
          break; // Stop after first match
        }
      }
    }

    return aggregations;
  }

  /**
   * Extracts GROUP BY clauses from the query
   * @param query The natural language query
   * @returns Array of group by clauses or undefined if none found
   */
  private extractGroupBy(query: string): GroupByClause[] | undefined {
    if (!query) return undefined;

    const groupByClauses: GroupByClause[] = [];
    const lowerQuery = query.toLowerCase();

    // Common group by patterns
    const groupByPatterns: Array<{
      pattern: RegExp;
      column: string;
      interval?: 'day' | 'week' | 'month' | 'quarter' | 'year';
    }> = [
      // Group by date patterns
      {
        pattern: /(?:group\s+by|grouping by|grouped by|aggregate by|summarize by|break down by)\s+(?:the\s+)?(date|time|timestamp|day|week|month|quarter|year)(?:\s+by\s+(day|week|month|quarter|year))?/i,
        column: 'timestamp',
        interval: 'day' // Default interval for date grouping
      },
      // Group by asset
      {
        pattern: /(?:group\s+by|grouping by|grouped by|aggregate by|summarize by|break down by)\s+(?:the\s+)?(asset|coin|token|currency|symbol)/i,
        column: 'asset'
      },
      // Group by action type
      {
        pattern: /(?:group\s+by|grouping by|grouped by|aggregate by|summarize by|break down by)\s+(?:the\s+)?(action|type|transaction type|txn type)/i,
        column: 'action'
      },
      // Group by status
      {
        pattern: /(?:group\s+by|grouping by|grouped by|aggregate by|summarize by|break down by)\s+(?:the\s+)?(status|state)/i,
        column: 'status'
      },
      // Group by wallet/address
      {
        pattern: /(?:group\s+by|grouping by|grouped by|aggregate by|summarize by|break down by)\s+(?:the\s+)?(wallet|address|account|from|to)/i,
        column: 'wallet_address'
      }
    ];

    // Check for explicit group by patterns
    for (const { pattern, column, interval } of groupByPatterns) {
      const match = lowerQuery.match(pattern);
      if (match) {
        const matchedColumn = match[1] || column;
        let matchedInterval: 'day' | 'week' | 'month' | 'quarter' | 'year' | undefined = undefined;
        
        // For date patterns, try to extract the interval from the match
        if (column === 'timestamp' && match[2]) {
          const intervalMatch = match[2].toLowerCase();
          if (['day', 'week', 'month', 'quarter', 'year'].includes(intervalMatch)) {
            matchedInterval = intervalMatch as 'day' | 'week' | 'month' | 'quarter' | 'year';
          }
        }
        
        // If no interval matched but we have a default interval, use it
        if (!matchedInterval && interval) {
          matchedInterval = interval;
        }
        
        const groupByClause: GroupByClause = {
          column: matchedColumn,
          ...(matchedInterval && { interval: matchedInterval })
        };
        
        groupByClauses.push(groupByClause);
      }
    }

    // If no explicit group by, try to infer from aggregations and other context
    if (groupByClauses.length === 0) {
      // If we have a date-related query, default to grouping by date
      const dateKeywords = ['today', 'yesterday', 'this week', 'last week', 'this month', 'last month', 'year to date'];
      if (dateKeywords.some(keyword => lowerQuery.includes(keyword))) {
        groupByClauses.push({ 
          column: 'timestamp', 
          interval: 'day' 
        } as const);
      }
      
      // If we have asset filters but no group by, group by asset
      if (lowerQuery.includes('asset') || lowerQuery.includes('coin') || lowerQuery.includes('token')) {
        groupByClauses.push({ column: 'asset' } as const);
      }
      
      // If we have action type filters but no group by, group by action
      if (lowerQuery.includes('action') || lowerQuery.includes('type') || lowerQuery.includes('transaction')) {
        groupByClauses.push({ column: 'action' } as const);
      }
    }

    return groupByClauses.length > 0 ? groupByClauses : undefined;
  }

  /**
   * Extracts ORDER BY clauses from the query
   * @param query The natural language query
   * @returns Array of order by clauses or undefined if none found
   */
  private extractOrderBy(query: string): OrderByClause[] | undefined {
    if (!query) return undefined;

    const orderByClauses: OrderByClause[] = [];
    const lowerQuery = query.toLowerCase();

    // Common order by patterns
    const orderByPatterns: Array<{
      pattern: RegExp;
      column: string;
      direction: 'ASC' | 'DESC';
      nulls?: 'FIRST' | 'LAST';
    }> = [
      // Date ordering
      {
        pattern: /(?:order\s+by\s+)?(?:newest|most recent|latest|recent) first/i,
        column: 'timestamp',
        direction: 'DESC',
        nulls: 'LAST'
      },
      {
        pattern: /(?:order\s+by\s+)?(?:oldest|earliest|first) first/i,
        column: 'timestamp',
        direction: 'ASC',
        nulls: 'FIRST'
      },
      // Amount ordering
      {
        pattern: /(?:order\s+by\s+)?(?:highest|largest|biggest|most)\s+(?:amount|value|quantity)/i,
        column: 'amount',
        direction: 'DESC',
        nulls: 'LAST'
      },
      {
        pattern: /(?:order\s+by\s+)?(?:lowest|smallest|least)\s+(?:amount|value|quantity)/i,
        column: 'amount',
        direction: 'ASC',
        nulls: 'FIRST'
      },
      // Explicit column ordering
      {
        pattern: /order\s+by\s+(\w+)(?:\s+(asc|desc|ascending|descending))?(?:\s+nulls\s+(first|last))?/i,
        column: '', // Will be set from match[1] in the processing code
        direction: 'ASC' // Default direction
        // nulls will be set based on the match if specified
      }
    ];

    // Check for explicit order by patterns
    for (const { pattern, column, direction, nulls } of orderByPatterns) {
      const match = lowerQuery.match(pattern);
      if (match) {
        let resolvedColumn = column;
        let resolvedDirection = direction;
        let resolvedNulls = nulls;

        // Handle the explicit column ordering pattern
        if (pattern.toString().includes('order\\s+by\\s+(\\w+)') && match[1]) {
          resolvedColumn = match[1].toLowerCase();
          
          // Set direction if specified
          if (match[2]) {
            const dir = match[2].toLowerCase();
            resolvedDirection = (dir === 'desc' || dir === 'descending') ? 'DESC' : 'ASC';
          }

          // Set nulls ordering if specified
          if (match[3]) {
            resolvedNulls = match[3].toUpperCase() as 'FIRST' | 'LAST';
          }
        }

        // Only add if we don't already have this column in our clauses
        if (!orderByClauses.some(ob => ob.column === resolvedColumn)) {
          const clause: OrderByClause = {
            column: resolvedColumn,
            direction: resolvedDirection
          };
          
          if (resolvedNulls) {
            clause.nulls = resolvedNulls;
          }
          
          orderByClauses.push(clause);
        }
      }
    }

    // If no explicit order by found, try to infer from the query
    if (orderByClauses.length === 0) {
      // Default ordering for time-based queries
      if (lowerQuery.includes('latest') || lowerQuery.includes('recent') || 
          lowerQuery.includes('newest') || lowerQuery.includes('last')) {
        orderByClauses.push({
          column: 'timestamp',
          direction: 'DESC',
          nulls: 'LAST'
        });
      }
      // Default ordering for financial queries
      else if (lowerQuery.includes('total') || lowerQuery.includes('sum') || 
               lowerQuery.includes('amount') || lowerQuery.includes('value')) {
        orderByClauses.push({
          column: 'amount',
          direction: 'DESC',
          nulls: 'LAST'
        });
      }
    }

    return orderByClauses.length > 0 ? orderByClauses : undefined;
  }

  private initializeMetadata(): void {
    // Define core metadata for the actions report
    const coreFields: FieldMetadata[] = [
      {
        column: 'timestamp',
        description: 'Transaction timestamp',
        type: 'timestamp',
        category: 'temporal',
        aliases: ['time', 'date', 'transaction time', 'txn time', 'txn date'],
        common_queries: ['Show transactions from last week', 'What happened yesterday?'],
        aggregatable: false,
        filterable: true
      },
      {
        column: 'asset',
        description: 'Cryptocurrency symbol',
        type: 'string',
        category: 'asset',
        aliases: ['coin', 'token', 'crypto', 'currency'],
        common_queries: ['Show all BTC transactions', 'Filter by ETH'],
        aggregatable: false,
        filterable: true
      },
      {
        column: 'amount',
        description: 'Transaction amount',
        type: 'number',
        category: 'financial',
        aliases: ['quantity', 'qty', 'volume', 'size'],
        common_queries: ['Show transactions over 1 BTC', 'Total volume by asset'],
        aggregatable: true,
        filterable: true
      },
      {
        column: 'action',
        description: 'Transaction type',
        type: 'string',
        category: 'classification',
        aliases: ['type', 'txn type', 'transaction type'],
        common_queries: ['Show all buys', 'Filter by sell transactions'],
        aggregatable: false,
        filterable: true
      }
    ];

    // Initialize field metadata
    coreFields.forEach((field: FieldMetadata) => {
      this.fieldMetadata.set(field.column, field);
      this.columnToMetadata.set(field.column, field);
      
      // Map aliases to column names
      field.aliases.forEach(alias => {
        this.aliasToColumn.set(alias.toLowerCase(), field.column);
      });
    });
  }

  // ========================================================================
  // MAIN PARSING ENTRY POINT
  // ========================================================================
  parseQuery(query: string): QueryParseResult {
    if (!query || typeof query !== 'string') {
      throw new Error('Query must be a non-empty string');
    }

    const normalizedQuery = query.toLowerCase().trim();
    const intent = this.determineIntent(normalizedQuery);
    const assets = this.extractAssets(normalizedQuery);
    const timeRange = this.parseTimeRange(normalizedQuery);
    const filters = this.extractFilters(normalizedQuery, assets, timeRange);
    const aggregations = this.extractAggregations(normalizedQuery);
    const groupBy = this.extractGroupBy(normalizedQuery) || [];
    const orderBy = this.extractOrderBy(normalizedQuery) || [];
    const columns = this.determineColumns(intent, assets, filters, aggregations, groupBy);

    const result: QueryParseResult = {
      intent,
      assets: assets || [],
      filters: filters || [],
      aggregations: aggregations || [],
      groupBy,
      orderBy,
      columns,
      metadata: {
        query,
        timestamp: new Date().toISOString(),
      },
    };

    if (timeRange) {
      result.timeRange = timeRange;
    }

    return result;
  }

  // ========================================================================
  // STEP 1: UNDERSTAND - Intent and Component Extraction
  // ========================================================================
  private determineIntent(query: string): QueryParseResult['intent'] {
    if (!query) return 'list';
    
    const lowerQuery = query.toLowerCase();
    
    if (lowerQuery.includes('list') || lowerQuery.includes('show') || lowerQuery.includes('display')) {
      return 'list';
    } else if (lowerQuery.includes('filter') || lowerQuery.includes('where') || lowerQuery.includes('with')) {
      return 'filter';
    } else if (lowerQuery.includes('sum') || lowerQuery.includes('total') || lowerQuery.includes('average')) {
      return 'aggregation';
    } else if (lowerQuery.includes('compare') || /\bvs\b|versus/.test(lowerQuery)) {
      return 'comparison';
    } else if (lowerQuery.includes('trend') || lowerQuery.includes('over time') || /by\s+(month|year|week|day)/.test(lowerQuery)) {
      return 'trend';
    } else if (lowerQuery.includes('balance') || lowerQuery.includes('position')) {
      return 'balance';
    }
    return 'list'; // Default intent
  }

  // ========================================================================
  // TIME RANGE PARSING
  // ========================================================================
  private parseTimeRange(query: string): TimeRange | undefined {
    if (!query) return undefined;
    
    try {
      // Parse relative time expressions (e.g., "last 7 days")
      const relativeMatch = query.match(DATE_PATTERNS.RELATIVE);
      if (relativeMatch) {
        const [_, direction = 'last', amount = '1', unit = 'day'] = relativeMatch;
        const result = this.parseRelativeTimeRange(direction, amount, unit);
        if (result) return result;
      }

      // Parse absolute date ranges (e.g., "from Jan 1 to Jan 31")
      const rangeMatch = query.match(DATE_PATTERNS.RANGE);
      if (rangeMatch) {
        const [_, _from, start, _to, end] = rangeMatch;
        const startDate = this.parseDateString(start);
        const endDate = this.parseDateString(end);
        
        if (startDate && endDate) {
          return {
            type: 'absolute' as const,
            value: `${start} to ${end}`,
            startDate,
            endDate
          };
        }
      }

      // Parse single date
      const dateMatch = query.match(DATE_PATTERNS.ABSOLUTE);
      if (dateMatch) {
        const dateStr = dateMatch[0];
        const date = this.parseDateString(dateStr);
        
        if (date) {
          return {
            type: 'absolute' as const,
            value: dateStr,
            startDate: date,
            endDate: date
          };
        }
      }

      // Default to last 30 days if no specific range is found
      return this.parseRelativeTimeRange('last', '30', 'days');
    } catch (error) {
      console.error('Error parsing time range:', error);
      // Fallback to last 30 days on error
      return this.parseRelativeTimeRange('last', '30', 'days');
    }
  }


  private extractWallets(query: string): string[] {
    const walletPatterns = [
      // Ethereum addresses (0x...)
      /0x[a-fA-F0-9]{40}/g,
      // Bitcoin addresses (legacy, segwit, taproot)
      /[13][a-km-zA-HJ-NP-Z1-9]{25,34}/g,
      /bc1[ac-hj-np-z02-9]{11,71}/g,
      // Solana addresses
      /[1-9A-HJ-NP-Za-km-z]{32,44}/g,
      // Cardano addresses (starts with addr1, addr_test1, stake1, stake_test1)
      /(addr1|addr_test1|stake1|stake_test1)[a-z0-9]+/g,
      // XRP addresses (starts with r)
      /r[1-9A-HJ-NP-Za-km-z]{24,34}/g
    ];

    const wallets = new Set<string>();
    
    for (const pattern of walletPatterns) {
      const matches = query.match(pattern) || [];
      matches.forEach(wallet => wallets.add(wallet));
    }

    return Array.from(wallets);
  }

  private parseRelativeTimeRange(direction: string, amount: string, unit: string): TimeRange | undefined {
    const numAmount = parseInt(amount, 10);
    if (isNaN(numAmount)) return undefined;
    
    const unitLower = unit.toLowerCase();
    const now = new Date();
    const startDate = new Date(now);
    
    // Helper function to format date as YYYY-MM-DD
    const formatDate = (date: Date): string => {
      return date.toISOString().split('T')[0] || '';
    };
    
    if (direction.toLowerCase() === 'last') {
      let start: Date;
      
      switch (unitLower) {
        case 'day':
        case 'days':
          start = new Date(now);
          start.setDate(now.getDate() - numAmount);
          break;
        case 'week':
        case 'weeks':
          start = new Date(now);
          start.setDate(now.getDate() - (numAmount * 7));
          break;
        case 'month':
        case 'months':
          start = new Date(now);
          start.setMonth(now.getMonth() - numAmount);
          break;
        case 'quarter':
        case 'quarters':
          start = new Date(now);
          start.setMonth(now.getMonth() - (numAmount * 3));
          break;
        case 'year':
        case 'years':
          start = new Date(now);
          start.setFullYear(now.getFullYear() - numAmount);
          break;
        default:
          return undefined;
      }
      
      const startDateStr = formatDate(start);
      const endDateStr = formatDate(now);
      
      if (!startDateStr || !endDateStr) return undefined;
      
      return {
        type: 'relative',
        value: `last ${amount} ${unit}`,
        startDate: startDateStr,
        endDate: endDateStr
      };
    } else if (direction.toLowerCase() === 'next') {
      let end: Date = new Date(now);
      
      switch (unitLower) {
        case 'day':
        case 'days':
          end.setDate(now.getDate() + numAmount);
          break;
        case 'week':
        case 'weeks':
          end.setDate(now.getDate() + (numAmount * 7));
          break;
        case 'month':
        case 'months':
          end.setMonth(now.getMonth() + numAmount);
          break;
        case 'quarter':
        case 'quarters':
          end.setMonth(now.getMonth() + (numAmount * 3));
          break;
        case 'year':
        case 'years':
          end.setFullYear(now.getFullYear() + numAmount);
          break;
        default:
          return undefined;
      }
      
      const startDateStr = formatDate(now);
      const endDateStr = formatDate(end);
      
      if (!startDateStr || !endDateStr) return undefined;
      
      return {
        type: 'relative',
        value: `next ${amount} ${unit}`,
        startDate: startDateStr,
        endDate: endDateStr
      };
    }
    
    return undefined;
  }

  private parseDateString(dateStr: string | undefined): string {
    if (!dateStr) return new Date().toISOString().split('T')[0] || '';
    
    try {
      const date = new Date(dateStr);
      return isNaN(date.getTime()) ? new Date().toISOString().split('T')[0] || '' : date.toISOString().split('T')[0] || '';
    } catch (e) {
      return new Date().toISOString().split('T')[0] || '';
    }
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

  /**
   * Determines which columns to include in the SELECT statement based on query context
   * @param intent The query intent (list, filter, aggregation, etc.)
   * @param assets Array of assets mentioned in the query
   * @param filters Array of filter conditions
   * @param aggregations Array of aggregation functions
   * @param groupBy Array of group by clauses
   * @returns Array of column mappings to include in the result
   */
  private determineColumns(
    intent: QueryParseResult['intent'],
    assets: string[],
    filters: FilterCondition[] = [],
    aggregations: Aggregation[] = [],
    groupBy: GroupByClause[] = []
  ): ColumnMapping[] {
    const columns: ColumnMapping[] = [];
    
    // Always include timestamp and asset columns for context
    const baseColumns: ColumnMapping[] = [
      {
        name: 'timestamp',
        type: 'timestamp',
        displayName: 'Timestamp',
        description: 'Transaction timestamp'
      },
      {
        name: 'asset',
        type: 'string',
        displayName: 'Asset',
        description: 'Cryptocurrency symbol'
      }
    ];

    // Add base columns if not already present
    baseColumns.forEach(col => {
      if (!columns.some(c => c.name === col.name)) {
        columns.push(col);
      }
    });

    // Add columns used in filters
    filters.forEach(filter => {
      if (!columns.some(c => c.name === filter.column)) {
        const metadata = this.getFieldMetadata(filter.column);
        if (metadata) {
          columns.push({
            name: filter.column,
            type: metadata.type as any,
            displayName: metadata.description,
            description: `Filtered by: ${filter.operator} ${filter.value}`
          });
        }
      }
    });

    // Add columns used in aggregations
    aggregations.forEach(agg => {
      if (!columns.some(c => c.name === agg.alias || c.name === agg.column)) {
        const metadata = this.getFieldMetadata(agg.column);
        columns.push({
          name: agg.alias || `${agg.function}_${agg.column}`,
          type: 'number', // Aggregations always return numbers
          displayName: `${agg.function.toUpperCase()}(${metadata?.description || agg.column})`,
          description: `Aggregated ${metadata?.description || agg.column}`,
          isAggregation: true
        });
      }
    });

    // Add columns used in GROUP BY
    groupBy.forEach(gb => {
      if (!columns.some(c => c.name === gb.column)) {
        const metadata = this.getFieldMetadata(gb.column);
        columns.push({
          name: gb.column,
          type: metadata?.type as any || 'string',
          displayName: metadata?.description || gb.column,
          description: `Grouped by ${gb.column}`,
          isGroupBy: true
        });
      }
    });

    // Add intent-specific columns
    switch (intent) {
      case 'list':
        // For list queries, include action and amount by default
        if (!columns.some(c => c.name === 'action')) {
          columns.push({
            name: 'action',
            type: 'string',
            displayName: 'Action',
            description: 'Transaction type'
          });
        }
        if (!columns.some(c => c.name === 'amount')) {
          columns.push({
            name: 'amount',
            type: 'number',
            displayName: 'Amount',
            description: 'Transaction amount'
          });
        }
        break;
        
      case 'aggregation':
        // For aggregation queries, ensure we have at least one aggregation
        if (aggregations.length === 0) {
          // Default to count if no aggregations specified
          columns.push({
            name: 'count',
            type: 'number',
            displayName: 'Count',
            description: 'Number of records',
            isAggregation: true
          });
        }
        break;
        
      case 'balance':
        // For balance queries, include balance-related fields
        const balanceFields = ['assetBalance', 'costBasis', 'carryingValue'];
        balanceFields.forEach(field => {
          if (!columns.some(c => c.name === field)) {
            const metadata = this.getFieldMetadata(field);
            if (metadata) {
              columns.push({
                name: field,
                type: metadata.type as any,
                displayName: metadata.description,
                description: metadata.description
              });
            }
          }
        });
        break;
    }

    // If we have assets filtered but no asset column, add it
    if (assets.length > 0 && !columns.some(c => c.name === 'asset')) {
      columns.push({
        name: 'asset',
        type: 'string',
        displayName: 'Asset',
        description: 'Cryptocurrency symbol'
      });
    }

    // Ensure all columns have required properties
    return columns.map(col => ({
      ...col,
      displayName: col.displayName || col.name,
      description: col.description || `Column: ${col.name}`,
      type: col.type || 'string'
    }));
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
        name: 'gain_loss',
        type: 'number',
        displayName: 'Gain/Loss',
        description: 'Capital gains and losses (short-term, long-term, undated)',
        userTerm: 'gain/loss',
        mappedColumns: ['shortTermGainLoss', 'longTermGainLoss', 'undatedGainLoss'],
        confirmed: false
      });
    }

    // Cost basis mapping
    if (query.includes('cost basis') || query.includes('cost')) {
      mappings.push({
        name: 'cost_basis',
        type: 'number',
        displayName: 'Cost Basis',
        description: 'Cost basis acquired and disposed',
        userTerm: 'cost basis',
        mappedColumns: ['costBasisAcquired', 'costBasisRelieved'],
        confirmed: false
      });
    }

    // Value mapping
    if (query.includes('value') || query.includes('worth')) {
      mappings.push({
        name: 'value',
        type: 'number',
        displayName: 'Value',
        description: 'Carrying value and fair market value',
        userTerm: 'value',
        mappedColumns: ['carryingValue', 'fairMarketValueDisposed'],
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
            name: 'financial_totals',
            type: 'number',
            displayName: 'Financial Totals',
            description: 'Default financial aggregation columns',
            userTerm: 'financial totals',
            mappedColumns: ['shortTermGainLoss', 'longTermGainLoss', 'undatedGainLoss'],
            confirmed: false
          }
        ];
      
      case 'filter':
        return [
          {
            name: 'transaction_data',
            type: 'string',
            displayName: 'Transaction Data',
            description: 'Basic transaction filtering columns',
            userTerm: 'transaction data',
            mappedColumns: ['asset', 'action', 'wallet', 'timestamp'],
            confirmed: false
          }
        ];
      
      default:
        return [
          {
            name: 'general_analysis',
            type: 'string',
            displayName: 'General Analysis',
            description: 'General purpose analysis columns',
            userTerm: 'general analysis',
            mappedColumns: ['asset', 'assetUnitAdj', 'carryingValue'],
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