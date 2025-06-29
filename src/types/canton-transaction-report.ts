/**
 * Core data interfaces for Canton Transaction Reports
 * Based on the Canton Transaction schema structure
 */

import { ConnectionDetails } from './session-types.js';
import { BigQueryConfig, DataSource } from './actions-report.js';

// ============================================================================
// CANTON TRANSACTION - Core Data Structure
// ============================================================================

export interface CantonTransactionRecord {
  // Transaction identifiers
  parenttransactionId: string;
  linetransactionId?: string;
  
  // Timestamps
  dateTime: string;
  dateTimeSEC?: number;
  
  // Wallet information
  walletId: string;
  walletName?: string;
  
  // Operation details
  operation: string;
  
  // Asset information
  assetTicker: string;
  assetbitwaveId?: string;
  assetAmount: number;
  
  // Value information
  exchangeRate?: number;
  exchangeRateSource?: string;
  assetvalueInBaseCurrency?: number;
  
  // Fee information
  feeAmount?: number;
  feeAsset?: string;
  feeType?: string;
  
  // Reward information
  rewardType?: string;
  rewardFeeType?: string;
  
  // Address information
  fromAddress?: string;
  toAddress?: string;
}

// ============================================================================
// MONTHLY ACTIVITY REPORT - Aggregated Data Structure
// ============================================================================

export interface MonthlyActivityRecord {
  year_month: string;
  operation: string;
  assetTicker: string;
  fromAddress?: string;
  toAddress?: string;
  feeType?: string;
  rewardFeeType?: string;
  rewardType?: string;
  totalAssetAmount: number;
  totaltxncount: number;
}

// ============================================================================
// REPORT PARAMETERS - Common Parameter Types
// ============================================================================

export interface ReportParameters {
  walletId?: string;
  startDate?: string;
  endDate?: string;
  limit?: number;
  connectionDetails?: ConnectionDetails;
}

// ============================================================================
// QUERY & RESPONSE TYPES
// ============================================================================

export interface QueryRequest {
  query: string;
  reportType?: 'canton_transaction' | 'monthly_activity';
  filters?: Record<string, any>;
  parameters?: ReportParameters;
}

export interface ColumnMapping {
  name: string;
  type: 'string' | 'number' | 'boolean' | 'timestamp' | 'date';
  displayName: string;
  description?: string;
  isAggregation?: boolean;
  isGroupBy?: boolean;
  userTerm?: string;
  mappedColumns?: string[];
  confirmed?: boolean;
}

export type TimeRange = {
  type: 'relative' | 'absolute';
  value: string;
  startDate?: string;
  endDate?: string;
  [key: string]: string; // Allow additional string properties
};

export type FilterOperator = 
  | '=' | '!=' | '<>' 
  | '>' | '<' | '>=' | '<=' 
  | 'IN' | 'NOT IN' 
  | 'LIKE' | 'NOT LIKE' 
  | 'BETWEEN' 
  | 'IS NULL' | 'IS NOT NULL'
  | 'IS DISTINCT FROM' | 'IS NOT DISTINCT FROM';

export type FilterCondition = {
  column: string;
  operator: FilterOperator;
  value: any;
  logicalOperator?: 'AND' | 'OR';
  not?: boolean;
};

export type Aggregation = {
  column: string;
  function: 'sum' | 'count' | 'avg' | 'min' | 'max';
  alias?: string;
  distinct?: boolean;
};

export type GroupByClause = {
  column: string;
  order?: 'ASC' | 'DESC';
  interval?: 'day' | 'week' | 'month' | 'year' | 'quarter';
};

export type OrderByClause = {
  column: string;
  direction: 'ASC' | 'DESC';
  nulls?: 'FIRST' | 'LAST';
};

export interface QueryMetadata {
  query: string;
  timestamp: string;
  isDistinct?: boolean;
  isCount?: boolean;
  hasSubquery?: boolean;
}

export interface QueryParseResult {
  intent: 'list' | 'filter' | 'aggregation' | 'comparison' | 'trend' | 'balance';
  assets: string[];
  timeRange?: TimeRange;
  filters: FilterCondition[];
  aggregations: Aggregation[];
  groupBy: GroupByClause[];
  orderBy: OrderByClause[];
  columns: ColumnMapping[];
  metadata: QueryMetadata;
  aggregationType?: 'sum' | 'count' | 'avg' | 'min' | 'max';
  limit?: number;
}

export interface QueryResult {
  success: boolean;
  data?: any;
  summary?: string;
  error?: {
    type: 'MAPPING_ERROR' | 'DATA_ERROR' | 'COMPUTATION_ERROR' | 'VALIDATION_ERROR';
    message: string;
    suggestions?: string[];
  };
  metadata: {
    rows_processed: number;
    execution_time_ms: number;
    cached: boolean;
    columns_used: string[];
  };
}

// ============================================================================
// FIELD METADATA - For Natural Language Query Support
// ============================================================================

export interface FieldMetadata {
  column: string;
  description: string;
  type: 'string' | 'number' | 'date' | 'boolean' | 'timestamp';
  category: 'identifier' | 'temporal' | 'financial' | 'operational' | 'address' | 'fee' | 'reward' | 'asset' | 'classification';
  aliases: string[];
  common_queries: string[];
  aggregatable: boolean;
  filterable: boolean;
}

// Canton Transaction Report Field Metadata
export const CANTON_TRANSACTION_METADATA: FieldMetadata[] = [
  // Transaction identifiers
  {
    column: 'parenttransactionId',
    description: 'Parent transaction identifier',
    type: 'string',
    category: 'identifier',
    aliases: ['transaction', 'txn', 'parent transaction'],
    common_queries: ['transaction details', 'specific transaction'],
    aggregatable: false,
    filterable: true
  },
  {
    column: 'linetransactionId',
    description: 'Line item transaction identifier',
    type: 'string',
    category: 'identifier',
    aliases: ['line transaction', 'line item', 'sub transaction'],
    common_queries: ['line item details', 'transaction components'],
    aggregatable: false,
    filterable: true
  },
  
  // Temporal
  {
    column: 'dateTime',
    description: 'Human-readable timestamp of transaction',
    type: 'string',
    category: 'temporal',
    aliases: ['date', 'time', 'transaction date', 'when'],
    common_queries: ['date range', 'period analysis', 'time series'],
    aggregatable: false,
    filterable: true
  },
  {
    column: 'dateTimeSEC',
    description: 'Unix timestamp of transaction',
    type: 'number',
    category: 'temporal',
    aliases: ['timestamp', 'unix time', 'epoch'],
    common_queries: ['date range', 'period analysis'],
    aggregatable: false,
    filterable: true
  },
  
  // Wallet information
  {
    column: 'walletId',
    description: 'Wallet identifier',
    type: 'string',
    category: 'identifier',
    aliases: ['wallet', 'account', 'wallet id'],
    common_queries: ['wallet analysis', 'account breakdown'],
    aggregatable: false,
    filterable: true
  },
  {
    column: 'walletName',
    description: 'Human-readable wallet name',
    type: 'string',
    category: 'identifier',
    aliases: ['wallet name', 'account name'],
    common_queries: ['wallet analysis', 'account breakdown'],
    aggregatable: false,
    filterable: true
  },
  
  // Operation details
  {
    column: 'operation',
    description: 'Type of operation (buy, sell, transfer, etc.)',
    type: 'string',
    category: 'operational',
    aliases: ['transaction type', 'action', 'activity type'],
    common_queries: ['buys', 'sells', 'transfers', 'staking rewards'],
    aggregatable: true,
    filterable: true
  },
  
  // Asset information
  {
    column: 'assetTicker',
    description: 'Asset symbol/ticker (BTC, ETH, etc.)',
    type: 'string',
    category: 'asset',
    aliases: ['asset', 'coin', 'token', 'cryptocurrency', 'ticker'],
    common_queries: ['BTC analysis', 'asset breakdown', 'token performance'],
    aggregatable: true,
    filterable: true
  },
  {
    column: 'assetbitwaveId',
    description: 'Bitwave identifier for the asset',
    type: 'string',
    category: 'identifier',
    aliases: ['asset id', 'bitwave id'],
    common_queries: ['asset lookup', 'asset details'],
    aggregatable: false,
    filterable: true
  },
  {
    column: 'assetAmount',
    description: 'Quantity of the asset in the transaction',
    type: 'number',
    category: 'financial',
    aliases: ['amount', 'quantity', 'volume', 'units'],
    common_queries: ['transaction volume', 'asset quantities'],
    aggregatable: true,
    filterable: true
  },
  
  // Value information
  {
    column: 'exchangeRate',
    description: 'Exchange rate used for the transaction',
    type: 'number',
    category: 'financial',
    aliases: ['rate', 'price', 'conversion rate'],
    common_queries: ['price analysis', 'rate comparison'],
    aggregatable: true,
    filterable: true
  },
  {
    column: 'exchangeRateSource',
    description: 'Source of the exchange rate data',
    type: 'string',
    category: 'financial',
    aliases: ['rate source', 'price source', 'data provider'],
    common_queries: ['rate sources', 'price providers'],
    aggregatable: false,
    filterable: true
  },
  {
    column: 'assetvalueInBaseCurrency',
    description: 'Value of the asset in base currency',
    type: 'number',
    category: 'financial',
    aliases: ['value', 'fiat value', 'usd value', 'base currency value'],
    common_queries: ['transaction value', 'portfolio value'],
    aggregatable: true,
    filterable: true
  },
  
  // Fee information
  {
    column: 'feeAmount',
    description: 'Amount of fee paid for the transaction',
    type: 'number',
    category: 'fee',
    aliases: ['fee', 'gas', 'transaction fee'],
    common_queries: ['fee analysis', 'gas costs', 'transaction expenses'],
    aggregatable: true,
    filterable: true
  },
  {
    column: 'feeAsset',
    description: 'Asset used to pay the fee',
    type: 'string',
    category: 'fee',
    aliases: ['fee currency', 'gas token'],
    common_queries: ['fee assets', 'gas token analysis'],
    aggregatable: false,
    filterable: true
  },
  {
    column: 'feeType',
    description: 'Type of fee (gas, trading, etc.)',
    type: 'string',
    category: 'fee',
    aliases: ['fee category', 'fee classification'],
    common_queries: ['fee types', 'fee breakdown'],
    aggregatable: true,
    filterable: true
  },
  
  // Reward information
  {
    column: 'rewardType',
    description: 'Type of reward (staking, mining, etc.)',
    type: 'string',
    category: 'reward',
    aliases: ['reward category', 'earning type'],
    common_queries: ['reward analysis', 'earning types'],
    aggregatable: true,
    filterable: true
  },
  {
    column: 'rewardFeeType',
    description: 'Type of fee associated with rewards',
    type: 'string',
    category: 'reward',
    aliases: ['reward fee', 'staking fee'],
    common_queries: ['reward fee analysis', 'staking costs'],
    aggregatable: true,
    filterable: true
  },
  
  // Address information
  {
    column: 'fromAddress',
    description: 'Source blockchain address',
    type: 'string',
    category: 'address',
    aliases: ['source', 'sender', 'from'],
    common_queries: ['address analysis', 'transaction sources'],
    aggregatable: true,
    filterable: true
  },
  {
    column: 'toAddress',
    description: 'Destination blockchain address',
    type: 'string',
    category: 'address',
    aliases: ['destination', 'recipient', 'to'],
    common_queries: ['address analysis', 'transaction destinations'],
    aggregatable: true,
    filterable: true
  }
];

// Monthly Activity Report Field Metadata
export const MONTHLY_ACTIVITY_METADATA: FieldMetadata[] = [
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
    category: 'asset',
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
  }
];

// ============================================================================
// BIGQUERY & DATA CONNECTION TYPES
// ============================================================================

// These are imported from actions-report.js
