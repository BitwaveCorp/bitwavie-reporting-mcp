/**
 * Core data interfaces for the Actions Report and derivative reports
 * Based on the 64-column Actions Report structure
 */

import { ConnectionDetails } from './session-types.js';

// ============================================================================
// ACTIONS REPORT - Core Data Structure (64 columns)
// ============================================================================

export interface ActionRecord {
  // Identifiers
  orgId: string;
  runId: string;
  txnId: string;
  eventId: string;
  lotId?: string;
  
  // Timestamps
  timestampSEC: number;
  timestamp: string;
  lotAcquisitionTimestampSEC?: number;
  
  // Actions & Status
  action: string;
  status: string;
  
  // Assets
  asset: string;
  assetId: string;
  assetUnitAdj?: number;
  assetBalance?: number;
  
  // Financial Data - Cost Basis
  costBasisAcquired?: number;
  costBasisRelieved?: number;
  originalCostBasisDisposed?: number;
  
  // Financial Data - Gains/Losses
  shortTermGainLoss?: number;
  longTermGainLoss?: number;
  undatedGainLoss?: number;
  
  // Financial Data - Valuations
  fairMarketValueDisposed?: number;
  carryingValue?: number;
  txnExchangeRate?: number;
  
  // Financial Data - Adjustments & Impairments
  impairmentExpense?: number;
  impairmentReversal?: number;
  impairmentExpenseDisposed?: number;
  fairValueAdjustmentUpward?: number;
  fairValueAdjustmentDownward?: number;
  revaluationAdjustmentUpward?: number;
  revaluationAdjustmentDownward?: number;
  
  // Categorization
  category?: string;
  categoryCode?: string;
  categoryType?: string;
  inventory?: string;
  subsidiaryId?: string;
  
  // Wallets & Transfers
  wallet?: string;
  toWallet?: string;
  fromWallet?: string;
  isInternalTransfer?: boolean;
  transferLotId?: string;
  originalLotId?: string;
  
  // Other Fields
  description?: string;
  metadata?: string;
  baseCurrency?: string;
  isFee?: boolean;
  isTrade?: boolean;
}

// ============================================================================
// DERIVATIVE REPORTS - Data Structures
// ============================================================================

// 1. LOTS REPORT
export interface LotsReportRecord {
  lotId: string;
  txnId?: string;
  asset: string;
  assetId: string;
  timestampSEC: number;
  unitsAcquired: number;
  unitsDisposed: number;
  qty: number;
  costBasisAcquired: number;
  costBasisRelieved: number;
  costBasis: number;
  impairmentExpense: number;
  impairmentReversal: number;
  revaluationAdjustmentUpward: number;
  revaluationAdjustmentDownward: number;
  carryingValue: number;
  adjustedToValue: number;
}

// 2. VALUATION ROLLFORWARD REPORT
export interface ValuationRollforwardRecord {
  asset: string;
  original_subsidiary?: string;
  original_inventory?: string;
  original_wallet?: string;
  
  // Cost Basis Movement
  starting_cost_basis: number;
  cost_basis_acquired: number;
  cost_basis_disposed: number;
  ending_cost_basis: number;
  
  // Impairment Movement
  starting_impairment_in_inventory: number;
  impairment_expense: number;
  impairment_disposed: number;
  impairment_reversal: number;
  ending_impairment_in_inventory: number;
  
  // Carrying Value
  ending_carrying_value: number;
  
  // Unrealized Adjustments
  starting_unrealized: number;
  gaap_fair_value_adjust_up: number;
  gaap_fair_value_adjust_down: number;
  IFRS_revaluation_adjust_up: number;
  IFRS_revaluation_adjust_down: number;
  ending_unrealized: number;
  
  // Market Value
  ending_market_value: number;
  
  // Period Realized Gains/Losses
  period_shortterm_gainloss: number;
  period_longterm_gainloss: number;
  period_undated_gainloss: number;
}

// 3. INVENTORY BALANCE REPORT
export interface InventoryBalanceRecord {
  asset: string;
  assetId: string;
  inventory: string;
  subsidiaryId?: string;
  qty: number;
  costBasisAcquired: number;
  costBasisRelieved: number;
  costBasis: number;
  impairmentExpense: number;
  impairmentExpenseReversal: number;
  fairValueAdjustmentUpward: number;
  fairValueAdjustmentDownward: number;
  revaluationAdjustmentUpward: number;
  revaluationAdjustmentDownward: number;
  impairmentExpenseDisposed: number;
  carryingValue: number;
}

// ============================================================================
// QUERY & RESPONSE TYPES
// ============================================================================

export interface ReportParameters {
  asOfDate?: string;
  asOfSEC?: string;
  startDate?: string;
  endDate?: string;
  runId?: string;
  orgId?: string;
  limit?: number; // Maximum number of rows to return (default: 5000)
  connectionDetails?: ConnectionDetails;
}

export interface QueryRequest {
  query: string;
  reportType?: 'actions' | 'lots' | 'rollforward' | 'inventory';
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
// FIELD METADATA - Data Dictionary for Query Translation
// ============================================================================

export interface FieldMetadata {
  column: string;
  description: string;
  type: 'string' | 'number' | 'boolean' | 'timestamp';
  category: 'identifier' | 'financial' | 'asset' | 'temporal' | 'classification';
  aliases: string[];
  common_queries: string[];
  aggregatable: boolean;
  filterable: boolean;
}

export const ACTIONS_REPORT_METADATA: FieldMetadata[] = [
  // Identifiers
  {
    column: 'orgId',
    description: 'Organization identifier',
    type: 'string',
    category: 'identifier',
    aliases: ['org', 'organization'],
    common_queries: ['company data', 'org breakdown'],
    aggregatable: false,
    filterable: true
  },
  {
    column: 'runId',
    description: 'Calculation run identifier',
    type: 'string',
    category: 'identifier',
    aliases: ['run', 'calculation run'],
    common_queries: ['run comparison', 'latest run'],
    aggregatable: false,
    filterable: true
  },
  {
    column: 'txnId',
    description: 'Transaction identifier',
    type: 'string',
    category: 'identifier',
    aliases: ['transaction', 'txn'],
    common_queries: ['transaction details', 'specific transaction'],
    aggregatable: false,
    filterable: true
  },
  {
    column: 'lotId',
    description: 'Lot identifier for FIFO tracking',
    type: 'string',
    category: 'identifier',
    aliases: ['lot', 'batch'],
    common_queries: ['lot performance', 'specific lot'],
    aggregatable: false,
    filterable: true
  },
  
  // Temporal
  {
    column: 'timestampSEC',
    description: 'Unix timestamp of transaction',
    type: 'number',
    category: 'temporal',
    aliases: ['time', 'date', 'when'],
    common_queries: ['date range', 'period analysis', 'time series'],
    aggregatable: false,
    filterable: true
  },
  {
    column: 'timestamp',
    description: 'Human-readable timestamp',
    type: 'string',
    category: 'temporal',
    aliases: ['datetime', 'transaction date'],
    common_queries: ['date range', 'period analysis'],
    aggregatable: false,
    filterable: true
  },
  
  // Assets
  {
    column: 'asset',
    description: 'Asset symbol/ticker (BTC, ETH, etc.)',
    type: 'string',
    category: 'asset',
    aliases: ['coin', 'token', 'cryptocurrency', 'symbol', 'ticker'],
    common_queries: ['BTC analysis', 'asset breakdown', 'token performance'],
    aggregatable: false,
    filterable: true
  },
  {
    column: 'assetUnitAdj',
    description: 'Quantity change (positive for buys, negative for sells)',
    type: 'number',
    category: 'asset',
    aliases: ['quantity', 'amount', 'units', 'qty'],
    common_queries: ['total bought', 'volume analysis', 'quantity held'],
    aggregatable: true,
    filterable: true
  },
  
  // Financial - Gains/Losses
  {
    column: 'shortTermGainLoss',
    description: 'Short-term capital gains/losses (≤1 year)',
    type: 'number',
    category: 'financial',
    aliases: ['short term gain', 'short term loss', 'stcg', 'short term'],
    common_queries: ['short term gains', 'tax analysis', 'capital gains'],
    aggregatable: true,
    filterable: true
  },
  {
    column: 'longTermGainLoss',
    description: 'Long-term capital gains/losses (>1 year)',
    type: 'number',
    category: 'financial',
    aliases: ['long term gain', 'long term loss', 'ltcg', 'long term'],
    common_queries: ['long term gains', 'tax analysis', 'capital gains'],
    aggregatable: true,
    filterable: true
  },
  {
    column: 'undatedGainLoss',
    description: 'Gains/losses without term classification',
    type: 'number',
    category: 'financial',
    aliases: ['undated gain', 'unclassified gain', 'other gains'],
    common_queries: ['total gains', 'unclassified gains'],
    aggregatable: true,
    filterable: true
  },
  
  // Financial - Cost Basis
  {
    column: 'costBasisAcquired',
    description: 'USD cost basis of assets acquired',
    type: 'number',
    category: 'financial',
    aliases: ['cost basis', 'purchase price', 'acquisition cost'],
    common_queries: ['cost basis', 'total invested', 'purchase analysis'],
    aggregatable: true,
    filterable: true
  },
  {
    column: 'costBasisRelieved',
    description: 'USD cost basis of assets disposed',
    type: 'number',
    category: 'financial',
    aliases: ['cost basis sold', 'disposed cost', 'sale cost basis'],
    common_queries: ['cost basis movement', 'disposal analysis'],
    aggregatable: true,
    filterable: true
  },
  
  // Financial - Valuations
  {
    column: 'carryingValue',
    description: 'Current book value (cost basis adjusted for impairments)',
    type: 'number',
    category: 'financial',
    aliases: ['book value', 'carrying amount', 'net value'],
    common_queries: ['portfolio value', 'book value', 'net worth'],
    aggregatable: true,
    filterable: true
  },
  {
    column: 'fairMarketValueDisposed',
    description: 'Fair market value of disposed assets',
    type: 'number',
    category: 'financial',
    aliases: ['fmv disposed', 'sale proceeds', 'disposal value'],
    common_queries: ['sale proceeds', 'disposal analysis'],
    aggregatable: true,
    filterable: true
  },
  
  // Classification
  {
    column: 'wallet',
    description: 'Wallet or account identifier',
    type: 'string',
    category: 'classification',
    aliases: ['account', 'address', 'exchange'],
    common_queries: ['wallet breakdown', 'account analysis', 'exchange comparison'],
    aggregatable: false,
    filterable: true
  },
  {
    column: 'category',
    description: 'Transaction category classification',
    type: 'string',
    category: 'classification',
    aliases: ['type', 'classification', 'category'],
    common_queries: ['category breakdown', 'transaction types'],
    aggregatable: false,
    filterable: true
  },
  {
    column: 'action',
    description: 'Transaction action (buy, sell, transfer, etc.)',
    type: 'string',
    category: 'classification',
    aliases: ['operation', 'transaction type', 'activity'],
    common_queries: ['buy analysis', 'sell analysis', 'transfer tracking'],
    aggregatable: false,
    filterable: true
  }
];

// ============================================================================
// BIGQUERY & DATA CONNECTION TYPES
// ============================================================================

export interface BigQueryConfig {
  projectId: string;
  datasetId: string;
  tableId: string;
  keyFilename?: string;
  credentials?: any;
}

export interface DataSource {
  type: 'bigquery' | 'csv';
  config: BigQueryConfig | { filePath: string };
}

// ReportParameters interface is now defined above