/**
 * Schema Type Registry Service
 * 
 * Central registry for schema type definitions in the system.
 * Provides metadata about different schema types, their compatible reports,
 * column mappings, semantic rules, and query patterns.
 */

import { logFlow } from '../utils/logging.js';

// Types
export interface ColumnMapping {
  domainConcept: string;      // Business/domain concept (e.g., "realized gain")
  columnNames: string[];      // Actual column names in the schema
  aliases: string[];          // Alternative terms users might use
  description: string;        // Description of this mapping
}

export interface SemanticRule {
  ruleName: string;           // Unique identifier for the rule
  description: string;        // Human-readable description
  validationExpression: string; // SQL-like validation expression
  ruleType: 'testing' | 'information' | 'validation'; // Type of rule
}

export interface QueryAspect {
  aspectType: 'aggregation' | 'filter' | 'inclusion' | 'exclusion' | 'grouping' | 'sorting' | 'time_series';
  columnNames: string[];      // Columns typically used for this aspect
  patterns: string[];         // Common query patterns
  description?: string;       // Description of this query aspect
  exampleQuery?: string;      // Example natural language query
  exampleSql?: string;        // Example SQL for this query aspect
}

export interface SchemaLearning {
  concept: string;            // The concept being learned
  mappings: string[];         // Column mappings discovered
  examples: string[];         // Example queries
  frequency: number;          // How often this concept appears
  lastUpdated: Date;          // When this learning was last updated
}

export interface SchemaTypeDefinition {
  id: string;                 // Unique identifier (e.g., "actions", "transactions")
  name: string;               // Display name (e.g., "GL Actions Schema")
  description: string;        // Description of the schema type
  compatibleReports: string[]; // IDs of reports compatible with this schema
  minimumRequiredColumns: string[]; // List of column names that must be present in this schema type
  otherIncludedColumns: string[]; // List of additional column names that may be present
  columnMappings: ColumnMapping[]; // Domain-specific column mappings
  semanticRules: SemanticRule[]; // Business/semantic rules for this schema
  queryAspects: QueryAspect[]; // Common query patterns by aspect type
  learnings: SchemaLearning[]; // Accumulated learnings from user interactions
  
  // New fields for enhanced LLM schema support
  columnDescriptions?: Record<string, string>;  // Column name -> description mapping
  columnCategories?: Record<string, string[]>;  // Category name -> column names mapping
  simpleSemanticRules?: string[];  // Simplified semantic rules as strings for LLM context
  simpleLearnings?: string[];      // Simplified learnings as strings for LLM context
  exampleQueries?: Array<{
    description: string;      // Description of what this query demonstrates
    query: string;           // Natural language query example
    sql: string;             // Corresponding SQL query
  }>;
}

/**
 * Schema Type Registry Service
 * 
 * Manages the registration and retrieval of schema type definitions.
 */
export class SchemaTypeRegistry {
  private schemaTypes: Map<string, SchemaTypeDefinition> = new Map();
  
  constructor() {
    this.registerBuiltInSchemaTypes();
    logFlow('SCHEMA_TYPE_REGISTRY', 'INFO', 'Schema Type Registry initialized', {
      schemaTypeCount: this.schemaTypes.size,
      schemaTypeIds: Array.from(this.schemaTypes.keys())
    });
  }
  
  /**
   * Register the built-in schema types
   */
  private registerBuiltInSchemaTypes(): void {
    // Register Actions Schema Type
    this.registerSchemaType({
      id: 'actions',
      name: 'GL Actions Schema',
      description: 'General Ledger actions schema for cryptocurrency accounting with gain/loss information',
      compatibleReports: ['inventory-balance', 'valuation-rollforward', 'lots-report'],
      minimumRequiredColumns: [
        'timestamp',
        'action',
        'txnId',
        'lotId',
        'asset',
        'assetUnitAdj',
        'txnExchangeRate',
        'fairMarketValueDisposed',
        'shortTermGainLoss',
        'costBasisAcquired',
        'costBasisRelieved'
      ],
      otherIncludedColumns: [
        'orgId',
        'runId',
        'timestampSEC',
        'status',
        'eventId',
        'assetId',
        'assetBalance',
        'lotAcquisitionTimestampSEC',
        'carryingValue',
        'impairmentExpense',
        'longTermGainLoss',
        'undatedGainLoss',
        'costAverageRate',
        'isTrade',
        'lineError',
        'transferLotId',
        'originalLotId',
        'inventory',
        'fromInventory',
        'toInventory',
        'impairmentReversal'
      ],
      
      // Enhanced metadata for LLM schema support
      columnDescriptions: {
        'timestamp': 'The date and time when the transaction occurred in UTC',
        'timestampSEC': 'Unix timestamp in seconds representing when the transaction occurred',
        'action': 'Type of accounting action (BUY, SELL, TRANSFER, SWAP, etc.)',
        'txnId': 'Unique identifier for the transaction',
        'lotId': 'Identifier for the specific lot involved in the transaction',
        'asset': 'Ticker symbol of the cryptocurrency asset (e.g., BTC, ETH, USDC)',
        'assetId': 'Unique identifier for the cryptocurrency asset',
        'assetUnitAdj': 'Amount of cryptocurrency involved in the transaction',
        'assetBalance': 'Remaining balance of the cryptocurrency after the transaction',
        'txnExchangeRate': 'Exchange rate used for the transaction in USD',
        'fairMarketValueDisposed': 'Fair market value of the asset at time of disposal in USD',
        'shortTermGainLoss': 'Short-term capital gain or loss (held < 1 year) in USD',
        'longTermGainLoss': 'Long-term capital gain or loss (held > 1 year) in USD',
        'undatedGainLoss': 'Gain or loss without a specific holding period in USD',
        'costBasisAcquired': 'Original acquisition cost in USD for assets acquired',
        'costBasisRelieved': 'Cost basis relieved in USD for assets disposed',
        'carryingValue': 'Current carrying value of the asset in USD',
        'impairmentExpense': 'Impairment expense recognized for the asset in USD',
        'costAverageRate': 'Average cost basis per unit of the asset in USD',
        'lotAcquisitionTimestampSEC': 'Unix timestamp when the lot was originally acquired',
        'revaluationAdjustmentUpward': 'Positive revaluation adjustment to reflect an increase in fair value',
        'revaluationAdjustmentDownward': 'Negative revaluation adjustment to reflect a decrease in fair value',
        'fairValueAdjustmentUpward': 'Increase in fair value for financial reporting purposes',
        'fairValueAdjustmentDownward': 'Decrease in fair value for financial reporting purposes',
        'categoryCode': 'Internal or system-level category code for classifying transactions',
        'category': 'User-defined or standardized category for the transaction (e.g., Revenue, Expense, Capital Gain)',
        'isInternalTransfer': 'Boolean flag indicating whether the transaction is an internal wallet transfer',
        'metadata': 'Additional key-value data associated with the transaction',
        'wallet': 'Name or ID of the wallet involved in the transaction',
        'contact': 'Counterparty or related contact entity for the transaction',
        'description': 'Free-text description of the transaction',
        'toWallet': 'Destination wallet for transfer transactions',
        'fromWallet': 'Origin wallet for transfer transactions',
        'baseCurrency': 'The currency in which the asset or transaction is primarily denominated',
        'isFee': 'Boolean flag indicating if the transaction is a fee',
        'categoryType': 'Type of category (e.g., Income, Expense, Transfer)',
        'capitalizeTradingFees': 'Boolean flag indicating if trading fees should be added to the assets cost basis',
        'wrappingAdjustment': 'Adjustment related to wrapping or unwrapping assets (e.g., ETH to WETH)',
        'forceZeroGainLoss': 'Boolean flag to override gain/loss calculation and force it to zero',
        'excludedGainLoss': 'Amount of gain/loss excluded from calculations or reports',
        'runTimestampSEC': 'Unix timestamp when the transaction or report was processed',
        'digitalAssetCategoryId': 'Unique identifier for the digital asset category',
        'digitalAssetCategory': 'Descriptive name of the digital asset category',
        'subsidiaryId': 'Unique identifier for the legal entity or subsidiary',
        'subsidiaryName': 'Name of the legal entity or subsidiary',
        'originalCostBasisDisposed': 'Original cost basis of disposed assets before any impairment or adjustment',
        'impairmentExpenseDisposed': 'Impairment expense associated with the disposed portion of the asset',
        'reportingCurrencyTxnExchangeRate': 'Exchange rate to convert transaction value into the reporting currency',
        'reportingCurrencyOriginalExchangeRate': 'Original exchange rate at the time of acquisition for reporting currency',
        'functionalReportingCurrency': 'The functional currency used for financial reporting',
        'isIntercompanyTransfer': 'Boolean flag indicating if the transaction is between entities within the same organization',
        'isMultiSubsidiaryTxn': 'Boolean flag indicating if the transaction involves multiple subsidiaries'
      },
      
      columnCategories: {
        'Time': ['timestamp', 'timestampSEC', 'lotAcquisitionTimestampSEC'],
        'Asset': ['asset', 'assetId'],
        'Quantity': ['assetUnitAdj', 'assetBalance'],
        'Transaction': ['txnId', 'action', 'eventId', 'isTrade'],
        'Lot': ['lotId'],
        'Valuation': ['shortTermGainLoss', 'longTermGainLoss', 'undatedGainLoss', 'costBasisAcquired', 'costBasisRelieved', 'carryingValue', 'impairmentExpense'],
        'Pricing': ['txnExchangeRate', 'fairMarketValueDisposed', 'costAverageRate'],
        'Organization': ['orgId', 'runId'],
        'Status': ['status']
      },
      
      simpleSemanticRules: [
        'shortTermGainLoss is only non-zero when action is SELL or DISPOSAL and holding period < 1 year',
        'longTermGainLoss is only non-zero when action is SELL or DISPOSAL and holding period > 1 year',
        'assetUnitAdj is positive for acquisitions and negative for disposals',
        'costBasisAcquired represents the USD value at time of acquisition',
        'costBasisRelieved represents the USD value of cost basis being removed from inventory',
        'txnExchangeRate represents the market price of the asset at the time of the transaction'
      ],
      
      exampleQueries: [
        {
          description: 'Total gain/loss by asset',
          query: 'What is the total gain or loss for each cryptocurrency asset?',
          sql: 'SELECT asset, SUM(shortTermGainLoss + longTermGainLoss + undatedGainLoss) as totalGainLoss FROM `table` GROUP BY asset ORDER BY totalGainLoss DESC'
        },
        {
          description: 'Transaction count by action type',
          query: 'How many transactions of each action type do I have?',
          sql: 'SELECT action, COUNT(*) as transactionCount FROM `table` GROUP BY action ORDER BY transactionCount DESC'
        },
        {
          description: 'Asset balance over time',
          query: 'Show me the balance of Bitcoin over time',
          sql: 'SELECT timestamp, assetBalance FROM `table` WHERE asset = "BTC" ORDER BY timestamp ASC'
        }
      ],
        
      columnMappings: [
        {
          domainConcept: 'realized gain',
          columnNames: ['realized_pl', 'realized_gain_loss'],
          aliases: ['profit', 'loss', 'p&l', 'gain', 'realized'],
          description: 'Realized profit or loss from disposing of assets'
        },
        {
          domainConcept: 'unrealized gain',
          columnNames: ['unrealized_pl', 'fair_value_adjustment'],
          aliases: ['mark to market', 'mtm', 'unrealized', 'paper gain', 'paper loss'],
          description: 'Unrealized profit or loss from holding assets'
        },
        {
          domainConcept: 'asset',
          columnNames: ['asset', 'symbol', 'ticker'],
          aliases: ['coin', 'token', 'cryptocurrency'],
          description: 'Cryptocurrency symbol/ticker'
        }
      ],
      semanticRules: [
        {
          ruleName: 'realized_pl_validation',
          description: 'Realized P&L should only be present when action is "dispose"',
          validationExpression: 'action = "dispose" OR realized_pl IS NULL',
          ruleType: 'validation'
        },
        {
          ruleName: 'cost_basis_test',
          description: 'Cost basis should never be negative',
          validationExpression: 'cost_basis >= 0',
          ruleType: 'testing'
        }
      ],
      queryAspects: [
        {
          aspectType: 'aggregation',
          columnNames: ['qty', 'cost_basis', 'fair_value', 'realized_pl', 'unrealized_pl', 'assetUnitAdj', 'assetBalance', 'costBasisAcquired', 'costBasisRelieved'],
          patterns: ['SUM({column})', 'AVG({column})', 'MAX({column})', 'MIN({column})', 'COUNT({column})']
        },
        {
          aspectType: 'filter',
          columnNames: ['asset', 'action', 'timestamp', 'inventory', 'txnId', 'lotId', 'wallet', 'isInternalTransfer', 'categoryCode'],
          patterns: ['{column} = {value}', '{column} IN ({values})', '{column} BETWEEN {value1} AND {value2}', '{column} LIKE {pattern}']
        },
        {
          aspectType: 'inclusion',
          columnNames: ['txnId', 'lotId', 'originalLotId', 'transferLotId'],
          patterns: ['SELECT * FROM actions WHERE {column} IN (SELECT {column} FROM actions WHERE {condition})']
        },
        {
          aspectType: 'exclusion',
          columnNames: ['asset', 'action', 'wallet', 'inventory', 'categoryCode'],
          patterns: ['SELECT * FROM actions WHERE {column} NOT IN ({values})', 'SELECT * FROM actions WHERE {column} != {value}']
        },
        {
          aspectType: 'grouping',
          columnNames: ['asset', 'action', 'wallet', 'inventory', 'categoryCode', 'category'],
          patterns: ['GROUP BY {column}', 'GROUP BY DATE_TRUNC({timeUnit}, {column})']
        },
        {
          aspectType: 'sorting',
          columnNames: ['timestamp', 'qty', 'cost_basis', 'fair_value', 'realized_pl', 'unrealized_pl'],
          patterns: ['ORDER BY {column} ASC', 'ORDER BY {column} DESC']
        },
        {
          aspectType: 'time_series',
          columnNames: ['timestamp', 'timestampSEC'],
          patterns: ['DATE_TRUNC(DAY, {column})', 'DATE_TRUNC(MONTH, {column})', 'DATE_TRUNC(YEAR, {column})']
        }
      ],
      learnings: []
    });
    
    // Register Transaction Schema Type
    this.registerSchemaType({
      id: 'transaction',
      name: 'Transaction Schema',
      description: 'Schema for transaction data with transaction IDs and timestamps',
      compatibleReports: ['transaction-history', 'transaction-summary'],
      minimumRequiredColumns: [
        'transaction_id',
        'transaction_created_at',
        'transaction_materialized'
      ],
      otherIncludedColumns: [
        'operation',
        'stream_name',
        'global_position',
        'version',
        'inserted_at',
        'transaction_realized',
        'wallet_id_array'
      ],
      columnMappings: [],
      semanticRules: [],
      queryAspects: [],
      learnings: []
    });
    
    // Register Canton Transaction Schema Type
    this.registerSchemaType({
      id: 'canton_transaction',
      name: 'Canton Transaction Schema',
      description: 'Schema for Canton transaction data with wallet and asset information',
      compatibleReports: ['canton-transaction-history', 'wallet-activity'],
      minimumRequiredColumns: [
        'parenttransactionId',
        'dateTime',
        'walletId',
        'operation',
        'assetTicker',
        'assetAmount'
      ],
      otherIncludedColumns: [
        'linetransactionId',
        'dateTimeSEC',
        'walletName',
        'assetbitwaveId',
        'exchangeRate',
        'exchangeRateSource',
        'assetvalueInBaseCurrency',
        'feeAmount',
        'feeAsset',
        'categorizationStatus',
        'reconciliationStatus',
        'contactId',
        'categoryId',
        'description',
        'fromAddress',
        'toAddress',
        'type',
        'combinedParentTxnId',
        'transactionType',
        'feeType',
        'rewardFeeType',
        'rewardType',
        'eventId',
        'rootTxn'
      ],

      columnDescriptions: {
        'parenttransactionId': 'Identifier that groups related transaction line items together',
        'dateTime': 'The date and time when the transaction occurred',
        'dateTimeSEC': 'Unix timestamp in seconds representing when the transaction occurred',
        'walletId': 'Unique identifier for the wallet involved in the transaction',
        'walletName': 'Human-readable name of the wallet involved in the transaction',
        'operation': 'The type of operation performed (buy, sell, transfer, etc.)',
        'assetTicker': 'The ticker symbol of the cryptocurrency asset',
        'assetbitwaveId': 'Bitwavie-specific identifier for the cryptocurrency asset',
        'assetAmount': 'The amount of cryptocurrency involved in the transaction',
        'linetransactionId': 'Unique identifier for each individual transaction line item',
        'exchangeRate': 'The exchange rate used to convert the asset value to the base currency',
        'exchangeRateSource': 'Source of the exchange rate data (e.g., CoinGecko, Binance)',
        'assetvalueInBaseCurrency': 'Value of the asset in the base currency (usually USD)',
        'feeAmount': 'Amount of fees paid for the transaction',
        'feeAsset': 'Asset used to pay the transaction fee',
        'categorizationStatus': 'Status of transaction categorization (e.g., categorized, uncategorized)',
        'reconciliationStatus': 'Status of transaction reconciliation (e.g., reconciled, unreconciled)',
        'contactId': 'Identifier for the counterparty or contact involved in the transaction',
        'categoryId': 'Identifier for the transaction category',
        'description': 'Human-readable description of the transaction',
        'fromAddress': 'Blockchain address where the transaction originated',
        'toAddress': 'Blockchain address where the transaction was sent',
        'type': 'General transaction type classification',
        'combinedParentTxnId': 'Cross-system identifier that can link related transactions',
        'transactionType': 'Detailed classification of the transaction type',
        'feeType': 'Type of fee associated with the transaction',
        'rewardFeeType': 'Type of fee associated with reward transactions',
        'rewardType': 'Type of reward (e.g., staking, mining, interest)',
        'eventId': 'Identifier for the blockchain event associated with the transaction',
        'rootTxn': 'Identifier for the root transaction in a chain of related transactions'
      },
      
      columnCategories: {
        'Time': ['dateTime', 'dateTimeSEC'],
        'Asset': ['assetTicker', 'assetbitwaveId'],
        'Transaction': ['parenttransactionId', 'linetransactionId', 'operation', 'combinedParentTxnId', 'transactionType', 'type', 'rootTxn', 'eventId'],
        'Wallet': ['walletId', 'walletName', 'fromAddress', 'toAddress'],
        'Quantity': ['assetAmount'],
        'Valuation': ['assetvalueInBaseCurrency'],
        'Pricing': ['exchangeRate', 'exchangeRateSource'],
        'Status': ['categorizationStatus', 'reconciliationStatus'],
        'Fee': ['feeAmount', 'feeAsset', 'feeType', 'rewardFeeType'],
        'Metadata': ['description', 'contactId', 'categoryId'],
        'Blockchain': [ 'rewardType']
      },
      
      exampleQueries: [
        {
          description: 'Transactions by wallet',
          query: 'Show me all transactions for wallet W123',
          sql: 'SELECT dateTime, operation, assetTicker, assetAmount FROM canton_transactions WHERE walletId = "W123" ORDER BY dateTime DESC'
        },
        {
          description: 'Asset inflows and outflows',
          query: 'What are the total inflows and outflows of Bitcoin by month?',
          sql: 'SELECT DATE_TRUNC(dateTime, MONTH) as month, SUM(CASE WHEN assetAmount > 0 THEN assetAmount ELSE 0 END) as inflow, SUM(CASE WHEN assetAmount < 0 THEN ABS(assetAmount) ELSE 0 END) as outflow FROM canton_transactions WHERE assetTicker = "BTC" GROUP BY month ORDER BY month'
        },
        {
          description: 'Fee analysis',
          query: 'How much have I spent on transaction fees in total?',
          sql: 'SELECT feeAsset, SUM(feeAmount) as totalFees FROM canton_transactions WHERE feeAmount > 0 GROUP BY feeAsset ORDER BY totalFees DESC'
        },
        {
          description: 'Transaction count by operation type',
          query: 'How many transactions of each operation type do I have?',
          sql: 'SELECT operation, COUNT(*) as transactionCount FROM canton_transactions GROUP BY operation ORDER BY transactionCount DESC'
        }
      ],
      columnMappings: [
        {
          domainConcept: 'transaction amount',
          columnNames: ['assetAmount'],
          aliases: ['amount', 'quantity', 'volume', 'size', 'units'],
          description: 'The amount of cryptocurrency involved in the transaction'
        },
        {
          domainConcept: 'transaction value',
          columnNames: ['assetvalueInBaseCurrency'],
          aliases: ['value', 'usd value', 'dollar value', 'fiat value', 'worth'],
          description: 'The value of the transaction in the base currency (usually USD)'
        },
        {
          domainConcept: 'wallet',
          columnNames: ['walletId', 'walletName'],
          aliases: ['account', 'address', 'storage', 'portfolio'],
          description: 'The wallet or account involved in the transaction'
        },
        {
          domainConcept: 'transaction type',
          columnNames: ['operation', 'transactionType', 'type'],
          aliases: ['action', 'activity', 'event', 'transaction category'],
          description: 'The type or category of the transaction (buy, sell, transfer, etc.)'
        },
        {
          domainConcept: 'transaction fee',
          columnNames: ['feeAmount', 'feeAsset', 'feeType'],
          aliases: ['gas', 'commission', 'network fee', 'transaction cost'],
          description: 'The fee paid to process the transaction'
        },
        {
          domainConcept: 'cryptocurrency',
          columnNames: ['assetTicker', 'assetbitwaveId'],
          aliases: ['coin', 'token', 'asset', 'crypto'],
          description: 'The cryptocurrency involved in the transaction'
        },
        {
          domainConcept: 'transaction date',
          columnNames: ['dateTime', 'dateTimeSEC'],
          aliases: ['time', 'date', 'timestamp', 'when'],
          description: 'When the transaction occurred'
        }
      ],
      semanticRules: [],
      queryAspects: [
        {
          aspectType: 'aggregation',
          columnNames: ['assetAmount', 'assetvalueInBaseCurrency', 'feeAmount', 'exchangeRate'],
          patterns: ['SUM({column})', 'AVG({column})', 'MAX({column})', 'MIN({column})']
        },
        {
          aspectType: 'filter',
          columnNames: ['walletId', 'assetTicker', 'operation', 'dateTime', 'categorizationStatus', 'reconciliationStatus'],
          patterns: ['{column} = {value}', '{column} IN ({values})', '{column} BETWEEN {value1} AND {value2}']
        },
        {
          aspectType: 'inclusion',
          columnNames: ['parenttransactionId', 'linetransactionId', 'combinedParentTxnId', 'rootTxn', 'eventId'],
          patterns: ['SELECT * FROM canton_transactions WHERE {column} IN (SELECT {column} FROM canton_transactions WHERE {condition})']
        },
        {
          aspectType: 'exclusion',
          columnNames: ['walletId', 'assetTicker', 'operation', 'transactionType'],
          patterns: ['SELECT * FROM canton_transactions WHERE {column} NOT IN ({values})', 'SELECT * FROM canton_transactions WHERE {column} != {value}']
        },
        {
          aspectType: 'grouping',
          columnNames: ['walletId', 'walletName', 'assetTicker', 'operation', 'transactionType'],
          patterns: ['GROUP BY {column}', 'GROUP BY DATE_TRUNC({timeUnit}, {column})']
        },
        {
          aspectType: 'sorting',
          columnNames: ['dateTime', 'assetAmount', 'assetvalueInBaseCurrency'],
          patterns: ['ORDER BY {column} ASC', 'ORDER BY {column} DESC']
        },
        {
          aspectType: 'time_series',
          columnNames: ['dateTime', 'dateTimeSEC'],
          patterns: ['DATE_TRUNC(DAY, {column})', 'DATE_TRUNC(MONTH, {column})', 'DATE_TRUNC(YEAR, {column})']
        }
      ],
      learnings: []
    });

  }
  
  /**
   * Register a new schema type
   * @param definition Schema type definition
   */
  public registerSchemaType(definition: SchemaTypeDefinition): void {
    this.schemaTypes.set(definition.id, definition);
    logFlow('SCHEMA_TYPE_REGISTRY', 'INFO', `Registered schema type: ${definition.id}`, {
      name: definition.name,
      compatibleReports: definition.compatibleReports.length
    });
  }
  
  /**
   * Get all registered schema types
   * @returns Array of schema type definitions
   */
  public getAllSchemaTypes(): SchemaTypeDefinition[] {
    return Array.from(this.schemaTypes.values());
  }
  
  /**
   * Get a schema type by ID
   * @param id Schema type ID
   * @returns Schema type definition or null if not found
   */
  public getSchemaTypeById(id: string): SchemaTypeDefinition | null {
    const schemaType = this.schemaTypes.get(id);
    if (!schemaType) {
      logFlow('SCHEMA_TYPE_REGISTRY', 'INFO', `Schema type not found: ${id}`);
      return null;
    }
    return schemaType;
  }
  
  /**
   * Get compatible reports for a schema type
   * @param schemaTypeId Schema type ID
   * @returns Array of report IDs compatible with the schema type
   */
  public getCompatibleReports(schemaTypeId: string): string[] {
    const schemaType = this.getSchemaTypeById(schemaTypeId);
    return schemaType ? schemaType.compatibleReports : [];
  }
  
  /**
   * Get column mappings for a schema type
   * @param schemaTypeId Schema type ID
   * @returns Array of column mappings for the schema type
   */
  public getColumnMappings(schemaTypeId: string): ColumnMapping[] {
    const schemaType = this.getSchemaTypeById(schemaTypeId);
    return schemaType ? schemaType.columnMappings : [];
  }
  
  /**
   * Find column mapping by domain concept
   * @param schemaTypeId Schema type ID
   * @param concept Domain concept to find
   * @returns Column mapping or null if not found
   */
  public findColumnMappingByConcept(schemaTypeId: string, concept: string): ColumnMapping | null {
    const schemaType = this.getSchemaTypeById(schemaTypeId);
    if (!schemaType) return null;
    
    return schemaType.columnMappings.find(mapping => 
      mapping.domainConcept.toLowerCase() === concept.toLowerCase() ||
      mapping.aliases.some(alias => alias.toLowerCase() === concept.toLowerCase())
    ) || null;
  }
  
  /**
   * Get LLM context for a schema type
   * @param schemaTypeId Schema type ID
   * @returns Formatted schema type information for LLM prompts
   */
  public getSchemaTypeForLLM(schemaTypeId: string): string {
    const schemaType = this.getSchemaTypeById(schemaTypeId);
    if (!schemaType) {
      return "No schema type information available.";
    }
    
    let context = `Schema Type: ${schemaType.name}\n\n`;
    
    // Add required and included columns
    context += "Required Columns:\n";
    for (const column of schemaType.minimumRequiredColumns) {
      context += `- ${column}\n`;
    }
    context += "\nOther Included Columns:\n";
    for (const column of schemaType.otherIncludedColumns) {
      context += `- ${column}\n`;
    }
    context += "\n";
    
    // Add column mappings
    context += "Column Mappings:\n";
    for (const mapping of schemaType.columnMappings) {
      context += `- "${mapping.domainConcept}" refers to: ${mapping.columnNames.join(", ")}\n`;
      context += `  Aliases: ${mapping.aliases.join(", ")}\n`;
      context += `  Description: ${mapping.description}\n\n`;
    }
    
    // Add semantic rules
    context += "Semantic Rules:\n";
    for (const rule of schemaType.semanticRules) {
      context += `- ${rule.description} (${rule.ruleType})\n`;
    }
    
    return context;
  }
  
  /**
   * Validate if a set of columns matches a schema type's requirements
   * @param schemaTypeId Schema type ID
   * @param columns Array of column names to validate
   * @returns Object with validation result and missing required columns
   */
  public validateColumnsForSchemaType(schemaTypeId: string, columns: string[]): {
    isValid: boolean;
    missingRequiredColumns: string[];
    matchScore: number; // 0-100 score indicating how well the columns match
  } {
    const schemaType = this.getSchemaTypeById(schemaTypeId);
    if (!schemaType) {
      return { isValid: false, missingRequiredColumns: [], matchScore: 0 };
    }
    
    // Check for required columns
    const missingRequiredColumns = schemaType.minimumRequiredColumns.filter(
      requiredCol => !columns.includes(requiredCol)
    );
    
    // Calculate match score
    const requiredColumnsPresent = schemaType.minimumRequiredColumns.length - missingRequiredColumns.length;
    const otherColumnsPresent = schemaType.otherIncludedColumns.filter(
      col => columns.includes(col)
    ).length;
    
    const totalExpectedColumns = schemaType.minimumRequiredColumns.length + schemaType.otherIncludedColumns.length;
    const totalMatchingColumns = requiredColumnsPresent + otherColumnsPresent;
    
    // Score is weighted - required columns are more important
    const requiredWeight = 0.7;
    const otherWeight = 0.3;
    
    const requiredScore = schemaType.minimumRequiredColumns.length > 0 ?
      (requiredColumnsPresent / schemaType.minimumRequiredColumns.length) * 100 * requiredWeight : 0;
      
    const otherScore = schemaType.otherIncludedColumns.length > 0 ?
      (otherColumnsPresent / schemaType.otherIncludedColumns.length) * 100 * otherWeight : 0;
    
    const matchScore = Math.round(requiredScore + otherScore);
    
    return {
      isValid: missingRequiredColumns.length === 0,
      missingRequiredColumns,
      matchScore
    };
  }
  
  /**
   * Detect the most likely schema type for a set of columns
   * @param columns Array of column names to match against schema types
   * @returns The best matching schema type ID and match score, or null if no good match
   */
  public detectSchemaTypeFromColumns(columns: string[]): {
    schemaTypeId: string;
    matchScore: number;
  } | null {
    if (!columns || columns.length === 0) {
      return null;
    }
    
    const results = this.getAllSchemaTypes().map(schemaType => {
      const validation = this.validateColumnsForSchemaType(schemaType.id, columns);
      return {
        schemaTypeId: schemaType.id,
        matchScore: validation.matchScore
      };
    });
    
    // Sort by match score descending
    results.sort((a, b) => b.matchScore - a.matchScore);
    
    // Return the best match if it has a reasonable score (>50)
    const bestMatch = results.length > 0 ? results[0] : null;
    if (bestMatch && bestMatch.matchScore > 50) {
      return bestMatch;
    }
    
    return null;
  }
  
  /**
   * Record a new learning about a schema type
   * @param schemaTypeId Schema type ID
   * @param concept Concept being learned
   * @param mapping Column mapping discovered
   * @param example Example query
   */
  public recordLearning(
    schemaTypeId: string, 
    concept: string, 
    mapping?: string, 
    example?: string
  ): void {
    const schemaType = this.getSchemaTypeById(schemaTypeId);
    if (!schemaType) return;
    
    // Find existing learning or create new one
    let learning = schemaType.learnings.find(l => l.concept.toLowerCase() === concept.toLowerCase());
    
    if (!learning) {
      learning = {
        concept,
        mappings: [],
        examples: [],
        frequency: 0,
        lastUpdated: new Date()
      };
      schemaType.learnings.push(learning);
    }
    
    // Update learning
    if (mapping && !learning.mappings.includes(mapping)) {
      learning.mappings.push(mapping);
    }
    
    if (example && !learning.examples.includes(example)) {
      learning.examples.push(example);
    }
    
    learning.frequency++;
    learning.lastUpdated = new Date();
    
    logFlow('SCHEMA_TYPE_REGISTRY', 'INFO', `Recorded learning for schema type ${schemaTypeId}`, {
      concept,
      mapping,
      frequency: learning.frequency
    });
  }
}

// Export singleton instance
export const schemaTypeRegistry = new SchemaTypeRegistry();
