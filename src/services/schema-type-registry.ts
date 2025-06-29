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
  aspectType: 'aggregation' | 'filter' | 'inclusion' | 'exclusion';
  columnNames: string[];      // Columns typically used for this aspect
  patterns: string[];         // Common query patterns
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
      description: 'General Ledger actions schema for cryptocurrency accounting',
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
        'impairmentReversal',
        'revaluationAdjustmentUpward',
        'revaluationAdjustmentDownward',
        'fairValueAdjustmentUpward',
        'fairValueAdjustmentDownward',
        'categoryCode',
        'category',
        'isInternalTransfer',
        'metadata',
        'wallet',
        'contact',
        'description',
        'toWallet',
        'fromWallet',
        'baseCurrency',
        'isFee',
        'categoryType',
        'capitalizeTradingFees',
        'wrappingAdjustment',
        'forceZeroGainLoss',
        'excludedGainLoss',
        'runTimestampSEC',
        'digitalAssetCategoryId',
        'digitalAssetCategory',
        'subsidiaryId',
        'subsidiaryName',
        'originalCostBasisDisposed',
        'impairmentExpenseDisposed',
        'reportingCurrencyTxnExchangeRate',
        'reportingCurrencyOriginalExchangeRate',
        'functionalReportingCurrency',
        'isIntercompanyTransfer',
        'isMultiSubsidiaryTxn'
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
          columnNames: ['qty', 'cost_basis', 'fair_value', 'realized_pl', 'unrealized_pl'],
          patterns: ['SUM({column})', 'AVG({column})']
        },
        {
          aspectType: 'filter',
          columnNames: ['asset', 'action', 'timestamp', 'inventory'],
          patterns: ['{column} = {value}', '{column} IN ({values})']
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
      columnMappings: [],
      semanticRules: [],
      queryAspects: [],
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
