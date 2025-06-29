/**
 * Schema Manager Service
 * 
 * Handles dynamic discovery and caching of BigQuery table schema information.
 * Provides rich metadata about columns including data types, sample values, and patterns.
 * Automatically refreshes schema information on a configurable interval.
 */

import { BigQuery, Dataset, Table } from '@google-cloud/bigquery';
import { logFlow } from '../utils/logging.js';
import { ConnectionManager } from './connection-manager.js';
import { SchemaTypeRegistry, SchemaTypeDefinition } from './schema-type-registry.js';

// Types
export interface SchemaConfig {
  projectId: string;
  datasetId: string;
  tableId: string;
  schemaType?: string;       // Type of schema (e.g., 'actions', 'canton_transaction')
  refreshIntervalMs?: number; // Default: 5 minutes
}

export interface ColumnMetadata {
  name: string;
  type: string;
  description: string;
  mode: string; // NULLABLE, REQUIRED, REPEATED
  aggregatable: boolean;
  sampleValues?: any[];
  patterns?: string[];
  statistics?: {
    min?: any;
    max?: any;
    distinctCount?: number;
    nullCount?: number;
    avgLength?: number;
  };
}

export interface TableSchema {
  columns: ColumnMetadata[];
  lastUpdated: Date;
  version: number;
}

export class SchemaManager {
  private bigquery: BigQuery | null = null;
  private dataset: Dataset | null = null;
  private table: Table | null = null;
  private config: SchemaConfig | null = null;
  private schemaTypeRegistry: SchemaTypeRegistry;
  
  private schema: TableSchema | null = null;
  private refreshTimer: NodeJS.Timeout | null = null;
  private refreshing: boolean = false;
  private refreshIntervalMs: number = 5 * 60 * 1000; // Default: 5 minutes
  private schemaVersion: number = 0;
  
  // Predefined metadata to supplement BigQuery schema information
  private predefinedColumnMetadata: Record<string, Partial<ColumnMetadata>> = {
    // Time-related columns
    'timestamp': { 
      description: 'Date and time when the transaction occurred', 
      aggregatable: false
    },
    
    // Asset identification columns
    'asset': { 
      description: 'Cryptocurrency symbol/ticker (e.g., BTC, ETH, SOL)', 
      aggregatable: false
    },
    'assetName': { 
      description: 'Full name of the cryptocurrency (e.g., Bitcoin, Ethereum, Solana)', 
      aggregatable: false
    },
    
    // Transaction type columns
    'action': { 
      description: 'Type of transaction (buy, sell, transfer, stake, etc.)', 
      aggregatable: false
    },
    'transactionType': { 
      description: 'Category of transaction (trade, transfer, income, etc.)', 
      aggregatable: false
    },
    
    // Quantity columns
    'amount': { 
      description: 'Quantity of cryptocurrency in the transaction', 
      aggregatable: true
    },
    'balance': { 
      description: 'Current balance of the cryptocurrency', 
      aggregatable: true
    },
    
    // Financial columns
    'price': { 
      description: 'Price per unit of the cryptocurrency at transaction time', 
      aggregatable: true
    },
    'value': { 
      description: 'Total value of the transaction in fiat currency', 
      aggregatable: true
    },
    'fee': { 
      description: 'Transaction fee paid', 
      aggregatable: true
    },
    
    // Gain/Loss columns
    'shortTermGainLoss': { 
      description: 'Realized gain or loss for assets held less than a year', 
      aggregatable: true
    },
    'longTermGainLoss': { 
      description: 'Realized gain or loss for assets held more than a year', 
      aggregatable: true
    },
    'undatedGainLoss': { 
      description: 'Gain or loss where the holding period is unknown', 
      aggregatable: true
    },
    'totalGainLoss': { 
      description: 'Total realized gain or loss across all holding periods', 
      aggregatable: true
    },
    'unrealizedGainLoss': { 
      description: 'Potential gain or loss for assets still held', 
      aggregatable: true
    },
    
    // Cost basis columns
    'costBasisAcquired': { 
      description: 'Cost basis of assets acquired in the transaction', 
      aggregatable: true
    },
    'costBasisRelieved': { 
      description: 'Cost basis of assets disposed in the transaction', 
      aggregatable: true
    },
    'carryingValue': { 
      description: 'Current carrying value of the assets', 
      aggregatable: true
    },
    'fairMarketValueDisposed': { 
      description: 'Fair market value of assets at time of disposal', 
      aggregatable: true
    },
    
    // Other columns
    'assetUnitAdj': { 
      description: 'Adjustment to asset units', 
      aggregatable: true
    },
    'wallet': { 
      description: 'Wallet address or identifier', 
      aggregatable: false
    },
    'exchange': { 
      description: 'Exchange or platform where the transaction occurred', 
      aggregatable: false
    }
  };
  
  constructor() {
    // Initialize empty - configuration happens via configure()
    this.schemaTypeRegistry = new SchemaTypeRegistry();
  }
  
  /**
   * Configure the SchemaManager with BigQuery connection details
   * @param config Configuration object with BigQuery project, dataset, and table IDs
   * @param sessionDetails Optional session details for multi-tenant support
   */
  public async configure(config: SchemaConfig, sessionDetails?: any): Promise<void> {
    this.config = config;
    
    // Set custom refresh interval if provided
    if (config.refreshIntervalMs) {
      this.refreshIntervalMs = config.refreshIntervalMs;
    }
    
    // Get connection details from ConnectionManager if available
    const connectionManager = ConnectionManager.getInstance();
    const projectId = connectionManager.getProjectId(sessionDetails) || config.projectId;
    const datasetId = connectionManager.getDatasetId(sessionDetails) || config.datasetId;
    const tableId = connectionManager.getTableId(sessionDetails) || config.tableId;
    const privateKey = connectionManager.getPrivateKey(sessionDetails);
    
    // Initialize BigQuery client with appropriate auth
    const clientOptions: any = { projectId };
    
    // If private key is available, use it for authentication
    if (privateKey) {
      try {
        const credentials = JSON.parse(privateKey);
        clientOptions.credentials = credentials;
        logFlow('SCHEMA_MANAGER', 'INFO', 'Using private key authentication for BigQuery');
      } catch (error) {
        logFlow('SCHEMA_MANAGER', 'ERROR', 'Failed to parse private key JSON', { error });
      }
    }
    
    this.bigquery = new BigQuery(clientOptions);
    this.dataset = this.bigquery.dataset(datasetId);
    this.table = this.dataset.table(tableId);
    
    logFlow('SCHEMA_MANAGER', 'INFO', 'Configured SchemaManager', {
      project: projectId,
      dataset: datasetId,
      table: tableId,
      refreshInterval: this.refreshIntervalMs,
      usingPrivateKey: !!privateKey,
      source: sessionDetails ? 'session' : 'environment'
    });

    
    // Initial schema fetch
    await this.refreshSchema();
    
    // Start refresh timer
    this.startRefreshTimer();
  }
  
  /**
   * Get the current schema information
   * @returns The current table schema or null if not yet fetched
   */
  public getSchema(): TableSchema | null {
    return this.schema;
  }
  
  /**
   * Get a list of all available column names
   * @returns Array of column names
   */
  public getColumnNames(): string[] {
    if (!this.schema) {
      return [];
    }
    
    return this.schema.columns.map(col => col.name);
  }
  
  /**
   * Get detailed metadata for a specific column
   * @param columnName Name of the column to get metadata for
   * @returns Column metadata or null if column not found
   */
  public getColumnMetadata(columnName: string): ColumnMetadata | null {
    if (!this.schema) {
      return null;
    }
    
    return this.schema.columns.find(col => 
      col.name.toLowerCase() === columnName.toLowerCase()
    ) || null;
  }
  
  /**
   * Find columns similar to the provided input string
   * @param input Search term
   * @param limit Maximum number of results to return
   * @returns Array of similar column names
   */
  public findSimilarColumns(input: string, limit: number = 3): string[] {
    if (!this.schema) {
      return [];
    }
    
    // Simple similarity scoring based on string inclusion and position
    const scoredColumns = this.schema.columns.map(column => {
      const columnName = column.name;
      const lowerColumn = columnName.toLowerCase();
      const lowerInput = input.toLowerCase();
      
      // Score based on:
      // 1. Exact match (highest score)
      if (lowerColumn === lowerInput) return { columnName, score: 100 };
      
      // 2. Starts with input
      if (lowerColumn.startsWith(lowerInput)) return { columnName, score: 80 };
      
      // 3. Contains input
      if (lowerColumn.includes(lowerInput)) return { columnName, score: 60 };
      
      // 4. Words in common
      const columnWords = new Set(lowerColumn.split(/[^a-z0-9]+/).filter(Boolean));
      const inputWords = new Set(lowerInput.split(/[^a-z0-9]+/).filter(Boolean));
      const commonWords = [...inputWords].filter(word => columnWords.has(word)).length;
      
      return { columnName, score: commonWords * 10 };
    });
    
    // Sort by score and return top N
    return scoredColumns
      .sort((a, b) => b.score - a.score)
      .slice(0, limit)
      .filter(item => item.score > 0)
      .map(item => item.columnName);
  }
  
  /**
   * Get columns that are likely to be aggregatable (numeric types)
   * @returns Array of aggregatable column names
   */
  public getAggregatableColumns(): string[] {
    if (!this.schema) {
      return [];
    }
    
    return this.schema.columns
      .filter(col => col.aggregatable)
      .map(col => col.name);
  }
  
  /**
   * Get columns suitable for filtering (dimension columns)
   * @returns Array of filterable column names
   */
  public getFilterableColumns(): string[] {
    if (!this.schema) {
      return [];
    }
    
    return this.schema.columns
      .filter(col => !col.aggregatable)
      .map(col => col.name);
  }
  
  /**
   * Get schema information formatted for LLM prompts
   * @returns Formatted schema information string
   */
  public getSchemaForLLM(): string {
    if (!this.schema) {
      console.log('SCHEMA_CHOICE1: No schema available');
      return "No schema information available.";
    }
    
    let schemaText = `Table Schema (${this.config?.tableId}):\n\n`;
    
    // Check if we have a known schema type
    const schemaType = this.config?.schemaType ? 
      this.schemaTypeRegistry.getSchemaTypeById(this.config.schemaType) : null;
    
    if (schemaType) {
      console.log(`SCHEMA_CHOICE2: Using known schema type: ${schemaType.id}`);
      return this.getSchemaForLLMFromKnownType(schemaType);
    } else {
      console.log('SCHEMA_CHOICE3: Using generic schema categorization');
      
      // Group columns by category for better organization
      const categories: Record<string, ColumnMetadata[]> = {
      'Time': [],
      'Asset': [],
      'Quantity': [],
      'Transaction': [],
      'Identifier': [],
      'Wallet': [],
      'Pricing': [],
      'Status': [],
      'Valuation': [],
      'Address': [],
      'Tagging': [],
      'Metadata': [],
      'Error': [],
      'Inventory': [],
      'Entity': [],
      'Currency': [],
      'Account': [],
      'Other': []
    };
    
    // Categorize columns
    if (this.schema && this.schema.columns) {
      this.schema.columns.forEach(col => {
        const name = col.name.toLowerCase();
        
        // Use a safer approach with default arrays if a category doesn't exist
        const addToCategory = (category: string) => {
          if (categories[category]) {
            categories[category].push(col);
          }
        };
        
        // Time columns
        if (name.includes('time') || name.includes('date') || name.includes('timestamp')) {
          addToCategory('Time');
        }
        // Asset columns
        else if (name.includes('asset') || name.includes('coin') || name.includes('ticker')) {
          addToCategory('Asset');
        }
        // Quantity columns
        else if (name.includes('quantity') || name.includes('amount') || name.includes('balance') || name.includes('total')) {
          addToCategory('Quantity');
        }
        // Transaction columns
        else if (name.includes('transaction') || name.includes('type') || name.includes('direction') || 
                name.includes('trade') || name.includes('transfer') || name.includes('fee')) {
          addToCategory('Transaction');
        }
        // Identifier columns
        else if (name.includes('id') || name.endsWith('id') || name.includes('identifier')) {
          addToCategory('Identifier');
        }
        // Wallet columns
        else if (name.includes('wallet')) {
          addToCategory('Wallet');
        }
        // Pricing columns
        else if (name.includes('price') || name.includes('rate') || name.includes('exchange')) {
          addToCategory('Pricing');
        }
        // Status columns
        else if (name.includes('status') || name.includes('categorized') || name.includes('synced') || 
                name.includes('failed')) {
          addToCategory('Status');
        }
        // Valuation columns
        else if (name.includes('gain') || name.includes('loss') || name.includes('cost') || 
                name.includes('basis') || name.includes('value') || name.includes('impairment') || 
                name.includes('revaluation') || name.includes('adjustment')) {
          addToCategory('Valuation');
        }
        // Address columns
        else if (name.includes('address')) {
          addToCategory('Address');
        }
        // Tagging/Labels columns
        else if (name.includes('category') || name.includes('tag') || name.includes('label') || 
                name.includes('contact')) {
          addToCategory('Tagging');
        }
        // Metadata columns
        else if (name.includes('metadata')) {
          addToCategory('Metadata');
        }
        // Error columns
        else if (name.includes('error')) {
          addToCategory('Error');
        }
        // Inventory columns
        else if (name.includes('inventory')) {
          addToCategory('Inventory');
        }
        // Entity columns
        else if (name.includes('subsidiary') || name.includes('organization') || 
                name.includes('department') || name.includes('entity')) {
          addToCategory('Entity');
        }
        // Currency columns
        else if (name.includes('currency')) {
          addToCategory('Currency');
        }
        // Account columns
        else if (name.includes('account')) {
          addToCategory('Account');
        }
        // Other columns
        else {
          addToCategory('Other');
        }
      });
    }
    
    // Format each category
    Object.entries(categories).forEach(([category, columns]) => {
      if (columns.length === 0) return;
      
      schemaText += `${category} Columns:\n`;
      
      columns.forEach(col => {
        schemaText += `- ${col.name} (${col.type}): ${col.description}\n`;
        
        // Add sample values if available
        if (col.sampleValues && col.sampleValues.length > 0) {
          const samples = col.sampleValues.slice(0, 3).map(v => 
            typeof v === 'string' ? `"${v}"` : v
          ).join(', ');
          schemaText += `  Sample values: ${samples}...\n`;
        }
        
        // Add statistics if available
        if (col.statistics) {
          const stats = [];
          if (col.statistics.min !== undefined) stats.push(`min: ${col.statistics.min}`);
          if (col.statistics.max !== undefined) stats.push(`max: ${col.statistics.max}`);
          if (col.statistics.distinctCount !== undefined) stats.push(`distinct values: ${col.statistics.distinctCount}`);
          
          if (stats.length > 0) {
            schemaText += `  Statistics: ${stats.join(', ')}\n`;
          }
        }
      });
      
      schemaText += '\n';
    });
    
      return schemaText;
    }
    
    return schemaText;
  }
  
  /**
   * Generate schema information for LLM from a known schema type
   * @param schemaType The schema type definition
   * @returns Formatted schema information string
   */
  private getSchemaForLLMFromKnownType(schemaType: SchemaTypeDefinition): string {
    console.log(`SCHEMA_CHOICE4: Generating schema info for ${schemaType.id}`);
    
    let schemaText = `Table Schema (${this.config?.tableId}) - Type: ${schemaType.name}\n\n`;
    schemaText += `${schemaType.description}\n\n`;
    
    // Use schema-specific categories if available, otherwise use default categories
    const categories: Record<string, ColumnMetadata[]> = {};
    
    if (schemaType.columnCategories) {
      console.log('SCHEMA_CHOICE5: Using schema-specific column categories');
      
      // Initialize categories
      Object.keys(schemaType.columnCategories).forEach(category => {
        categories[category] = [];
      });
      
      // Add an "Other" category for uncategorized columns
      categories['Other'] = [];
      
      // Categorize columns based on schema type definition
      if (this.schema && this.schema.columns) {
        this.schema.columns.forEach(col => {
          let categorized = false;
          
          // Find which category this column belongs to
          for (const [category, columnNames] of Object.entries(schemaType.columnCategories || {})) {
            if (Array.isArray(columnNames) && columnNames.includes(col.name)) {
              if (categories[category]) {
                categories[category].push(col);
              }
              categorized = true;
              break;
            }
          }
          
          // If not found in any category, add to "Other"
          if (!categorized && categories['Other']) {
            categories['Other'].push(col);
          }
        });
      }
    } else {
      console.log('SCHEMA_CHOICE6: No schema-specific categories, using generic categorization');
      // Fall back to generic categorization
      categories['Time'] = [];
      categories['Asset'] = [];
      categories['Quantity'] = [];
      categories['Transaction'] = [];
      categories['Identifier'] = [];
      categories['Wallet'] = [];
      categories['Pricing'] = [];
      categories['Status'] = [];
      categories['Valuation'] = [];
      categories['Address'] = [];
      categories['Tagging'] = [];
      categories['Metadata'] = [];
      categories['Error'] = [];
      categories['Inventory'] = [];
      categories['Entity'] = [];
      categories['Currency'] = [];
      categories['Account'] = [];
      categories['Other'] = [];
      
      // Use generic categorization logic
      if (this.schema && this.schema.columns) {
        this.schema.columns.forEach(col => {
          const name = col.name.toLowerCase();
          
          // Use a safer approach with default arrays if a category doesn't exist
          const addToCategory = (category: string) => {
            if (categories[category]) {
              categories[category].push(col);
            }
          };
          
          // Time columns
          if (name.includes('time') || name.includes('date') || name.includes('timestamp')) {
            addToCategory('Time');
          }
          // Asset columns
          else if (name.includes('asset') || name.includes('coin') || name.includes('ticker')) {
            addToCategory('Asset');
          }
          // Quantity columns
          else if (name.includes('quantity') || name.includes('amount') || name.includes('balance') || 
                  name.includes('count') || name.includes('total')) {
            addToCategory('Quantity');
          }
          // Transaction columns
          else if (name.includes('transaction') || name.includes('txn') || name.includes('tx') || 
                  name.includes('trade') || name.includes('order')) {
            addToCategory('Transaction');
          }
          // Other categories follow the same pattern as in the original method
          else {
            addToCategory('Other');
          }
        });
      }
    }
    
    // Format each category
    Object.entries(categories).forEach(([category, columns]) => {
      if (columns.length === 0) return;
      
      schemaText += `${category} Columns:\n`;
      
      columns.forEach(col => {
        // Use schema-specific description if available
        const description = schemaType.columnDescriptions?.[col.name] || col.description;
        
        // Highlight if this is a required column
        const isRequired = schemaType.minimumRequiredColumns.includes(col.name);
        
        schemaText += `- ${col.name} (${col.type})${isRequired ? ' [REQUIRED]' : ''}: ${description}\n`;
        
        // Add sample values if available
        if (col.sampleValues && col.sampleValues.length > 0) {
          const samples = col.sampleValues.slice(0, 3).map(v => 
            typeof v === 'string' ? `"${v}"` : v
          ).join(', ');
          schemaText += `  Sample values: ${samples}...\n`;
        }
        
        // Add statistics if available
        if (col.statistics) {
          const stats = [];
          if (col.statistics.min !== undefined) stats.push(`min: ${col.statistics.min}`);
          if (col.statistics.max !== undefined) stats.push(`max: ${col.statistics.max}`);
          if (col.statistics.distinctCount !== undefined) stats.push(`distinct values: ${col.statistics.distinctCount}`);
          
          if (stats.length > 0) {
            schemaText += `  Statistics: ${stats.join(', ')}\n`;
          }
        }
      });
      
      schemaText += '\n';
    });
    
    // Add semantic rules if available
    if (schemaType.simpleSemanticRules && schemaType.simpleSemanticRules.length > 0) {
      schemaText += "Column Relationships and Rules:\n";
      schemaType.simpleSemanticRules.forEach((rule: string) => {
        schemaText += `- ${rule}\n`;
      });
      schemaText += '\n';
    }
    
    // Add example queries if available
    if (schemaType.exampleQueries && schemaType.exampleQueries.length > 0) {
      schemaText += "Example Queries:\n";
      schemaType.exampleQueries.forEach((example) => {
        schemaText += `- ${example.description}:\n`;
        schemaText += `  Natural Language: "${example.query}"\n`;
        schemaText += `  SQL: ${example.sql}\n\n`;
      });
    }
    
    return schemaText;
  }

  /**
   * Start the schema refresh timer
   */
  private startRefreshTimer(): void {
    if (this.refreshTimer) {
      clearInterval(this.refreshTimer);
    }
    
    this.refreshTimer = setInterval(() => {
      this.refreshSchema().catch(error => {
        logFlow('SCHEMA_MANAGER', 'ERROR', 'Error refreshing schema', {
          error: error instanceof Error ? error.message : String(error)
        });
      });
    }, this.refreshIntervalMs);
    
    logFlow('SCHEMA_MANAGER', 'INFO', 'Started schema refresh timer', {
      intervalMs: this.refreshIntervalMs
    });
  }
  
  /**
   * Stop the schema refresh timer
   */
  public stopRefreshTimer(): void {
    if (this.refreshTimer) {
      clearInterval(this.refreshTimer);
      this.refreshTimer = null;
      
      logFlow('SCHEMA_MANAGER', 'INFO', 'Stopped schema refresh timer');
    }
  }
  
  /**
   * Refresh the schema from BigQuery
   * @returns Promise that resolves when refresh is complete
   */
  public async refreshSchema(): Promise<void> {
    if (this.refreshing) {
      logFlow('SCHEMA_MANAGER', 'INFO', 'Schema refresh already in progress, skipping');
      return;
    }
    
    if (!this.table) {
      throw new Error('BigQuery table not initialized. Call configure() first.');
    }
    
    this.refreshing = true;
    
    try {
      logFlow('SCHEMA_MANAGER', 'ENTRY', 'Refreshing schema from BigQuery');
      
      // Get table metadata
      const [metadata] = await this.table.getMetadata();
      const fields = metadata.schema.fields;
      
      // Get sample data for better metadata
      const [sampleRows] = await this.table.getRows({ maxResults: 100 });
      
      // Process schema information
      const columns: ColumnMetadata[] = [];
      
      for (const field of fields) {
        const name = field.name;
        const type = field.type;
        const mode = field.mode || 'NULLABLE';
        
        // Get predefined metadata if available
        const predefined = this.predefinedColumnMetadata[name] || {};
        
        // Determine if column is aggregatable
        const aggregatable = predefined.aggregatable !== undefined 
          ? predefined.aggregatable 
          : this.isLikelyAggregatable(name, type);
        
        // Extract sample values
        const sampleValues = sampleRows
          .map(row => row[name])
          .filter(value => value !== null && value !== undefined)
          .slice(0, 5);
        
        // Calculate basic statistics
        const statistics: ColumnMetadata['statistics'] = {};
        
        if (sampleValues.length > 0) {
          if (['INTEGER', 'FLOAT', 'NUMERIC', 'BIGNUMERIC'].includes(type)) {
            statistics.min = Math.min(...sampleValues.filter(v => typeof v === 'number'));
            statistics.max = Math.max(...sampleValues.filter(v => typeof v === 'number'));
          } else if (type === 'STRING') {
            statistics.avgLength = sampleValues
              .filter(v => typeof v === 'string')
              .reduce((sum, str) => sum + str.length, 0) / sampleValues.length;
          }
          
          // Count distinct values
          statistics.distinctCount = new Set(sampleValues).size;
          
          // Count nulls
          statistics.nullCount = sampleRows.filter(row => row[name] === null || row[name] === undefined).length;
        }
        
        // Create column metadata
        columns.push({
          name,
          type,
          description: predefined.description || field.description || `${name} column`,
          mode,
          aggregatable,
          sampleValues,
          statistics
        });
      }
      
      // Update schema with new version
      this.schemaVersion++;
      this.schema = {
        columns,
        lastUpdated: new Date(),
        version: this.schemaVersion
      };
      
      logFlow('SCHEMA_MANAGER', 'EXIT', 'Schema refresh completed', {
        columnCount: columns.length,
        version: this.schemaVersion
      });
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      logFlow('SCHEMA_MANAGER', 'ERROR', 'Error refreshing schema', { error: errorMessage });      console.error('Error refreshing schema:', error);
      throw error;
    } finally {
      this.refreshing = false;
    }
  }
  
  /**
   * Helper method to determine if a column is likely aggregatable based on its name and type
   * @param column Column name
   * @param type Column data type
   * @returns Boolean indicating if column is likely aggregatable
   */
  private isLikelyAggregatable(column: string, type?: string): boolean {
    // Financial and numeric columns are typically aggregatable
    const financialColumns = [
      'shortTermGainLoss', 'longTermGainLoss', 'undatedGainLoss', 'totalGainLoss', 'unrealizedGainLoss',
      'costBasisAcquired', 'costBasisRelieved', 'carryingValue', 'fairMarketValueDisposed', 
      'assetUnitAdj', 'amount', 'balance', 'price', 'value', 'fee'
    ];
    
    // Check if column name matches known financial columns
    const lowerColumn = column.toLowerCase();
    if (financialColumns.some(fc => lowerColumn.includes(fc.toLowerCase()))) {
      return true;
    }
    
    // Check if column name contains keywords suggesting numeric values
    const numericKeywords = ['amount', 'total', 'sum', 'count', 'avg', 'balance', 'value', 'price', 
                            'cost', 'fee', 'gain', 'loss', 'profit', 'revenue', 'expense'];
    if (numericKeywords.some(keyword => lowerColumn.includes(keyword))) {
      return true;
    }
    
    // Check data type - numeric types are typically aggregatable
    if (type) {
      const aggregatableTypes = ['INTEGER', 'FLOAT', 'NUMERIC', 'BIGNUMERIC'];
      if (aggregatableTypes.includes(type.toUpperCase())) {
        return true;
      }
    }
    
    // Default to false for other columns
    return false;
  }
  
  /**
   * Clean up resources when the schema manager is no longer needed
   */
  public dispose(): void {
    this.stopRefreshTimer();
    this.schema = null;
    this.bigquery = null;
    this.dataset = null;
    this.table = null;
    this.config = null;
    
    logFlow('SCHEMA_MANAGER', 'INFO', 'SchemaManager disposed');
  }
}
