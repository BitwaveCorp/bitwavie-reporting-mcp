/**
 * Report Registry Service
 * 
 * Central registry for all predefined reports in the system.
 * Provides methods to register, list, search, and retrieve reports.
 */

import { BigQueryClient } from './bigquery-client.js';
import { logFlow } from '../utils/logging.js';

// Import report generators
import { InventoryBalanceGenerator } from '../reports/inventory-balance.js';
import { ValuationRollforwardGenerator } from '../reports/valuation-rollforward.js';
import { LotsReportGenerator } from '../reports/lots-report.js';
import { MonthlyActivityReportGenerator } from '../reports/monthly-activity-report.js';

/**
 * Parameter metadata for a report
 */
export interface ReportParameterMetadata {
  name: string;
  description: string;
  type: 'string' | 'number' | 'date' | 'boolean';
  required: boolean;
  defaultValue?: any;
}

/**
 * Metadata for a report
 */
export interface ReportMetadata {
  id: string;
  name: string;
  description: string;
  keywords: string[];
  parameters: ReportParameterMetadata[];
  compatibleSchemaTypes?: string[]; // List of schema types this report is compatible with
}

/**
 * Constructor type for report generator classes
 */
export interface ReportGeneratorConstructor {
  new (bigQueryClient: BigQueryClient): any;
}

/**
 * Report Registry Service
 * 
 * Manages the registration and retrieval of predefined reports.
 */
export class ReportRegistry {
  private reports: Map<string, { 
    metadata: ReportMetadata;
    generatorClass: ReportGeneratorConstructor;
  }> = new Map();
  
  /**
   * Create a new ReportRegistry
   * @param bigQueryClient BigQuery client for executing queries
   */
  constructor(private bigQueryClient: BigQueryClient) {
    this.registerBuiltInReports();
    logFlow('REPORT_REGISTRY', 'INFO', 'Report Registry initialized', {
      reportCount: this.reports.size,
      reportIds: Array.from(this.reports.keys())
    });
  }
  
  /**
   * Register the built-in reports
   */
  private registerBuiltInReports(): void {
    // Register Inventory Balance Report
    this.registerReport({
      id: 'inventory-balance',
      name: 'Inventory Balance Report',
      description: 'Point-in-time snapshot of current inventory positions including quantity, cost basis, carrying value, and adjustments',
      keywords: ['inventory', 'balance', 'position', 'holdings', 'assets', 'snapshot', 'current', 'portfolio'],
      compatibleSchemaTypes: ['actions'],
      parameters: [
        {
          name: 'asOfDate',
          description: 'The date for which to generate the report',
          type: 'date',
          required: false,
          defaultValue: 'CURRENT_DATE()'
        },
        {
          name: 'assets',
          description: 'Specific assets to include (comma-separated)',
          type: 'string',
          required: false
        },
        {
          name: 'inventory',
          description: 'Filter by specific inventory/account classification',
          type: 'string',
          required: false
        }
      ]
    }, InventoryBalanceGenerator);
    
    // Register Valuation Rollforward Report
    this.registerReport({
      id: 'valuation-rollforward',
      name: 'Valuation Rollforward Report',
      description: 'Period-based rollforward movements showing cost basis, impairment, carrying value, and market value changes',
      keywords: ['valuation', 'rollforward', 'movement', 'changes', 'period', 'cost basis', 'impairment', 'carrying value'],
      compatibleSchemaTypes: ['actions'],
      parameters: [
        {
          name: 'startDate',
          description: 'Start date for the rollforward period',
          type: 'date',
          required: true
        },
        {
          name: 'endDate',
          description: 'End date for the rollforward period',
          type: 'date',
          required: true
        },
        {
          name: 'assets',
          description: 'Specific assets to include (comma-separated)',
          type: 'string',
          required: false
        }
      ]
    }, ValuationRollforwardGenerator);
    
    // Register Lots Report
    this.registerReport({
      id: 'lots-report',
      name: 'Lots Report',
      description: 'Detailed view of individual lots with acquisition date, cost basis, and current valuation',
      keywords: ['lots', 'acquisitions', 'purchases', 'detail', 'fifo', 'lifo', 'tax lots', 'cost basis'],
      compatibleSchemaTypes: ['actions'],
      parameters: [
        {
          name: 'asOfDate',
          description: 'The date for which to generate the report',
          type: 'date',
          required: false,
          defaultValue: 'CURRENT_DATE()'
        },
        {
          name: 'assets',
          description: 'Specific assets to include (comma-separated)',
          type: 'string',
          required: false
        },
        {
          name: 'includeDisposed',
          description: 'Whether to include disposed lots',
          type: 'boolean',
          required: false,
          defaultValue: false
        }
      ]
    }, LotsReportGenerator);
    
    // Register Monthly Activity Report
    this.registerReport({
      id: 'monthly-activity-report',
      name: 'Total Activity Report - By Month and Type',
      description: 'Summarizes transaction activity by month and type, showing counts and amounts for canton transactions',
      keywords: ['activity', 'monthly', 'transactions', 'summary', 'canton', 'wallet', 'operations'],
      compatibleSchemaTypes: ['canton_transaction'],
      parameters: [
        {
          name: 'walletId',
          description: 'The wallet ID to generate the report for',
          type: 'string',
          required: true
        },
        {
          name: 'startDate',
          description: 'Start date for the activity period',
          type: 'date',
          required: true
        },
        {
          name: 'endDate',
          description: 'End date for the activity period',
          type: 'date',
          required: true,
          defaultValue: 'CURRENT_DATE()'
        },
        {
          name: 'assets',
          description: 'Specific assets to include (comma-separated)',
          type: 'string',
          required: false
        },
        {
          name: 'operations',
          description: 'Specific operation types to include (comma-separated)',
          type: 'string',
          required: false
        }
      ]
    }, MonthlyActivityReportGenerator);
  }
  
  /**
   * Register a new report
   * @param metadata Report metadata
   * @param generatorClass Report generator class
   */
  public registerReport(metadata: ReportMetadata, generatorClass: ReportGeneratorConstructor): void {
    this.reports.set(metadata.id, { metadata, generatorClass });
    logFlow('REPORT_REGISTRY', 'INFO', `Registered report: ${metadata.id}`, {
      name: metadata.name,
      parameterCount: metadata.parameters.length
    });
  }
  
  /**
   * Get all registered reports
   * @returns Array of report metadata
   */
  public getAllReports(): ReportMetadata[] {
    return Array.from(this.reports.values()).map(entry => entry.metadata);
  }
  
  /**
   * Get reports compatible with a specific schema type
   * @param schemaType Schema type to filter by
   * @returns Array of report metadata compatible with the specified schema type
   */
  public getReportsForSchemaType(schemaType?: string): ReportMetadata[] {
    if (!schemaType) {
      // If no schema type provided, return all reports
      return this.getAllReports();
    }
    
    // Filter reports by compatibility with the specified schema type
    return this.getAllReports().filter(report => 
      // Include reports that don't specify compatibility (assumed compatible with all)
      !report.compatibleSchemaTypes || 
      // Or reports that explicitly list this schema type
      report.compatibleSchemaTypes.includes(schemaType)
    );
  }
  
  /**
   * Get a report by ID
   * @param id Report ID
   * @returns Report metadata and generator instance, or null if not found
   */
  public getReportById(id: string): { metadata: ReportMetadata; generator: any } | null {
    const entry = this.reports.get(id);
    if (!entry) {
      logFlow('REPORT_REGISTRY', 'INFO', `Report not found: ${id}`);
      return null;
    }
    
    try {
      const generator = new entry.generatorClass(this.bigQueryClient);
      return { metadata: entry.metadata, generator };
    } catch (error) {
      logFlow('REPORT_REGISTRY', 'ERROR', `Error creating generator for report: ${id}`, error);
      return null;
    }
  }
  
  /**
   * Search for reports by query
   * @param query Search query
   * @returns Array of matching report metadata
   */
  public searchReports(query: string): ReportMetadata[] {
    const normalizedQuery = query.toLowerCase();
    
    return this.getAllReports().filter(report => {
      // Match on name, description, or keywords
      return (
        report.name.toLowerCase().includes(normalizedQuery) ||
        report.description.toLowerCase().includes(normalizedQuery) ||
        report.keywords.some(keyword => keyword.toLowerCase().includes(normalizedQuery))
      );
    });
  }
}
