/**
 * Result Formatter Service
 * 
 * Formats query results for user consumption.
 * Provides performance metrics and visualization hints.
 */

import { logFlow } from '../utils/logging.js';
import { ExecutionResult } from './query-executor.js';
import { TranslationResult } from './llm-query-translator.js';

// Types
export interface FormattingOptions {
  maxRows?: number; // Default: 100
  includePerformanceMetrics?: boolean; // Default: true
  suggestVisualizations?: boolean; // Default: true
  formatNumbers?: boolean; // Default: true
  formatDates?: boolean; // Default: true
}

export interface FormattedResult {
  content: Array<{
    type: string;
    text?: string;
    // Remove table type as we're moving away from this format
  }>;
  // Enhanced rawData field for direct JSON access
  rawData?: {
    headers: string[];
    rows: any[];
    displayRows: number;   // Number of rows to display in UI (max 100)
    truncated: boolean;    // Whether the display data was truncated
    exceedsDownloadLimit: boolean; // Whether the data exceeds 5000 row download limit
  };
  metadata: {
    rowCount: number;
    totalRows: number;
    executionTimeMs: number;
    bytesProcessed?: number;
    visualizationHint?: string;
  };
}

export class ResultFormatter {
  private options: FormattingOptions = {
    maxRows: 100,
    includePerformanceMetrics: true,
    suggestVisualizations: true,
    formatNumbers: true,
    formatDates: true
  };
  
  constructor(options?: FormattingOptions) {
    if (options) {
      this.options = { ...this.options, ...options };
    }
    
    logFlow('RESULT_FORMATTER', 'INFO', 'Result Formatter initialized', this.options);
  }
  
  /**
   * Format query results for presentation
   * @param executionResult The execution result from QueryExecutor
   * @param translationResult Optional translation result for context
   * @returns Formatted result for presentation
   */
  public formatResults(
    executionResult: ExecutionResult,
    translationResult?: TranslationResult
  ): FormattedResult {
    logFlow('RESULT_FORMATTER', 'ENTRY', 'Formatting results', {
      success: executionResult.success,
      rowCount: executionResult.data?.length || 0,
      executionTimeMs: executionResult.metadata?.executionTimeMs || 0
    });
    
    // If execution failed, format the error
    if (!executionResult.success || !executionResult.data) {
      return this.formatError(executionResult);
    }
    
    const data = executionResult.data;
    const totalRows = data.length;
    const displayRows = Math.min(totalRows, this.options.maxRows || 100);
    
    // Extract column headers
    const headers = Object.keys(data[0] || {});
    
    // Format the rows
    const rows = data.slice(0, displayRows).map(row => {
      return headers.map(header => {
        const value = row[header];
        
        // Format numbers if enabled
        if (this.options.formatNumbers && typeof value === 'number') {
          // Format currency values
          if (header.toLowerCase().includes('value') || 
              header.toLowerCase().includes('price') || 
              header.toLowerCase().includes('cost') || 
              header.toLowerCase().includes('fee') ||
              header.toLowerCase().includes('gain') ||
              header.toLowerCase().includes('loss')) {
            return this.formatCurrency(value);
          }
          
          // Format percentages
          if (header.toLowerCase().includes('percent') || 
              header.toLowerCase().includes('rate') ||
              header.toLowerCase().includes('ratio')) {
            return this.formatPercentage(value);
          }
          
          // Format other numbers
          return this.formatNumber(value);
        }
        
        // Format dates if enabled
        if (this.options.formatDates && 
            (header.toLowerCase().includes('date') || 
             header.toLowerCase().includes('time') ||
             header.toLowerCase().includes('timestamp'))) {
          if (value instanceof Date) {
            return this.formatDate(value);
          } else if (typeof value === 'string' && /^\d{4}-\d{2}-\d{2}/.test(value)) {
            return this.formatDate(new Date(value));
          }
        }
        
        // Return value as is for other types
        return value;
      });
    });
    
    // Create the formatted result with only rawData for tables
    const formattedResult: FormattedResult = {
      // Keep content array for text messages, but don't include table data here
      content: [],
      
      // Use rawData as the primary data format for tables
      rawData: {
        headers,
        rows: executionResult.data || [],
        // Limit displayed rows in UI to 100
        displayRows: Math.min(displayRows, 100),
        // Indicate if data was truncated
        truncated: displayRows > 100,
        // Flag if dataset exceeds download limit
        exceedsDownloadLimit: totalRows > 5000
      },
      
      metadata: {
        rowCount: displayRows,
        totalRows,
        // Ensure we handle null metadata object completely
        executionTimeMs: executionResult.metadata ? (executionResult.metadata.executionTimeMs || 0) : 0,
        ...(executionResult.metadata && executionResult.metadata.bytesProcessed !== undefined && {
          bytesProcessed: executionResult.metadata.bytesProcessed
        })
      }
    };
    
    // Add performance metrics if enabled
    if (this.options.includePerformanceMetrics) {
      let metricsText = '';
      
      // Add row count information
      if (totalRows > displayRows) {
        metricsText += `Showing ${displayRows} of ${totalRows} rows. `;
      } else {
        metricsText += `${totalRows} rows returned. `;
      }
      
      // Add execution time
      const executionTimeSeconds = executionResult.metadata.executionTimeMs / 1000;
      metricsText += `Query executed in ${executionTimeSeconds.toFixed(2)} seconds. `;
      
      // Add bytes processed if available
      if (executionResult.metadata.bytesProcessed) {
        const bytesProcessedMB = executionResult.metadata.bytesProcessed / (1024 * 1024);
        metricsText += `${bytesProcessedMB.toFixed(2)} MB processed.`;
      }
      
      // Add metrics to content
      formattedResult.content.push({
        type: 'text',
        text: metricsText
      });
    }
    
    // Add visualization hint if enabled
    if (this.options.suggestVisualizations) {
      const visualizationHint = this.suggestVisualization(headers, data);
      
      if (visualizationHint) {
        formattedResult.metadata.visualizationHint = visualizationHint;
        
        // Add visualization hint to content
        formattedResult.content.push({
          type: 'text',
          text: `**Visualization Suggestion:** ${visualizationHint}`
        });
      }
    }
    
    // Log the actual query results for debugging
    const resultPreview = data.map(row => {
      // Create a simplified version of each row for logging
      const simplifiedRow: Record<string, any> = {};
      headers.forEach(header => {
        simplifiedRow[header] = row[header];
      });
      return simplifiedRow;
    });
    
    logFlow('RESULT_FORMATTER', 'EXIT', 'Results formatting completed', {
      contentItems: formattedResult.content.length,
      displayedRows: displayRows,
      totalRows,
      resultPreview: JSON.stringify(resultPreview)
    });
    
    return formattedResult;
  }
  
  /**
   * Format an error result
   * @param executionResult The failed execution result
   * @returns Formatted error result
   */
  private formatError(executionResult: ExecutionResult): FormattedResult {
    logFlow('RESULT_FORMATTER', 'ENTRY', 'Formatting error', {
      errorMessage: executionResult.error?.message
    });
    
    // Create error message
    let errorText = `**Query Execution Error**\n\n`;
    errorText += `${executionResult.error?.message}\n\n`;
    
    // Add details if available
    if (executionResult.error?.details) {
      errorText += `**Details:** ${executionResult.error.details}\n\n`;
    }
    
    // Add retry information
    if (executionResult.metadata && executionResult.metadata.retryCount && executionResult.metadata.retryCount > 0) {
      errorText += `Attempted ${executionResult.metadata.retryCount} automatic corrections without success.\n\n`;
    }
    
    // Add suggestions for fixing the error
    errorText += `**Suggestions:**\n`;
    errorText += `- Try simplifying your query\n`;
    errorText += `- Check column names and data types\n`;
    errorText += `- Ensure your filters use valid values\n`;
    
    // Create the formatted result
    const formattedResult: FormattedResult = {
      content: [
        {
          type: 'text',
          text: errorText
        }
      ],
      metadata: {
        rowCount: 0,
        totalRows: 0,
        executionTimeMs: executionResult.metadata ? (executionResult.metadata.executionTimeMs || 0) : 0
      }
    };
    
    logFlow('RESULT_FORMATTER', 'EXIT', 'Error formatting completed');
    
    return formattedResult;
  }
  
  /**
   * Format a number for display
   * @param value The number to format
   * @returns Formatted number string
   */
  private formatNumber(value: number): string {
    // Use Intl.NumberFormat for locale-aware formatting
    return new Intl.NumberFormat('en-US', {
      maximumFractionDigits: 2
    }).format(value);
  }
  
  /**
   * Format a currency value for display
   * @param value The currency value to format
   * @returns Formatted currency string
   */
  private formatCurrency(value: number): string {
    // Use Intl.NumberFormat for locale-aware currency formatting
    return new Intl.NumberFormat('en-US', {
      style: 'currency',
      currency: 'USD',
      minimumFractionDigits: 2,
      maximumFractionDigits: 2
    }).format(value);
  }
  
  /**
   * Format a percentage value for display
   * @param value The percentage value to format
   * @returns Formatted percentage string
   */
  private formatPercentage(value: number): string {
    // Convert to percentage and format
    const percentage = value * (value < 10 ? 100 : 1); // Assume values < 10 are decimals needing conversion
    
    return new Intl.NumberFormat('en-US', {
      style: 'percent',
      minimumFractionDigits: 2,
      maximumFractionDigits: 2
    }).format(percentage / 100);
  }
  
  /**
   * Format a date for display
   * @param value The date to format
   * @returns Formatted date string
   */
  private formatDate(value: Date): string {
    // Use Intl.DateTimeFormat for locale-aware date formatting
    return new Intl.DateTimeFormat('en-US', {
      year: 'numeric',
      month: 'short',
      day: 'numeric',
      hour: 'numeric',
      minute: 'numeric'
    }).format(value);
  }
  
  /**
   * Suggest an appropriate visualization based on the data
   * @param headers Column headers
   * @param data Query result data
   * @returns Visualization suggestion
   */
  private suggestVisualization(headers: string[], data: any[]): string | undefined {
    if (data.length === 0) {
      return undefined;
    }
    
    // Check for time series data
    const hasTimeColumn = headers.some(header => 
      header.toLowerCase().includes('date') || 
      header.toLowerCase().includes('time') ||
      header.toLowerCase().includes('timestamp')
    );
    
    // Check for numeric columns
    const numericColumns = headers.filter(header => {
      const sample = data[0][header];
      return typeof sample === 'number';
    });
    
    // Check for categorical columns
    const categoricalColumns = headers.filter(header => {
      const sample = data[0][header];
      return typeof sample === 'string' && !header.toLowerCase().includes('date');
    });
    
    // Suggest visualizations based on data characteristics
    
    // Time series with numeric values
    if (hasTimeColumn && numericColumns.length > 0) {
      return "Line chart showing trends over time";
    }
    
    // Single categorical column with numeric values
    if (categoricalColumns.length === 1 && numericColumns.length === 1) {
      return data.length > 10 ? "Bar chart" : "Column chart";
    }
    
    // Multiple numeric columns
    if (numericColumns.length > 1) {
      return "Multi-series bar chart or stacked column chart";
    }
    
    // Single numeric column with many rows
    if (numericColumns.length === 1 && data.length > 20) {
      return "Histogram showing distribution";
    }
    
    // Two numeric columns
    if (numericColumns.length === 2) {
      return "Scatter plot";
    }
    
    // Single categorical column with many distinct values
    if (categoricalColumns.length === 1) {
      const columnName = categoricalColumns[0];
      // Make sure the column name exists before using it as an index
      if (columnName !== undefined) {
        const distinctValues = new Set(data.map(row => row[columnName])).size;
        if (distinctValues > 10) {
          return "Treemap or pie chart";
        }
      }
    }
    
    // Default to table view
    return "Table view (current)";
  }
}
