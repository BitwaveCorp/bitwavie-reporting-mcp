/**
 * Query Confirmation Formatter Service
 * 
 * Formats query interpretations for user confirmation.
 * Presents a clear, structured interpretation of the query
 * separating filter logic from aggregation operations.
 */

import { logFlow } from '../utils/logging.js';
import { TranslationResult } from './llm-query-translator.js';

// Types
export interface ConfirmationOptions {
  includeSQL?: boolean; // Whether to include the raw SQL in the confirmation
  includeSampleData?: boolean; // Whether to include sample data in the confirmation
  suggestAlternatives?: boolean; // Whether to suggest alternative interpretations
}

export interface ConfirmationResponse {
  content: Array<{
    type: string;
    text: string;
  }>;
  needsConfirmation: boolean;
}

export class QueryConfirmationFormatter {
  private options: ConfirmationOptions = {
    includeSQL: false,
    includeSampleData: false,
    suggestAlternatives: true
  };
  
  constructor(options?: ConfirmationOptions) {
    if (options) {
      this.options = { ...this.options, ...options };
    }
    
    logFlow('CONFIRMATION_FORMATTER', 'INFO', 'Query Confirmation Formatter initialized', this.options);
  }
  
  /**
   * Format a translation result for user confirmation
   * @param translationResult The translation result from LLMQueryTranslator
   * @returns Formatted confirmation response
   */
  public formatConfirmation(translationResult: TranslationResult): ConfirmationResponse {
    logFlow('CONFIRMATION_FORMATTER', 'ENTRY', 'Formatting confirmation', {
      originalQuery: translationResult.originalQuery,
      confidence: translationResult.confidence
    });
    
    // Start with the interpreted query
    let confirmationText = `${translationResult.interpretedQuery}\n\n`;
    
    // Add filter operations (population definition)
    if (translationResult.components.filterOperations.description) {
      confirmationText += `**Identify data where:**\n`;
      
      // Format the filter description with bullet points
      const filterDesc = translationResult.components.filterOperations.description;
      
      // Check if the filter description already contains bullet points or line breaks
      if (filterDesc.includes('\n') || filterDesc.includes('- ')) {
        confirmationText += filterDesc;
      } else {
        // Add a single bullet point if it's a simple filter
        confirmationText += `- ${filterDesc}`;
      }
      
      confirmationText += '\n\n';
    }
    
    // Add aggregation operations (results generation)
    if (translationResult.components.aggregationOperations.description) {
      confirmationText += `**Calculate and show:**\n`;
      confirmationText += `- ${translationResult.components.aggregationOperations.description}\n`;
      
      // Add grouping information if present
      if (translationResult.components.groupByOperations.description) {
        confirmationText += `- Broken down by: ${translationResult.components.groupByOperations.description}\n`;
      }
      
      // Add sorting information if present
      if (translationResult.components.orderByOperations.description) {
        confirmationText += `- Sorted by: ${translationResult.components.orderByOperations.description}\n`;
      }
      
      // Add limit information if present
      if (translationResult.components.limitOperations.description) {
        confirmationText += `- ${translationResult.components.limitOperations.description}\n`;
      }
      
      confirmationText += '\n';
    }
    
    // Include SQL if enabled
    if (this.options.includeSQL) {
      confirmationText += `**SQL Query:**\n\`\`\`sql\n${translationResult.sql}\n\`\`\`\n\n`;
    }
    
    // Add confidence level indicator
    const confidenceLevel = this.getConfidenceLevelDescription(translationResult.confidence);
    confirmationText += `Confidence: ${confidenceLevel}\n\n`;
    
    // Add alternative interpretations if available and enabled
    if (this.options.suggestAlternatives && 
        translationResult.alternativeInterpretations && 
        translationResult.alternativeInterpretations.length > 0) {
      confirmationText += `**Alternative interpretations:**\n`;
      
      translationResult.alternativeInterpretations.forEach((alt, index) => {
        confirmationText += `${index + 1}. ${alt}\n`;
      });
      
      confirmationText += '\n';
    }
    
    // Add confirmation prompt
    confirmationText += `Is this interpretation correct? You can:\n`;
    confirmationText += `1. Confirm by saying "yes" or "correct"\n`;
    confirmationText += `2. Modify specific parts (e.g., "change the date range to last 30 days")\n`;
    confirmationText += `3. Provide a completely new query\n`;
    
    logFlow('CONFIRMATION_FORMATTER', 'EXIT', 'Confirmation formatting completed', {
      textLength: confirmationText.length
    });
    
    return {
      content: [{
        type: 'text',
        text: confirmationText
      }],
      needsConfirmation: true
    };
  }
  
  /**
   * Format an error response for user confirmation
   * @param originalQuery The original query
   * @param errorMessage The error message
   * @param sql The SQL that caused the error (optional)
   * @returns Formatted error response
   */
  public formatErrorResponse(
    originalQuery: string,
    errorMessage: string,
    sql?: string
  ): ConfirmationResponse {
    logFlow('CONFIRMATION_FORMATTER', 'ENTRY', 'Formatting error response', {
      originalQuery,
      errorMessage
    });
    
    let errorText = `I encountered an error while processing your query: "${originalQuery}"\n\n`;
    errorText += `**Error:** ${errorMessage}\n\n`;
    
    // Include SQL if available and enabled
    if (sql && this.options.includeSQL) {
      errorText += `**SQL Query that caused the error:**\n\`\`\`sql\n${sql}\n\`\`\`\n\n`;
    }
    
    // Add guidance for the user
    errorText += `You can:\n`;
    errorText += `1. Try rephrasing your query to be more specific\n`;
    errorText += `2. Provide more context about what you're looking for\n`;
    errorText += `3. Ask for help with a simpler query first\n`;
    
    logFlow('CONFIRMATION_FORMATTER', 'EXIT', 'Error response formatting completed', {
      textLength: errorText.length
    });
    
    return {
      content: [{
        type: 'text',
        text: errorText
      }],
      needsConfirmation: true
    };
  }
  
  /**
   * Format column selection options for user confirmation
   * @param columns Available columns
   * @param query The original query
   * @param message Optional message to include
   * @returns Formatted column selection response
   */
  public formatColumnSelectionOptions(
    columns: string[],
    query: string,
    message?: string
  ): ConfirmationResponse {
    logFlow('CONFIRMATION_FORMATTER', 'ENTRY', 'Formatting column selection options', {
      columnCount: columns.length,
      query
    });
    
    let selectionText = message || `I'm not sure which columns you want to analyze for: "${query}"\n\n`;
    selectionText += `Please select from these available columns:\n\n`;
    
    // Group columns by category for better organization
    const categories: Record<string, string[]> = {
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
    columns.forEach(col => {
      const name = col.toLowerCase();
      
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
    
    // Format each category
    let index = 1;
    Object.entries(categories).forEach(([category, cols]) => {
      if (cols.length === 0) return;
      
      selectionText += `**${category} Columns:**\n`;
      
      cols.forEach(col => {
        selectionText += `${index}. ${col}\n`;
        index++;
      });
      
      selectionText += '\n';
    });
    
    // Add guidance for the user
    selectionText += `You can:\n`;
    selectionText += `1. Enter the number of the column you want to use\n`;
    selectionText += `2. Type "use [column name]" to select a specific column\n`;
    selectionText += `3. Provide a new query that specifies the column\n`;
    
    logFlow('CONFIRMATION_FORMATTER', 'EXIT', 'Column selection options formatting completed', {
      textLength: selectionText.length
    });
    
    return {
      content: [{
        type: 'text',
        text: selectionText
      }],
      needsConfirmation: true
    };
  }
  
  /**
   * Get a description of the confidence level
   * @param confidence Confidence score (0-1)
   * @returns Description of confidence level
   */
  private getConfidenceLevelDescription(confidence: number): string {
    if (confidence >= 0.9) {
      return "Very High";
    } else if (confidence >= 0.75) {
      return "High";
    } else if (confidence >= 0.5) {
      return "Moderate";
    } else if (confidence >= 0.25) {
      return "Low";
    } else {
      return "Very Low";
    }
  }
}
