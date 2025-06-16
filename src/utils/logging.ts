/**
 * Logging Utilities
 * 
 * Provides consistent logging functions for the application.
 */

/**
 * Enhanced logging function with timestamps and flow tracking
 * @param stage The current processing stage
 * @param direction The log direction (ENTRY, EXIT, ERROR, INFO)
 * @param message The log message
 * @param data Optional data to include in the log
 */
export function logFlow(stage: string, direction: 'ENTRY' | 'EXIT' | 'ERROR' | 'INFO', message: string, data: any = null): void {
  const timestamp = new Date().toISOString();
  const directionSymbol = getDirectionSymbol(direction);
  
  // Format the log message
  let logMessage = `[${timestamp}] ${directionSymbol} ${stage} | ${message}`;
  
  // Add data if provided
  if (data !== null) {
    try {
      // Try to stringify the data, but handle circular references
      const safeData = safeStringify(data);
      logMessage += ` | ${safeData}`;
    } catch (error) {
      logMessage += ` | [Error stringifying data: ${error instanceof Error ? error.message : String(error)}]`;
    }
  }
  
  // Log with appropriate level
  switch (direction) {
    case 'ERROR':
      console.error(logMessage);
      break;
    case 'INFO':
      console.info(logMessage);
      break;
    default:
      console.log(logMessage);
  }
}

/**
 * Get a symbol representing the log direction
 * @param direction The log direction
 * @returns A symbol representing the direction
 */
function getDirectionSymbol(direction: 'ENTRY' | 'EXIT' | 'ERROR' | 'INFO'): string {
  switch (direction) {
    case 'ENTRY':
      return '➡️';
    case 'EXIT':
      return '⬅️';
    case 'ERROR':
      return '❌';
    case 'INFO':
      return 'ℹ️';
    default:
      return '•';
  }
}

/**
 * Safely stringify an object, handling circular references
 * @param obj The object to stringify
 * @returns A JSON string representation of the object
 */
function safeStringify(obj: any): string {
  // Handle primitive types directly
  if (obj === null || obj === undefined || typeof obj !== 'object') {
    return String(obj);
  }
  
  // Use a cache to detect circular references
  const cache: any[] = [];
  
  return JSON.stringify(obj, (key, value) => {
    // Handle circular references
    if (typeof value === 'object' && value !== null) {
      if (cache.includes(value)) {
        return '[Circular Reference]';
      }
      cache.push(value);
    }
    
    // Handle Error objects
    if (value instanceof Error) {
      return {
        name: value.name,
        message: value.message,
        stack: value.stack
      };
    }
    
    // Handle Date objects
    if (value instanceof Date) {
      return value.toISOString();
    }
    
    // Handle functions
    if (typeof value === 'function') {
      return '[Function]';
    }
    
    return value;
  }, 2);
}
