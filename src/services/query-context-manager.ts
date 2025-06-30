import { v4 as uuidv4 } from 'uuid';
import { QueryContext } from '../types/query-context';
import { logFlow } from '../utils/logging.js';

/**
 * QueryContextManager provides a way to store and retrieve query context information
 * throughout the query lifecycle without modifying existing functionality
 */
export class QueryContextManager {
  private contexts: Map<string, QueryContext> = new Map();
  
  /**
   * Creates a new query context and returns the context ID
   * 
   * @param sql The SQL query
   * @param originalPrompt The original prompt sent to LLM (optional)
   * @param userQuery The user's natural language query (optional)
   * @param connectionDetails Connection details (optional)
   * @returns The context ID that can be used to retrieve the context later
   */
  public createContext(
    sql: string,
    originalPrompt: string | undefined,
    userQuery: string | undefined,
    connectionDetails: any | undefined
  ): string {
    const requestId = uuidv4();
    const context: QueryContext = {
      sql,
      originalPrompt,
      userQuery,
      connectionDetails,
      requestId,
      timestamp: Date.now()
    };
    
    this.contexts.set(requestId, context);
    
    logFlow('QUERY_CONTEXT', 'INFO', 'Created query context', {
      requestId,
      hasOriginalPrompt: !!originalPrompt,
      hasUserQuery: !!userQuery,
      sqlLength: sql.length
    });
    
    return requestId;
  }
  
  /**
   * Retrieves a query context by its ID
   * 
   * @param contextId The context ID
   * @returns The query context or undefined if not found
   */
  public getContext(contextId: string): QueryContext | undefined {
    const context = this.contexts.get(contextId);
    
    logFlow('QUERY_CONTEXT', 'INFO', 'Retrieved query context', {
      requestId: contextId,
      found: !!context,
      hasOriginalPrompt: !!context?.originalPrompt,
      hasUserQuery: !!context?.userQuery
    });
    
    return context;
  }
  
  /**
   * Adds a context ID comment to SQL
   * 
   * @param sql The SQL query
   * @param contextId The context ID
   * @returns SQL with context ID comment
   */
  public addContextIdToSql(sql: string, contextId: string): string {
    return `-- QUERY_CONTEXT_ID: ${contextId}\n${sql}`;
  }
  
  /**
   * Extracts context ID from SQL with comment
   * 
   * @param sql SQL potentially containing a context ID comment
   * @returns The context ID if found, undefined otherwise
   */
  public extractContextIdFromSql(sql: string): string | undefined {
    const match = sql.match(/-- QUERY_CONTEXT_ID: ([a-f0-9-]+)/i);
    return match ? match[1] : undefined;
  }
  
  /**
   * Cleans up old contexts (older than 1 hour)
   * This should be called periodically to prevent memory leaks
   */
  public cleanupOldContexts(): void {
    const oneHourAgo = Date.now() - 60 * 60 * 1000;
    let removedCount = 0;
    
    for (const [contextId, context] of this.contexts.entries()) {
      if (context.timestamp < oneHourAgo) {
        this.contexts.delete(contextId);
        removedCount++;
      }
    }
    
    if (removedCount > 0) {
      logFlow('QUERY_CONTEXT', 'INFO', 'Cleaned up old contexts', {
        removedCount,
        remainingCount: this.contexts.size
      });
    }
  }
}
