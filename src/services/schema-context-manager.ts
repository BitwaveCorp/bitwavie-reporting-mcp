/**
 * Schema Context Manager Service
 * 
 * Manages schema context information throughout the query lifecycle,
 * ensuring that schemaType is properly propagated between components,
 * particularly during analyze mode.
 * 
 * This is an additive, non-disruptive enhancement to ensure schema-aware
 * query analysis without modifying existing functionality.
 */

import { v4 as uuidv4 } from 'uuid';
import { logFlow } from '../utils/logging.js';

export interface SchemaContext {
  schemaType: string;
  projectId?: string | undefined;
  datasetId?: string | undefined;
  tableId?: string | undefined;
  timestamp: number;
}

export class SchemaContextManager {
  private static instance: SchemaContextManager;
  private contexts: Map<string, SchemaContext>;
  private cleanupInterval: NodeJS.Timeout | null = null;
  private readonly expirationMs: number = 30 * 60 * 1000; // 30 minutes

  private constructor() {
    this.contexts = new Map<string, SchemaContext>();
    this.startCleanupTimer();
  }

  /**
   * Get the singleton instance of SchemaContextManager
   */
  public static getInstance(): SchemaContextManager {
    if (!SchemaContextManager.instance) {
      SchemaContextManager.instance = new SchemaContextManager();
    }
    return SchemaContextManager.instance;
  }

  /**
   * Create a new schema context and return its ID
   * @param schemaType The schema type to store
   * @param projectId Optional project ID
   * @param datasetId Optional dataset ID
   * @param tableId Optional table ID
   * @returns The context ID
   */
  public createContext(
    schemaType: string,
    projectId?: string,
    datasetId?: string,
    tableId?: string
  ): string {
    const contextId = uuidv4();
    
    // Create context with only the required properties first
    const context = {
      schemaType,
      timestamp: Date.now()
    } as SchemaContext;
    
    // Add optional properties only if they are defined
    if (projectId) context.projectId = projectId;
    if (datasetId) context.datasetId = datasetId;
    if (tableId) context.tableId = tableId;

    this.contexts.set(contextId, context);

    logFlow('SCHEMA_CONTEXT', 'INFO', 'Created schema context', {
      contextId,
      schemaType,
      projectId: projectId || 'Not provided',
      datasetId: datasetId || 'Not provided',
      tableId: tableId || 'Not provided'
    });

    return contextId;
  }

  /**
   * Retrieve a schema context by its ID
   * @param contextId The context ID
   * @returns The schema context or undefined if not found
   */
  public getContext(contextId: string): SchemaContext | undefined {
    const context = this.contexts.get(contextId);
    
    logFlow('SCHEMA_CONTEXT', 'INFO', 'Retrieved schema context', {
      contextId,
      found: !!context,
      schemaType: context?.schemaType || 'Not found'
    });
    
    return context;
  }

  /**
   * Embed a schema context ID in a query string as a SQL comment
   * @param query The query to embed the context ID in
   * @param contextId The context ID to embed
   * @returns The query with the embedded context ID
   */
  public embedContextId(query: string, contextId: string): string {
    return `/* SCHEMA_CONTEXT_ID:${contextId} */\n${query}`;
  }

  /**
   * Extract a schema context ID from a query string
   * @param query The query to extract the context ID from
   * @returns The extracted context ID or undefined if not found
   */
  public extractContextId(query: string): string | undefined {
    const match = query.match(/\/\* SCHEMA_CONTEXT_ID:([0-9a-f-]+) \*\//);
    const contextId = match?.[1];
    
    logFlow('SCHEMA_CONTEXT', 'INFO', 'Extracted schema context ID from query', {
      found: !!contextId,
      contextId: contextId || 'Not found'
    });
    
    return contextId;
  }

  /**
   * Start the cleanup timer to remove expired contexts
   */
  private startCleanupTimer(): void {
    if (this.cleanupInterval) {
      clearInterval(this.cleanupInterval);
    }
    
    this.cleanupInterval = setInterval(() => {
      this.cleanupExpiredContexts();
    }, 5 * 60 * 1000); // Run cleanup every 5 minutes
  }

  /**
   * Clean up expired contexts
   */
  private cleanupExpiredContexts(): void {
    const now = Date.now();
    let expiredCount = 0;
    
    for (const [contextId, context] of this.contexts.entries()) {
      if (now - context.timestamp > this.expirationMs) {
        this.contexts.delete(contextId);
        expiredCount++;
      }
    }
    
    if (expiredCount > 0) {
      logFlow('SCHEMA_CONTEXT', 'INFO', `Cleaned up ${expiredCount} expired schema contexts`);
    }
  }
}
