import { ConnectionDetails } from './session-types';

/**
 * QueryContext holds all context information for a query throughout its lifecycle
 * This is used specifically for enhanced error correction without changing existing functionality
 */
export interface QueryContext {
  sql: string;
  originalPrompt: string | undefined;
  userQuery: string | undefined;
  connectionDetails: ConnectionDetails | undefined;
  requestId: string;
  timestamp: number;
}
