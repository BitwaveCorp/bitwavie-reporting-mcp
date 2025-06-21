/**
 * Session data types for BigQuery connection management
 */
// We need to import the module first to be able to augment it
import * as express from 'express';
import session from 'express-session';

// Connection details stored in session
export interface ConnectionDetails {
  projectId: string;
  datasetId: string;
  tableId: string;
  isConnected: boolean;
}

// Request to validate connection details
export interface ValidateConnectionRequest {
  projectId: string;
  datasetId: string;
  tableId: string;
  privateKey: string;
}

// Response from connection validation
export interface ValidateConnectionResponse {
  success: boolean;
  message: string;
  connectionDetails?: {
    projectId: string;
    datasetId: string;
    tableId: string;
  };
}

// Request to validate admin key
export interface AdminKeyRequest {
  adminKey: string;
}

// Response from admin operations
export interface AdminResponse {
  success: boolean;
  message: string;
}

// Response with table mappings
export interface MappingsResponse {
  success: boolean;
  message?: string;
  mappings?: Array<{
    name: string;
    projectId: string;
    datasetId: string;
    tableId: string;
  }>;
}

// Extend Express.Session interface to include our custom properties
declare module 'express-session' {
  interface Session {
    connectionDetails?: ConnectionDetails;
    isAdmin?: boolean;
  }
}
