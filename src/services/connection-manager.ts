/**
 * ConnectionManager - Centralized service for managing BigQuery connection details
 * 
 * This singleton class provides a central place to resolve table references and connection details,
 * supporting both environment variables and session-based connection details.
 */

import { logFlow } from "../utils/logging";

export class ConnectionManager {
  private static instance: ConnectionManager;
  
  private constructor() {}
  
  /**
   * Get the singleton instance of ConnectionManager
   */
  public static getInstance(): ConnectionManager {
    if (!ConnectionManager.instance) {
      ConnectionManager.instance = new ConnectionManager();
    }
    return ConnectionManager.instance;
  }
  
  /**
   * Get the project ID from session details or environment variables
   * @param sessionDetails Optional session connection details
   * @returns Project ID string
   */
  public getProjectId(sessionDetails?: any): string {
    logFlow('WALKTHROUGH_SHOWTABLE6A', 'INFO', 'getProjectId', {
      projectId: sessionDetails?.projectId,
      source: sessionDetails ? 'session' : 'environment'
    });
    return sessionDetails?.projectId || process.env.GOOGLE_CLOUD_PROJECT_ID || '';
  }
  
  /**
   * Get the dataset ID from session details or environment variables
   * @param sessionDetails Optional session connection details
   * @returns Dataset ID string
   */
  public getDatasetId(sessionDetails?: any): string {
    logFlow('WALKTHROUGH_SHOWTABLE6A', 'INFO', 'getDatasetId', {
      datasetId: sessionDetails?.datasetId,
      source: sessionDetails ? 'session' : 'environment'
    });
    return sessionDetails?.datasetId || process.env.BIGQUERY_DATASET_ID || '';
  }
  
  /**
   * Get the table ID from session details or environment variables
   * @param sessionDetails Optional session connection details
   * @returns Table ID string
   */
  public getTableId(sessionDetails?: any): string {
    logFlow('WALKTHROUGH_SHOWTABLE6A', 'INFO', 'getTableId', {
      tableId: sessionDetails?.tableId,
      source: sessionDetails ? 'session' : 'environment'
    }); 
    return sessionDetails?.tableId || process.env.BIGQUERY_TABLE_ID || '';
  }
  
  /**
   * Get the private key from session details (no environment fallback for security)
   * @param sessionDetails Optional session connection details
   * @returns Private key string or undefined
   */
  public getPrivateKey(sessionDetails?: any): string | undefined {
    logFlow('WALKTHROUGH_SHOWTABLE6A', 'INFO', 'getPrivateKey', {
      privateKey: sessionDetails?.privateKey,
      source: sessionDetails ? 'session' : 'environment'
    });
    return sessionDetails?.privateKey;
  }
  
  /**
   * Get the fully qualified BigQuery table ID in the format `project.dataset.table`
   * @param sessionDetails Optional session connection details
   * @returns Fully qualified table ID string
   */
  public getFullyQualifiedTableId(sessionDetails?: any): string {
    const projectId = this.getProjectId(sessionDetails);
    const datasetId = this.getDatasetId(sessionDetails);
    const tableId = this.getTableId(sessionDetails);
    return `\`${projectId}.${datasetId}.${tableId}\``;
  }
  
  /**
   * Check if all required connection details are present
   * @param sessionDetails Optional session connection details
   * @returns Boolean indicating if connection is complete
   */
  public hasCompleteConnectionDetails(sessionDetails?: any): boolean {
    return !!(
      this.getProjectId(sessionDetails) && 
      this.getDatasetId(sessionDetails) && 
      this.getTableId(sessionDetails)
    );
  }
  
  /**
   * Log the current connection details (safely - without private key)
   * @param sessionDetails Optional session connection details
   * @returns Object with connection details (excluding private key)
   */
  public logConnectionDetails(sessionDetails?: any): object {
    const details = {
      projectId: this.getProjectId(sessionDetails),
      datasetId: this.getDatasetId(sessionDetails),
      tableId: this.getTableId(sessionDetails),
      hasPrivateKey: !!this.getPrivateKey(sessionDetails),
      source: sessionDetails ? 'session' : 'environment'
    };
    console.log('[ConnectionManager] Current connection details:', details);
    return details;
  }
}
