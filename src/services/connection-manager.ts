/**
 * ConnectionManager - Centralized service for managing BigQuery connection details
 * 
 * This singleton class provides a central place to resolve table references and connection details,
 * supporting both environment variables and session-based connection details.
 */

import { logFlow } from "../utils/logging.js";

// Define the global function type
declare global {
  var getSessionConnectionDetails: () => any;
}

export class ConnectionManager {
  private static instance: ConnectionManager;
  
  private constructor() {}
  
  /**
   * Get connection details from session storage
   * @returns Connection details from session storage or null if not available
   */
  public getSessionConnectionDetails(): any {
    try {
      console.log('ALTERNATE_DATASOURCE13: ConnectionManager.getSessionConnectionDetails called');
      
      if (global.getSessionConnectionDetails) {
        console.log('ALTERNATE_DATASOURCE14: global.getSessionConnectionDetails function exists');
        
        const sessionDetails = global.getSessionConnectionDetails();
        console.log('ALTERNATE_DATASOURCE15: Session details retrieved', {
          hasSessionDetails: !!sessionDetails,
          projectId: sessionDetails?.projectId ? 'present' : 'not present',
          datasetId: sessionDetails?.datasetId ? 'present' : 'not present',
          tableId: sessionDetails?.tableId ? 'present' : 'not present',
          hasPrivateKey: !!sessionDetails?.privateKey
        });
        
        logFlow('CONNECTION_MANAGER', 'INFO', 'Retrieved session connection details', {
          hasSessionDetails: !!sessionDetails,
          projectId: sessionDetails?.projectId ? 'present' : 'not present',
          datasetId: sessionDetails?.datasetId ? 'present' : 'not present',
          tableId: sessionDetails?.tableId ? 'present' : 'not present',
          hasPrivateKey: !!sessionDetails?.privateKey
        });
        
        return sessionDetails;
      } else {
        console.log('ALTERNATE_DATASOURCE16: global.getSessionConnectionDetails function does not exist');
      }
    } catch (error) {
      console.log('ALTERNATE_DATASOURCE17: Error getting session connection details', error);
      logFlow('CONNECTION_MANAGER', 'ERROR', 'Error getting session connection details', error);
    }
    
    console.log('ALTERNATE_DATASOURCE18: Returning null from getSessionConnectionDetails');
    return null;
  }
  
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
    // Try to get session details if not provided
    const details = sessionDetails || this.getSessionConnectionDetails();
    
    logFlow('WALKTHROUGH_SHOWTABLE6A', 'INFO', 'getProjectId', {
      projectId: details?.projectId,
      source: details ? 'session' : 'environment'
    });
    return details?.projectId || process.env.GOOGLE_CLOUD_PROJECT_ID || '';
  }
  
  /**
   * Get the dataset ID from session details or environment variables
   * @param sessionDetails Optional session connection details
   * @returns Dataset ID string
   */
  public getDatasetId(sessionDetails?: any): string {
    // Try to get session details if not provided
    const details = sessionDetails || this.getSessionConnectionDetails();
    
    logFlow('WALKTHROUGH_SHOWTABLE6A', 'INFO', 'getDatasetId', {
      datasetId: details?.datasetId,
      source: details ? 'session' : 'environment'
    });
    return details?.datasetId || process.env.BIGQUERY_DATASET_ID || '';
  }
  
  /**
   * Get the table ID from session details or environment variables
   * @param sessionDetails Optional session connection details
   * @returns Table ID string
   */
  public getTableId(sessionDetails?: any): string {
    // Try to get session details if not provided
    const details = sessionDetails || this.getSessionConnectionDetails();
    
    logFlow('WALKTHROUGH_SHOWTABLE6A', 'INFO', 'getTableId', {
      tableId: details?.tableId,
      source: details ? 'session' : 'environment'
    });
    return details?.tableId || process.env.BIGQUERY_TABLE_ID || '';
  }
  

  
  /**
   * Get the private key from session details (no environment fallback for security)
   * @param sessionDetails Optional session connection details
   * @returns Private key string or undefined
   */
  public getPrivateKey(sessionDetails?: any): string | undefined {
    // Try to get session details if not provided
    const details = sessionDetails || this.getSessionConnectionDetails();
    
    // First check if privateKey is directly in the details object
    // Then check if it's in the session object structure (req.session.privateKey)
    const privateKey = details?.privateKey || details?.session?.privateKey;
    
    logFlow('WALKTHROUGH_SHOWTABLE6A', 'INFO', 'getPrivateKey', {
      hasPrivateKey: !!privateKey,
      privateKeySource: privateKey ? (details?.privateKey ? 'details.privateKey' : 'session.privateKey') : 'not found',
      source: details ? 'session' : 'environment'
    });
    
    return privateKey;
  }
  
  /**
   * Get the fully qualified BigQuery table ID in the format `project.dataset.table`
   * @param sessionDetails Optional session connection details
   * @returns Fully qualified table ID string
   */
  public getFullyQualifiedTableId(sessionDetails?: any): string {
    // Try to get session details if not provided
    const details = sessionDetails || this.getSessionConnectionDetails();
    
    const projectId = this.getProjectId(details);
    const datasetId = this.getDatasetId(details);
    const tableId = this.getTableId(details);
    return `\`${projectId}.${datasetId}.${tableId}\``;
  }
  
  /**
   * Check if all required connection details are present
   * @param sessionDetails Optional session connection details
   * @returns Boolean indicating if connection is complete
   */
  public hasCompleteConnectionDetails(sessionDetails?: any): boolean {
    // Try to get session details if not provided
    const sessionData = sessionDetails || this.getSessionConnectionDetails();
    
    return !!(
      this.getProjectId(sessionData) && 
      this.getDatasetId(sessionData) && 
      this.getTableId(sessionData)
    );
  }
  
  /**
   * Log the current connection details (safely - without private key)
   * @param sessionDetails Optional session connection details
   * @returns Object with safe connection details
   */
  public logConnectionDetails(sessionDetails?: any): object {
    // Try to get session details if not provided
    const sessionData = sessionDetails || this.getSessionConnectionDetails();
    
    // Get values from session or environment
    const projectId = this.getProjectId(sessionData);
    const datasetId = this.getDatasetId(sessionData);
    const tableId = this.getTableId(sessionData);
    const privateKey = this.getPrivateKey(sessionData);
    
    // Determine the source of each value
    const projectIdSource = sessionData?.projectId ? 'session' : 'environment';
    const datasetIdSource = sessionData?.datasetId ? 'session' : 'environment';
    const tableIdSource = sessionData?.tableId ? 'session' : 'environment';
    const privateKeySource = privateKey ? 'session' : 'none';
    
    // Create detailed object for logging
    const connectionDetails = {
      projectId,
      datasetId,
      tableId,
      hasPrivateKey: !!privateKey,
      source: sessionDetails && (sessionDetails.projectId || sessionDetails.datasetId || sessionDetails.tableId || privateKey) ? 'session' : 'environment',
      detailedSources: {
        projectId: projectIdSource,
        datasetId: datasetIdSource,
        tableId: tableIdSource,
        privateKey: privateKeySource
      },
      sessionDetailsProvided: !!sessionDetails,
      sessionHasConnectionDetails: !!sessionDetails?.connectionDetails,
      sessionHasPrivateKey: !!sessionDetails?.privateKey || !!sessionDetails?.session?.privateKey
    };
    
    console.log('[ConnectionManager] Current connection details:', connectionDetails);
    return connectionDetails;
  }
}
