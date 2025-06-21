/**
 * Connection Handler Service
 * 
 * Handles:
 * - BigQuery connection validation
 * - Admin key validation
 * - Table mapping management
 */

import { BigQuery } from '@google-cloud/bigquery';
import { 
  ValidateConnectionRequest, 
  ValidateConnectionResponse,
  AdminKeyRequest,
  AdminResponse,
  MappingsResponse
} from '../types/session-types.js';
import { 
  validateTableMapping, 
  validateAdminKey, 
  addTableMapping, 
  removeTableMapping, 
  getTableMappings 
} from '../utils/table-mapping-utils.js';
import { logFlow } from '../utils/logging.js';

/**
 * Validates connection details and tests the connection
 */
export async function validateConnection(
  request: ValidateConnectionRequest
): Promise<ValidateConnectionResponse> {
  try {
    logFlow('CONNECTION-HANDLER', 'INFO', 'Validating connection', {
      projectId: request.projectId,
      datasetId: request.datasetId,
      tableId: request.tableId
    });

    // Validate required fields
    if (!request.projectId || !request.datasetId || !request.tableId || !request.privateKey) {
      return {
        success: false,
        message: 'Missing required fields: projectId, datasetId, tableId, and privateKey are required'
      };
    }

    // Validate mapping exists and private key matches
    const isValidMapping = await validateTableMapping(
      request.projectId,
      request.datasetId,
      request.tableId,
      request.privateKey
    );

    if (!isValidMapping) {
      return {
        success: false,
        message: 'Invalid connection details or private key'
      };
    }

    // Test the connection by running a simple query
    try {
      // Parse the private key JSON
      let privateKeyJson;
      try {
        privateKeyJson = JSON.parse(request.privateKey);
      } catch (error) {
        return {
          success: false,
          message: 'Invalid private key JSON format'
        };
      }

      // Create BigQuery client with the provided credentials
      const bigquery = new BigQuery({
        projectId: request.projectId,
        credentials: privateKeyJson
      });

      // Run a simple query to test the connection
      const query = `SELECT * FROM \`${request.projectId}.${request.datasetId}.${request.tableId}\` LIMIT 1`;
      
      logFlow('CONNECTION-HANDLER', 'INFO', 'Testing connection with query', { query });
      
      const [rows] = await bigquery.query({ query });
      
      logFlow('CONNECTION-HANDLER', 'INFO', 'Connection test successful', {
        rowCount: rows.length
      });

      // Connection successful
      return {
        success: true,
        message: 'Connection successful',
        connectionDetails: {
          projectId: request.projectId,
          datasetId: request.datasetId,
          tableId: request.tableId
        }
      };
    } catch (error) {
      logFlow('CONNECTION-HANDLER', 'ERROR', 'Connection test failed', error);
      
      return {
        success: false,
        message: `Connection test failed: ${error instanceof Error ? error.message : String(error)}`
      };
    }
  } catch (error) {
    logFlow('CONNECTION-HANDLER', 'ERROR', 'Error validating connection', error);
    
    return {
      success: false,
      message: `Error validating connection: ${error instanceof Error ? error.message : String(error)}`
    };
  }
}

/**
 * Validates admin key
 */
export async function validateAdmin(
  request: AdminKeyRequest
): Promise<AdminResponse> {
  try {
    // Validate required fields
    if (!request.adminKey) {
      return {
        success: false,
        message: 'Admin key is required'
      };
    }

    // Validate admin key
    const isValidAdminKey = await validateAdminKey(request.adminKey);

    if (!isValidAdminKey) {
      return {
        success: false,
        message: 'Invalid admin key'
      };
    }

    return {
      success: true,
      message: 'Admin key validated successfully'
    };
  } catch (error) {
    logFlow('CONNECTION-HANDLER', 'ERROR', 'Error validating admin key', error);
    
    return {
      success: false,
      message: `Error validating admin key: ${error instanceof Error ? error.message : String(error)}`
    };
  }
}

/**
 * Adds a new mapping
 */
export async function addNewMapping(
  name: string,
  projectId: string,
  datasetId: string,
  tableId: string
): Promise<AdminResponse> {
  try {
    // Validate required fields
    if (!name || !projectId || !datasetId || !tableId) {
      return {
        success: false,
        message: 'Missing required fields: name, projectId, datasetId, and tableId are required'
      };
    }

    // Add mapping
    await addTableMapping(projectId, datasetId, tableId, name);

    return {
      success: true,
      message: 'Mapping added successfully'
    };
  } catch (error) {
    logFlow('CONNECTION-HANDLER', 'ERROR', 'Error adding mapping', error);
    
    return {
      success: false,
      message: `Error adding mapping: ${error instanceof Error ? error.message : String(error)}`
    };
  }
}

/**
 * Removes an existing mapping
 */
export async function removeExistingMapping(
  name: string
): Promise<AdminResponse> {
  try {
    // Get all mappings
    const mappings = await getTableMappings();
    
    // Find the mapping with the given name
    let found = false;
    for (const key in mappings) {
      if (mappings[key] === name) {
        // Extract project, dataset, and table from the key
        const parts = key.split('.');
        if (parts.length === 3) {
          const projectId = parts[0];
          const datasetId = parts[1];
          const tableId = parts[2];
          
          if (projectId && datasetId && tableId) {
            // Remove the mapping
            await removeTableMapping(projectId, datasetId, tableId);
            found = true;
          } else {
            logFlow('CONNECTION-HANDLER', 'ERROR', 'Invalid mapping key parts', { projectId, datasetId, tableId });
          }
        } else {
          logFlow('CONNECTION-HANDLER', 'ERROR', 'Invalid mapping key format', { key });
        }
        break;
      }
    }

    if (!found) {
      return {
        success: false,
        message: `Mapping with name '${name}' not found`
      };
    }

    return {
      success: true,
      message: 'Mapping removed successfully'
    };
  } catch (error) {
    logFlow('CONNECTION-HANDLER', 'ERROR', 'Error removing mapping', error);
    
    return {
      success: false,
      message: `Error removing mapping: ${error instanceof Error ? error.message : String(error)}`
    };
  }
}

/**
 * Gets all mappings
 */
export async function getMappings(): Promise<MappingsResponse> {
  try {
    // Get all mappings
    const mappingsObj = await getTableMappings();
    
    // Convert to array format
    const mappings = Object.entries(mappingsObj)
      .map(([key, name]) => {
        const parts = key.split('.');
        if (parts.length === 3) {
          return {
            name,
            projectId: parts[0],
            datasetId: parts[1],
            tableId: parts[2]
          };
        }
        return null;
      })
      .filter((item): item is {
        name: string;
        projectId: string;
        datasetId: string;
        tableId: string;
      } => item !== null);

    return {
      success: true,
      mappings
    };
  } catch (error) {
    logFlow('CONNECTION-HANDLER', 'ERROR', 'Error getting mappings', error);
    
    return {
      success: false,
      message: `Error getting mappings: ${error instanceof Error ? error.message : String(error)}`
    };
  }
}
