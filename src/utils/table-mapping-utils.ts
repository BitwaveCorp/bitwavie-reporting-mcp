/**
 * Table Mapping Utilities
 * 
 * Handles file-based storage for table mappings and admin keys
 */

import * as fs from 'fs/promises';
import * as path from 'path';
import { logFlow } from './logging.js';

// Constants for file paths
const DATA_DIR = path.join(process.cwd(), 'data');
const TABLE_MAPPINGS_FILE = path.join(DATA_DIR, 'table-mappings.json');
const ADMIN_KEYS_FILE = path.join(DATA_DIR, 'admin-keys.json');

// Default structure for mappings file
const DEFAULT_MAPPINGS = {
  // Format: "projectId.datasetId.tableId": "privateKey"
};

// Default structure for admin keys file
const DEFAULT_ADMIN_KEYS = {
  admin_keys: []
};

/**
 * Ensures the data directory and required files exist
 */
export async function ensureDataFilesExist(): Promise<void> {
  try {
    // Create data directory if it doesn't exist
    try {
      await fs.mkdir(DATA_DIR, { recursive: true });
      logFlow('TABLE-MAPPING', 'INFO', `Created data directory: ${DATA_DIR}`);
    } catch (error) {
      // Directory might already exist, which is fine
      logFlow('TABLE-MAPPING', 'INFO', `Data directory already exists: ${DATA_DIR}`);
    }

    // Check if mappings file exists, create if not
    try {
      await fs.access(TABLE_MAPPINGS_FILE);
      logFlow('TABLE-MAPPING', 'INFO', `Table mappings file exists: ${TABLE_MAPPINGS_FILE}`);
    } catch (error) {
      // File doesn't exist, create it
      await fs.writeFile(TABLE_MAPPINGS_FILE, JSON.stringify(DEFAULT_MAPPINGS, null, 2));
      logFlow('TABLE-MAPPING', 'INFO', `Created table mappings file: ${TABLE_MAPPINGS_FILE}`);
    }

    // Check if admin keys file exists, create if not
    try {
      await fs.access(ADMIN_KEYS_FILE);
      logFlow('TABLE-MAPPING', 'INFO', `Admin keys file exists: ${ADMIN_KEYS_FILE}`);
    } catch (error) {
      // File doesn't exist, create it
      await fs.writeFile(ADMIN_KEYS_FILE, JSON.stringify(DEFAULT_ADMIN_KEYS, null, 2));
      logFlow('TABLE-MAPPING', 'INFO', `Created admin keys file: ${ADMIN_KEYS_FILE}`);
    }
  } catch (error) {
    logFlow('TABLE-MAPPING', 'ERROR', 'Error ensuring data files exist', error);
    throw error;
  }
}

/**
 * Reads the table mappings from file
 */
export async function getTableMappings(): Promise<Record<string, string>> {
  try {
    await ensureDataFilesExist();
    const data = await fs.readFile(TABLE_MAPPINGS_FILE, 'utf8');
    return JSON.parse(data);
  } catch (error) {
    logFlow('TABLE-MAPPING', 'ERROR', 'Error reading table mappings', error);
    return {};
  }
}

/**
 * Writes table mappings to file
 */
export async function saveTableMappings(mappings: Record<string, string>): Promise<void> {
  try {
    await ensureDataFilesExist();
    await fs.writeFile(TABLE_MAPPINGS_FILE, JSON.stringify(mappings, null, 2));
    logFlow('TABLE-MAPPING', 'INFO', 'Table mappings saved successfully');
  } catch (error) {
    logFlow('TABLE-MAPPING', 'ERROR', 'Error saving table mappings', error);
    throw error;
  }
}

/**
 * Adds a new table mapping
 */
export async function addTableMapping(
  projectId: string,
  datasetId: string,
  tableId: string,
  privateKey: string
): Promise<void> {
  try {
    const mappings = await getTableMappings();
    const key = `${projectId}.${datasetId}.${tableId}`;
    
    mappings[key] = privateKey;
    
    await saveTableMappings(mappings);
    logFlow('TABLE-MAPPING', 'INFO', 'Table mapping added successfully', { key });
  } catch (error) {
    logFlow('TABLE-MAPPING', 'ERROR', 'Error adding table mapping', error);
    throw error;
  }
}

/**
 * Removes a table mapping
 */
export async function removeTableMapping(
  projectId: string,
  datasetId: string,
  tableId: string
): Promise<boolean> {
  try {
    const mappings = await getTableMappings();
    const key = `${projectId}.${datasetId}.${tableId}`;
    
    if (mappings[key]) {
      delete mappings[key];
      await saveTableMappings(mappings);
      logFlow('TABLE-MAPPING', 'INFO', 'Table mapping removed successfully', { key });
      return true;
    } else {
      logFlow('TABLE-MAPPING', 'INFO', 'Table mapping not found', { key });
      return false;
    }
  } catch (error) {
    logFlow('TABLE-MAPPING', 'ERROR', 'Error removing table mapping', error);
    return false;
  }
}

/**
 * Validates a table mapping exists and private key matches
 */
export async function validateTableMapping(
  projectId: string,
  datasetId: string,
  tableId: string,
  privateKey: string
): Promise<boolean> {
  try {
    const mappings = await getTableMappings();
    const key = `${projectId}.${datasetId}.${tableId}`;
    
    if (mappings[key] && mappings[key] === privateKey) {
      logFlow('TABLE-MAPPING', 'INFO', 'Table mapping validated successfully', { key });
      return true;
    } else {
      logFlow('TABLE-MAPPING', 'INFO', 'Table mapping validation failed', { key });
      return false;
    }
  } catch (error) {
    logFlow('TABLE-MAPPING', 'ERROR', 'Error validating table mapping', error);
    return false;
  }
}

/**
 * Gets admin keys from file
 */
export async function getAdminKeys(): Promise<string[]> {
  try {
    await ensureDataFilesExist();
    const data = await fs.readFile(ADMIN_KEYS_FILE, 'utf8');
    const { admin_keys } = JSON.parse(data);
    return admin_keys || [];
  } catch (error) {
    logFlow('TABLE-MAPPING', 'ERROR', 'Error reading admin keys', error);
    return [];
  }
}

/**
 * Validates an admin key
 */
export async function validateAdminKey(adminKey: string): Promise<boolean> {
  try {
    const adminKeys = await getAdminKeys();
    return adminKeys.includes(adminKey);
  } catch (error) {
    logFlow('TABLE-MAPPING', 'ERROR', 'Error validating admin key', error);
    return false;
  }
}

/**
 * Gets the default private key for a table from mappings
 */
export async function getDefaultPrivateKey(
  projectId: string,
  datasetId: string,
  tableId: string
): Promise<string | null> {
  try {
    const mappings = await getTableMappings();
    const key = `${projectId}.${datasetId}.${tableId}`;
    
    if (mappings[key]) {
      logFlow('TABLE-MAPPING', 'INFO', 'Default private key found for table', { key });
      return mappings[key];
    } else {
      logFlow('TABLE-MAPPING', 'INFO', 'No default private key found for table', { key });
      return null;
    }
  } catch (error) {
    logFlow('TABLE-MAPPING', 'ERROR', 'Error getting default private key', error);
    return null;
  }
}
