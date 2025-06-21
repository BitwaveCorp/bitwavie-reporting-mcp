/**
 * Connection Router
 * 
 * Defines Express routes for:
 * - Connection validation
 * - Admin operations
 * - Table mapping management
 */

import express from 'express';
import { Router, RequestHandler } from 'express';
import { 
  validateConnection, 
  validateAdmin, 
  addNewMapping, 
  removeExistingMapping, 
  getMappings 
} from '../services/connection-handler';
import { 
  ValidateConnectionRequest, 
  AdminKeyRequest 
} from '../types/session-types';
import { logFlow } from '../utils/logging';

// Create a router
const router = Router();

// Export the router
export { router as connectionRouter };

// Validate table access
const validateTableAccess: RequestHandler = async (req, res) => {
  try {
    const request = req.body as ValidateConnectionRequest;
    const response = await validateConnection(request);

    if (response.success) {
      // Store connection details in session if validation successful
      req.session.connectionDetails = {
        isConnected: true,
        projectId: request.projectId,
        datasetId: request.datasetId,
        tableId: request.tableId
      };
    }

    res.json(response);
  } catch (error) {
    logFlow('CONNECTION-ROUTES', 'ERROR', 'Error validating table access', error);
    
    res.status(500).json({
      success: false,
      message: `Error validating table access: ${error instanceof Error ? error.message : String(error)}`
    });
  }
};

// Verify admin key
const verifyAdminKey: RequestHandler = async (req, res) => {
  try {
    const request = req.body as AdminKeyRequest;
    const response = await validateAdmin(request);

    if (response.success) {
      // Store admin status in session if validation successful
      req.session.isAdmin = true;
    }

    res.json(response);
  } catch (error) {
    logFlow('CONNECTION-ROUTES', 'ERROR', 'Error verifying admin key', error);
    
    res.status(500).json({
      success: false,
      message: `Error verifying admin key: ${error instanceof Error ? error.message : String(error)}`
    });
  }
};

// Check admin status
const checkAdminStatus: RequestHandler = (req, res) => {
  try {
    const isAdmin = req.session.isAdmin === true;
    
    res.json({
      success: true,
      isAdmin
    });
  } catch (error) {
    logFlow('CONNECTION-ROUTES', 'ERROR', 'Error checking admin status', error);
    
    res.status(500).json({
      success: false,
      message: `Error checking admin status: ${error instanceof Error ? error.message : String(error)}`
    });
  }
};

// Logout admin
const logoutAdmin: RequestHandler = (req, res) => {
  try {
    // Remove admin status from session
    req.session.isAdmin = false;
    
    res.json({
      success: true,
      message: 'Admin logged out successfully'
    });
  } catch (error) {
    logFlow('CONNECTION-ROUTES', 'ERROR', 'Error logging out admin', error);
    
    res.status(500).json({
      success: false,
      message: `Error logging out admin: ${error instanceof Error ? error.message : String(error)}`
    });
  }
};

// Get table mappings (admin only)
const getTableMappings: RequestHandler = async (req, res) => {
  try {
    // Check if user is admin
    if (req.session.isAdmin !== true) {
      res.status(403).json({
        success: false,
        message: 'Unauthorized: Admin access required'
      });
      return;
    }

    const response = await getMappings();
    res.json(response);
  } catch (error) {
    logFlow('CONNECTION-ROUTES', 'ERROR', 'Error getting table mappings', error);
    
    res.status(500).json({
      success: false,
      message: `Error getting table mappings: ${error instanceof Error ? error.message : String(error)}`
    });
  }
};

// Add table mapping (admin only)
const addTableMapping: RequestHandler = async (req, res) => {
  try {
    // Check if user is admin
    if (req.session.isAdmin !== true) {
      res.status(403).json({
        success: false,
        message: 'Unauthorized: Admin access required'
      });
      return;
    }

    const { name, projectId, datasetId, tableId } = req.body;
    const response = await addNewMapping(name, projectId, datasetId, tableId);
    res.json(response);
  } catch (error) {
    logFlow('CONNECTION-ROUTES', 'ERROR', 'Error adding table mapping', error);
    
    res.status(500).json({
      success: false,
      message: `Error adding table mapping: ${error instanceof Error ? error.message : String(error)}`
    });
  }
};

// Remove table mapping (admin only)
const removeTableMapping: RequestHandler = async (req, res) => {
  try {
    // Check if user is admin
    if (req.session.isAdmin !== true) {
      res.status(403).json({
        success: false,
        message: 'Unauthorized: Admin access required'
      });
      return;
    }

    const { name } = req.params;
    // Ensure name is not undefined before passing to removeExistingMapping
    if (!name) {
      res.status(400).json({
        success: false,
        message: 'Missing mapping name parameter'
      });
      return;
    }
    
    const response = await removeExistingMapping(name as string);
    res.json(response);
  } catch (error) {
    logFlow('CONNECTION-ROUTES', 'ERROR', 'Error removing table mapping', error);
    
    res.status(500).json({
      success: false,
      message: `Error removing table mapping: ${error instanceof Error ? error.message : String(error)}`
    });
  }
};

// Check connection status
const checkConnectionStatus: RequestHandler = (req, res) => {
  try {
    const isConnected = req.session.connectionDetails?.isConnected === true;
    const connectionDetails = isConnected ? {
      projectId: req.session.connectionDetails?.projectId,
      datasetId: req.session.connectionDetails?.datasetId,
      tableId: req.session.connectionDetails?.tableId
    } : null;
    
    res.json({
      success: true,
      isConnected,
      connectionDetails
    });
  } catch (error) {
    logFlow('CONNECTION-ROUTES', 'ERROR', 'Error checking connection status', error);
    
    res.status(500).json({
      success: false,
      message: `Error checking connection status: ${error instanceof Error ? error.message : String(error)}`
    });
  }
};

// Clear connection
const clearConnection: RequestHandler = (req, res) => {
  try {
    // Remove connection details from session
    delete req.session.connectionDetails;
    
    res.json({
      success: true,
      message: 'Connection cleared successfully'
    });
  } catch (error) {
    logFlow('CONNECTION-ROUTES', 'ERROR', 'Error clearing connection', error);
    
    res.status(500).json({
      success: false,
      message: `Error clearing connection: ${error instanceof Error ? error.message : String(error)}`
    });
  }
};

// Register routes
router.post('/validate-table-access', validateTableAccess);
router.post('/admin/verify', verifyAdminKey);
router.get('/admin/status', checkAdminStatus);
router.post('/admin/logout', logoutAdmin);
router.get('/admin/mappings', getTableMappings);
router.post('/admin/mappings', addTableMapping);
router.delete('/admin/mappings/:name', removeTableMapping);
router.get('/connection/status', checkConnectionStatus);
router.post('/connection/clear', clearConnection);
