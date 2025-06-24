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
} from '../services/connection-handler.js';
import { 
  ValidateConnectionRequest, 
  AdminKeyRequest 
} from '../types/session-types.js';
import { logFlow } from '../utils/logging.js';

// Create a router
const router = Router();

// Export the router
export { router as connectionRouter };

// Validate table access
const validateTableAccess: RequestHandler = async (req, res) => {
  try {
    const request = req.body as ValidateConnectionRequest;
    
    // Check if this is a clear connection request
    if (request.action === 'clear') {
      // Log the session details before clearing
      logFlow('CONNECTION-ROUTES', 'INFO', 'Clearing connection from session via action parameter', {
        sessionId: req.sessionID,
        hasConnectionDetails: !!req.session?.connectionDetails,
        hasPrivateKey: !!(req.session as any)?.privateKey
      });
      
      // Remove connection details from session
      delete req.session.connectionDetails;
      
      // Also remove private key if it exists
      if ((req.session as any).privateKey) {
        delete (req.session as any).privateKey;
        logFlow('CONNECTION-ROUTES', 'INFO', 'Private key removed from session');
      }
      
      res.json({
        success: true,
        message: 'Connection cleared successfully'
      });
      return;
    }
    
    logFlow('CONNECTION-ROUTES', 'INFO', 'Validating table access with request', {
      hasProjectId: !!request.projectId,
      hasDatasetId: !!request.datasetId,
      hasTableId: !!request.tableId,
      hasPrivateKey: !!request.privateKey
    });
    
    const response = await validateConnection(request);

    if (response.success) {
      // Store connection details in session if validation successful
      req.session.connectionDetails = {
        isConnected: true,
        projectId: request.projectId,
        datasetId: request.datasetId,
        tableId: request.tableId
      };
      
      // Log that we've saved connection details to the session
      logFlow('CONNECTION-ROUTES', 'INFO', 'Connection details saved to session', {
        projectId: request.projectId,
        datasetId: request.datasetId,
        tableId: request.tableId,
        sessionId: req.sessionID
      });
      
      // Store private key separately in session
      if (request.privateKey) {
        (req.session as any).privateKey = request.privateKey;
        logFlow('CONNECTION-ROUTES', 'INFO', 'Private key saved to session', {
          hasPrivateKey: true,
          sessionId: req.sessionID
        });
      }
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
    // Log the session ID and whether connection details exist
    logFlow('CONNECTION-ROUTES', 'INFO', 'Checking connection status', {
      sessionId: req.sessionID,
      hasSession: !!req.session,
      hasConnectionDetails: !!req.session?.connectionDetails,
      hasPrivateKey: !!(req.session as any)?.privateKey
    });
    
    const isConnected = req.session.connectionDetails?.isConnected === true;
    const connectionDetails = isConnected ? {
      projectId: req.session.connectionDetails?.projectId,
      datasetId: req.session.connectionDetails?.datasetId,
      tableId: req.session.connectionDetails?.tableId
    } : null;
    
    // Log the connection details being returned
    logFlow('CONNECTION-ROUTES', 'INFO', 'Connection status result', {
      isConnected,
      hasConnectionDetails: !!connectionDetails,
      projectId: connectionDetails?.projectId || 'Not provided',
      datasetId: connectionDetails?.datasetId || 'Not provided',
      tableId: connectionDetails?.tableId || 'Not provided'
    });
    
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
    // Log the session details before clearing
    logFlow('CONNECTION-ROUTES', 'INFO', 'Clearing connection from session', {
      sessionId: req.sessionID,
      hasConnectionDetails: !!req.session?.connectionDetails,
      hasPrivateKey: !!(req.session as any)?.privateKey
    });
    
    // Remove connection details from session
    delete req.session.connectionDetails;
    
    // Also remove private key if it exists
    if ((req.session as any).privateKey) {
      delete (req.session as any).privateKey;
      logFlow('CONNECTION-ROUTES', 'INFO', 'Private key removed from session');
    }
    
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

// Debug endpoint to show session details
const getSessionDetails: RequestHandler = async (req, res) => {
  try {
    // Extract session details safely
    const sessionDetails = {
      hasSession: !!req.session,
      connectionDetails: req.session?.connectionDetails || null,
      privateKeyExists: !!(req.session as any)?.privateKey,
      sessionID: req.sessionID,
      sessionKeys: req.session ? Object.keys(req.session) : [],
      timestamp: new Date().toISOString()
    };
    
    logFlow('CONNECTION-ROUTES', 'INFO', 'Session details requested', {
      sessionID: req.sessionID,
      hasConnectionDetails: !!req.session?.connectionDetails
    });
    
    res.json(sessionDetails);
  } catch (error) {
    logFlow('CONNECTION-ROUTES', 'ERROR', 'Error getting session details', { error });
    res.status(500).json({
      error: 'Failed to get session details',
      message: error instanceof Error ? error.message : 'Unknown error'
    });
  }
};

// Register routes
router.post('/validate-table-access', validateTableAccess);
router.post('/admin/verify', verifyAdminKey);
router.get('/admin/status', checkAdminStatus);
router.post('/admin/logout', logoutAdmin);
router.get('/admin/mappings', getTableMappings);
router.post('/admin/mappings/add', addTableMapping);
router.post('/admin/mappings/remove', removeTableMapping);
router.get('/status', checkConnectionStatus);
router.post('/clear', clearConnection);
router.get('/session-details', getSessionDetails);
