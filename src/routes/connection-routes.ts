/**
 * Connection Routes
 * 
 * Defines Express routes for:
 * - Connection validation
 * - Admin operations
 * - Table mapping management
 */

import express from 'express';
import * as path from 'path';
import { logFlow } from '../utils/logging.js';
import { connectionRouter } from './connection-router.js';

/**
 * Registers all connection routes with the Express server
 */
export function registerConnectionRoutes(app: express.Express): void {
  
  // Serve static files from the public directory
  app.use(express.static(path.join(process.cwd(), 'dist', 'public')));

  // Serve admin page
  app.get('/admin', (req, res) => {
    try {
      res.sendFile(path.join(process.cwd(), 'dist', 'public', 'admin.html'));
    } catch (error) {
      res.status(500).send(`Error serving admin page: ${error instanceof Error ? error.message : String(error)}`);
    }
  });

  // Mount the API routes under /api
  app.use('/api', connectionRouter);
  
  logFlow('CONNECTION-ROUTES', 'INFO', 'Connection routes registered');
}
