/**
 * Connection UI Injector
 * 
 * Utility to inject the connection UI component into the reporting page header
 */

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

// Get the directory name
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Path to the connection UI HTML snippet
const connectionUIPath = path.join(__dirname, '..', 'public', 'connection-ui.html');

/**
 * Read the connection UI HTML snippet
 * @returns The HTML snippet as a string
 */
function readConnectionUISnippet(): string {
  try {
    return fs.readFileSync(connectionUIPath, 'utf8');
  } catch (error) {
    console.error('Error reading connection UI snippet:', error);
    return '';
  }
}

/**
 * Inject the connection UI into HTML content
 * @param html The HTML content to inject the connection UI into
 * @returns The HTML content with the connection UI injected
 */
export function injectConnectionUI(html: string): string {
  const connectionUISnippet = readConnectionUISnippet();
  
  if (!connectionUISnippet) {
    return html;
  }
  
  // Add the connection UI CSS and JS
  const cssLink = '<link rel="stylesheet" href="/connection-ui.css">';
  const jsScript = '<script src="/connection-ui.js"></script>';
  
  // Find the end of the head tag to inject the CSS
  const headEndIndex = html.indexOf('</head>');
  if (headEndIndex !== -1) {
    html = html.slice(0, headEndIndex) + cssLink + html.slice(headEndIndex);
  }
  
  // Find a suitable location to inject the connection UI (e.g., after the header)
  // This is a simplistic approach - in a real app, you might want to use a more robust method
  const headerEndIndex = html.indexOf('</header>');
  if (headerEndIndex !== -1) {
    html = html.slice(0, headerEndIndex) + connectionUISnippet + html.slice(headerEndIndex);
  } else {
    // If no header tag is found, try to inject after the body opening tag
    const bodyStartIndex = html.indexOf('<body>');
    if (bodyStartIndex !== -1) {
      html = html.slice(0, bodyStartIndex + 6) + connectionUISnippet + html.slice(bodyStartIndex + 6);
    }
  }
  
  // Find the end of the body tag to inject the JS
  const bodyEndIndex = html.indexOf('</body>');
  if (bodyEndIndex !== -1) {
    html = html.slice(0, bodyEndIndex) + jsScript + html.slice(bodyEndIndex);
  }
  
  return html;
}

/**
 * Create middleware to inject the connection UI into HTML responses
 * @returns Express middleware function
 */
export function createConnectionUIMiddleware() {
  return (req: any, res: any, next: any) => {
    // Store the original res.send function
    const originalSend = res.send;
    
    // Override res.send to inject the connection UI
    res.send = function(body: any) {
      // Only inject if the response is HTML
      const contentType = res.get('Content-Type');
      if (contentType && contentType.includes('text/html') && typeof body === 'string') {
        body = injectConnectionUI(body);
      }
      
      // Call the original send function
      return originalSend.call(this, body);
    };
    
    next();
  };
}
