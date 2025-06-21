/**
 * Connection UI Component
 * 
 * Provides a simple UI for users to connect to their own BigQuery tables
 * via session-based connection details.
 */

(function() {
  // State variables
  let isConnected = false;
  let connectionDetails = null;
  let isLoading = false;

  // Initialize the component
  function init() {
    // Check if already connected
    checkConnectionStatus();
    
    // Add event listeners
    document.addEventListener('DOMContentLoaded', () => {
      const connectForm = document.getElementById('bq-connection-form');
      const clearButton = document.getElementById('bq-connection-clear');
      
      if (connectForm) {
        connectForm.addEventListener('submit', handleConnect);
      }
      
      if (clearButton) {
        clearButton.addEventListener('click', handleClear);
      }
    });
  }

  // Check connection status
  async function checkConnectionStatus() {
    try {
      const response = await fetch('/api/connection/status', {
        method: 'GET',
        headers: {
          'Content-Type': 'application/json'
        }
      });
      
      const data = await response.json();
      
      if (data.success) {
        isConnected = data.isConnected;
        connectionDetails = data.connectionDetails;
        updateUI();
      }
    } catch (error) {
      console.error('Error checking connection status:', error);
    }
  }

  // Handle connection form submission
  async function handleConnect(event) {
    event.preventDefault();
    
    if (isLoading) return;
    
    const projectId = document.getElementById('bq-project-id').value.trim();
    const datasetId = document.getElementById('bq-dataset-id').value.trim();
    const tableId = document.getElementById('bq-table-id').value.trim();
    
    if (!projectId || !datasetId || !tableId) {
      showMessage('Please fill in all fields', 'error');
      return;
    }
    
    setLoading(true);
    
    try {
      const response = await fetch('/api/validate-table-access', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          projectId,
          datasetId,
          tableId
        })
      });
      
      const data = await response.json();
      
      if (data.success) {
        isConnected = true;
        connectionDetails = { projectId, datasetId, tableId };
        showMessage('Connected successfully', 'success');
        updateUI();
      } else {
        showMessage(data.message || 'Connection failed', 'error');
      }
    } catch (error) {
      console.error('Error connecting to BigQuery:', error);
      showMessage('Connection error', 'error');
    } finally {
      setLoading(false);
    }
  }

  // Handle clear connection
  async function handleClear() {
    if (isLoading || !isConnected) return;
    
    setLoading(true);
    
    try {
      const response = await fetch('/api/connection/clear', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        }
      });
      
      const data = await response.json();
      
      if (data.success) {
        isConnected = false;
        connectionDetails = null;
        showMessage('Connection cleared', 'success');
        updateUI();
      } else {
        showMessage(data.message || 'Failed to clear connection', 'error');
      }
    } catch (error) {
      console.error('Error clearing connection:', error);
      showMessage('Error clearing connection', 'error');
    } finally {
      setLoading(false);
    }
  }

  // Update UI based on connection status
  function updateUI() {
    const connectionForm = document.getElementById('bq-connection-form');
    const connectionStatus = document.getElementById('bq-connection-status');
    const connectionDetails = document.getElementById('bq-connection-details');
    const clearButton = document.getElementById('bq-connection-clear');
    const connectButton = document.getElementById('bq-connect-button');
    
    if (!connectionForm || !connectionStatus || !connectionDetails || !clearButton || !connectButton) {
      return;
    }
    
    if (isConnected) {
      // Show connected state
      connectionStatus.textContent = 'Connected';
      connectionStatus.className = 'bq-status-connected';
      
      // Show connection details
      connectionDetails.style.display = 'block';
      connectionDetails.innerHTML = `
        <div>Project: ${connectionDetails?.projectId || 'N/A'}</div>
        <div>Dataset: ${connectionDetails?.datasetId || 'N/A'}</div>
        <div>Table: ${connectionDetails?.tableId || 'N/A'}</div>
      `;
      
      // Update form fields if they exist
      const projectInput = document.getElementById('bq-project-id');
      const datasetInput = document.getElementById('bq-dataset-id');
      const tableInput = document.getElementById('bq-table-id');
      
      if (projectInput && datasetInput && tableInput && connectionDetails) {
        projectInput.value = connectionDetails.projectId || '';
        datasetInput.value = connectionDetails.datasetId || '';
        tableInput.value = connectionDetails.tableId || '';
      }
      
      // Show clear button, hide connect button
      clearButton.style.display = 'block';
      connectButton.style.display = 'none';
    } else {
      // Show disconnected state
      connectionStatus.textContent = 'Not Connected';
      connectionStatus.className = 'bq-status-disconnected';
      
      // Hide connection details
      connectionDetails.style.display = 'none';
      connectionDetails.innerHTML = '';
      
      // Show connect button, hide clear button
      clearButton.style.display = 'none';
      connectButton.style.display = 'block';
    }
  }

  // Show message to user
  function showMessage(message, type) {
    const messageEl = document.getElementById('bq-connection-message');
    
    if (!messageEl) return;
    
    messageEl.textContent = message;
    messageEl.className = `bq-message bq-message-${type}`;
    messageEl.style.display = 'block';
    
    // Hide message after 3 seconds
    setTimeout(() => {
      messageEl.style.display = 'none';
    }, 3000);
  }

  // Set loading state
  function setLoading(loading) {
    isLoading = loading;
    
    const connectButton = document.getElementById('bq-connect-button');
    const clearButton = document.getElementById('bq-connection-clear');
    
    if (connectButton) {
      connectButton.disabled = loading;
      connectButton.textContent = loading ? 'Connecting...' : 'Connect';
    }
    
    if (clearButton) {
      clearButton.disabled = loading;
    }
  }

  // Initialize the component
  init();
})();
