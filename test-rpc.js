import axios from 'axios';

async function testMcpServer() {
  // Use the HTTP server port instead of WebSocket server port
  const mcpServerUrl = 'http://localhost:8080/rpc';
  const requestId = 'test-' + Date.now();
  
  console.log(`Testing MCP server at ${mcpServerUrl}`);
  
  try {
    // First test the connection
    console.log('\n--- Testing connection ---');
    const testResponse = await axios.post(mcpServerUrl, {
      jsonrpc: '2.0',
      method: 'test_connection',
      id: requestId
    }, {
      headers: {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
      },
      timeout: 5000
    });
    
    console.log('Test connection response:', JSON.stringify(testResponse.data, null, 2));
    
    // Then get the list of available tools
    console.log('\n--- Getting tools list ---');
    const listResponse = await axios.post(mcpServerUrl, {
      jsonrpc: '2.0',
      method: 'tools/list',
      id: requestId + '-list'
    }, {
      headers: {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
      },
      timeout: 5000
    });
    
    console.log('Tools list response:', JSON.stringify(listResponse.data, null, 2));
    console.log('Response structure:', JSON.stringify({
      hasResult: !!listResponse.data.result,
      resultType: typeof listResponse.data.result,
      hasTools: !!listResponse.data.result?.tools,
      toolsType: typeof listResponse.data.result?.tools,
      isArray: Array.isArray(listResponse.data.result?.tools),
      toolsLength: listResponse.data.result?.tools?.length || 0
    }));
    
    const toolsList = listResponse.data.result?.tools || [];
    console.log('Available tools:', toolsList);
    
    // Check if analyze_actions_data tool is available
    const analyzeTool = toolsList.find(tool => tool.name === 'analyze_actions_data');
    if (!analyzeTool) {
      throw new Error('analyze_actions_data tool not found in available tools');
    }
    
    // Test the direct analyze_actions_data endpoint
    console.log('\n--- Testing direct analyze_actions_data method ---');
    const directRequest = {
      jsonrpc: '2.0',
      method: 'analyze_actions_data',
      params: [{
        query: 'Show me recent transactions',
        context: 'Testing the analyze_actions_data endpoint'
      }],
      id: requestId + '-direct'
    };
    
    try {
      console.log('Sending direct analyze_actions_data request:', JSON.stringify(directRequest, null, 2));
      const directResponse = await axios.post(mcpServerUrl, directRequest, {
        headers: {
          'Content-Type': 'application/json',
          'Accept': 'application/json'
        },
        timeout: 10000
      });
      
      console.log('Direct analyze_actions_data response:', JSON.stringify(directResponse.data, null, 2));
    } catch (directError) {
      console.error('Error with direct method call:', directError.message);
      if (directError.response) {
        console.error('Response data:', JSON.stringify(directError.response.data, null, 2));
        console.error('Response status:', directError.response.status);
      }
    }
    
    // Test the tools/call approach
    console.log('\n--- Testing tools/call method ---');
    const toolsCallRequest = {
      jsonrpc: '2.0',
      method: 'tools/call',
      params: {
        name: 'analyze_actions_data',
        arguments: {
          query: 'Show me recent transactions',
          context: 'Testing the tools/call endpoint'
        }
      },
      id: requestId + '-tools-call'
    };
    
    try {
      console.log('Sending tools/call request:', JSON.stringify(toolsCallRequest, null, 2));
      const toolsCallResponse = await axios.post(mcpServerUrl, toolsCallRequest, {
        headers: {
          'Content-Type': 'application/json',
          'Accept': 'application/json'
        },
        timeout: 10000
      });
      
      console.log('tools/call response:', JSON.stringify(toolsCallResponse.data, null, 2));
    } catch (toolsCallError) {
      console.error('Error with tools/call method:', toolsCallError.message);
      if (toolsCallError.response) {
        console.error('Response data:', JSON.stringify(toolsCallError.response.data, null, 2));
        console.error('Response status:', toolsCallError.response.status);
      }
    }
    
  } catch (error) {
    console.error('Error testing MCP server:', error.message);
    if (error.response) {
      console.error('Response data:', JSON.stringify(error.response.data, null, 2));
      console.error('Response status:', error.response.status);
    } else if (error.request) {
      console.error('No response received');
    } else {
      console.error('Error details:', error);
    }
  }
}

testMcpServer();
