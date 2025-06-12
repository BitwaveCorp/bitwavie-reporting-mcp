// Simple script to test the MCP server
import { createInterface } from 'readline/promises';
import { spawn } from 'child_process';
import { fileURLToPath } from 'url';
import { dirname } from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Start the server as a child process
const server = spawn('node', ['--no-warnings', 'dist/server.js']);

// Handle server output
server.stdout.on('data', (data) => {
  console.log('\n📡 Server response:');
  try {
    const response = JSON.parse(data.toString());
    console.log(JSON.stringify(response, null, 2));
  } catch (e) {
    console.log(data.toString());
  }
});

server.stderr.on('data', (data) => {
  console.error('Server error:', data.toString());
});

// Set up readline for user input
const rl = createInterface({
  input: process.stdin,
  output: process.stdout
});

// Function to send a query to the server
function sendQuery(query) {
  const message = {
    jsonrpc: '2.0',
    method: 'tools/call',
    params: {
      name: 'analyze_actions_data',
      arguments: {
        query: query
      }
    },
    id: Date.now()
  };
  
  console.log('\n📤 Sending query:', query);
  server.stdin.write(JSON.stringify(message) + '\n');
}

// Start the interactive prompt
async function promptUser() {
  try {
    const query = await rl.question('\n🔍 Enter your query (or type "exit" to quit): ');
    
    if (query.toLowerCase() === 'exit') {
      console.log('👋 Goodbye!');
      server.kill();
      rl.close();
      return;
    }
    
    sendQuery(query);
  } catch (error) {
    console.error('Error reading input:', error);
    rl.close();
    process.exit(1);
  }
}

// Handle server responses and prompt for next query
server.stdout.on('data', () => {
  // After getting a response, prompt for the next query
  setTimeout(promptUser, 100);
});

// Handle process exit
process.on('SIGINT', () => {
  console.log('\n👋 Shutting down...');
  server.kill();
  rl.close();
  process.exit();
});

console.log('🚀 MCP Server Test Client');
console.log('Enter a natural language query to test the server.');
console.log('Example: "What is the CANTON balance for January?"');
console.log('Type "exit" to quit.\n');

// Start the first prompt
promptUser();
