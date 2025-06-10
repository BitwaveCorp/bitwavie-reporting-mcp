# Reporting MCP Server: Configuration Guide

This guide provides detailed instructions for configuring and running the Reporting MCP Server.

## Prerequisites

Before configuring the server, ensure you have:

1. Node.js v16 or higher installed
2. Access to a Google Cloud project with BigQuery enabled
3. A service account with BigQuery access permissions
4. The service account key file (JSON) downloaded to your local machine

## Environment Configuration

The Reporting MCP Server uses environment variables for configuration. Create a `.env` file in the root directory with the following variables:

```
# Google Cloud Configuration
GOOGLE_CLOUD_PROJECT_ID=your-project-id
GOOGLE_APPLICATION_CREDENTIALS=./reporting-mcp/keys/your-service-account-key.json

# BigQuery Configuration
BIGQUERY_DATASET_ID=your_dataset_id
BIGQUERY_TABLE_ID=your_table_id

# Server Configuration
MCP_SERVER_PORT=8080
MCP_SERVER_HOST=localhost
MCP_SERVER_TRANSPORT=stdio
```

### Environment Variables Explained

| Variable | Description | Example |
|----------|-------------|---------|
| `GOOGLE_CLOUD_PROJECT_ID` | Your Google Cloud project ID | `bitwave-solutions` |
| `GOOGLE_APPLICATION_CREDENTIALS` | Path to your service account key file | `./reporting-mcp/keys/bitwave-solutions-a99267d2687a.json` |
| `BIGQUERY_DATASET_ID` | ID of your BigQuery dataset | `0_Bitwavie_MCP` |
| `BIGQUERY_TABLE_ID` | ID of your BigQuery table | `2622d4df5b2a15ec811e_gl_actions` |
| `MCP_SERVER_PORT` | Port for the server to listen on (for socket transport) | `8080` |
| `MCP_SERVER_HOST` | Host for the server to bind to (for socket transport) | `localhost` |
| `MCP_SERVER_TRANSPORT` | Transport mechanism (`stdio` or `socket`) | `stdio` |

## Service Account Setup

1. **Create a Service Account**:
   - Go to the [Google Cloud Console](https://console.cloud.google.com/)
   - Navigate to "IAM & Admin" > "Service Accounts"
   - Click "Create Service Account"
   - Enter a name and description for the service account
   - Click "Create and Continue"

2. **Assign Permissions**:
   - Add the following roles:
     - `BigQuery Data Viewer`
     - `BigQuery Job User`
   - Click "Continue"

3. **Create a Key**:
   - Click "Create Key"
   - Select "JSON" as the key type
   - Click "Create"
   - Save the downloaded key file

4. **Store the Key Securely**:
   - Create a `keys` directory inside the `reporting-mcp` directory
   - Move the downloaded key file to this directory
   - Update the `GOOGLE_APPLICATION_CREDENTIALS` environment variable in your `.env` file

## BigQuery Setup

1. **Create a Dataset**:
   - Go to the [BigQuery Console](https://console.cloud.google.com/bigquery)
   - Click on your project ID
   - Click "Create Dataset"
   - Enter a dataset ID (e.g., `0_Bitwavie_MCP`)
   - Select a location
   - Click "Create Dataset"

2. **Create a Table**:
   - Click on your dataset
   - Click "Create Table"
   - Enter a table name (e.g., `gl_actions`)
   - Define the schema with the required fields
   - Click "Create Table"

3. **Verify Access**:
   - Run the `test-bigquery.js` script to verify that your service account can access the dataset and table:
   ```
   node test-bigquery.js
   ```

## Server Configuration

### Transport Configuration

The server supports two transport mechanisms:

1. **stdio** (Standard Input/Output):
   - Set `MCP_SERVER_TRANSPORT=stdio` in your `.env` file
   - This is the default and recommended transport for most use cases

2. **socket** (TCP Socket):
   - Set `MCP_SERVER_TRANSPORT=socket` in your `.env` file
   - Set `MCP_SERVER_HOST` and `MCP_SERVER_PORT` to specify the host and port to listen on
   - This transport is useful for remote connections

### Logging Configuration

You can configure logging by setting the following environment variables:

```
# Logging Configuration
LOG_LEVEL=info
LOG_FORMAT=json
LOG_FILE=./logs/server.log
```

| Variable | Description | Options |
|----------|-------------|---------|
| `LOG_LEVEL` | The minimum level of logs to output | `error`, `warn`, `info`, `debug`, `trace` |
| `LOG_FORMAT` | The format of the log output | `json`, `pretty` |
| `LOG_FILE` | Path to the log file (optional) | Any valid file path |

## Running the Server

### Development Mode

To run the server in development mode:

```bash
npm run dev
```

This will start the server using `tsx` for TypeScript execution without compilation.

### Production Mode

To run the server in production mode:

1. Build the TypeScript code:
   ```bash
   npm run build
   ```

2. Start the server:
   ```bash
   npm start
   ```

### Testing the Server

To verify that the server is running correctly:

1. Start the server:
   ```bash
   npm run dev
   ```

2. In a separate terminal, run one of the test scripts:
   ```bash
   node test-derivative-reports.js
   ```

## Troubleshooting

### Common Issues

1. **"Method not found" errors**:
   - Ensure you're using the correct method names in your JSON-RPC requests
   - Check if the method names include the `mcp.` prefix
   - Verify that the server has registered the methods correctly

2. **BigQuery connection issues**:
   - Verify that the service account key file path is correct
   - Check that the service account has the necessary permissions
   - Ensure the Google Cloud project ID is correct

3. **Environment variable issues**:
   - Make sure your `.env` file is in the correct location
   - Verify that all required environment variables are set
   - Check for typos in environment variable names

### Debugging

To enable debug logging:

1. Set the `LOG_LEVEL` environment variable to `debug`:
   ```
   LOG_LEVEL=debug
   ```

2. Run the server with the debug flag:
   ```bash
   npm run dev:debug
   ```

3. Look for debug messages in the console or log file

## Security Considerations

1. **Service Account Key**:
   - Never commit the service account key to version control
   - Store the key in a secure location
   - Restrict the permissions of the service account to only what is necessary

2. **Environment Variables**:
   - Do not hardcode sensitive information in your code
   - Use environment variables for all configuration
   - Consider using a secrets manager for production deployments

3. **Network Security**:
   - When using socket transport, consider restricting access using firewall rules
   - Use HTTPS for all external communications
   - Implement proper authentication and authorization
