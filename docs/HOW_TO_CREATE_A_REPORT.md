# How to Create a New Report in Bitwavie Reporting System

This guide provides a step-by-step process for creating a new report in the Bitwavie reporting system. Follow these instructions to ensure your report is properly integrated with all necessary components.

## Table of Contents

1. [Understand the Schema](#1-understand-the-schema)
2. [Create Type Definitions](#2-create-type-definitions)
3. [Create the Report Generator Class](#3-create-the-report-generator-class)
4. [Register the Report](#4-register-the-report)
5. [Update Schema Type Registry](#5-update-schema-type-registry)
6. [Add Example Prompt to Server](#6-add-example-prompt-to-server)
7. [Testing](#7-testing)
8. [Troubleshooting](#8-troubleshooting)

## 1. Understand the Schema

Before creating a report, understand the schema you'll be working with:

- **File to check**: `src/services/schema-type-registry.ts`
- **What to look for**: Find the schema type definition for your target schema (e.g., `actions`, `canton_transaction`)
- **Key information to gather**:
  - Schema name
  - Available columns
  - Required columns
  - Existing reports for this schema

Example:
```typescript
// For canton_transaction schema
this.registerSchemaType('canton_transaction', {
  displayName: 'Canton Transaction',
  description: 'Canton transaction data schema',
  requiredColumns: ['parenttransactionId', 'dateTime', 'walletId', 'operation', 'assetTicker', 'assetAmount'],
  compatibleReports: ['monthly-activity-report']
});
```

## 2. Create Type Definitions

Create or update type definitions for your report:

### If creating a report for an existing schema with type definitions:

- **File to check**: `src/types/{schema-name}-report.ts` (e.g., `actions-report.ts`, `canton-transaction-report.ts`)
- **What to add**:
  - Interface for your report's record structure
  - Add to existing type definitions if needed

Example:
```typescript
// In src/types/canton-transaction-report.ts
export interface MyNewReportRecord {
  // Define the structure of your report's records
  field1: string;
  field2: number;
  // ...
}
```

### If creating a report for a new schema:

- **Create a new file**: `src/types/{schema-name}-report.ts`
- **What to include**:
  - Core data interface reflecting the schema structure
  - Report-specific interfaces
  - Query and response types
  - Field metadata for natural language queries

Example structure:
```typescript
/**
 * Core data interfaces for [Schema Name] Reports
 */

import { ConnectionDetails } from './session-types.js';

// Core Data Structure
export interface SchemaRecord {
  // Define the base schema structure
}

// Report-Specific Record
export interface MyReportRecord {
  // Define the structure for your specific report
}

// Report Parameters
export interface ReportParameters {
  // Define common parameters
  startDate?: string;
  endDate?: string;
  // Other parameters...
  connectionDetails?: ConnectionDetails;
}

// Query & Response Types
export interface QueryRequest {
  // Define query structure
}

// Field Metadata
export interface FieldMetadata {
  // Define field metadata structure
}

// Field Metadata Array
export const SCHEMA_METADATA: FieldMetadata[] = [
  // Define metadata for each field
];
```

## 3. Create the Report Generator Class

Create a new file for your report generator:

- **File to create**: `src/reports/{report-name}.ts`
- **What to include**:
  - Import necessary services and types
  - Create a class that handles report generation
  - Implement required methods:
    - Constructor
    - SQL building method
    - Result transformation method
    - generateReport method

Example structure:
```typescript
/**
 * [Report Name] Generator
 * 
 * [Brief description of what the report does]
 */

import { BigQueryClient } from '../services/bigquery-client.js';
import { QueryExecutor } from '../services/query-executor.js';
import { ConnectionManager } from '../services/connection-manager.js';
import { logFlow } from '../utils/logging.js';
import { 
  ReportRecord,
  ReportParameters,
  FieldMetadata 
} from '../types/{schema-name}-report.js';

export class MyReportGenerator {
  private queryExecutor: QueryExecutor;
  private connectionManager: ConnectionManager;
  
  // Field metadata for natural language query mapping
  private static readonly FIELD_METADATA: FieldMetadata[] = [
    // Define field metadata
  ];

  /**
   * Constructor
   */
  constructor(bigQueryClient: BigQueryClient) {
    // Initialize with connection manager and project ID
    this.connectionManager = ConnectionManager.getInstance();
    const projectId = this.connectionManager.getProjectId() || process.env.GOOGLE_CLOUD_PROJECT_ID;
    this.queryExecutor = new QueryExecutor(projectId);
  }
  
  /**
   * Build SQL query
   */
  buildSql(params: ReportParameters): string {
    // Construct and return SQL query
    return `SELECT ... FROM ... WHERE ...`;
  }
  
  /**
   * Transform query results
   */
  transformResults(rows: any[]): ReportRecord[] {
    // Transform and return results
    return rows.map(row => ({
      // Map row fields to report record structure
    }));
  }
  
  /**
   * Generate report
   */
  async generateReport(parameters: Record<string, any>): Promise<{
    data: any[];
    columns: string[];
    executionTimeMs: number;
    bytesProcessed: number;
    sql: string;
    metadata?: any;
  }> {
    // Implement report generation logic
    // 1. Extract and validate parameters
    // 2. Build SQL query
    // 3. Execute query
    // 4. Transform results
    // 5. Generate summary statistics
    // 6. Return formatted results
  }
}
```

### Key Components to Include:

1. **Parameter Validation**:
   ```typescript
   if (!reportParams.requiredParam) {
     throw new Error('Required parameter is missing');
   }
   ```

2. **SQL Query Building**:
   ```typescript
   const sql = this.buildSql(reportParams);
   ```

3. **Query Execution**:
   ```typescript
   const executionResult = await this.queryExecutor.executeQuery(sql);
   ```

4. **Result Transformation**:
   ```typescript
   const results = this.transformResults(executionResult.data);
   ```

5. **Return Structure** (critical for download button):
   ```typescript
   return {
     data: results,
     columns,
     executionTime,  // IMPORTANT: Use executionTime, not executionTimeMs
     bytesProcessed: executionResult.metadata.bytesProcessed || 0,
     sql,
     metadata: {
       summary,
       totalRecords: results.length,
       // Other metadata...
     }
   };
   ```

## 4. Register the Report

Register your report in the report registry:

- **File to modify**: `src/services/report-registry.ts`
- **What to add**:
  - Import your report generator class
  - Register the report with metadata

Example:
```typescript
// At the top of the file
import { MyReportGenerator } from '../reports/my-report.js';

// In the constructor or initialization method
this.registerReport({
  id: 'my-report-id',
  name: 'My Report Name',
  description: 'Description of what the report does',
  keywords: ['keyword1', 'keyword2', 'keyword3'],
  compatibleSchemaTypes: ['schema_name'],  // IMPORTANT: Match with schema type registry
  parameters: [
    {
      name: 'param1',
      type: 'string',
      required: true,
      description: 'Description of parameter 1'
    },
    {
      name: 'param2',
      type: 'date',
      required: true,
      description: 'Description of parameter 2'
    },
    // Additional parameters...
  ]
}, MyReportGenerator);
```

## 5. Update Schema Type Registry

Ensure your report is listed as compatible with the appropriate schema:

- **File to modify**: `src/services/schema-type-registry.ts`
- **What to update**:
  - Add your report ID to the `compatibleReports` array for the relevant schema

Example:
```typescript
this.registerSchemaType('schema_name', {
  displayName: 'Schema Display Name',
  description: 'Schema description',
  requiredColumns: ['column1', 'column2', 'column3'],
  compatibleReports: [
    'existing-report-1',
    'existing-report-2',
    'my-report-id'  // Add your report ID here
  ]
});
```

## 6. Add Example Prompt to Server

Add an example prompt for your report in the server.ts file to help users understand how to use it:

- **File to modify**: `src/server.ts`
- **Function to update**: `formatReport` in the `listAvailableReports` method
- **What to add**: Add a case for your report ID with an example prompt that includes all required parameters

Example:
```typescript
// In the formatReport function's switch statement
switch(report.id) {
  case 'inventory-balance':
    examplePrompt = '/inventory-balance asOfDate=2025-06-15';
    break;
  case 'valuation-rollforward':
    examplePrompt = '/valuation-rollforward startDate=2025-01-01 endDate=2025-03-31';
    break;
  case 'my-report-id':
    examplePrompt = '/my-report-id param1=value1 param2=value2 param3=value3';
    break;
  default:
    examplePrompt = `/${report.id}`;
}
```

This example prompt will be displayed in the UI when users list available reports, helping them understand how to use your report with the correct parameters.

## 7. Testing

Test your report thoroughly:

1. **Build the project**:
   ```bash
   npm run build
   ```

2. **Start the server**:
   ```bash
   npm start
   ```

3. **Connect to a dataset** with the appropriate schema

4. **Verify report availability** in the UI

5. **Test report generation** with various parameters

6. **Verify download functionality** works correctly

## 8. Troubleshooting

Common issues and solutions:

### Download Button Not Appearing
- Ensure your `generateReport` method returns `executionTime` (not `executionTimeMs`)
- Check that the report is returning valid data

### Report Not Appearing in UI
- Verify `compatibleSchemaTypes` in report registration matches the schema name
- Check that the report is properly registered in `report-registry.ts`
- Ensure the schema type registry includes your report ID

### TypeScript Build Errors
- Check that all interfaces and types are properly defined
- Ensure all required imports are present
- Verify that method signatures match expected types

### Query Execution Errors
- Validate SQL syntax
- Check parameter handling and SQL injection prevention
- Verify column names match the schema

## Example Workflow

Here's a complete workflow for creating a new report:

1. **Identify the schema** you'll be working with
2. **Create or update type definitions** in `src/types/{schema-name}-report.ts`
3. **Create the report generator** in `src/reports/{report-name}.ts`
4. **Register the report** in `src/services/report-registry.ts`
5. **Update the schema type registry** in `src/services/schema-type-registry.ts`
6. **Add example prompt** in `src/server.ts`
7. **Build and test** the report

Following these steps will ensure your report is properly integrated with all components of the Bitwavie reporting system.
