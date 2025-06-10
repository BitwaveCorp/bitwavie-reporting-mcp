# Reporting MCP Server

A TypeScript-based Model Context Protocol (MCP) server for crypto accounting reporting and analytics. Part of a modular 6-server crypto accounting system.

## 🎯 Overview

The Reporting MCP Server provides two core capabilities:

1. **Dynamic Analytical Queries** - Natural language to SQL conversion with 5-step process
2. **Predefined Derivative Reports** - Lots, Valuation Rollforward, and Inventory Balance reports

## 🏗️ Architecture

This server is part of a modular crypto accounting system:

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Ingestion     │───▶│     Pricing     │───▶│   Calculation   │
│      MCP        │    │      MCP        │    │      MCP        │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│ Categorization  │───▶│   Reporting     │───▶│   Decisions     │
│      MCP        │    │   MCP (THIS)    │    │      MCP        │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## 📊 Core Capabilities

### Capability 1: Dynamic Analytical Queries

Natural language queries converted to SQL through a 5-step process:

1. **UNDERSTAND** - Parse user intent
2. **MAP** - Identify relevant columns (with user confirmation)
3. **AGGREGATE** - Apply aggregation functions
4. **FILTER** - Apply inclusions/exclusions
5. **PRESENT** - Format and display results

**Example:**
```
Query: "total gain loss for ETH and BTC in March 2025 excluding Treasury wallet"

Result:
- Maps to: shortTermGainLoss, longTermGainLoss, undatedGainLoss
- Filters: asset IN ('ETH', 'BTC'), date range, wallet exclusion
- Aggregates: SUM() by asset
```

### Capability 2: Predefined Derivative Reports

#### 1. Lots Report
- Lot-level inventory positions
- Cost basis tracking (acquired/relieved)
- Carrying value with impairment adjustments
- Fair value and revaluation tracking

#### 2. Valuation Rollforward Report
- Period-based cost basis movements
- Starting → Acquired → Disposed → Ending
- Impairment and adjustment tracking
- Realized and unrealized gain/loss analysis

#### 3. Inventory Balance Report
- Point-in-time inventory snapshots
- Portfolio composition and concentration
- Asset/inventory/subsidiary breakdowns
- Reconciliation and variance analysis

## 🚀 Quick Start

### Prerequisites

- Node.js 18+ 
- TypeScript 5.3+
- Google Cloud BigQuery access
- Service account with BigQuery permissions

### Installation

```bash
# Clone the repository
git clone https://github.com/your-org/crypto-accounting-mcp-system.git
cd crypto-accounting-mcp-system/reporting-mcp

# Install dependencies
npm install

# Copy environment template
cp .env.example .env

# Configure environment variables
vim .env
```

### Configuration

1. **Set up BigQuery:**
```bash
# Set your project ID
export GOOGLE_CLOUD_PROJECT_ID=your-project-id

# Authenticate with Google Cloud
gcloud auth application-default login

# Or use service account key
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-key.json
```

2. **Configure environment:**
```bash
# Required environment variables
GOOGLE_CLOUD_PROJECT_ID=your-gcp-project
BIGQUERY_DATASET_ID=crypto_accounting
BIGQUERY_TABLE_ID=actions_report
```

### Development

```bash
# Development with hot reload
npm run dev

# Build TypeScript
npm run build

# Run tests
npm test

# Run with coverage
npm run test:coverage

# Lint and format
npm run lint
npm run format
```

### Production

```bash
# Build for production
npm run build

# Start production server
npm start

# Docker deployment
npm run docker:build
npm run docker:run

# Cloud Run deployment
npm run deploy:cloud-run
```

## 🔧 Usage

### MCP Tools Available

#### 1. `analyze_actions_data`
Analyze Actions Report data using natural language queries.

```json
{
  "query": "total gain loss for ETH and BTC in March 2025",
  "parameters": {
    "runId": "run_123",
    "orgId": "org_456"
  }
}
```

#### 2. `generate_lots_report`
Generate lot-level inventory positions report.

```json
{
  "parameters": {
    "runId": "run_123",
    "asOfDate": "2025-03-31"
  },
  "filters": {
    "assets": ["BTC", "ETH"],
    "minQty": 0.001
  }
}
```

#### 3. `generate_valuation_rollforward`
Generate period-based rollforward movements.

```json
{
  "parameters": {
    "runId": "run_123",
    "startDate": "2025-03-01",
    "endDate": "2025-03-31"
  },
  "groupBy": ["asset", "subsidiary"]
}
```

#### 4. `generate_inventory_balance`
Generate current inventory positions snapshot.

```json
{
  "parameters": {
    "runId": "run_123",
    "asOfDate": "2025-03-31"
  },
  "groupBy": ["asset", "inventory"]
}
```

#### 5. `configure_data_source`
Configure BigQuery or CSV data source.

```json
{
  "type": "bigquery",
  "config": {
    "projectId": "your-project",
    "datasetId": "crypto_accounting",
    "tableId": "actions_report"
  }
}
```

#### 6. `validate_column_mapping`
Validate column mappings before query execution.

```json
{
  "query": "show me BTC gains",
  "reportType": "actions"
}
```

### Natural Language Query Examples

```bash
# Gain/loss analysis
"total realized gains for BTC in 2025"
"short term vs long term gains by asset"
"impairment losses for Q1 2025"

# Portfolio analysis
"current portfolio value by asset"
"cost basis movement for ETH last month"
"largest lot positions"

# Wallet analysis
"Treasury wallet holdings"
"trading activity excluding cold storage"
"internal transfers between wallets"

# Time-based analysis
"monthly trading volume"
"year-over-year portfolio growth"
"period-end balances"
```

## 🏗️ Project Structure

```
reporting-mcp/
├── src/
│   ├── server.ts                 # Main MCP server
│   ├── types/
│   │   └── actions-report.ts     # Core data interfaces
│   ├── services/
│   │   ├── query-parser.ts       # Natural language processing
│   │   └── bigquery-client.ts    # Database connection
│   └── reports/
│       ├── lots-report.ts        # Lots report generator
│       ├── valuation-rollforward.ts # Rollforward generator
│       └── inventory-balance.ts  # Inventory generator
├── tests/                        # Test files
├── dist/                        # Compiled JavaScript
├── package.json                 # Dependencies & scripts
├── tsconfig.json               # TypeScript config
├── Dockerfile                  # Container config
├── .env.example               # Environment template
└── README.md                  # This file
```

## 🧪 Testing

```bash
# Run all tests
npm test

# Watch mode
npm run test:watch

# Coverage report
npm run test:coverage

# Test specific functionality
npm test -- --testNamePattern="QueryParser"
npm test -- --testNamePattern="BigQueryClient"
npm test -- --testNamePattern="LotsReport"
```

### Test Categories

- **Unit Tests**: Individual component testing
- **Integration Tests**: BigQuery connection and data flow
- **End-to-End Tests**: Complete query processing pipeline
- **Performance Tests**: Large dataset handling

## 📊 Data Schema

### Actions Report Structure (64 columns)

The server expects Actions Report data with the following key columns:

```typescript
interface ActionRecord {
  // Identifiers
  orgId: string;
  runId: string;
  txnId: string;
  eventId: string;
  lotId?: string;
  
  // Temporal
  timestampSEC: number;
  timestamp: string;
  
  // Assets
  asset: string;
  assetUnitAdj?: number;
  
  // Financial
  costBasisAcquired?: number;
  costBasisRelieved?: number;
  shortTermGainLoss?: number;
  longTermGainLoss?: number;
  undatedGainLoss?: number;
  carryingValue?: number;
  
  // Classification
  wallet?: string;
  inventory?: string;
  action: string;
  status: string;
}
```

## 🚀 Deployment

### Local Development

```bash
npm run dev
```

### Docker Container

```bash
docker build -t reporting-mcp-server .
docker run -p 3000:3000 reporting-mcp-server
```

### Google Cloud Run

```bash
# Deploy to Cloud Run
gcloud run deploy reporting-mcp-server \
  --source . \
  --platform managed \
  --region us-central1 \
  --allow-unauthenticated
```

### Environment Variables for Production

```bash
# Required
GOOGLE_CLOUD_PROJECT_ID=production-project
BIGQUERY_DATASET_ID=crypto_accounting_prod
BIGQUERY_TABLE_ID=actions_report
NODE_ENV=production

# Optional
QUERY_CACHE_TTL_MS=300000
MAX_QUERY_ROWS=1000000
LOG_LEVEL=info
```

## 🔒 Security

- Service account authentication for BigQuery
- Input validation and sanitization
- Query parameter binding (SQL injection prevention)
- Rate limiting and request validation
- Non-root Docker container execution

## 📈 Performance

- Query result caching (5-minute TTL)
- BigQuery job optimization
- Streaming for large datasets
- Connection pooling
- Efficient aggregation queries

## 🤝 Integration

### With Other MCP Servers

```typescript
// Future integration patterns
const ingestionData = await ingestionMCP.getValidatedTransactions();
const pricedData = await pricingMCP.enrichWithPrices(ingestionData);
const calculatedData = await calculationMCP.computeGainsLosses(pricedData);
const categorizedData = await categorizationMCP.applyTags(calculatedData);

// Generate reports from categorized data
const reports = await reportingMCP.generateReports(categorizedData);
```

### API Integration

```bash
# Health check
curl http://localhost:3000/health

# MCP communication via stdio
echo '{"method": "tools/list"}' | node dist/server.js
```

## 📚 API Reference

### Field Metadata

Each report includes comprehensive field metadata for natural language processing:

```typescript
interface FieldMetadata {
  column: string;           // Database column name
  description: string;      // Human-readable description
  type: 'string' | 'number' | 'boolean' | 'timestamp';
  category: 'identifier' | 'financial' | 'asset' | 'temporal' | 'classification';
  aliases: string[];        // Alternative names/terms
  common_queries: string[]; // Typical user query patterns
  aggregatable: boolean;    // Can be used in SUM/AVG/etc
  filterable: boolean;      // Can be used in WHERE clauses
}
```

### Query Processing Pipeline

```
Natural Language Query
        ↓
1. Intent Classification
        ↓
2. Entity Extraction (assets, dates, wallets)
        ↓
3. Column Mapping (with confirmation)
        ↓
4. SQL Generation
        ↓
5. Query Execution
        ↓
6. Result Formatting
        ↓
Structured Response
```

## 🐛 Troubleshooting

### Common Issues

1. **BigQuery Permission Errors**
```bash
# Ensure service account has roles:
# - BigQuery Data Viewer
# - BigQuery User
gcloud projects add-iam-policy-binding PROJECT_ID \
  --member="serviceAccount:SERVICE_ACCOUNT_EMAIL" \
  --role="roles/bigquery.dataViewer"
```

2. **Query Timeout Issues**
```bash
# Increase timeout in environment
BIGQUERY_JOB_TIMEOUT_MS=120000
QUERY_TIMEOUT_MS=120000
```

3. **Memory Issues with Large Datasets**
```bash
# Limit result size
MAX_QUERY_ROWS=100000
BIGQUERY_MAX_RESULTS=10000
```

### Debug Mode

```bash
DEBUG_MODE=true
VERBOSE_LOGGING=true
ENABLE_QUERY_LOGGING=true
npm run dev
```

## 📄 License

MIT License - see LICENSE file for details.

## 🤝 Contributing

1. Fork the repository
2. Create feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open Pull Request

### Development Guidelines

- Follow TypeScript strict mode
- Add tests for new functionality
- Update documentation
- Follow conventional commit messages
- Ensure CI/CD pipeline passes

## 📞 Support

- GitHub Issues: [Create an issue](https://github.com/your-org/crypto-accounting-mcp-system/issues)
- Documentation: [Wiki](https://github.com/your-org/crypto-accounting-mcp-system/wiki)
- Team Chat: [Internal Slack/Teams channel]

---

**Part of the Crypto Accounting MCP System** | [System Overview](../README.md) | [Other MCP Servers](../)