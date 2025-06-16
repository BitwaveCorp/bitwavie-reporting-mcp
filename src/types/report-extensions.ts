/**
 * Extensions to the core report types to support the enhanced NLQ flow
 */

import { ColumnMapping, FilterCondition, GroupByClause, OrderByClause, TimeRange } from './actions-report.js';

/**
 * Extended ReportParameters interface for the enhanced NLQ flow
 */
export interface ExtendedReportParameters {
  // Original ReportParameters fields
  runId?: string;
  orgId?: string;
  startDate?: string;
  endDate?: string;
  asOfDate?: string;
  asOfSEC?: number;
  
  // Extended fields for NLQ
  columns?: ColumnMapping[];
  filters?: FilterCondition[];
  aggregationType?: 'sum' | 'count' | 'avg' | 'min' | 'max';
  groupBy?: GroupByClause[];
  orderBy?: OrderByClause[];
  limit?: number;
  timeRange?: TimeRange;
}

/**
 * Schema configuration for the SchemaManager
 */
export interface SchemaConfig {
  projectId: string;
  datasetId: string;
  tableId: string;
  refreshIntervalMs?: number;
}
