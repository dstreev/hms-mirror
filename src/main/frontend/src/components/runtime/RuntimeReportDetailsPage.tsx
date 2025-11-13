import React, { useState, useEffect } from 'react';
import { useSearchParams, useNavigate } from 'react-router-dom';
import {
  ArrowLeftIcon,
  ArrowPathIcon,
  ArrowDownTrayIcon,
  DocumentTextIcon,
  CheckCircleIcon,
  XCircleIcon,
  ExclamationTriangleIcon,
  CodeBracketIcon,
  XMarkIcon,
  ClockIcon,
  ChartBarIcon,
  ChevronDownIcon,
  ChevronRightIcon
} from '@heroicons/react/24/outline';
import YamlViewer from '../reports/YamlViewer';

interface ReportDetails {
  key: string;
  name: string;
  timestamp: string;
  strategy?: string;
  config: any;
  dataset: any;
  connections: any;
  job: any;
  jobExecution: any;
  summary: {
    totalTables: number;
    successfulTables: number;
    failedTables: number;
    databaseCount: number;
  };
  tables: TableInfo[];
  databases: Array<{ name: string; filteredOut?: Record<string, string> }>;
  runStatus?: RunStatusInfo;
}

interface RunStatusInfo {
  start?: string;
  end?: string;
  comment?: string;
  progress?: string;
  duration?: number;
  errorMessages?: string[];
  warningMessages?: string[];
  configMessages?: string[];
  operationStatistics?: {
    counts?: { databases?: number; tables?: number };
    skipped?: { databases?: number; tables?: number };
    issues?: { databases?: number; tables?: number };
    failures?: { databases?: number; tables?: number };
    successes?: { databases?: number; tables?: number };
  };
  stages?: Record<string, string>;
}

interface TableInfo {
  name: string;
  database: string;
  status: 'completed' | 'failed' | 'partial';
  strategy: string;
  phaseState: string;
  start?: string;
  steps?: Array<{mark: string; description: string; action: string}>;
  issues: string[];
  errors: string[];
  hasLeft: boolean;
  hasRight: boolean;
}

interface TableDetailsData {
  tableName: string;
  environment: string;
  name: string;
  exists: boolean;
  owner: string;
  strategy: string;
  phaseState: string;
  issues: string[];
  errors: string[];
  definition: string[];
  sql: Array<{ description: string; action: string }>;
  addProperties: Record<string, any>;
  statistics: Record<string, any>;
  partitions: Record<string, any>;
}

const RuntimeReportDetailsPage: React.FC = () => {
  const [searchParams] = useSearchParams();
  const key = searchParams.get('key');
  const navigate = useNavigate();

  const [report, setReport] = useState<ReportDetails | null>(null);
  const [reportFiles, setReportFiles] = useState<string[]>([]);
  const [loading, setLoading] = useState(true);
  const [filesLoading, setFilesLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [isDownloading, setIsDownloading] = useState(false);
  const [activeTab, setActiveTab] = useState<'overview' | 'databases' | 'config-detail' | 'dataset-detail' | 'connections-detail' | 'job-detail' | 'config' | 'status'>('overview');
  const [expandedDatabases, setExpandedDatabases] = useState<Set<string>>(new Set());
  const [databaseTables, setDatabaseTables] = useState<Record<string, TableInfo[]>>({});

  const [sessionConfig, setSessionConfig] = useState<string | null>(null);
  const [runStatus, setRunStatus] = useState<string | null>(null);

  const [showTableDetailsModal, setShowTableDetailsModal] = useState(false);
  const [tableDetails, setTableDetails] = useState<TableDetailsData | null>(null);
  const [tableDetailsLoading, setTableDetailsLoading] = useState(false);
  const [showTableOverviewModal, setShowTableOverviewModal] = useState(false);
  const [selectedTable, setSelectedTable] = useState<TableInfo | null>(null);

  // View mode toggles for detail tabs
  const [configViewMode, setConfigViewMode] = useState<'json' | 'form'>('form');
  const [datasetViewMode, setDatasetViewMode] = useState<'json' | 'form'>('form');
  const [connectionsViewMode, setConnectionsViewMode] = useState<'json' | 'form'>('form');

  useEffect(() => {
    if (key) {
      loadReportDetails();
      loadReportFiles();
    }
  }, [key]);

  const loadReportDetails = async () => {
    if (!key) return;

    try {
      setLoading(true);
      setError(null);

      const response = await fetch(`/hms-mirror/api/v1/runtime/reports/details?key=${encodeURIComponent(key)}`);

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }

      const data = await response.json();

      if (data.status === 'SUCCESS') {
        setReport(data);
      } else {
        setError(data.message || 'Failed to load report details');
      }

    } catch (error: any) {
      console.error('Failed to load report details:', error);
      setError(error.message || 'Failed to load report details');
    } finally {
      setLoading(false);
    }
  };

  const loadReportFiles = async () => {
    if (!key) return;

    try {
      setFilesLoading(true);
      const response = await fetch(`/hms-mirror/api/v1/runtime/reports/files?key=${encodeURIComponent(key)}`);

      if (response.ok) {
        const data = await response.json();
        if (data.status === 'SUCCESS' && data.files) {
          setReportFiles(data.files);

          // Load session-config.yaml if available
          if (data.files.includes('session-config.yaml')) {
            loadFile('session-config.yaml', setSessionConfig);
          }

          // Load run-status.yaml if available
          if (data.files.includes('run-status.yaml')) {
            loadFile('run-status.yaml', setRunStatus);
          }
        }
      }
    } catch (err: any) {
      console.error('Failed to load report files:', err);
    } finally {
      setFilesLoading(false);
    }
  };

  const loadFile = async (filename: string, setter: (content: string) => void) => {
    if (!key) return;

    try {
      const response = await fetch(
        `/hms-mirror/api/v1/runtime/reports/file?key=${encodeURIComponent(key)}&filename=${encodeURIComponent(filename)}`
      );

      if (response.ok) {
        const content = await response.text();
        setter(content);
      }
    } catch (err: any) {
      console.error(`Failed to load ${filename}:`, err);
    }
  };

  const handleViewTableDetails = async (tableName: string, database: string, environment: 'LEFT' | 'RIGHT') => {
    if (!key) return;

    try {
      setTableDetailsLoading(true);
      setShowTableDetailsModal(true);
      setTableDetails(null);

      const response = await fetch(
        `/hms-mirror/api/v1/runtime/reports/table-details?key=${encodeURIComponent(key)}&database=${encodeURIComponent(database)}&table=${encodeURIComponent(tableName)}&environment=${environment}`
      );

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }

      const data = await response.json();

      if (data.status === 'SUCCESS' && data.data) {
        setTableDetails(data.data);
      } else {
        throw new Error(data.message || 'Failed to load table details');
      }

    } catch (error: any) {
      console.error('Failed to load table details:', error);
      alert('Failed to load table details: ' + error.message);
      setShowTableDetailsModal(false);
    } finally {
      setTableDetailsLoading(false);
    }
  };

  const handleDownloadFile = async (filename: string) => {
    if (!key) return;

    try {
      const response = await fetch(
        `/hms-mirror/api/v1/runtime/reports/file?key=${encodeURIComponent(key)}&filename=${encodeURIComponent(filename)}`
      );

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }

      const blob = await response.blob();
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = filename;
      document.body.appendChild(a);
      a.click();
      window.URL.revokeObjectURL(url);
      document.body.removeChild(a);
    } catch (error: any) {
      console.error('Failed to download file:', error);
      alert('Failed to download file: ' + error.message);
    }
  };

  const handleBack = () => {
    navigate('/reports');
  };

  const handleDownloadReportZip = async () => {
    if (!key) return;

    try {
      setIsDownloading(true);

      // Create a download URL for the zip endpoint
      const downloadUrl = `/hms-mirror/api/v1/reports/download-by-key?key=${encodeURIComponent(key)}`;

      const response = await fetch(downloadUrl);
      if (!response.ok) {
        throw new Error(`Failed to download: ${response.statusText}`);
      }

      // Create a blob from the response
      const blob = await response.blob();

      // Create a temporary anchor element and trigger download
      const url = window.URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      link.download = `${key}_report.zip`;
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      window.URL.revokeObjectURL(url);

    } catch (error) {
      console.error('Failed to download report artifacts:', error);
      alert('Failed to download report artifacts. Please try again.');
    } finally {
      setIsDownloading(false);
    }
  };

  const formatDuration = (milliseconds?: number): string => {
    if (!milliseconds) return 'N/A';

    const seconds = Math.floor(milliseconds / 1000);
    const minutes = Math.floor(seconds / 60);
    const hours = Math.floor(minutes / 60);

    if (hours > 0) {
      return `${hours}h ${minutes % 60}m ${seconds % 60}s`;
    } else if (minutes > 0) {
      return `${minutes}m ${seconds % 60}s`;
    } else {
      return `${seconds}s`;
    }
  };

  const getProgressBadgeColor = (progress?: string): string => {
    switch (progress) {
      case 'COMPLETED': return 'bg-green-100 text-green-800';
      case 'IN_PROGRESS': return 'bg-blue-100 text-blue-800';
      case 'FAILED': return 'bg-red-100 text-red-800';
      case 'CANCELLED': return 'bg-gray-100 text-gray-800';
      default: return 'bg-gray-100 text-gray-800';
    }
  };

  const getStrategyBadgeColor = (strategy?: string): string => {
    if (!strategy) return 'bg-gray-100 text-gray-800';

    switch (strategy.toUpperCase()) {
      case 'SCHEMA_ONLY': return 'bg-purple-100 text-purple-800';
      case 'LINKED': return 'bg-blue-100 text-blue-800';
      case 'SQL': return 'bg-green-100 text-green-800';
      case 'EXPORT_IMPORT': return 'bg-yellow-100 text-yellow-800';
      case 'HYBRID': return 'bg-indigo-100 text-indigo-800';
      case 'STORAGE_MIGRATION': return 'bg-teal-100 text-teal-800';
      case 'ICEBERG_CONVERSION': return 'bg-cyan-100 text-cyan-800';
      default: return 'bg-gray-100 text-gray-800';
    }
  };

  const toggleDatabase = (databaseName: string) => {
    const newExpanded = new Set(expandedDatabases);

    if (newExpanded.has(databaseName)) {
      // Collapse database
      newExpanded.delete(databaseName);
    } else {
      // Expand database - load tables if not already loaded
      newExpanded.add(databaseName);

      if (!databaseTables[databaseName] && report) {
        // Filter tables for this database from the report
        const tablesForDb = report.tables.filter(t => t.database === databaseName);
        setDatabaseTables(prev => ({
          ...prev,
          [databaseName]: tablesForDb
        }));
      }
    }

    setExpandedDatabases(newExpanded);
  };

  const getDatabaseStats = (databaseName: string) => {
    if (!report) return { total: 0, completed: 0, failed: 0, partial: 0 };

    const tables = report.tables.filter(t => t.database === databaseName);
    return {
      total: tables.length,
      completed: tables.filter(t => t.status === 'completed').length,
      failed: tables.filter(t => t.status === 'failed').length,
      partial: tables.filter(t => t.status === 'partial').length
    };
  };

  if (loading) {
    return (
      <div className="container mx-auto px-4 py-6 max-w-7xl">
        <div className="flex items-center justify-center py-12">
          <ArrowPathIcon className="h-8 w-8 text-blue-500 animate-spin" />
          <p className="mt-2 text-sm text-gray-600">Loading report details...</p>
        </div>
      </div>
    );
  }

  if (error || !report) {
    return (
      <div className="container mx-auto px-4 py-6 max-w-7xl">
        <div className="mb-4">
          <button
            onClick={handleBack}
            className="inline-flex items-center text-sm text-blue-600 hover:text-blue-800"
          >
            <ArrowLeftIcon className="h-4 w-4 mr-1" />
            Back to Reports
          </button>
        </div>
        <div className="rounded-md bg-red-50 p-4">
          <h3 className="text-sm font-medium text-red-800">Error</h3>
          <p className="mt-1 text-sm text-red-700">{error || 'Report not found'}</p>
        </div>
      </div>
    );
  }

  return (
    <div className="container mx-auto px-4 py-6 max-w-7xl">
      {/* Header */}
      <div className="mb-6">
        <button
          onClick={handleBack}
          className="inline-flex items-center text-sm text-blue-600 hover:text-blue-800 mb-4"
        >
          <ArrowLeftIcon className="h-4 w-4 mr-1" />
          Back to Reports
        </button>

        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-3xl font-bold text-gray-900">Report Details</h1>
            <p className="mt-1 text-sm text-gray-600">
              Conversion result: {report.key}
            </p>
          </div>
          <div className="flex items-center space-x-3">
            <button
              onClick={handleDownloadReportZip}
              disabled={isDownloading}
              className="inline-flex items-center px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-offset-2 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              {isDownloading ? (
                <>
                  <svg className="animate-spin h-4 w-4 mr-2" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                    <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
                    <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                  </svg>
                  Downloading...
                </>
              ) : (
                <>
                  <ArrowDownTrayIcon className="h-5 w-5 mr-2" />
                  Download Report
                </>
              )}
            </button>
            {report.strategy && (
              <span className="inline-flex items-center px-3 py-1 rounded-full text-sm font-medium bg-blue-100 text-blue-800">
                {report.strategy}
              </span>
            )}
          </div>
        </div>
      </div>

      {/* Summary Cards */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-6 mb-8">
        <div className="bg-white rounded-lg shadow p-6 border border-gray-200">
          <div className="text-sm font-medium text-gray-500">Total Tables</div>
          <div className="text-2xl font-bold text-gray-900">{report.summary.totalTables}</div>
        </div>
        <div className="bg-white rounded-lg shadow p-6 border border-gray-200">
          <div className="text-sm font-medium text-green-600">Successful</div>
          <div className="text-2xl font-bold text-green-600">
            {report.runStatus?.operationStatistics?.successes?.tables || report.summary.successfulTables}
          </div>
        </div>
        <div className="bg-white rounded-lg shadow p-6 border border-gray-200">
          <div className="text-sm font-medium text-red-600">Failed</div>
          <div className="text-2xl font-bold text-red-600">
            {report.runStatus?.operationStatistics?.failures?.tables || report.summary.failedTables}
          </div>
        </div>
      </div>

      {/* Tabs */}
      <div className="bg-white rounded-lg shadow border border-gray-200">
        <div className="border-b border-gray-200">
          <nav className="-mb-px flex space-x-8">
            {[
              { id: 'overview', name: 'Overview' },
              { id: 'databases', name: 'Database Results' },
              ...(report.config ? [{ id: 'config-detail', name: 'Configuration' }] : []),
              ...(report.dataset ? [{ id: 'dataset-detail', name: 'Dataset' }] : []),
              ...(report.connections ? [{ id: 'connections-detail', name: 'Connections' }] : []),
              ...(report.job || report.jobExecution ? [{ id: 'job-detail', name: 'Job' }] : []),
              ...(sessionConfig ? [{ id: 'config', name: 'Session Config' }] : []),
              ...(runStatus ? [{ id: 'status', name: 'Run Status' }] : [])
            ].map((tab) => (
              <button
                key={tab.id}
                onClick={() => setActiveTab(tab.id as any)}
                className={`py-4 px-6 border-b-2 font-medium text-sm ${
                  activeTab === tab.id
                    ? 'border-blue-500 text-blue-600'
                    : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
                }`}
              >
                {tab.name}
              </button>
            ))}
          </nav>
        </div>

        <div className="p-6">
          {activeTab === 'overview' && (
            <div className="space-y-6">
              {/* Execution Summary */}
              <div>
                <h3 className="text-lg font-medium text-gray-900 mb-4">Execution Summary</h3>
                <div className="grid grid-cols-1 md:grid-cols-2 gap-4 text-sm">
                  <div><span className="text-gray-600">Created:</span> {report.timestamp || 'N/A'}</div>
                  <div><span className="text-gray-600">Databases:</span> {report.summary.databaseCount}</div>
                  {report.strategy && (
                    <div>
                      <span className="text-gray-600">Data Strategy:</span>{' '}
                      <span className={`inline-flex items-center px-2 py-1 rounded text-xs font-medium ${getStrategyBadgeColor(report.strategy)}`}>
                        {report.strategy}
                      </span>
                    </div>
                  )}
                  {report.dataset?.name && (
                    <div><span className="text-gray-600">Dataset:</span> {report.dataset.name}</div>
                  )}
                  {report.runStatus?.start && (
                    <div><span className="text-gray-600">Start Time:</span> {report.runStatus.start}</div>
                  )}
                  {report.runStatus?.end && (
                    <div><span className="text-gray-600">End Time:</span> {report.runStatus.end}</div>
                  )}
                  {report.runStatus?.duration !== undefined && (
                    <div><span className="text-gray-600">Duration:</span> {formatDuration(report.runStatus.duration)}</div>
                  )}
                  {report.runStatus?.progress && (
                    <div>
                      <span className="text-gray-600">Progress:</span>{' '}
                      <span className={`inline-flex items-center px-2 py-1 rounded text-xs font-medium ${getProgressBadgeColor(report.runStatus.progress)}`}>
                        {report.runStatus.progress}
                      </span>
                    </div>
                  )}
                </div>
              </div>

              {/* Run Comment */}
              {report.runStatus?.comment && (
                <div>
                  <h3 className="text-lg font-medium text-gray-900 mb-2">Comment</h3>
                  <div className="bg-gray-50 rounded-lg p-4 text-sm text-gray-700">
                    {report.runStatus.comment}
                  </div>
                </div>
              )}

              {/* Operational Statistics */}
              {report.runStatus?.operationStatistics && (
                <div>
                  <h3 className="text-lg font-medium text-gray-900 mb-4">Operational Statistics</h3>
                  <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                    {report.runStatus.operationStatistics.counts && (
                      <div className="bg-gray-50 rounded-lg p-4">
                        <div className="text-sm font-medium text-gray-500 mb-2">Counts</div>
                        <div className="space-y-1 text-sm">
                          <div>Tables: {report.runStatus.operationStatistics.counts.tables || 0}</div>
                          <div>Databases: {report.runStatus.operationStatistics.counts.databases || 0}</div>
                        </div>
                      </div>
                    )}
                    {report.runStatus.operationStatistics.successes && (
                      <div className="bg-green-50 rounded-lg p-4">
                        <div className="text-sm font-medium text-green-700 mb-2">Successes</div>
                        <div className="space-y-1 text-sm text-green-900">
                          <div>Tables: {report.runStatus.operationStatistics.successes.tables || 0}</div>
                          <div>Databases: {report.runStatus.operationStatistics.successes.databases || 0}</div>
                        </div>
                      </div>
                    )}
                    {report.runStatus.operationStatistics.failures && (
                      <div className="bg-red-50 rounded-lg p-4">
                        <div className="text-sm font-medium text-red-700 mb-2">Failures</div>
                        <div className="space-y-1 text-sm text-red-900">
                          <div>Tables: {report.runStatus.operationStatistics.failures.tables || 0}</div>
                          <div>Databases: {report.runStatus.operationStatistics.failures.databases || 0}</div>
                        </div>
                      </div>
                    )}
                    {report.runStatus.operationStatistics.skipped && (
                      <div className="bg-yellow-50 rounded-lg p-4">
                        <div className="text-sm font-medium text-yellow-700 mb-2">Skipped</div>
                        <div className="space-y-1 text-sm text-yellow-900">
                          <div>Tables: {report.runStatus.operationStatistics.skipped.tables || 0}</div>
                          <div>Databases: {report.runStatus.operationStatistics.skipped.databases || 0}</div>
                        </div>
                      </div>
                    )}
                    {report.runStatus.operationStatistics.issues && (
                      <div className="bg-orange-50 rounded-lg p-4">
                        <div className="text-sm font-medium text-orange-700 mb-2">Issues</div>
                        <div className="space-y-1 text-sm text-orange-900">
                          <div>Tables: {report.runStatus.operationStatistics.issues.tables || 0}</div>
                          <div>Databases: {report.runStatus.operationStatistics.issues.databases || 0}</div>
                        </div>
                      </div>
                    )}
                  </div>
                </div>
              )}

              {/* Migration Results with corrected progress bar */}
              <div>
                <h3 className="text-lg font-medium text-gray-900 mb-4">Migration Results</h3>
                <div className="bg-gray-50 rounded-lg p-4">
                  {report.runStatus?.operationStatistics ? (
                    <>
                      <div className="text-sm text-gray-600 mb-2">
                        Successfully migrated {report.runStatus.operationStatistics.successes?.tables || 0} out of {report.runStatus.operationStatistics.counts?.tables || 0} tables
                        {(report.runStatus.operationStatistics.counts?.tables || 0) > 0 && (
                          <span> ({Math.round(((report.runStatus.operationStatistics.successes?.tables || 0) / (report.runStatus.operationStatistics.counts?.tables || 0)) * 100)}% success rate)</span>
                        )}
                      </div>
                      {(report.runStatus.operationStatistics.counts?.tables || 0) > 0 && (
                        <div className="w-full bg-gray-200 rounded-full h-2">
                          <div
                            className="bg-green-600 h-2 rounded-full"
                            style={{ width: `${((report.runStatus.operationStatistics.successes?.tables || 0) / (report.runStatus.operationStatistics.counts?.tables || 0)) * 100}%` }}
                          ></div>
                        </div>
                      )}
                    </>
                  ) : (
                    <>
                      <div className="text-sm text-gray-600 mb-2">
                        Successfully migrated {report.summary.successfulTables} out of {report.summary.totalTables} tables
                        {report.summary.totalTables > 0 && (
                          <span> ({Math.round((report.summary.successfulTables / report.summary.totalTables) * 100)}% success rate)</span>
                        )}
                      </div>
                      {report.summary.totalTables > 0 && (
                        <div className="w-full bg-gray-200 rounded-full h-2">
                          <div
                            className="bg-green-600 h-2 rounded-full"
                            style={{ width: `${(report.summary.successfulTables / report.summary.totalTables) * 100}%` }}
                          ></div>
                        </div>
                      )}
                    </>
                  )}
                </div>
              </div>

              {/* Error Messages */}
              {report.runStatus?.errorMessages && report.runStatus.errorMessages.length > 0 && (
                <div>
                  <h3 className="text-lg font-medium text-red-700 mb-2 flex items-center">
                    <XCircleIcon className="h-5 w-5 mr-2" />
                    Error Messages
                  </h3>
                  <div className="bg-red-50 rounded-lg p-4 border border-red-200">
                    <ul className="list-disc list-inside space-y-1 text-sm text-red-800">
                      {report.runStatus.errorMessages.map((msg, idx) => (
                        <li key={idx}>{msg}</li>
                      ))}
                    </ul>
                  </div>
                </div>
              )}

              {/* Warning Messages */}
              {report.runStatus?.warningMessages && report.runStatus.warningMessages.length > 0 && (
                <div>
                  <h3 className="text-lg font-medium text-yellow-700 mb-2 flex items-center">
                    <ExclamationTriangleIcon className="h-5 w-5 mr-2" />
                    Warning Messages
                  </h3>
                  <div className="bg-yellow-50 rounded-lg p-4 border border-yellow-200">
                    <ul className="list-disc list-inside space-y-1 text-sm text-yellow-800">
                      {report.runStatus.warningMessages.map((msg, idx) => (
                        <li key={idx}>{msg}</li>
                      ))}
                    </ul>
                  </div>
                </div>
              )}

              {/* Config Messages */}
              {report.runStatus?.configMessages && report.runStatus.configMessages.length > 0 && (
                <div>
                  <h3 className="text-lg font-medium text-blue-700 mb-2 flex items-center">
                    <DocumentTextIcon className="h-5 w-5 mr-2" />
                    Configuration Messages
                  </h3>
                  <div className="bg-blue-50 rounded-lg p-4 border border-blue-200">
                    <ul className="list-disc list-inside space-y-1 text-sm text-blue-800">
                      {Array.from(report.runStatus.configMessages).map((msg, idx) => (
                        <li key={idx}>{msg}</li>
                      ))}
                    </ul>
                  </div>
                </div>
              )}

              {/* Generated Reports Section */}
              <div>
                <div className="flex items-center justify-between mb-4">
                  <h3 className="text-lg font-medium text-gray-900">Generated Reports</h3>
                  {filesLoading && (
                    <ArrowPathIcon className="h-5 w-5 text-gray-400 animate-spin" />
                  )}
                </div>

                {reportFiles.length === 0 && !filesLoading ? (
                  <div className="rounded-md bg-yellow-50 p-4 border border-yellow-200">
                    <p className="text-sm text-yellow-700">
                      No report files available for this conversion result.
                    </p>
                  </div>
                ) : (
                  <div className="bg-gray-50 rounded-md divide-y divide-gray-200">
                    {reportFiles.map(filename => (
                      <div key={filename} className="flex items-center justify-between p-3">
                        <div className="flex items-center">
                          <DocumentTextIcon className="h-5 w-5 text-gray-400 mr-3" />
                          <span className="text-sm text-gray-900">{filename}</span>
                        </div>
                        <button
                          onClick={() => handleDownloadFile(filename)}
                          className="inline-flex items-center px-3 py-1 border border-gray-300 shadow-sm text-sm font-medium rounded-md text-gray-700 bg-white hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500"
                        >
                          <ArrowDownTrayIcon className="h-4 w-4 mr-1" />
                          Download
                        </button>
                      </div>
                    ))}
                  </div>
                )}
              </div>
            </div>
          )}

          {activeTab === 'databases' && (
            <div>
              <h3 className="text-lg font-medium text-gray-900 mb-4">Database Migration Results</h3>
              {report.databases.length === 0 ? (
                <div className="text-center py-8 text-gray-500">
                  No databases found
                </div>
              ) : (
                <div className="space-y-2">
                  {report.databases.map((database) => {
                    const dbName = database.name;
                    const isExpanded = expandedDatabases.has(dbName);
                    const stats = getDatabaseStats(dbName);
                    const tables = databaseTables[dbName] || [];

                    return (
                      <div key={dbName} className="border border-gray-200 rounded-lg overflow-hidden">
                        {/* Database Name Header */}
                        <div className="px-4 py-2 bg-gray-100 border-b border-gray-200">
                          <h3 className="font-semibold text-gray-900">{dbName}</h3>
                        </div>

                        {/* Filtered Out Tables Section */}
                        {database.filteredOut && Object.keys(database.filteredOut).length > 0 && (
                          <div className="px-4 py-3 bg-yellow-50 border-b border-yellow-200">
                            <h4 className="text-sm font-medium text-yellow-900 mb-2">Filtered Out Tables</h4>
                            <div className="space-y-1">
                              {Object.entries(database.filteredOut).map(([tableName, reason]) => (
                                <div key={tableName} className="text-sm text-yellow-800">
                                  <span className="font-medium">{tableName}:</span> <span className="text-yellow-700">{reason}</span>
                                </div>
                              ))}
                            </div>
                          </div>
                        )}

                        {/* Collapsible Tables Section */}
                        <button
                          onClick={() => toggleDatabase(dbName)}
                          className="w-full px-4 py-3 bg-gray-50 hover:bg-gray-100 flex items-center justify-between text-left transition-colors"
                        >
                          <div className="flex items-center space-x-3">
                            {isExpanded ? (
                              <ChevronDownIcon className="h-5 w-5 text-gray-500" />
                            ) : (
                              <ChevronRightIcon className="h-5 w-5 text-gray-500" />
                            )}
                            <span className="text-gray-700">Tables</span>
                          </div>

                          <div className="flex items-center space-x-4 text-sm">
                            <span className="text-gray-600">
                              Total: <span className="font-medium">{stats.total}</span>
                            </span>
                            {stats.completed > 0 && (
                              <span className="text-green-600">
                                <CheckCircleIcon className="h-4 w-4 inline mr-1" />
                                {stats.completed}
                              </span>
                            )}
                            {stats.failed > 0 && (
                              <span className="text-red-600">
                                <XCircleIcon className="h-4 w-4 inline mr-1" />
                                {stats.failed}
                              </span>
                            )}
                            {stats.partial > 0 && (
                              <span className="text-yellow-600">
                                <ExclamationTriangleIcon className="h-4 w-4 inline mr-1" />
                                {stats.partial}
                              </span>
                            )}
                          </div>
                        </button>

                        {/* Database Tables - Shown when expanded */}
                        {isExpanded && (
                          <div className="bg-white">
                            {tables.length === 0 ? (
                              <div className="px-4 py-6 text-center text-gray-500 text-sm">
                                No tables found in this database
                              </div>
                            ) : (
                              <div className="overflow-x-auto">
                                <table className="min-w-full divide-y divide-gray-200">
                                  <thead className="bg-gray-50">
                                    <tr>
                                      <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Phase State</th>
                                      <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Table</th>
                                      <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Strategy</th>
                                      <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Actions</th>
                                    </tr>
                                  </thead>
                                  <tbody className="bg-white divide-y divide-gray-200">
                                    {tables.map((table, idx) => (
                                      <tr
                                        key={idx}
                                        className="hover:bg-gray-100 cursor-pointer"
                                        onClick={() => {
                                          setSelectedTable(table);
                                          setShowTableOverviewModal(true);
                                        }}
                                      >
                                        <td className="px-4 py-3 whitespace-nowrap text-sm text-gray-900">{table.phaseState}</td>
                                        <td className="px-4 py-3 whitespace-nowrap text-sm font-medium text-blue-600">{table.name}</td>
                                        <td className="px-4 py-3 whitespace-nowrap text-sm text-gray-500">{table.strategy}</td>
                                        <td className="px-4 py-3 whitespace-nowrap text-sm" onClick={(e) => e.stopPropagation()}>
                                          <div className="flex space-x-2">
                                            {table.hasLeft && (
                                              <button
                                                onClick={() => handleViewTableDetails(table.name, table.database, 'LEFT')}
                                                className="text-blue-600 hover:text-blue-800 font-medium"
                                              >
                                                LEFT
                                              </button>
                                            )}
                                            {table.hasRight && (
                                              <button
                                                onClick={() => handleViewTableDetails(table.name, table.database, 'RIGHT')}
                                                className="text-blue-600 hover:text-blue-800 font-medium"
                                              >
                                                RIGHT
                                              </button>
                                            )}
                                          </div>
                                        </td>
                                      </tr>
                                    ))}
                                  </tbody>
                                </table>
                              </div>
                            )}
                          </div>
                        )}
                      </div>
                    );
                  })}
                </div>
              )}
            </div>
          )}

          {activeTab === 'config-detail' && report.config && (
            <div>
              <div className="flex items-center justify-between mb-4">
                <h3 className="text-lg font-medium text-gray-900">Configuration Details</h3>
                <div className="flex items-center space-x-2">
                  <button
                    onClick={() => setConfigViewMode('form')}
                    className={`px-3 py-1 rounded-md text-sm font-medium ${
                      configViewMode === 'form'
                        ? 'bg-blue-600 text-white'
                        : 'bg-gray-200 text-gray-700 hover:bg-gray-300'
                    }`}
                  >
                    Form View
                  </button>
                  <button
                    onClick={() => setConfigViewMode('json')}
                    className={`px-3 py-1 rounded-md text-sm font-medium ${
                      configViewMode === 'json'
                        ? 'bg-blue-600 text-white'
                        : 'bg-gray-200 text-gray-700 hover:bg-gray-300'
                    }`}
                  >
                    JSON
                  </button>
                </div>
              </div>

              {configViewMode === 'json' ? (
                <div className="bg-gray-50 rounded-lg p-6">
                  <pre className="text-sm text-gray-800 font-mono whitespace-pre-wrap">
                    {JSON.stringify(report.config, null, 2)}
                  </pre>
                </div>
              ) : (
                <div className="space-y-6">
                  <div className="bg-white border border-gray-200 rounded-lg p-6">
                    <h4 className="text-md font-semibold text-gray-900 mb-4">Basic Information</h4>
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                      <div>
                        <label className="block text-sm font-medium text-gray-700 mb-1">Name</label>
                        <div className="text-sm text-gray-900 bg-gray-50 rounded px-3 py-2">{report.config.name || 'N/A'}</div>
                      </div>
                      <div>
                        <label className="block text-sm font-medium text-gray-700 mb-1">Data Strategy</label>
                        <div className="text-sm text-gray-900 bg-gray-50 rounded px-3 py-2">{report.config.dataStrategy || 'N/A'}</div>
                      </div>
                      {report.config.description && (
                        <div className="md:col-span-2">
                          <label className="block text-sm font-medium text-gray-700 mb-1">Description</label>
                          <div className="text-sm text-gray-900 bg-gray-50 rounded px-3 py-2">{report.config.description}</div>
                        </div>
                      )}
                    </div>
                  </div>

                  {Object.keys(report.config).filter(key => !['name', 'dataStrategy', 'description'].includes(key)).length > 0 && (
                    <div className="bg-white border border-gray-200 rounded-lg p-6">
                      <h4 className="text-md font-semibold text-gray-900 mb-4">Additional Settings</h4>
                      <div className="bg-gray-50 rounded-lg p-4">
                        <pre className="text-sm text-gray-800 font-mono whitespace-pre-wrap">
                          {JSON.stringify(
                            Object.fromEntries(
                              Object.entries(report.config).filter(([key]) => !['name', 'dataStrategy', 'description'].includes(key))
                            ),
                            null,
                            2
                          )}
                        </pre>
                      </div>
                    </div>
                  )}
                </div>
              )}
            </div>
          )}

          {activeTab === 'dataset-detail' && report.dataset && (
            <div>
              <div className="flex items-center justify-between mb-4">
                <h3 className="text-lg font-medium text-gray-900">Dataset Details</h3>
                <div className="flex items-center space-x-2">
                  <button
                    onClick={() => setDatasetViewMode('form')}
                    className={`px-3 py-1 rounded-md text-sm font-medium ${
                      datasetViewMode === 'form'
                        ? 'bg-blue-600 text-white'
                        : 'bg-gray-200 text-gray-700 hover:bg-gray-300'
                    }`}
                  >
                    Form View
                  </button>
                  <button
                    onClick={() => setDatasetViewMode('json')}
                    className={`px-3 py-1 rounded-md text-sm font-medium ${
                      datasetViewMode === 'json'
                        ? 'bg-blue-600 text-white'
                        : 'bg-gray-200 text-gray-700 hover:bg-gray-300'
                    }`}
                  >
                    JSON
                  </button>
                </div>
              </div>

              {datasetViewMode === 'json' ? (
                <div className="bg-gray-50 rounded-lg p-6">
                  <pre className="text-sm text-gray-800 font-mono whitespace-pre-wrap">
                    {JSON.stringify(report.dataset, null, 2)}
                  </pre>
                </div>
              ) : (
                <div className="space-y-6">
                  <div className="bg-white border border-gray-200 rounded-lg p-6">
                    <h4 className="text-md font-semibold text-gray-900 mb-4">Basic Information</h4>
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                      <div>
                        <label className="block text-sm font-medium text-gray-700 mb-1">Name</label>
                        <div className="text-sm text-gray-900 bg-gray-50 rounded px-3 py-2">{report.dataset.name || 'N/A'}</div>
                      </div>
                      {report.dataset.description && (
                        <div className="md:col-span-2">
                          <label className="block text-sm font-medium text-gray-700 mb-1">Description</label>
                          <div className="text-sm text-gray-900 bg-gray-50 rounded px-3 py-2">{report.dataset.description}</div>
                        </div>
                      )}
                    </div>
                  </div>

                  {report.dataset.databases && report.dataset.databases.length > 0 && (
                    <div className="bg-white border border-gray-200 rounded-lg p-6">
                      <h4 className="text-md font-semibold text-gray-900 mb-4">Databases ({report.dataset.databases.length})</h4>
                      <div className="space-y-3">
                        {report.dataset.databases.map((db: any, idx: number) => (
                          <div key={idx} className="bg-gray-50 rounded-lg p-4">
                            <div className="font-medium text-gray-900 mb-2">{db.databaseName || db.name}</div>
                            {db.tables && db.tables.length > 0 && (
                              <div className="text-sm text-gray-600">
                                Tables: {db.tables.length}
                                <div className="mt-2 flex flex-wrap gap-2">
                                  {db.tables.slice(0, 10).map((table: string, tIdx: number) => (
                                    <span key={tIdx} className="inline-block bg-white px-2 py-1 rounded text-xs border border-gray-200">
                                      {table}
                                    </span>
                                  ))}
                                  {db.tables.length > 10 && (
                                    <span className="inline-block px-2 py-1 text-xs text-gray-500">
                                      +{db.tables.length - 10} more
                                    </span>
                                  )}
                                </div>
                              </div>
                            )}
                          </div>
                        ))}
                      </div>
                    </div>
                  )}
                </div>
              )}
            </div>
          )}

          {activeTab === 'connections-detail' && report.connections && (
            <div>
              <div className="flex items-center justify-between mb-4">
                <h3 className="text-lg font-medium text-gray-900">Connections</h3>
                <div className="flex items-center space-x-2">
                  <button
                    onClick={() => setConnectionsViewMode('form')}
                    className={`px-3 py-1 rounded-md text-sm font-medium ${
                      connectionsViewMode === 'form'
                        ? 'bg-blue-600 text-white'
                        : 'bg-gray-200 text-gray-700 hover:bg-gray-300'
                    }`}
                  >
                    Form View
                  </button>
                  <button
                    onClick={() => setConnectionsViewMode('json')}
                    className={`px-3 py-1 rounded-md text-sm font-medium ${
                      connectionsViewMode === 'json'
                        ? 'bg-blue-600 text-white'
                        : 'bg-gray-200 text-gray-700 hover:bg-gray-300'
                    }`}
                  >
                    JSON
                  </button>
                </div>
              </div>

              {connectionsViewMode === 'json' ? (
                <div className="space-y-4">
                  {Object.entries(report.connections).map(([env, connection]: [string, any]) => (
                    <div key={env} className="bg-gray-50 rounded-lg p-6">
                      <h4 className="text-md font-medium text-gray-700 mb-3">{env} Environment</h4>
                      <pre className="text-sm text-gray-800 font-mono whitespace-pre-wrap">
                        {JSON.stringify(connection, null, 2)}
                      </pre>
                    </div>
                  ))}
                </div>
              ) : (
                <div className="space-y-6">
                  {Object.entries(report.connections).map(([env, connection]: [string, any]) => (
                    <div key={env} className="bg-white border border-gray-200 rounded-lg p-6">
                      <h4 className="text-md font-semibold text-gray-900 mb-4">{env} Environment</h4>
                      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                        <div>
                          <label className="block text-sm font-medium text-gray-700 mb-1">Name</label>
                          <div className="text-sm text-gray-900 bg-gray-50 rounded px-3 py-2">{connection.name || 'N/A'}</div>
                        </div>
                        {connection.environment && (
                          <div>
                            <label className="block text-sm font-medium text-gray-700 mb-1">Environment</label>
                            <div className="text-sm text-gray-900 bg-gray-50 rounded px-3 py-2">{connection.environment}</div>
                          </div>
                        )}
                        {connection.platformType && (
                          <div>
                            <label className="block text-sm font-medium text-gray-700 mb-1">Platform Type</label>
                            <div className="text-sm text-gray-900 bg-gray-50 rounded px-3 py-2">{connection.platformType}</div>
                          </div>
                        )}
                        {connection.hiveServer2 && (
                          <div>
                            <label className="block text-sm font-medium text-gray-700 mb-1">HiveServer2</label>
                            <div className="text-sm text-gray-900 bg-gray-50 rounded px-3 py-2">{connection.hiveServer2.uri || 'Configured'}</div>
                          </div>
                        )}
                        {connection.metastoreDirectUri && (
                          <div>
                            <label className="block text-sm font-medium text-gray-700 mb-1">Metastore URI</label>
                            <div className="text-sm text-gray-900 bg-gray-50 rounded px-3 py-2">{connection.metastoreDirectUri}</div>
                          </div>
                        )}
                      </div>

                      {Object.keys(connection).filter(key => !['name', 'environment', 'platformType', 'hiveServer2', 'metastoreDirectUri'].includes(key)).length > 0 && (
                        <div className="mt-4">
                          <label className="block text-sm font-medium text-gray-700 mb-2">Additional Configuration</label>
                          <div className="bg-gray-50 rounded-lg p-4">
                            <pre className="text-sm text-gray-800 font-mono whitespace-pre-wrap">
                              {JSON.stringify(
                                Object.fromEntries(
                                  Object.entries(connection).filter(([key]) => !['name', 'environment', 'platformType', 'hiveServer2', 'metastoreDirectUri'].includes(key))
                                ),
                                null,
                                2
                              )}
                            </pre>
                          </div>
                        </div>
                      )}
                    </div>
                  ))}
                </div>
              )}
            </div>
          )}

          {activeTab === 'job-detail' && (report.job || report.jobExecution) && (
            <div className="space-y-6">
              {report.job && (
                <div>
                  <h3 className="text-lg font-medium text-gray-900 mb-4">Job Details</h3>
                  <div className="bg-gray-50 rounded-lg p-6">
                    <pre className="text-sm text-gray-800 font-mono whitespace-pre-wrap">
                      {JSON.stringify(report.job, null, 2)}
                    </pre>
                  </div>
                </div>
              )}
              {report.jobExecution && (
                <div>
                  <h3 className="text-lg font-medium text-gray-900 mb-4">Job Execution</h3>
                  <div className="bg-gray-50 rounded-lg p-6">
                    <pre className="text-sm text-gray-800 font-mono whitespace-pre-wrap">
                      {JSON.stringify(report.jobExecution, null, 2)}
                    </pre>
                  </div>
                </div>
              )}
            </div>
          )}

          {activeTab === 'config' && sessionConfig && (
            <div>
              <h3 className="text-lg font-medium text-gray-900 mb-4">Session Configuration</h3>
              <YamlViewer content={sessionConfig} />
            </div>
          )}

          {activeTab === 'status' && runStatus && (
            <div>
              <h3 className="text-lg font-medium text-gray-900 mb-4">Run Status Details</h3>
              <YamlViewer content={runStatus} />
            </div>
          )}
        </div>
      </div>

      {/* Table Details Modal */}
      {showTableDetailsModal && (
        <div className="fixed inset-0 bg-gray-500 bg-opacity-75 flex items-center justify-center z-50">
          <div className="bg-white rounded-lg shadow-xl max-w-4xl w-full mx-4 max-h-[90vh] overflow-hidden flex flex-col">
            {/* Modal Header */}
            <div className="px-6 py-4 border-b border-gray-200 flex items-center justify-between">
              <h2 className="text-xl font-semibold text-gray-900">
                {tableDetails ? `${tableDetails.tableName} (${tableDetails.environment})` : 'Loading...'}
              </h2>
              <button
                onClick={() => setShowTableDetailsModal(false)}
                className="text-gray-400 hover:text-gray-500"
              >
                <XMarkIcon className="h-6 w-6" />
              </button>
            </div>

            {/* Modal Body */}
            <div className="flex-1 overflow-y-auto p-6">
              {tableDetailsLoading ? (
                <div className="flex items-center justify-center py-12">
                  <ArrowPathIcon className="h-8 w-8 text-blue-500 animate-spin" />
                  <p className="ml-3 text-sm text-gray-600">Loading table details...</p>
                </div>
              ) : tableDetails ? (
                <div className="space-y-6">
                  {/* Table Info */}
                  <div>
                    <h3 className="text-sm font-medium text-gray-700 mb-2">Table Information</h3>
                    <div className="bg-gray-50 rounded-lg p-4 grid grid-cols-2 gap-3 text-sm">
                      <div><span className="text-gray-600">Name:</span> {tableDetails.name}</div>
                      <div><span className="text-gray-600">Exists:</span> {tableDetails.exists ? 'Yes' : 'No'}</div>
                      <div><span className="text-gray-600">Owner:</span> {tableDetails.owner}</div>
                      <div><span className="text-gray-600">Strategy:</span> {tableDetails.strategy}</div>
                      <div><span className="text-gray-600">Phase State:</span> {tableDetails.phaseState}</div>
                    </div>
                  </div>

                  {/* Issues */}
                  {tableDetails.issues && tableDetails.issues.length > 0 && (
                    <div>
                      <h3 className="text-sm font-medium text-yellow-700 mb-2 flex items-center">
                        <ExclamationTriangleIcon className="h-4 w-4 mr-1" />
                        Issues
                      </h3>
                      <div className="bg-yellow-50 rounded-lg p-4 border border-yellow-200">
                        <ul className="list-disc list-inside space-y-1 text-sm text-yellow-800">
                          {tableDetails.issues.map((issue, idx) => (
                            <li key={idx}>{issue}</li>
                          ))}
                        </ul>
                      </div>
                    </div>
                  )}

                  {/* Errors */}
                  {tableDetails.errors && tableDetails.errors.length > 0 && (
                    <div>
                      <h3 className="text-sm font-medium text-red-700 mb-2 flex items-center">
                        <XCircleIcon className="h-4 w-4 mr-1" />
                        Errors
                      </h3>
                      <div className="bg-red-50 rounded-lg p-4 border border-red-200">
                        <ul className="list-disc list-inside space-y-1 text-sm text-red-800">
                          {tableDetails.errors.map((error, idx) => (
                            <li key={idx}>{error}</li>
                          ))}
                        </ul>
                      </div>
                    </div>
                  )}

                  {/* Statistics */}
                  {tableDetails.statistics && Object.keys(tableDetails.statistics).length > 0 && (
                    <div>
                      <h3 className="text-sm font-medium text-gray-700 mb-2">Statistics</h3>
                      <div className="bg-gray-50 rounded-lg p-4 overflow-x-auto">
                        <pre className="text-sm text-gray-800 font-mono">
                          {JSON.stringify(tableDetails.statistics, null, 2)}
                        </pre>
                      </div>
                    </div>
                  )}

                  {/* Partitions */}
                  {tableDetails.partitions && Object.keys(tableDetails.partitions).length > 0 && (
                    <div>
                      <h3 className="text-sm font-medium text-gray-700 mb-2">Partitions</h3>
                      <div className="bg-gray-50 rounded-lg p-4 overflow-x-auto">
                        <pre className="text-sm text-gray-800 font-mono">
                          {JSON.stringify(tableDetails.partitions, null, 2)}
                        </pre>
                      </div>
                    </div>
                  )}

                  {/* Table Definition */}
                  {tableDetails.definition && tableDetails.definition.length > 0 && (
                    <div>
                      <h3 className="text-sm font-medium text-gray-700 mb-2 flex items-center">
                        <CodeBracketIcon className="h-4 w-4 mr-1" />
                        Table Definition
                      </h3>
                      <div className="bg-gray-900 rounded-lg p-4 overflow-x-auto">
                        <pre className="text-sm text-gray-100 font-mono">
                          {tableDetails.definition.join('\n')}
                        </pre>
                      </div>
                    </div>
                  )}

                  {/* SQL Scripts */}
                  {tableDetails.sql && tableDetails.sql.length > 0 && (
                    <div>
                      <h3 className="text-sm font-medium text-gray-700 mb-2">SQL Scripts</h3>
                      <div className="space-y-3">
                        {tableDetails.sql.map((script, idx) => (
                          <div key={idx} className="border border-gray-200 rounded-lg overflow-hidden">
                            <div className="bg-gray-50 px-4 py-2 text-sm font-medium text-gray-700">
                              {script.description}
                            </div>
                            <div className="bg-gray-900 p-4 overflow-x-auto">
                              <pre className="text-sm text-gray-100 font-mono">
                                {script.action}
                              </pre>
                            </div>
                          </div>
                        ))}
                      </div>
                    </div>
                  )}

                  {/* Additional Properties */}
                  {tableDetails.addProperties && Object.keys(tableDetails.addProperties).length > 0 && (
                    <div>
                      <h3 className="text-sm font-medium text-gray-700 mb-2">Additional Properties</h3>
                      <div className="bg-gray-50 rounded-lg p-4">
                        <pre className="text-sm text-gray-800 font-mono">
                          {JSON.stringify(tableDetails.addProperties, null, 2)}
                        </pre>
                      </div>
                    </div>
                  )}
                </div>
              ) : (
                <div className="text-center py-8 text-gray-500">
                  No table details available
                </div>
              )}
            </div>
          </div>
        </div>
      )}

      {/* Table Overview Modal */}
      {showTableOverviewModal && selectedTable && (
        <div className="fixed inset-0 bg-gray-500 bg-opacity-75 flex items-center justify-center z-50">
          <div className="bg-white rounded-lg shadow-xl max-w-3xl w-full mx-4 max-h-[90vh] overflow-hidden flex flex-col">
            {/* Modal Header */}
            <div className="px-6 py-4 border-b border-gray-200 flex items-center justify-between">
              <h2 className="text-xl font-semibold text-gray-900">
                {selectedTable.database}.{selectedTable.name}
              </h2>
              <button
                onClick={() => setShowTableOverviewModal(false)}
                className="text-gray-400 hover:text-gray-500"
              >
                <XMarkIcon className="h-6 w-6" />
              </button>
            </div>

            {/* Modal Body */}
            <div className="flex-1 overflow-y-auto p-6">
              <div className="space-y-6">
                {/* Table Information */}
                <div>
                  <h3 className="text-sm font-medium text-gray-700 mb-2">Table Information</h3>
                  <div className="bg-gray-50 rounded-lg p-4 grid grid-cols-2 gap-3 text-sm">
                    <div><span className="text-gray-600">Database:</span> {selectedTable.database}</div>
                    <div><span className="text-gray-600">Table:</span> {selectedTable.name}</div>
                    <div><span className="text-gray-600">Phase State:</span> {selectedTable.phaseState}</div>
                    <div><span className="text-gray-600">Strategy:</span> {selectedTable.strategy}</div>
                    {selectedTable.start && (
                      <div className="col-span-2">
                        <span className="text-gray-600">Start:</span> {new Date(selectedTable.start).toLocaleString()}
                      </div>
                    )}
                  </div>
                </div>

                {/* Steps Table */}
                {selectedTable.steps && selectedTable.steps.length > 0 && (
                  <div>
                    <h3 className="text-sm font-medium text-gray-700 mb-2">Steps</h3>
                    <div className="bg-gray-50 rounded-lg overflow-hidden">
                      <table className="min-w-full text-sm">
                        <thead className="bg-gray-200">
                          <tr>
                            <th className="px-3 py-2 text-left text-gray-700 font-medium">Mark</th>
                            <th className="px-3 py-2 text-left text-gray-700 font-medium">Description</th>
                            <th className="px-3 py-2 text-left text-gray-700 font-medium">Action</th>
                          </tr>
                        </thead>
                        <tbody className="divide-y divide-gray-200">
                          {selectedTable.steps.map((step, idx) => (
                            <tr key={idx} className="hover:bg-gray-100">
                              <td className="px-3 py-2 text-gray-900">{step.mark}</td>
                              <td className="px-3 py-2 text-gray-700">{step.description}</td>
                              <td className="px-3 py-2 font-mono text-xs text-gray-600">{step.action}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </div>
                )}

                {/* Issues */}
                {selectedTable.issues && selectedTable.issues.length > 0 && (
                  <div>
                    <h3 className="text-sm font-medium text-yellow-700 mb-2 flex items-center">
                      <ExclamationTriangleIcon className="h-4 w-4 mr-1" />
                      Issues ({selectedTable.issues.length})
                    </h3>
                    <div className="bg-yellow-50 rounded-lg p-4 border border-yellow-200">
                      <ul className="list-disc list-inside space-y-1 text-sm text-yellow-800">
                        {selectedTable.issues.map((issue, idx) => (
                          <li key={idx}>{issue}</li>
                        ))}
                      </ul>
                    </div>
                  </div>
                )}

                {/* Errors */}
                {selectedTable.errors && selectedTable.errors.length > 0 && (
                  <div>
                    <h3 className="text-sm font-medium text-red-700 mb-2 flex items-center">
                      <XCircleIcon className="h-4 w-4 mr-1" />
                      Errors ({selectedTable.errors.length})
                    </h3>
                    <div className="bg-red-50 rounded-lg p-4 border border-red-200">
                      <ul className="list-disc list-inside space-y-1 text-sm text-red-800">
                        {selectedTable.errors.map((error, idx) => (
                          <li key={idx}>{error}</li>
                        ))}
                      </ul>
                    </div>
                  </div>
                )}

                {/* Environment Details Links */}
                <div>
                  <h3 className="text-sm font-medium text-gray-700 mb-3">Environment Details</h3>
                  <div className="flex space-x-3">
                    {selectedTable.hasLeft && (
                      <button
                        onClick={() => {
                          setShowTableOverviewModal(false);
                          handleViewTableDetails(selectedTable.name, selectedTable.database, 'LEFT');
                        }}
                        className="flex-1 bg-blue-600 hover:bg-blue-700 text-white px-4 py-2 rounded-lg font-medium transition-colors"
                      >
                        View LEFT Environment
                      </button>
                    )}
                    {selectedTable.hasRight && (
                      <button
                        onClick={() => {
                          setShowTableOverviewModal(false);
                          handleViewTableDetails(selectedTable.name, selectedTable.database, 'RIGHT');
                        }}
                        className="flex-1 bg-green-600 hover:bg-green-700 text-white px-4 py-2 rounded-lg font-medium transition-colors"
                      >
                        View RIGHT Environment
                      </button>
                    )}
                  </div>
                  {!selectedTable.hasLeft && !selectedTable.hasRight && (
                    <p className="text-sm text-gray-500 italic">No environment details available</p>
                  )}
                </div>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default RuntimeReportDetailsPage;
