import React, { useState, useEffect } from 'react';
import { useNavigate, useSearchParams } from 'react-router-dom';
import {
  DocumentTextIcon,
  CalendarIcon,
  ClockIcon,
  CheckCircleIcon,
  XCircleIcon,
  ExclamationTriangleIcon,
  FolderIcon,
  EyeIcon,
  ArrowLeftIcon,
  HomeIcon,
  CommandLineIcon,
  GlobeAltIcon,
  ChevronLeftIcon,
  ChevronRightIcon,
  MagnifyingGlassIcon,
  XMarkIcon
} from '@heroicons/react/24/outline';

interface MigrationReport {
  id: string;
  name: string;
  path: string;
  timestamp: string;
  status: 'completed' | 'failed' | 'partial' | 'unknown';
  database: string;
  summary: {
    totalTables: number;
    successfulTables: number;
    failedTables: number;
  };
  artifacts: {
    migrationReport: boolean;
    errorLog: boolean;
    sqlScripts: boolean;
    executionPlan: boolean;
  };
}

interface Directory {
  name: string;
  path: string;
}

interface ConversionResult {
  key: string;
  created: string;
  config?: {
    name: string;
    dataStrategy?: string;
  };
  dataset?: {
    name: string;
    description?: string;
  };
  connections?: Record<string, {
    name: string;
    description?: string;
    environment?: string;
    platformType?: string;
  }>;
  job?: {
    name: string;
    strategy?: string;
    key?: string;
  };
  runStatus?: {
    progress?: string;
    errorMessages?: string[];
    warningMessages?: string[];
  };
}

interface ReportsBrowseResponse {
  currentPath: string;
  directories: Directory[];
  reports: MigrationReport[];
  message?: string;
}

const ReportsPage: React.FC = () => {
  const navigate = useNavigate();
  const [searchParams, setSearchParams] = useSearchParams();
  const currentPath = searchParams.get('path') || '';
  const [activeTab, setActiveTab] = useState<'cli' | 'web'>('web');

  const [browseData, setBrowseData] = useState<ReportsBrowseResponse>({
    currentPath: '',
    directories: [],
    reports: []
  });
  const [rocksDBResults, setRocksDBResults] = useState<ConversionResult[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Pagination state for Web Reports
  const [webReportsPage, setWebReportsPage] = useState(0);
  const [webReportsTotalCount, setWebReportsTotalCount] = useState(0);
  const WEB_REPORTS_PAGE_SIZE = 10;

  // Search state for Web Reports
  const [searchTerm, setSearchTerm] = useState('');
  const [searchInput, setSearchInput] = useState(''); // Separate state for input field

  useEffect(() => {
    console.log('ReportsPage mounted, currentPath:', currentPath);
    loadAllReports();
  }, [currentPath, webReportsPage, searchTerm]); // searchTerm triggers API call, not searchInput

  const loadAllReports = async () => {
    try {
      console.log('loadAllReports called, making API calls...');
      setIsLoading(true);
      setError(null);

      // Load filesystem reports
      const params = new URLSearchParams();
      if (currentPath) {
        params.append('path', currentPath);
      }

      const fsUrl = `/hms-mirror/api/v1/reports/browse?${params}`;
      console.log('Filesystem reports URL:', fsUrl);
      const fsResponse = await fetch(fsUrl);
      console.log('Filesystem reports response status:', fsResponse.status);

      let fsData: ReportsBrowseResponse = {
        currentPath: '',
        directories: [],
        reports: []
      };

      if (fsResponse.ok) {
        fsData = await fsResponse.json();
        console.log('Filesystem reports data:', fsData);
      }

      setBrowseData(fsData);

      // Load RocksDB conversion results (only at root level)
      if (!currentPath) {
        try {
          const searchParam = searchTerm.trim() ? `&search=${encodeURIComponent(searchTerm.trim())}` : '';
          const rocksUrl = `/hms-mirror/api/v1/runtime/reports?page=${webReportsPage}&pageSize=${WEB_REPORTS_PAGE_SIZE}${searchParam}`;
          console.log('RocksDB reports URL:', rocksUrl);
          const rocksResponse = await fetch(rocksUrl);
          console.log('RocksDB reports response status:', rocksResponse.status);

          if (rocksResponse.ok) {
            const rocksData = await rocksResponse.json();
            console.log('RocksDB reports data:', rocksData);
            if (rocksData.status === 'success' && rocksData.data) {
              setRocksDBResults(rocksData.data);
              setWebReportsTotalCount(rocksData.totalCount || 0);
            }
          }
        } catch (rocksError) {
          console.error('Failed to load RocksDB reports:', rocksError);
          // Don't fail the whole page if RocksDB reports fail
        }
      } else {
        setRocksDBResults([]);
        setWebReportsTotalCount(0);
      }

    } catch (error) {
      console.error('Failed to load reports:', error);
      setError('Failed to load migration reports. Please check if the reports directory exists.');
    } finally {
      setIsLoading(false);
    }
  };

  const handleDirectoryClick = (directory: Directory) => {
    setSearchParams({ path: directory.path });
  };

  const handleViewReport = (report: MigrationReport) => {
    navigate(`/reports/details?path=${encodeURIComponent(report.path)}`);
  };

  const handleViewRocksDBReport = (result: ConversionResult) => {
    navigate(`/runtime/reports/details?key=${encodeURIComponent(result.key)}`);
  };

  const formatRocksDBDate = (dateString: string) => {
    try {
      const date = new Date(dateString);
      return date.toLocaleString();
    } catch {
      return dateString;
    }
  };

  const handleBackToParent = () => {
    const pathParts = currentPath.split('/').filter(Boolean);
    if (pathParts.length > 0) {
      pathParts.pop();
      const parentPath = pathParts.join('/');
      setSearchParams(parentPath ? { path: parentPath } : {});
    }
  };

  const handleBackToRoot = () => {
    setSearchParams({});
  };

  const getStatusIcon = (status: string) => {
    switch (status) {
      case 'completed':
        return <CheckCircleIcon className="h-5 w-5 text-green-500" />;
      case 'failed':
        return <XCircleIcon className="h-5 w-5 text-red-500" />;
      case 'partial':
        return <ExclamationTriangleIcon className="h-5 w-5 text-yellow-500" />;
      default:
        return <ClockIcon className="h-5 w-5 text-gray-500" />;
    }
  };

  const getStatusClass = (status: string) => {
    switch (status) {
      case 'completed':
        return 'bg-green-100 text-green-800';
      case 'failed':
        return 'bg-red-100 text-red-800';
      case 'partial':
        return 'bg-yellow-100 text-yellow-800';
      default:
        return 'bg-gray-100 text-gray-800';
    }
  };

  const getBreadcrumbs = () => {
    if (!currentPath) return [];
    return currentPath.split('/').filter(Boolean);
  };

  const handlePreviousPage = () => {
    if (webReportsPage > 0) {
      setWebReportsPage(webReportsPage - 1);
    }
  };

  const handleNextPage = () => {
    const totalPages = Math.ceil(webReportsTotalCount / WEB_REPORTS_PAGE_SIZE);
    if (webReportsPage < totalPages - 1) {
      setWebReportsPage(webReportsPage + 1);
    }
  };

  const handleSearchInputChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    setSearchInput(e.target.value);
  };

  const handleSearchKeyPress = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter') {
      setSearchTerm(searchInput);
      setWebReportsPage(0); // Reset to first page when search executes
    }
  };

  const handleClearSearch = () => {
    setSearchInput('');
    setSearchTerm('');
    setWebReportsPage(0);
  };

  if (isLoading) {
    return (
      <div className="p-8">
        <div className="text-center py-8">
          <svg className="animate-spin h-8 w-8 text-gray-400 mx-auto" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
            <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
            <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
          </svg>
          <p className="text-gray-500 mt-2">Loading migration reports...</p>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="p-8">
        <div className="max-w-4xl mx-auto">
          <div className="bg-red-50 border border-red-200 rounded-md p-4 mb-6">
            <div className="text-sm text-red-700">{error}</div>
          </div>
          <div className="text-center py-8">
            <DocumentTextIcon className="h-16 w-16 text-gray-400 mx-auto mb-4" />
            <h2 className="text-xl font-medium text-gray-900 mb-2">No Reports Directory</h2>
            <p className="text-gray-600">Make sure to run some migrations first to generate reports</p>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="p-8">
      <div className="max-w-6xl mx-auto">
        {/* Header */}
        <div className="mb-8">
          <h1 className="text-3xl font-bold text-gray-900">Migration Reports</h1>
          <p className="text-gray-600 mt-2">Browse and analyze migration execution results</p>
        </div>

        {/* Tabbed Reports Section */}
        {(browseData.reports.length > 0 || browseData.directories.length > 0 || rocksDBResults.length > 0) && (
          <div className="bg-white rounded-lg shadow">
            {/* Tab Navigation */}
            <div className="border-b border-gray-200">
              <nav className="-mb-px flex" aria-label="Tabs">
                <button
                  onClick={() => setActiveTab('web')}
                  className={`${
                    activeTab === 'web'
                      ? 'border-blue-500 text-blue-600'
                      : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
                  } flex-1 whitespace-nowrap py-4 px-1 border-b-2 font-medium text-sm flex items-center justify-center`}
                >
                  <GlobeAltIcon className="h-5 w-5 mr-2" />
                  Web Reports ({rocksDBResults.length})
                </button>
                <button
                  onClick={() => setActiveTab('cli')}
                  className={`${
                    activeTab === 'cli'
                      ? 'border-blue-500 text-blue-600'
                      : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
                  } flex-1 whitespace-nowrap py-4 px-1 border-b-2 font-medium text-sm flex items-center justify-center`}
                >
                  <CommandLineIcon className="h-5 w-5 mr-2" />
                  CLI Reports ({browseData.reports.length})
                </button>
              </nav>
            </div>

            {/* Tab Content */}
            <div className="p-6">
              {/* Web Reports Tab */}
              {activeTab === 'web' && !currentPath && (
                <>
                  {/* Search Field */}
                  <div className="mb-4">
                    <div className="relative max-w-md">
                      <div className="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none">
                        <MagnifyingGlassIcon className="h-5 w-5 text-gray-400" />
                      </div>
                      <input
                        type="text"
                        value={searchInput}
                        onChange={handleSearchInputChange}
                        onKeyPress={handleSearchKeyPress}
                        placeholder="Search reports (regex)..."
                        className="block w-full pl-10 pr-10 py-2 border border-gray-300 rounded-md leading-5 bg-white placeholder-gray-500 focus:outline-none focus:placeholder-gray-400 focus:ring-1 focus:ring-blue-500 focus:border-blue-500 sm:text-sm"
                      />
                      {searchInput && (
                        <button
                          onClick={handleClearSearch}
                          className="absolute inset-y-0 right-0 pr-3 flex items-center"
                        >
                          <XMarkIcon className="h-5 w-5 text-gray-400 hover:text-gray-600" />
                        </button>
                      )}
                    </div>
                    <p className="mt-1 text-xs text-gray-500">
                      Supports regex patterns. Press Enter to search. Examples: <code className="bg-gray-100 px-1 rounded">prod.*</code>, <code className="bg-gray-100 px-1 rounded">test_\d+</code>
                    </p>
                  </div>

                  {/* Pagination Controls */}
                  {rocksDBResults.length > 0 && (
                    <div className="flex justify-between items-center mb-6">
                      <div className="text-sm text-gray-600">
                        Showing {rocksDBResults.length} report{rocksDBResults.length !== 1 ? 's' : ''}
                        {searchTerm && ` matching "${searchTerm}"`}
                      </div>
                      <div className="flex items-center space-x-2">
                        <button
                          onClick={handlePreviousPage}
                          disabled={webReportsPage === 0}
                          className="inline-flex items-center px-3 py-2 border border-gray-300 rounded-md text-sm font-medium text-gray-700 bg-white hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 disabled:opacity-50 disabled:cursor-not-allowed"
                        >
                          <ChevronLeftIcon className="h-4 w-4 mr-1" />
                          Previous
                        </button>
                        <span className="text-sm text-gray-600">
                          Page {webReportsPage + 1} of {Math.ceil(webReportsTotalCount / WEB_REPORTS_PAGE_SIZE)}
                        </span>
                        <button
                          onClick={handleNextPage}
                          disabled={webReportsPage >= Math.ceil(webReportsTotalCount / WEB_REPORTS_PAGE_SIZE) - 1}
                          className="inline-flex items-center px-3 py-2 border border-gray-300 rounded-md text-sm font-medium text-gray-700 bg-white hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 disabled:opacity-50 disabled:cursor-not-allowed"
                        >
                          Next
                          <ChevronRightIcon className="h-4 w-4 ml-1" />
                        </button>
                      </div>
                    </div>
                  )}

                  {rocksDBResults.length === 0 ? (
                    <div className="text-center py-12">
                      <DocumentTextIcon className="h-16 w-16 text-gray-400 mx-auto mb-4" />
                      <h2 className="text-xl font-medium text-gray-900 mb-2">
                        {searchTerm ? 'No Matching Reports Found' : 'No Web Reports Found'}
                      </h2>
                      <p className="text-gray-600">
                        {searchTerm
                          ? `No reports found containing "${searchTerm}"`
                          : 'No web-based migration reports found'
                        }
                      </p>
                      {!searchTerm && (
                        <p className="text-sm text-gray-500 mt-2">
                          Web reports are generated when executing migrations through the web interface
                        </p>
                      )}
                      {searchTerm && (
                        <button
                          onClick={handleClearSearch}
                          className="mt-4 inline-flex items-center px-4 py-2 border border-gray-300 rounded-md text-sm font-medium text-gray-700 bg-white hover:bg-gray-50"
                        >
                          Clear Search
                        </button>
                      )}
                    </div>
                  ) : (
                    <div className="grid grid-cols-1 gap-6">
                      {rocksDBResults.map((result) => {
                        const leftConnection = result.connections?.LEFT;
                        const rightConnection = result.connections?.RIGHT;
                        const hasErrors = result.runStatus?.errorMessages && result.runStatus.errorMessages.length > 0;
                        const hasWarnings = result.runStatus?.warningMessages && result.runStatus.warningMessages.length > 0;

                        return (
                          <div
                            key={result.key}
                            className="bg-white rounded-lg shadow-lg border border-gray-200 overflow-hidden hover:shadow-xl transition-shadow"
                          >
                            <div className="p-6">
                              {/* Header with created time and strategy */}
                              <div className="flex items-center justify-between mb-3">
                                <div className="flex items-center space-x-3">
                                  <CheckCircleIcon className="h-5 w-5 text-blue-500" />
                                  <div>
                                    <h3 className="text-lg font-semibold text-gray-900">
                                      {result.job?.name || result.key}
                                    </h3>
                                    <p className="text-xs text-gray-500 mt-0.5">
                                      {formatRocksDBDate(result.created)}
                                    </p>
                                  </div>
                                </div>
                                {result.job?.strategy && (
                                  <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-blue-100 text-blue-800">
                                    {result.job.strategy}
                                  </span>
                                )}
                              </div>

                              {/* Dataset and Connections */}
                              <div className="mt-3 space-y-2 text-sm">
                                {result.dataset && (
                                  <div className="flex items-start">
                                    <FolderIcon className="h-4 w-4 text-gray-400 mr-2 mt-0.5" />
                                    <div>
                                      <span className="font-medium text-gray-700">Dataset:</span>
                                      <span className="ml-2 text-gray-900">{result.dataset.name}</span>
                                      {result.dataset.description && (
                                        <span className="ml-2 text-gray-500">({result.dataset.description})</span>
                                      )}
                                    </div>
                                  </div>
                                )}

                                {leftConnection && (
                                  <div className="flex items-start">
                                    <GlobeAltIcon className="h-4 w-4 text-gray-400 mr-2 mt-0.5" />
                                    <div>
                                      <span className="font-medium text-gray-700">Left Connection:</span>
                                      <span className="ml-2 text-gray-900">{leftConnection.name}</span>
                                      {leftConnection.description && (
                                        <span className="ml-2 text-gray-500">({leftConnection.description})</span>
                                      )}
                                    </div>
                                  </div>
                                )}

                                {rightConnection && (
                                  <div className="flex items-start">
                                    <GlobeAltIcon className="h-4 w-4 text-gray-400 mr-2 mt-0.5" />
                                    <div>
                                      <span className="font-medium text-gray-700">Right Connection:</span>
                                      <span className="ml-2 text-gray-900">{rightConnection.name}</span>
                                      {rightConnection.description && (
                                        <span className="ml-2 text-gray-500">({rightConnection.description})</span>
                                      )}
                                    </div>
                                  </div>
                                )}
                              </div>

                              {/* Progress */}
                              {result.runStatus?.progress && (
                                <div className="mt-3">
                                  <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${
                                    result.runStatus.progress === 'COMPLETED' ? 'bg-green-100 text-green-800' :
                                    result.runStatus.progress === 'RUNNING' ? 'bg-blue-100 text-blue-800' :
                                    result.runStatus.progress === 'FAILED' ? 'bg-red-100 text-red-800' :
                                    result.runStatus.progress === 'PARTIAL' ? 'bg-yellow-100 text-yellow-800' :
                                    'bg-gray-100 text-gray-800'
                                  }`}>
                                    {result.runStatus.progress}
                                  </span>
                                </div>
                              )}

                              {/* Errors */}
                              {hasErrors && (
                                <div className="mt-3 p-3 bg-red-50 border border-red-200 rounded-md">
                                  <div className="flex items-start">
                                    <span className="text-red-600 font-medium text-sm">Errors ({result.runStatus!.errorMessages!.length}):</span>
                                  </div>
                                  <ul className="mt-2 space-y-1 text-xs text-red-700">
                                    {result.runStatus!.errorMessages!.slice(0, 3).map((error, idx) => (
                                      <li key={idx} className="truncate">{error}</li>
                                    ))}
                                    {result.runStatus!.errorMessages!.length > 3 && (
                                      <li className="text-red-600 font-medium">... and {result.runStatus!.errorMessages!.length - 3} more</li>
                                    )}
                                  </ul>
                                </div>
                              )}

                              {/* Warnings */}
                              {hasWarnings && (
                                <div className="mt-3 p-3 bg-yellow-50 border border-yellow-200 rounded-md">
                                  <div className="flex items-start">
                                    <span className="text-yellow-700 font-medium text-sm">Warnings ({result.runStatus!.warningMessages!.length}):</span>
                                  </div>
                                  <ul className="mt-2 space-y-1 text-xs text-yellow-700">
                                    {result.runStatus!.warningMessages!.slice(0, 3).map((warning, idx) => (
                                      <li key={idx} className="truncate">{warning}</li>
                                    ))}
                                    {result.runStatus!.warningMessages!.length > 3 && (
                                      <li className="text-yellow-700 font-medium">... and {result.runStatus!.warningMessages!.length - 3} more</li>
                                    )}
                                  </ul>
                                </div>
                              )}

                              {/* Actions */}
                              <div className="flex justify-end mt-4">
                                <button
                                  onClick={() => handleViewRocksDBReport(result)}
                                  className="inline-flex items-center px-4 py-2 bg-blue-600 text-white text-sm font-medium rounded-md hover:bg-blue-700 focus:ring-2 focus:ring-blue-500 focus:ring-offset-2"
                                >
                                  <EyeIcon className="h-4 w-4 mr-2" />
                                  View Details
                                </button>
                              </div>
                            </div>
                          </div>
                        );
                      })}
                    </div>
                  )}
                </>
              )}

              {/* CLI Reports Tab */}
              {activeTab === 'cli' && (
                <>
                  {/* Breadcrumbs */}
                  {currentPath && (
                    <div className="mb-6">
                      <nav className="flex items-center space-x-2 text-sm">
                        <button
                          onClick={handleBackToRoot}
                          className="flex items-center text-blue-600 hover:text-blue-700"
                        >
                          <HomeIcon className="h-4 w-4 mr-1" />
                          Reports
                        </button>
                        {getBreadcrumbs().map((segment, index, array) => (
                          <React.Fragment key={index}>
                            <span className="text-gray-400">/</span>
                            <span className="text-gray-600">{segment}</span>
                          </React.Fragment>
                        ))}
                        {currentPath && (
                          <button
                            onClick={handleBackToParent}
                            className="ml-4 flex items-center text-blue-600 hover:text-blue-700 text-sm"
                          >
                            <ArrowLeftIcon className="h-4 w-4 mr-1" />
                            Back
                          </button>
                        )}
                      </nav>
                    </div>
                  )}

                  {/* Directories */}
                  {browseData.directories.length > 0 && (
                    <div className="mb-8">
                      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                        {browseData.directories.map((directory) => (
                          <div
                            key={directory.path}
                            onClick={() => handleDirectoryClick(directory)}
                            className="bg-white rounded-lg shadow border border-gray-200 p-4 cursor-pointer hover:shadow-md transition-shadow"
                          >
                            <div className="flex items-center">
                              <FolderIcon className="h-8 w-8 text-blue-500 mr-3" />
                              <div>
                                <h3 className="text-sm font-medium text-gray-900">{directory.name}</h3>
                                <p className="text-xs text-gray-500">Click to browse</p>
                              </div>
                            </div>
                          </div>
                        ))}
                      </div>
                    </div>
                  )}

                  {/* CLI Reports */}
                  {browseData.reports.length === 0 ? (
                    <div className="text-center py-12">
                      <DocumentTextIcon className="h-16 w-16 text-gray-400 mx-auto mb-4" />
                      <h2 className="text-xl font-medium text-gray-900 mb-2">No CLI Reports Found</h2>
                      <p className="text-gray-600">
                        {browseData.message || "No migration reports found"}
                      </p>
                      <p className="text-sm text-gray-500 mt-2">
                        CLI reports are generated when running hms-mirror from the command line
                      </p>
                    </div>
                  ) : (
                    <div className="grid grid-cols-1 gap-6">
                      {browseData.reports.map((report) => (
                        <div
                          key={report.id}
                          className="bg-white rounded-lg shadow-lg border border-gray-200 overflow-hidden hover:shadow-xl transition-shadow"
                        >
                          <div className="p-6">
                            {/* Header */}
                            <div className="flex items-start justify-between mb-4">
                              <div className="flex items-start space-x-3">
                                <div className="flex-shrink-0 mt-1">
                                  {getStatusIcon(report.status)}
                                </div>
                                <div>
                                  <h3 className="text-lg font-semibold text-gray-900">{report.name}</h3>
                                  <p className="text-sm text-gray-600">Path: {report.path}</p>
                                </div>
                              </div>
                              <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${getStatusClass(report.status)}`}>
                                {report.status}
                              </span>
                            </div>

                            {/* Metadata */}
                            <div className="mb-4">
                              <div className="flex items-center text-sm text-gray-600">
                                <CalendarIcon className="h-4 w-4 mr-2" />
                                {report.timestamp}
                              </div>
                            </div>

                            {/* Summary */}
                            <div className="bg-gray-50 rounded-lg p-4 mb-4">
                              <h4 className="text-sm font-medium text-gray-900 mb-2">Migration Summary</h4>
                              <div className="grid grid-cols-1 md:grid-cols-4 gap-4 text-sm">
                                <div>
                                  <div className="text-gray-600">Database</div>
                                  <div className="font-medium text-blue-600">{report.database}</div>
                                </div>
                                <div>
                                  <div className="text-gray-600">Total Tables</div>
                                  <div className="font-medium">{report.summary.totalTables}</div>
                                </div>
                                <div>
                                  <div className="text-gray-600">Successful</div>
                                  <div className="font-medium text-green-600">{report.summary.successfulTables}</div>
                                </div>
                                <div>
                                  <div className="text-gray-600">Failed</div>
                                  <div className="font-medium text-red-600">{report.summary.failedTables}</div>
                                </div>
                              </div>
                            </div>

                            {/* Artifacts */}
                            <div className="mb-4">
                              <h4 className="text-sm font-medium text-gray-900 mb-2">Available Artifacts</h4>
                              <div className="flex flex-wrap gap-2">
                                {report.artifacts.migrationReport && (
                                  <span className="inline-flex items-center px-2 py-1 rounded text-xs font-medium bg-blue-100 text-blue-800">
                                    <FolderIcon className="h-3 w-3 mr-1" />
                                    Migration Report
                                  </span>
                                )}
                                {report.artifacts.errorLog && (
                                  <span className="inline-flex items-center px-2 py-1 rounded text-xs font-medium bg-red-100 text-red-800">
                                    <FolderIcon className="h-3 w-3 mr-1" />
                                    Execution Log
                                  </span>
                                )}
                                {report.artifacts.sqlScripts && (
                                  <span className="inline-flex items-center px-2 py-1 rounded text-xs font-medium bg-green-100 text-green-800">
                                    <FolderIcon className="h-3 w-3 mr-1" />
                                    SQL Scripts
                                  </span>
                                )}
                                {report.artifacts.executionPlan && (
                                  <span className="inline-flex items-center px-2 py-1 rounded text-xs font-medium bg-purple-100 text-purple-800">
                                    <FolderIcon className="h-3 w-3 mr-1" />
                                    Configuration
                                  </span>
                                )}
                              </div>
                            </div>

                            {/* Actions */}
                            <div className="flex justify-end">
                              <button
                                onClick={() => handleViewReport(report)}
                                className="inline-flex items-center px-4 py-2 bg-blue-600 text-white text-sm font-medium rounded-md hover:bg-blue-700 focus:ring-2 focus:ring-blue-500 focus:ring-offset-2"
                              >
                                <EyeIcon className="h-4 w-4 mr-2" />
                                View Details
                              </button>
                            </div>
                          </div>
                        </div>
                      ))}
                    </div>
                  )}
                </>
              )}
            </div>
          </div>
        )}

        {/* Empty State - shown only when no reports at all */}
        {browseData.reports.length === 0 && browseData.directories.length === 0 && rocksDBResults.length === 0 && (
          <div className="text-center py-12">
            <DocumentTextIcon className="h-16 w-16 text-gray-400 mx-auto mb-4" />
            <h2 className="text-xl font-medium text-gray-900 mb-2">No Reports Found</h2>
            <p className="text-gray-600">
              {browseData.message || "No migration reports found"}
            </p>
            <p className="text-sm text-gray-500 mt-2">
              Make sure to run some migrations first to generate reports
            </p>
          </div>
        )}
      </div>
    </div>
  );
};

export default ReportsPage;