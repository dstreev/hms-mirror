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
  HomeIcon
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

  const [browseData, setBrowseData] = useState<ReportsBrowseResponse>({
    currentPath: '',
    directories: [],
    reports: []
  });
  const [rocksDBResults, setRocksDBResults] = useState<ConversionResult[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    console.log('ReportsPage mounted, currentPath:', currentPath);
    loadAllReports();
  }, [currentPath]);

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
          const rocksUrl = '/hms-mirror/api/v1/runtime/reports';
          console.log('RocksDB reports URL:', rocksUrl);
          const rocksResponse = await fetch(rocksUrl);
          console.log('RocksDB reports response status:', rocksResponse.status);

          if (rocksResponse.ok) {
            const rocksData = await rocksResponse.json();
            console.log('RocksDB reports data:', rocksData);
            if (rocksData.status === 'success' && rocksData.data) {
              setRocksDBResults(rocksData.data);
            }
          }
        } catch (rocksError) {
          console.error('Failed to load RocksDB reports:', rocksError);
          // Don't fail the whole page if RocksDB reports fail
        }
      } else {
        setRocksDBResults([]);
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
            <h2 className="text-lg font-medium text-gray-900 mb-4">Directories</h2>
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

        {/* RocksDB Conversion Results */}
        {!currentPath && rocksDBResults.length > 0 && (
          <div className="mb-8">
            <h2 className="text-lg font-medium text-gray-900 mb-4">
              RocksDB Conversion Results ({rocksDBResults.length})
            </h2>
            <div className="grid grid-cols-1 gap-6">
              {rocksDBResults.map((result) => (
                <div
                  key={result.key}
                  className="bg-white rounded-lg shadow-lg border border-gray-200 overflow-hidden hover:shadow-xl transition-shadow"
                >
                  <div className="p-6">
                    {/* Header */}
                    <div className="flex items-start justify-between mb-4">
                      <div className="flex items-start space-x-3">
                        <div className="flex-shrink-0 mt-1">
                          <CheckCircleIcon className="h-5 w-5 text-blue-500" />
                        </div>
                        <div>
                          <h3 className="text-lg font-semibold text-gray-900">{result.key}</h3>
                          <p className="text-sm text-gray-600">RocksDB Conversion Result</p>
                        </div>
                      </div>
                      <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-blue-100 text-blue-800">
                        RocksDB
                      </span>
                    </div>

                    {/* Metadata */}
                    <div className="mb-4">
                      <div className="flex items-center text-sm text-gray-600">
                        <CalendarIcon className="h-4 w-4 mr-2" />
                        {formatRocksDBDate(result.created)}
                      </div>
                    </div>

                    {/* Summary */}
                    {(result.config || result.dataset) && (
                      <div className="bg-gray-50 rounded-lg p-4 mb-4">
                        <h4 className="text-sm font-medium text-gray-900 mb-2">Configuration Details</h4>
                        <div className="grid grid-cols-1 md:grid-cols-3 gap-4 text-sm">
                          {result.config && (
                            <>
                              <div>
                                <div className="text-gray-600">Configuration</div>
                                <div className="font-medium text-blue-600">{result.config.name}</div>
                              </div>
                              {result.config.dataStrategy && (
                                <div>
                                  <div className="text-gray-600">Data Strategy</div>
                                  <div className="font-medium">{result.config.dataStrategy}</div>
                                </div>
                              )}
                            </>
                          )}
                          {result.dataset && (
                            <div>
                              <div className="text-gray-600">Dataset</div>
                              <div className="font-medium">{result.dataset.name}</div>
                            </div>
                          )}
                        </div>
                      </div>
                    )}

                    {/* Actions */}
                    <div className="flex justify-end">
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
              ))}
            </div>
          </div>
        )}

        {/* Filesystem Reports */}
        {browseData.reports.length === 0 && browseData.directories.length === 0 && rocksDBResults.length === 0 ? (
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
        ) : browseData.reports.length > 0 ? (
          <div>
            <h2 className="text-lg font-medium text-gray-900 mb-4">
              Filesystem Migration Reports ({browseData.reports.length})
            </h2>
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
          </div>
        ) : null}
      </div>
    </div>
  );
};

export default ReportsPage;