import React, { useState, useEffect } from 'react';
import { useSearchParams, useNavigate } from 'react-router-dom';
import { 
  ArrowLeftIcon,
  DocumentTextIcon,
  ExclamationTriangleIcon,
  CheckCircleIcon,
  XCircleIcon,
  ArrowDownTrayIcon,
  FolderIcon,
  TableCellsIcon,
  CodeBracketIcon,
  XMarkIcon,
  FolderArrowDownIcon
} from '@heroicons/react/24/outline';
import YamlViewer from './YamlViewer';
import DistcpPlanViewer from './DistcpPlanViewer';

interface ReportDetails {
  id: string;
  name: string;
  database: string;
  configName: string;
  timestamp: string;
  duration: string;
  status: 'completed' | 'failed' | 'partial';
  summary: {
    totalTables: number;
    successfulTables: number;
    failedTables: number;
  };
  tables: TableResult[];
  artifacts: ArtifactFile[];
}

interface TableResult {
  name: string;
  status: 'completed' | 'failed' | 'partial';
  strategy: string;
  phaseState: string;
  issues: string[];
  errors: string[];
  hasLeft: boolean;
  hasRight: boolean;
}

interface ArtifactFile {
  name: string;
  type: 'report' | 'log' | 'sql' | 'plan';
  size: string;
  path: string;
  description: string;
}


const ReportDetailsPage: React.FC = () => {
  const [searchParams] = useSearchParams();
  const navigate = useNavigate();
  const path = searchParams.get('path');
  const [report, setReport] = useState<ReportDetails | null>(null);
  const [artifacts, setArtifacts] = useState<ArtifactFile[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [activeTab, setActiveTab] = useState<'overview' | 'tables' | 'artifacts' | 'config' | 'status' | 'distcpLeft' | 'distcpRight'>('overview');
  const [sessionConfig, setSessionConfig] = useState<string | null>(null);
  const [runStatus, setRunStatus] = useState<string | null>(null);
  const [leftDistcpPlan, setLeftDistcpPlan] = useState<string | null>(null);
  const [rightDistcpPlan, setRightDistcpPlan] = useState<string | null>(null);
  const [selectedTable, setSelectedTable] = useState<string | null>(null);
  const [tableDetailsMode, setTableDetailsMode] = useState<'LEFT' | 'RIGHT' | null>(null);
  const [tableDetails, setTableDetails] = useState<any>(null);
  const [showTableDetailsModal, setShowTableDetailsModal] = useState(false);
  const [isDownloading, setIsDownloading] = useState(false);

  useEffect(() => {
    if (path) {
      loadReportDetails(path);
      loadArtifacts(path);
      loadYamlFiles(path);
    }
  }, [path]);

  const loadYamlFiles = async (reportPath: string) => {
    try {
      // Load session-config.yaml
      console.log('Loading session-config.yaml from:', reportPath);
      const configResponse = await fetch(`/hms-mirror/api/reports/file?path=${encodeURIComponent(reportPath)}&file=session-config.yaml`);
      if (configResponse.ok) {
        const configText = await configResponse.text();
        console.log('Loaded session-config.yaml, length:', configText.length);
        setSessionConfig(configText);
      } else {
        console.log('session-config.yaml not found or error:', configResponse.status);
      }
      
      // Load run-status.yaml
      console.log('Loading run-status.yaml from:', reportPath);
      const statusResponse = await fetch(`/hms-mirror/api/reports/file?path=${encodeURIComponent(reportPath)}&file=run-status.yaml`);
      if (statusResponse.ok) {
        const statusText = await statusResponse.text();
        console.log('Loaded run-status.yaml, length:', statusText.length);
        setRunStatus(statusText);
      } else {
        console.log('run-status.yaml not found or error:', statusResponse.status);
      }
      
      // Try to find and load distcp plan files
      // We need to check the artifacts list for files matching the pattern
      try {
        const artifactsResponse = await fetch(`/hms-mirror/api/reports/artifacts?path=${encodeURIComponent(reportPath)}`);
        if (artifactsResponse.ok) {
          const artifactsList = await artifactsResponse.json();
          
          // Look for LEFT and RIGHT distcp_plans.yaml files
          const leftDistcpFile = artifactsList.find((a: any) => 
            a.name && a.name.includes('LEFT_distcp_plans.yaml')
          );
          const rightDistcpFile = artifactsList.find((a: any) => 
            a.name && a.name.includes('RIGHT_distcp_plans.yaml')
          );
          
          // Load LEFT distcp plan if found
          if (leftDistcpFile) {
            console.log('Loading LEFT distcp plan:', leftDistcpFile.name);
            const leftResponse = await fetch(`/hms-mirror/api/reports/file?path=${encodeURIComponent(reportPath)}&file=${encodeURIComponent(leftDistcpFile.name)}`);
            if (leftResponse.ok) {
              const leftText = await leftResponse.text();
              console.log('Loaded LEFT distcp plan, length:', leftText.length);
              setLeftDistcpPlan(leftText);
            }
          }
          
          // Load RIGHT distcp plan if found
          if (rightDistcpFile) {
            console.log('Loading RIGHT distcp plan:', rightDistcpFile.name);
            const rightResponse = await fetch(`/hms-mirror/api/reports/file?path=${encodeURIComponent(reportPath)}&file=${encodeURIComponent(rightDistcpFile.name)}`);
            if (rightResponse.ok) {
              const rightText = await rightResponse.text();
              console.log('Loaded RIGHT distcp plan, length:', rightText.length);
              setRightDistcpPlan(rightText);
            }
          }
        }
      } catch (error) {
        console.error('Failed to load distcp plans:', error);
      }
    } catch (error) {
      console.error('Failed to load YAML files:', error);
    }
  };

  const loadReportDetails = async (reportPath: string) => {
    try {
      setIsLoading(true);
      setError(null);
      
      const response = await fetch(`/hms-mirror/api/reports/details?path=${encodeURIComponent(reportPath)}`);
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }
      
      const data = await response.json();
      
      // Transform the API response to match our interface
      const transformedReport: ReportDetails = {
        id: data.id || reportPath.replace('/', '_'),
        name: data.name || 'Migration Report',
        database: data.database || 'Unknown Database',
        configName: data.config?.name || 'Configuration',
        timestamp: data.timestamp || new Date().toISOString(),
        duration: calculateDuration(data),
        status: data.status || 'unknown',
        summary: {
          totalTables: data.summary?.totalTables || 0,
          successfulTables: data.summary?.successfulTables || 0,
          failedTables: data.summary?.failedTables || 0
        },
        tables: data.tables || [],
        artifacts: [] // Will be loaded separately
      };
      
      setReport(transformedReport);
      
    } catch (error) {
      console.error('Failed to load report details:', error);
      setError('Failed to load report details. Please check if the report exists.');
    } finally {
      setIsLoading(false);
    }
  };
  
  const loadArtifacts = async (reportPath: string) => {
    try {
      const response = await fetch(`/hms-mirror/api/reports/artifacts?path=${encodeURIComponent(reportPath)}`);
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }
      
      const data = await response.json();
      const transformedArtifacts: ArtifactFile[] = data.map((artifact: any) => ({
        name: artifact.name,
        type: mapFileTypeToArtifactType(artifact.type),
        size: artifact.size,
        path: artifact.path,
        description: artifact.description
      }));
      
      setArtifacts(transformedArtifacts);
      
    } catch (error) {
      console.error('Failed to load artifacts:', error);
    }
  };
  
  const calculateDuration = (data: any) => {
    // Try to calculate duration from timestamps or return unknown
    return 'Unknown duration';
  };
  
  const mapFileTypeToArtifactType = (fileType: string): 'report' | 'log' | 'sql' | 'plan' => {
    switch (fileType) {
      case 'report': return 'report';
      case 'log': return 'log';
      case 'sql': return 'sql';
      case 'config': return 'plan';
      default: return 'report';
    }
  };

  const handleBack = () => {
    if (path) {
      // Extract the parent directory from the current path
      const pathParts = path.split('/');
      if (pathParts.length > 1) {
        pathParts.pop(); // Remove the report directory name
        const parentPath = pathParts.join('/');
        navigate(`/reports?path=${encodeURIComponent(parentPath)}`);
      } else {
        navigate('/reports');
      }
    } else {
      navigate('/reports');
    }
  };

  const handleDownloadArtifact = (artifact: ArtifactFile) => {
    // Create a download link for the artifact
    const downloadUrl = `/hms-mirror/api/reports/artifacts/download?path=${encodeURIComponent(artifact.path)}`;
    const link = document.createElement('a');
    link.href = downloadUrl;
    link.download = artifact.name;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  };

  const handleDownloadAll = async () => {
    if (!path) return;
    
    try {
      setIsDownloading(true);
      
      // Create a download URL for the zip endpoint
      const downloadUrl = `/hms-mirror/api/reports/download-all?path=${encodeURIComponent(path)}`;
      
      // Fetch the zip file
      const response = await fetch(downloadUrl);
      if (!response.ok) {
        throw new Error(`Failed to download: ${response.statusText}`);
      }
      
      // Get the blob from response
      const blob = await response.blob();
      
      // Create a temporary URL for the blob
      const url = window.URL.createObjectURL(blob);
      
      // Create a temporary anchor element and trigger download
      const link = document.createElement('a');
      link.href = url;
      link.download = `${path.replace(/\//g, '_')}_artifacts.zip`;
      document.body.appendChild(link);
      link.click();
      
      // Cleanup
      document.body.removeChild(link);
      window.URL.revokeObjectURL(url);
      
    } catch (error) {
      console.error('Failed to download all artifacts:', error);
      alert('Failed to download report artifacts. Please try again.');
    } finally {
      setIsDownloading(false);
    }
  };

  const handleViewTableDetails = async (tableName: string, mode: 'LEFT' | 'RIGHT') => {
    if (!path) return;
    
    try {
      setSelectedTable(tableName);
      setTableDetailsMode(mode);
      
      const response = await fetch(`/hms-mirror/api/reports/table-details?path=${encodeURIComponent(path)}&table=${encodeURIComponent(tableName)}&environment=${mode}`);
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }
      
      const details = await response.json();
      setTableDetails(details);
      setShowTableDetailsModal(true);
      
    } catch (error) {
      console.error('Failed to load table details:', error);
      alert('Failed to load table details. Please try again.');
    }
  };

  const getStatusIcon = (status: string) => {
    switch (status) {
      case 'completed':
        return <CheckCircleIcon className="h-4 w-4 text-green-500" />;
      case 'failed':
        return <XCircleIcon className="h-4 w-4 text-red-500" />;
      case 'partial':
        return <ExclamationTriangleIcon className="h-4 w-4 text-yellow-500" />;
      default:
        return null;
    }
  };

  const getArtifactIcon = (type: string) => {
    switch (type) {
      case 'report':
        return <DocumentTextIcon className="h-5 w-5" />;
      case 'log':
        return <FolderIcon className="h-5 w-5" />;
      case 'sql':
        return <CodeBracketIcon className="h-5 w-5" />;
      case 'plan':
        return <TableCellsIcon className="h-5 w-5" />;
      default:
        return <FolderIcon className="h-5 w-5" />;
    }
  };

  if (isLoading) {
    return (
      <div className="p-8">
        <div className="text-center py-8">
          <svg className="animate-spin h-8 w-8 text-gray-400 mx-auto" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
            <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
            <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
          </svg>
          <p className="text-gray-500 mt-2">Loading report details...</p>
        </div>
      </div>
    );
  }

  if (error || !report) {
    return (
      <div className="p-8">
        <div className="bg-red-50 border border-red-200 rounded-md p-4">
          <div className="text-sm text-red-700">{error || 'Report not found'}</div>
        </div>
      </div>
    );
  }

  return (
    <div className="p-8">
      <div className="max-w-6xl mx-auto">
        {/* Header */}
        <div className="mb-8">
          <button
            onClick={handleBack}
            className="flex items-center text-blue-600 hover:text-blue-700 mb-4"
          >
            <ArrowLeftIcon className="h-4 w-4 mr-2" />
            Back to Reports
          </button>
          <h1 className="text-3xl font-bold text-gray-900">{report.name}</h1>
          <p className="text-gray-600 mt-2">Database: {report.database} • Path: {path} • Configuration: {report.configName}</p>
        </div>

        {/* Summary Cards */}
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6 mb-8">
          <div className="bg-white rounded-lg shadow p-6">
            <div className="text-sm font-medium text-gray-500">Total Tables</div>
            <div className="text-2xl font-bold text-gray-900">{report.summary.totalTables}</div>
          </div>
          <div className="bg-white rounded-lg shadow p-6">
            <div className="text-sm font-medium text-green-600">Successful</div>
            <div className="text-2xl font-bold text-green-600">{report.summary.successfulTables}</div>
          </div>
          <div className="bg-white rounded-lg shadow p-6">
            <div className="text-sm font-medium text-red-600">Failed</div>
            <div className="text-2xl font-bold text-red-600">{report.summary.failedTables}</div>
          </div>
        </div>

        {/* Tabs */}
        <div className="bg-white rounded-lg shadow">
          <div className="border-b border-gray-200">
            <nav className="-mb-px flex space-x-8">
              {[
                { id: 'overview', name: 'Overview' },
                { id: 'tables', name: 'Table Results' },
                { id: 'artifacts', name: 'Artifacts' },
                ...(sessionConfig ? [{ id: 'config', name: 'Session Config' }] : []),
                ...(runStatus ? [{ id: 'status', name: 'Run Status' }] : []),
                ...(leftDistcpPlan ? [{ id: 'distcpLeft', name: 'DistCp Plan (LEFT)' }] : []),
                ...(rightDistcpPlan ? [{ id: 'distcpRight', name: 'DistCp Plan (RIGHT)' }] : [])
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
                <div>
                  <div className="flex items-center justify-between mb-4">
                    <h3 className="text-lg font-medium text-gray-900">Execution Summary</h3>
                    <button
                      onClick={handleDownloadAll}
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
                          <FolderArrowDownIcon className="h-5 w-5 mr-2" />
                          Download All Artifacts
                        </>
                      )}
                    </button>
                  </div>
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-4 text-sm">
                    <div><span className="text-gray-600">Started:</span> {report.timestamp}</div>
                    <div><span className="text-gray-600">Duration:</span> {report.duration}</div>
                    <div><span className="text-gray-600">Status:</span> 
                      <span className={`ml-2 px-2 py-1 rounded text-xs font-medium ${
                        report.status === 'completed' ? 'bg-green-100 text-green-800' :
                        report.status === 'failed' ? 'bg-red-100 text-red-800' :
                        'bg-yellow-100 text-yellow-800'
                      }`}>{report.status}</span>
                    </div>
                    <div><span className="text-gray-600">Configuration:</span> {report.configName}</div>
                    <div><span className="text-gray-600">Database:</span> {report.database}</div>
                  </div>
                </div>
                
                <div>
                  <h3 className="text-lg font-medium text-gray-900 mb-4">Migration Results</h3>
                  <div className="bg-gray-50 rounded-lg p-4">
                    <div className="text-sm text-gray-600 mb-2">
                      Successfully migrated {report.summary.successfulTables} out of {report.summary.totalTables} tables 
                      ({Math.round((report.summary.successfulTables / report.summary.totalTables) * 100)}% success rate)
                    </div>
                    <div className="w-full bg-gray-200 rounded-full h-2">
                      <div 
                        className="bg-green-600 h-2 rounded-full"
                        style={{ width: `${(report.summary.successfulTables / report.summary.totalTables) * 100}%` }}
                      ></div>
                    </div>
                  </div>
                </div>
              </div>
            )}

            {activeTab === 'tables' && (
              <div className="space-y-4">
                {report.tables.map((table) => (
                  <div key={table.name} className="border border-gray-200 rounded-lg p-4">
                    <div className="flex items-center justify-between mb-3">
                      <div className="flex items-center">
                        {getStatusIcon(table.status)}
                        <h4 className="text-lg font-medium text-gray-900 ml-2">{table.name}</h4>
                      </div>
                      <div className="flex items-center space-x-2">
                        <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${
                          table.status === 'completed' ? 'bg-green-100 text-green-800' :
                          table.status === 'failed' ? 'bg-red-100 text-red-800' :
                          'bg-yellow-100 text-yellow-800'
                        }`}>
                          {table.phaseState}
                        </span>
                      </div>
                    </div>
                    
                    <div className="mb-3">
                      <span className="text-sm font-medium text-gray-900">Strategy:</span>
                      <span className="ml-2 text-sm text-gray-600">{table.strategy}</span>
                    </div>
                    
                    {/* Issues and Errors */}
                    {(table.issues.length > 0 || table.errors.length > 0) && (
                      <div className="mb-3">
                        {table.errors.length > 0 && (
                          <div className="mb-2">
                            <h5 className="text-sm font-medium text-red-700 mb-1">Errors:</h5>
                            <ul className="list-disc list-inside space-y-1">
                              {table.errors.map((error, idx) => (
                                <li key={idx} className="text-sm text-red-600">{error}</li>
                              ))}
                            </ul>
                          </div>
                        )}
                        {table.issues.length > 0 && (
                          <div>
                            <h5 className="text-sm font-medium text-yellow-700 mb-1">Issues:</h5>
                            <ul className="list-disc list-inside space-y-1">
                              {table.issues.map((issue, idx) => (
                                <li key={idx} className="text-sm text-yellow-600">{issue}</li>
                              ))}
                            </ul>
                          </div>
                        )}
                      </div>
                    )}
                    
                    {/* Action buttons for LEFT and RIGHT */}
                    <div className="flex space-x-2">
                      {table.hasLeft && (
                        <button 
                          onClick={() => handleViewTableDetails(table.name, 'LEFT')}
                          className="inline-flex items-center px-3 py-2 border border-blue-300 rounded-md text-sm font-medium text-blue-700 hover:bg-blue-50">
                          <CodeBracketIcon className="h-4 w-4 mr-2" />
                          View LEFT Details
                        </button>
                      )}
                      {table.hasRight && (
                        <button 
                          onClick={() => handleViewTableDetails(table.name, 'RIGHT')}
                          className="inline-flex items-center px-3 py-2 border border-green-300 rounded-md text-sm font-medium text-green-700 hover:bg-green-50">
                          <CodeBracketIcon className="h-4 w-4 mr-2" />
                          View RIGHT Details
                        </button>
                      )}
                    </div>
                  </div>
                ))}
              </div>
            )}

            {activeTab === 'artifacts' && (
              <div className="space-y-4">
                {artifacts.map((artifact) => (
                  <div key={artifact.name} className="flex items-center justify-between border border-gray-200 rounded-lg p-4">
                    <div className="flex items-center">
                      <div className="text-blue-600 mr-3">
                        {getArtifactIcon(artifact.type)}
                      </div>
                      <div>
                        <h4 className="text-sm font-medium text-gray-900">{artifact.name}</h4>
                        <p className="text-sm text-gray-600">{artifact.description}</p>
                        <span className="text-xs text-gray-500">{artifact.size}</span>
                      </div>
                    </div>
                    <button
                      onClick={() => handleDownloadArtifact(artifact)}
                      className="inline-flex items-center px-3 py-2 border border-gray-300 rounded-md text-sm font-medium text-gray-700 hover:bg-gray-50"
                    >
                      <ArrowDownTrayIcon className="h-4 w-4 mr-2" />
                      Download
                    </button>
                  </div>
                ))}
              </div>
            )}

            {activeTab === 'config' && sessionConfig && (
              <YamlViewer content={sessionConfig} title="Session Configuration" />
            )}

            {activeTab === 'status' && runStatus && (
              <YamlViewer content={runStatus} title="Run Status" />
            )}

            {activeTab === 'distcpLeft' && leftDistcpPlan && (
              <DistcpPlanViewer content={leftDistcpPlan} title="DistCp Plan - LEFT Environment" environment="LEFT" />
            )}

            {activeTab === 'distcpRight' && rightDistcpPlan && (
              <DistcpPlanViewer content={rightDistcpPlan} title="DistCp Plan - RIGHT Environment" environment="RIGHT" />
            )}
          </div>
        </div>

        {/* Table Details Modal */}
        {showTableDetailsModal && tableDetails && (
          <div className="fixed inset-0 bg-gray-600 bg-opacity-50 overflow-y-auto h-full w-full z-50">
            <div className="relative top-20 mx-auto p-5 border w-11/12 md:w-3/4 lg:w-1/2 shadow-lg rounded-md bg-white">
              <div className="flex items-center justify-between mb-4">
                <h3 className="text-lg font-bold text-gray-900">
                  {tableDetails.environment} Environment: {tableDetails.tableName}
                </h3>
                <button
                  onClick={() => setShowTableDetailsModal(false)}
                  className="text-gray-400 hover:text-gray-600"
                >
                  <XMarkIcon className="h-6 w-6" />
                </button>
              </div>

              <div className="max-h-96 overflow-y-auto">
                {/* Table Info */}
                <div className="mb-6">
                  <h4 className="text-md font-semibold text-gray-900 mb-2">Table Information</h4>
                  <div className="bg-gray-50 rounded-lg p-4 space-y-2 text-sm">
                    <div><span className="font-medium">Name:</span> {tableDetails.name || 'N/A'}</div>
                    <div><span className="font-medium">Exists:</span> {tableDetails.exists ? 'Yes' : 'No'}</div>
                    <div><span className="font-medium">Create Strategy:</span> {tableDetails.createStrategy || 'N/A'}</div>
                    <div><span className="font-medium">Owner:</span> {tableDetails.owner || 'N/A'}</div>
                    <div><span className="font-medium">Strategy:</span> {tableDetails.strategy || 'N/A'}</div>
                    <div><span className="font-medium">Phase State:</span> 
                      <span className={`ml-2 px-2 py-1 rounded text-xs font-medium ${
                        tableDetails.phaseState === 'ERROR' ? 'bg-red-100 text-red-800' :
                        tableDetails.phaseState === 'SUCCESS' ? 'bg-green-100 text-green-800' :
                        'bg-yellow-100 text-yellow-800'
                      }`}>
                        {tableDetails.phaseState || 'Unknown'}
                      </span>
                    </div>
                  </div>
                </div>

                {/* Issues */}
                {tableDetails.issues && tableDetails.issues.length > 0 && (
                  <div className="mb-6">
                    <h4 className="text-md font-semibold text-yellow-700 mb-2">Issues</h4>
                    <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-4">
                      <ul className="list-disc list-inside space-y-1">
                        {tableDetails.issues.map((issue: string, idx: number) => (
                          <li key={idx} className="text-sm text-yellow-800">{issue}</li>
                        ))}
                      </ul>
                    </div>
                  </div>
                )}

                {/* Errors */}
                {tableDetails.errors && tableDetails.errors.length > 0 && (
                  <div className="mb-6">
                    <h4 className="text-md font-semibold text-red-700 mb-2">Errors</h4>
                    <div className="bg-red-50 border border-red-200 rounded-lg p-4">
                      <ul className="list-disc list-inside space-y-1">
                        {tableDetails.errors.map((error: string, idx: number) => (
                          <li key={idx} className="text-sm text-red-800">{error}</li>
                        ))}
                      </ul>
                    </div>
                  </div>
                )}

                {/* Table Definition */}
                {tableDetails.definition && tableDetails.definition.length > 0 && (
                  <div className="mb-6">
                    <h4 className="text-md font-semibold text-gray-900 mb-2">Table Definition</h4>
                    <div className="bg-gray-900 rounded-lg p-4">
                      <pre className="text-green-400 text-xs font-mono whitespace-pre-wrap">
                        {tableDetails.definition.join('\n')}
                      </pre>
                    </div>
                  </div>
                )}

                {/* SQL Scripts */}
                {tableDetails.sql && tableDetails.sql.length > 0 && (
                  <div className="mb-6">
                    <h4 className="text-md font-semibold text-gray-900 mb-2">SQL Scripts</h4>
                    <div className="space-y-3">
                      {tableDetails.sql.map((sqlItem: any, idx: number) => (
                        <div key={idx} className="bg-blue-50 border border-blue-200 rounded-lg p-3">
                          <div className="text-sm font-medium text-blue-800 mb-2">{sqlItem.description}</div>
                          <pre className="text-xs font-mono text-blue-700 whitespace-pre-wrap bg-white p-2 rounded border">
                            {sqlItem.action}
                          </pre>
                        </div>
                      ))}
                    </div>
                  </div>
                )}

                {/* Properties */}
                {tableDetails.addProperties && Object.keys(tableDetails.addProperties).length > 0 && (
                  <div className="mb-6">
                    <h4 className="text-md font-semibold text-gray-900 mb-2">Additional Properties</h4>
                    <div className="bg-gray-50 rounded-lg p-4">
                      {Object.entries(tableDetails.addProperties).map(([key, value]: [string, any]) => (
                        <div key={key} className="text-sm mb-1">
                          <span className="font-medium">{key}:</span> {String(value)}
                        </div>
                      ))}
                    </div>
                  </div>
                )}
              </div>

              <div className="flex justify-end mt-4 pt-4 border-t">
                <button
                  onClick={() => setShowTableDetailsModal(false)}
                  className="px-4 py-2 bg-gray-500 text-white rounded-md hover:bg-gray-600"
                >
                  Close
                </button>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
};

export default ReportDetailsPage;