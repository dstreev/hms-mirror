import React, { useState, useEffect } from 'react';
import { useSearchParams, useNavigate } from 'react-router-dom';
import { ArrowLeftIcon, ArrowPathIcon, ArrowDownTrayIcon, DocumentTextIcon } from '@heroicons/react/24/outline';
import { runtimeReportsApi, ConversionResult } from '../../services/api/runtimeReportsApi';

const RuntimeReportDetailsPage: React.FC = () => {
  const [searchParams] = useSearchParams();
  const key = searchParams.get('key');
  const navigate = useNavigate();
  const [report, setReport] = useState<ConversionResult | null>(null);
  const [reportFiles, setReportFiles] = useState<string[]>([]);
  const [loading, setLoading] = useState(true);
  const [filesLoading, setFilesLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (key) {
      loadReport();
      loadReportFiles();
    }
  }, [key]);

  const loadReport = async () => {
    if (!key) return;

    try {
      setLoading(true);
      setError(null);
      const result = await runtimeReportsApi.get(key);

      if (result) {
        setReport(result);
      } else {
        setError('Report not found');
      }
    } catch (err: any) {
      console.error('Failed to load report:', err);
      setError(err.message || 'Failed to load report');
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
        }
      } else {
        console.error('Failed to load report files');
      }
    } catch (err: any) {
      console.error('Failed to load report files:', err);
    } finally {
      setFilesLoading(false);
    }
  };

  const handleDownloadFile = async (filename: string) => {
    if (!key) return;

    try {
      const response = await fetch(
        `/hms-mirror/api/v1/runtime/reports/file?key=${encodeURIComponent(key)}&filename=${encodeURIComponent(filename)}`
      );

      if (response.ok) {
        const content = await response.text();
        const blob = new Blob([content], { type: 'text/plain' });
        const url = window.URL.createObjectURL(blob);
        const link = document.createElement('a');
        link.href = url;
        link.download = filename;
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        window.URL.revokeObjectURL(url);
      } else {
        console.error('Failed to download file');
      }
    } catch (err: any) {
      console.error('Failed to download file:', err);
    }
  };

  const handleBack = () => {
    navigate('/runtime/reports');
  };

  if (loading) {
    return (
      <div className="container mx-auto px-4 py-6 max-w-7xl">
        <div className="text-center py-12">
          <ArrowPathIcon className="h-12 w-12 text-gray-400 animate-spin mx-auto" />
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

        <h1 className="text-3xl font-bold text-gray-900">Report Details</h1>
        <p className="mt-1 text-sm text-gray-600">
          Conversion result: {report.key}
        </p>
      </div>

      {/* Content - Placeholder */}
      <div className="bg-white shadow rounded-lg border border-gray-200 p-6">
        <div className="space-y-6">
          <div>
            <h2 className="text-lg font-medium text-gray-900 mb-4">Summary</h2>
            <dl className="grid grid-cols-1 gap-x-4 gap-y-6 sm:grid-cols-2">
              <div>
                <dt className="text-sm font-medium text-gray-500">Key</dt>
                <dd className="mt-1 text-sm text-gray-900">{report.key}</dd>
              </div>
              <div>
                <dt className="text-sm font-medium text-gray-500">Created</dt>
                <dd className="mt-1 text-sm text-gray-900">
                  {runtimeReportsApi.formatDate(report.created)}
                </dd>
              </div>
              {report.config && (
                <>
                  <div>
                    <dt className="text-sm font-medium text-gray-500">Configuration</dt>
                    <dd className="mt-1 text-sm text-gray-900">{report.config.name}</dd>
                  </div>
                  {report.config.dataStrategy && (
                    <div>
                      <dt className="text-sm font-medium text-gray-500">Data Strategy</dt>
                      <dd className="mt-1 text-sm text-gray-900">{report.config.dataStrategy}</dd>
                    </div>
                  )}
                </>
              )}
              {report.dataset && (
                <div>
                  <dt className="text-sm font-medium text-gray-500">Dataset</dt>
                  <dd className="mt-1 text-sm text-gray-900">{report.dataset.name}</dd>
                </div>
              )}
            </dl>
          </div>

          {/* Report Files Section */}
          <div className="border-t border-gray-200 pt-6">
            <div className="flex items-center justify-between mb-4">
              <h2 className="text-lg font-medium text-gray-900">Generated Reports</h2>
              {filesLoading && (
                <ArrowPathIcon className="h-5 w-5 text-gray-400 animate-spin" />
              )}
            </div>

            {reportFiles.length === 0 && !filesLoading ? (
              <div className="rounded-md bg-yellow-50 p-4">
                <p className="text-sm text-yellow-700">
                  No report files available for this conversion result.
                </p>
              </div>
            ) : (
              <div className="space-y-6">
                {/* Session Configuration Files */}
                {reportFiles.filter(f => f.includes('session-')).length > 0 && (
                  <div>
                    <h3 className="text-sm font-medium text-gray-700 mb-2">Session Configuration</h3>
                    <div className="bg-gray-50 rounded-md divide-y divide-gray-200">
                      {reportFiles
                        .filter(f => f.includes('session-'))
                        .map(filename => (
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
                  </div>
                )}

                {/* Run Status */}
                {reportFiles.filter(f => f.includes('run-status')).length > 0 && (
                  <div>
                    <h3 className="text-sm font-medium text-gray-700 mb-2">Run Status</h3>
                    <div className="bg-gray-50 rounded-md divide-y divide-gray-200">
                      {reportFiles
                        .filter(f => f.includes('run-status'))
                        .map(filename => (
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
                  </div>
                )}

                {/* Database Reports (YAML files) */}
                {reportFiles.filter(f => f.endsWith('_hms-mirror.yaml')).length > 0 && (
                  <div>
                    <h3 className="text-sm font-medium text-gray-700 mb-2">Database Reports</h3>
                    <div className="bg-gray-50 rounded-md divide-y divide-gray-200">
                      {reportFiles
                        .filter(f => f.endsWith('_hms-mirror.yaml'))
                        .map(filename => (
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
                  </div>
                )}

                {/* SQL Scripts */}
                {reportFiles.filter(f => f.endsWith('.sql')).length > 0 && (
                  <div>
                    <h3 className="text-sm font-medium text-gray-700 mb-2">SQL Scripts</h3>
                    <div className="bg-gray-50 rounded-md divide-y divide-gray-200">
                      {reportFiles
                        .filter(f => f.endsWith('.sql'))
                        .map(filename => (
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
                  </div>
                )}
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
};

export default RuntimeReportDetailsPage;
