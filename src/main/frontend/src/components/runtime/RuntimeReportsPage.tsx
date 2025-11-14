import React, { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import {
  ArrowPathIcon,
  TrashIcon,
  DocumentTextIcon,
  ChevronRightIcon,
  CalendarIcon,
  FolderIcon,
  TableCellsIcon
} from '@heroicons/react/24/outline';
import { runtimeReportsApi, ConversionResult } from '../../services/api/runtimeReportsApi';
import ConfirmationDialog from '../common/ConfirmationDialog';

const RuntimeReportsPage: React.FC = () => {
  const navigate = useNavigate();
  const [reports, setReports] = useState<ConversionResult[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [page, setPage] = useState(0);
  const [hasMore, setHasMore] = useState(false);
  const [totalCount, setTotalCount] = useState(0);
  const [deleteDialog, setDeleteDialog] = useState<{
    isOpen: boolean;
    report: ConversionResult | null;
  }>({ isOpen: false, report: null });

  const PAGE_SIZE = 10;

  useEffect(() => {
    loadReports();
  }, [page]);

  const loadReports = async () => {
    try {
      setLoading(true);
      setError(null);
      const response = await runtimeReportsApi.list(page, PAGE_SIZE);

      if (response && response.status === 'success' && response.data) {
        if (page === 0) {
          setReports(response.data);
        } else {
          // Append new page to existing data
          setReports(prev => [...prev, ...response.data!]);
        }
        setHasMore(response.hasMore || false);
        setTotalCount(response.totalCount || 0);
      } else {
        setReports([]);
        setHasMore(false);
        setTotalCount(0);
      }
    } catch (err: any) {
      console.error('Failed to load migration reports:', err);
      setError(err.message || 'Failed to load migration reports');
      setReports([]);
    } finally {
      setLoading(false);
    }
  };

  const handleRefresh = () => {
    setPage(0);
    loadReports();
  };

  const handleLoadMore = () => {
    setPage(prev => prev + 1);
  };

  const handleDeleteClick = (report: ConversionResult) => {
    setDeleteDialog({ isOpen: true, report });
  };

  const handleDeleteConfirm = async () => {
    if (!deleteDialog.report) return;

    try {
      setError(null);
      const result = await runtimeReportsApi.delete(deleteDialog.report.key);

      if (result.success) {
        setDeleteDialog({ isOpen: false, report: null });
        // Refresh from page 0
        setPage(0);
        await loadReports();
      } else {
        setError(result.message || 'Failed to delete report');
        setDeleteDialog({ isOpen: false, report: null });
      }
    } catch (err: any) {
      console.error('Failed to delete report:', err);
      setError(err.message || 'Failed to delete report');
      setDeleteDialog({ isOpen: false, report: null });
    }
  };

  const handleDeleteCancel = () => {
    setDeleteDialog({ isOpen: false, report: null });
  };

  const handleViewDetails = (report: ConversionResult) => {
    navigate(`/runtime/reports/details?key=${encodeURIComponent(report.key)}`);
  };

  return (
    <div className="container mx-auto px-4 py-6 max-w-7xl">
      {/* Header */}
      <div className="mb-6">
        <div className="flex justify-between items-center">
          <div>
            <h1 className="text-3xl font-bold text-gray-900">Migration Reports</h1>
            <p className="mt-1 text-sm text-gray-600">
              View and manage completed migration execution reports
            </p>
          </div>
          <div className="flex items-center space-x-3">
            <button
              onClick={handleRefresh}
              disabled={loading}
              className="inline-flex items-center px-4 py-2 border border-gray-300 rounded-md shadow-sm text-sm font-medium text-gray-700 bg-white hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 disabled:opacity-50"
            >
              <ArrowPathIcon className={`h-5 w-5 mr-2 ${loading ? 'animate-spin' : ''}`} />
              Refresh
            </button>
          </div>
        </div>

        {/* Stats */}
        <div className="mt-4 flex items-center space-x-4 text-sm text-gray-600">
          <span>
            <strong>{totalCount}</strong> total report{totalCount !== 1 ? 's' : ''}
          </span>
          <span>•</span>
          <span>
            Showing <strong>{reports.length}</strong> of <strong>{totalCount}</strong>
          </span>
        </div>
      </div>

      {/* Error Message */}
      {error && (
        <div className="mb-4 rounded-md bg-red-50 p-4">
          <div className="flex">
            <div className="ml-3">
              <h3 className="text-sm font-medium text-red-800">Error</h3>
              <p className="mt-1 text-sm text-red-700">{error}</p>
            </div>
          </div>
        </div>
      )}

      {/* Loading State */}
      {loading && page === 0 && (
        <div className="text-center py-12">
          <ArrowPathIcon className="h-12 w-12 text-gray-400 animate-spin mx-auto" />
          <p className="mt-2 text-sm text-gray-600">Loading migration reports...</p>
        </div>
      )}

      {/* Empty State */}
      {!loading && reports.length === 0 && (
        <div className="text-center py-12">
          <DocumentTextIcon className="h-12 w-12 text-gray-400 mx-auto" />
          <h3 className="mt-2 text-sm font-medium text-gray-900">No migration reports found</h3>
          <p className="mt-1 text-sm text-gray-500">
            Migration reports will appear here after jobs are executed successfully.
          </p>
        </div>
      )}

      {/* Report Cards */}
      {!loading || page > 0 ? (
        <div className="space-y-4">
          {reports.map((report) => {
            const summary = runtimeReportsApi.getSummary(report);
            const leftConnection = report.connections?.LEFT;
            const rightConnection = report.connections?.RIGHT;
            const hasErrors = report.runStatus?.errorMessages && report.runStatus.errorMessages.length > 0;
            const hasWarnings = report.runStatus?.warningMessages && report.runStatus.warningMessages.length > 0;

            return (
              <div
                key={report.key}
                className="bg-white shadow rounded-lg border border-gray-200 hover:shadow-md transition-shadow"
              >
                <div className="p-6">
                  <div className="flex items-start justify-between">
                    <div className="flex-1 min-w-0">
                      {/* Header with created time and strategy */}
                      <div className="flex items-center justify-between mb-3">
                        <div className="flex items-center space-x-3">
                          <DocumentTextIcon className="h-5 w-5 text-blue-500" />
                          <div>
                            <h3 className="text-lg font-medium text-gray-900">
                              {report.job?.name || report.key}
                            </h3>
                            <p className="text-xs text-gray-500 mt-0.5">
                              {runtimeReportsApi.formatDate(report.created)}
                            </p>
                          </div>
                        </div>
                        {report.job?.strategy && (
                          <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-blue-100 text-blue-800">
                            {report.job.strategy}
                          </span>
                        )}
                      </div>

                      {/* Dataset and Connections */}
                      <div className="mt-3 space-y-2 text-sm">
                        {report.dataset && (
                          <div className="flex items-start">
                            <TableCellsIcon className="h-4 w-4 text-gray-400 mr-2 mt-0.5" />
                            <div>
                              <span className="font-medium text-gray-700">Dataset:</span>
                              <span className="ml-2 text-gray-900">{report.dataset.name}</span>
                              {report.dataset.description && (
                                <span className="ml-2 text-gray-500">({report.dataset.description})</span>
                              )}
                            </div>
                          </div>
                        )}

                        {leftConnection && (
                          <div className="flex items-start">
                            <FolderIcon className="h-4 w-4 text-gray-400 mr-2 mt-0.5" />
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
                            <FolderIcon className="h-4 w-4 text-gray-400 mr-2 mt-0.5" />
                            <div>
                              <span className="font-medium text-gray-700">Right Connection:</span>
                              <span className="ml-2 text-gray-900">{rightConnection.name}</span>
                              {rightConnection.description && (
                                <span className="ml-2 text-gray-500">({rightConnection.description})</span>
                              )}
                            </div>
                          </div>
                        )}

                        {/* Database and Table counts */}
                        {report.dataset && (
                          <div className="flex items-center text-xs text-gray-600">
                            <span className="font-medium">Databases:</span>
                            <span className="ml-1">{summary.databaseCount}</span>
                            <span className="mx-2">•</span>
                            <span className="font-medium">Tables:</span>
                            <span className="ml-1">{summary.tableCount}</span>
                          </div>
                        )}
                      </div>

                      {/* Progress */}
                      {report.runStatus?.progress && (
                        <div className="mt-3">
                          <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${
                            report.runStatus.progress === 'COMPLETED' ? 'bg-green-100 text-green-800' :
                            report.runStatus.progress === 'RUNNING' ? 'bg-blue-100 text-blue-800' :
                            report.runStatus.progress === 'FAILED' ? 'bg-red-100 text-red-800' :
                            report.runStatus.progress === 'PARTIAL' ? 'bg-yellow-100 text-yellow-800' :
                            'bg-gray-100 text-gray-800'
                          }`}>
                            {report.runStatus.progress}
                          </span>
                        </div>
                      )}

                      {/* Errors */}
                      {hasErrors && (
                        <div className="mt-3 p-3 bg-red-50 border border-red-200 rounded-md">
                          <div className="flex items-start">
                            <span className="text-red-600 font-medium text-sm">Errors ({report.runStatus!.errorMessages!.length}):</span>
                          </div>
                          <ul className="mt-2 space-y-1 text-xs text-red-700">
                            {report.runStatus!.errorMessages!.slice(0, 3).map((error, idx) => (
                              <li key={idx} className="truncate">{error}</li>
                            ))}
                            {report.runStatus!.errorMessages!.length > 3 && (
                              <li className="text-red-600 font-medium">... and {report.runStatus!.errorMessages!.length - 3} more</li>
                            )}
                          </ul>
                        </div>
                      )}

                      {/* Warnings */}
                      {hasWarnings && (
                        <div className="mt-3 p-3 bg-yellow-50 border border-yellow-200 rounded-md">
                          <div className="flex items-start">
                            <span className="text-yellow-700 font-medium text-sm">Warnings ({report.runStatus!.warningMessages!.length}):</span>
                          </div>
                          <ul className="mt-2 space-y-1 text-xs text-yellow-700">
                            {report.runStatus!.warningMessages!.slice(0, 3).map((warning, idx) => (
                              <li key={idx} className="truncate">{warning}</li>
                            ))}
                            {report.runStatus!.warningMessages!.length > 3 && (
                              <li className="text-yellow-700 font-medium">... and {report.runStatus!.warningMessages!.length - 3} more</li>
                            )}
                          </ul>
                        </div>
                      )}
                    </div>

                    {/* Actions */}
                    <div className="ml-4 flex-shrink-0 flex items-center space-x-2">
                      <button
                        onClick={() => handleViewDetails(report)}
                        className="inline-flex items-center px-3 py-2 border border-blue-300 rounded-md text-sm font-medium text-blue-700 bg-white hover:bg-blue-50"
                        title="View Details"
                      >
                        <DocumentTextIcon className="h-5 w-5 mr-1" />
                        Details
                      </button>
                      <button
                        onClick={() => handleDeleteClick(report)}
                        className="inline-flex items-center p-2 border border-red-300 rounded-md text-sm font-medium text-red-700 bg-white hover:bg-red-50"
                        title="Delete"
                      >
                        <TrashIcon className="h-5 w-5" />
                      </button>
                    </div>
                  </div>
                </div>
              </div>
            );
          })}

          {/* Load More Button */}
          {hasMore && !loading && (
            <div className="text-center py-4">
              <button
                onClick={handleLoadMore}
                className="inline-flex items-center px-4 py-2 border border-gray-300 rounded-md shadow-sm text-sm font-medium text-gray-700 bg-white hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500"
              >
                Load More
                <ChevronRightIcon className="ml-2 h-4 w-4" />
              </button>
            </div>
          )}

          {/* Loading More State */}
          {loading && page > 0 && (
            <div className="text-center py-4">
              <ArrowPathIcon className="h-8 w-8 text-gray-400 animate-spin mx-auto" />
            </div>
          )}
        </div>
      ) : null}

      {/* Delete Confirmation Dialog */}
      <ConfirmationDialog
        isOpen={deleteDialog.isOpen}
        title="Delete Report"
        message={`Are you sure you want to delete this report? This action cannot be undone.`}
        confirmText="Delete"
        cancelText="Cancel"
        variant="danger"
        onConfirm={handleDeleteConfirm}
        onCancel={handleDeleteCancel}
      />
    </div>
  );
};

export default RuntimeReportsPage;
