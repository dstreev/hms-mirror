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
            return (
              <div
                key={report.key}
                className="bg-white shadow rounded-lg border border-gray-200 hover:shadow-md transition-shadow"
              >
                <div className="p-6">
                  <div className="flex items-start justify-between">
                    <div className="flex-1 min-w-0">
                      <div className="flex items-center space-x-3 mb-2">
                        <DocumentTextIcon className="h-5 w-5 text-blue-500" />
                        <h3 className="text-lg font-medium text-gray-900 truncate">
                          {report.key}
                        </h3>
                      </div>

                      <div className="mt-3 grid grid-cols-2 gap-4 text-sm">
                        <div className="flex items-center">
                          <CalendarIcon className="h-4 w-4 text-gray-400 mr-2" />
                          <div>
                            <span className="text-gray-500">Created:</span>
                            <span className="ml-2 text-gray-900">
                              {runtimeReportsApi.formatDate(report.created)}
                            </span>
                          </div>
                        </div>

                        {report.config && (
                          <div className="flex items-center">
                            <FolderIcon className="h-4 w-4 text-gray-400 mr-2" />
                            <div>
                              <span className="text-gray-500">Configuration:</span>
                              <span className="ml-2 text-gray-900">{summary.configName}</span>
                            </div>
                          </div>
                        )}

                        {report.dataset && (
                          <>
                            <div className="flex items-center">
                              <TableCellsIcon className="h-4 w-4 text-gray-400 mr-2" />
                              <div>
                                <span className="text-gray-500">Dataset:</span>
                                <span className="ml-2 text-gray-900">{summary.datasetName}</span>
                              </div>
                            </div>

                            <div className="flex items-center">
                              <TableCellsIcon className="h-4 w-4 text-gray-400 mr-2" />
                              <div>
                                <span className="text-gray-500">Databases:</span>
                                <span className="ml-2 font-medium text-gray-900">
                                  {summary.databaseCount}
                                </span>
                                <span className="ml-3 text-gray-500">Tables:</span>
                                <span className="ml-2 font-medium text-gray-900">
                                  {summary.tableCount}
                                </span>
                              </div>
                            </div>
                          </>
                        )}
                      </div>

                      {report.config?.description && (
                        <p className="mt-3 text-sm text-gray-600">
                          {report.config.description}
                        </p>
                      )}

                      {report.config?.dataStrategy && (
                        <div className="mt-2">
                          <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-blue-100 text-blue-800">
                            {report.config.dataStrategy}
                          </span>
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
