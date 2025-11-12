import React, { useState, useEffect } from 'react';
import {
  ArrowPathIcon,
  TrashIcon,
  ClockIcon,
  ExclamationCircleIcon,
  ExclamationTriangleIcon,
  CheckCircleIcon,
  ChevronRightIcon
} from '@heroicons/react/24/outline';
import { runtimeJobsApi, RunStatus } from '../../services/api/runtimeJobsApi';
import ConfirmationDialog from '../common/ConfirmationDialog';

const RuntimeJobsPage: React.FC = () => {
  const [runStatuses, setRunStatuses] = useState<RunStatus[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [page, setPage] = useState(0);
  const [hasMore, setHasMore] = useState(false);
  const [totalCount, setTotalCount] = useState(0);
  const [deleteDialog, setDeleteDialog] = useState<{
    isOpen: boolean;
    runStatus: RunStatus | null;
  }>({ isOpen: false, runStatus: null });

  const PAGE_SIZE = 10;

  useEffect(() => {
    loadRunStatuses();
  }, [page]);

  const loadRunStatuses = async () => {
    try {
      setLoading(true);
      setError(null);
      const response = await runtimeJobsApi.list(page, PAGE_SIZE);

      if (response && response.status === 'success' && response.data) {
        if (page === 0) {
          setRunStatuses(response.data);
        } else {
          // Append new page to existing data
          setRunStatuses(prev => [...prev, ...response.data!]);
        }
        setHasMore(response.hasMore || false);
        setTotalCount(response.totalCount || 0);
      } else {
        setRunStatuses([]);
        setHasMore(false);
        setTotalCount(0);
      }
    } catch (err: any) {
      console.error('Failed to load runtime jobs:', err);
      setError(err.message || 'Failed to load runtime jobs');
      setRunStatuses([]);
    } finally {
      setLoading(false);
    }
  };

  const handleRefresh = () => {
    setPage(0);
    loadRunStatuses();
  };

  const handleLoadMore = () => {
    setPage(prev => prev + 1);
  };

  const handleDeleteClick = (runStatus: RunStatus) => {
    setDeleteDialog({ isOpen: true, runStatus });
  };

  const handleDeleteConfirm = async () => {
    if (!deleteDialog.runStatus) return;

    try {
      setError(null);
      const result = await runtimeJobsApi.delete(deleteDialog.runStatus.key);

      if (result.success) {
        setDeleteDialog({ isOpen: false, runStatus: null });
        // Refresh from page 0
        setPage(0);
        await loadRunStatuses();
      } else {
        setError(result.message || 'Failed to delete job status');
        setDeleteDialog({ isOpen: false, runStatus: null });
      }
    } catch (err: any) {
      console.error('Failed to delete job status:', err);
      setError(err.message || 'Failed to delete job status');
      setDeleteDialog({ isOpen: false, runStatus: null });
    }
  };

  const handleDeleteCancel = () => {
    setDeleteDialog({ isOpen: false, runStatus: null });
  };

  const formatDateTime = (dateTimeStr?: string) => {
    if (!dateTimeStr) return 'N/A';
    try {
      const date = new Date(dateTimeStr);
      return date.toLocaleString();
    } catch (e) {
      return dateTimeStr;
    }
  };

  const getDuration = (runStatus: RunStatus): string => {
    const durationMs = runtimeJobsApi.getDuration(runStatus);
    if (durationMs === null) return 'N/A';
    return runtimeJobsApi.formatDuration(durationMs);
  };

  const getStatusBadge = (runStatus: RunStatus) => {
    const isRunning = runtimeJobsApi.isRunning(runStatus);
    const hasErrors = runStatus.errorMessages.length > 0;
    const hasWarnings = runStatus.warningMessages.length > 0;

    if (isRunning) {
      return (
        <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-blue-100 text-blue-800">
          <ClockIcon className="h-4 w-4 mr-1 animate-spin" />
          Running
        </span>
      );
    } else if (hasErrors) {
      return (
        <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-red-100 text-red-800">
          <ExclamationCircleIcon className="h-4 w-4 mr-1" />
          Failed
        </span>
      );
    } else if (hasWarnings) {
      return (
        <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-yellow-100 text-yellow-800">
          <ExclamationTriangleIcon className="h-4 w-4 mr-1" />
          Completed with Warnings
        </span>
      );
    } else {
      return (
        <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-green-100 text-green-800">
          <CheckCircleIcon className="h-4 w-4 mr-1" />
          Completed
        </span>
      );
    }
  };

  return (
    <div className="container mx-auto px-4 py-6 max-w-7xl">
      {/* Header */}
      <div className="mb-6">
        <div className="flex justify-between items-center">
          <div>
            <h1 className="text-3xl font-bold text-gray-900">Runtime Jobs</h1>
            <p className="mt-1 text-sm text-gray-600">
              View job execution status and history
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
            <strong>{totalCount}</strong> total job{totalCount !== 1 ? 's' : ''}
          </span>
          <span>•</span>
          <span>
            Showing <strong>{runStatuses.length}</strong> of <strong>{totalCount}</strong>
          </span>
        </div>
      </div>

      {/* Error Message */}
      {error && (
        <div className="mb-4 rounded-md bg-red-50 p-4">
          <div className="flex">
            <ExclamationCircleIcon className="h-5 w-5 text-red-400" />
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
          <p className="mt-2 text-sm text-gray-600">Loading runtime jobs...</p>
        </div>
      )}

      {/* Empty State */}
      {!loading && runStatuses.length === 0 && (
        <div className="text-center py-12">
          <ClockIcon className="h-12 w-12 text-gray-400 mx-auto" />
          <h3 className="mt-2 text-sm font-medium text-gray-900">No jobs found</h3>
          <p className="mt-1 text-sm text-gray-500">
            Job execution history will appear here.
          </p>
        </div>
      )}

      {/* Job Cards */}
      {!loading || page > 0 ? (
        <div className="space-y-4">
          {runStatuses.map((runStatus) => (
            <div
              key={runStatus.key}
              className="bg-white shadow rounded-lg border border-gray-200 hover:shadow-md transition-shadow"
            >
              <div className="p-6">
                <div className="flex items-start justify-between">
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center space-x-3 mb-2">
                      {getStatusBadge(runStatus)}
                      {runStatus.appVersion && (
                        <span className="text-xs text-gray-500">
                          v{runStatus.appVersion}
                        </span>
                      )}
                    </div>

                    <h3 className="text-lg font-medium text-gray-900 truncate">
                      {runStatus.key}
                    </h3>

                    {runStatus.comment && (
                      <p className="mt-1 text-sm text-gray-600">{runStatus.comment}</p>
                    )}

                    <div className="mt-3 grid grid-cols-2 gap-4 text-sm">
                      <div>
                        <span className="text-gray-500">Started:</span>
                        <span className="ml-2 text-gray-900">{formatDateTime(runStatus.start)}</span>
                      </div>
                      <div>
                        <span className="text-gray-500">Ended:</span>
                        <span className="ml-2 text-gray-900">{formatDateTime(runStatus.end)}</span>
                      </div>
                      <div>
                        <span className="text-gray-500">Duration:</span>
                        <span className="ml-2 text-gray-900">{getDuration(runStatus)}</span>
                      </div>
                      <div>
                        <span className="text-gray-500">Status:</span>
                        <span className="ml-2 text-gray-900">{runStatus.progress || 'N/A'}</span>
                      </div>
                    </div>

                    {/* Statistics */}
                    {runStatus.operationStatistics && (
                      <div className="mt-3 flex space-x-6 text-sm">
                        {runStatus.operationStatistics.tableMirrors !== undefined && (
                          <div>
                            <span className="text-gray-500">Tables:</span>
                            <span className="ml-2 font-medium text-gray-900">
                              {runStatus.operationStatistics.tableMirrors}
                            </span>
                          </div>
                        )}
                        {runStatus.operationStatistics.successfulMirrors !== undefined && (
                          <div>
                            <span className="text-gray-500">Successful:</span>
                            <span className="ml-2 font-medium text-green-600">
                              {runStatus.operationStatistics.successfulMirrors}
                            </span>
                          </div>
                        )}
                        {runStatus.operationStatistics.failedMirrors !== undefined && (
                          <div>
                            <span className="text-gray-500">Failed:</span>
                            <span className="ml-2 font-medium text-red-600">
                              {runStatus.operationStatistics.failedMirrors}
                            </span>
                          </div>
                        )}
                      </div>
                    )}

                    {/* Messages Summary */}
                    <div className="mt-3 flex space-x-4 text-sm">
                      {runStatus.errorMessages.length > 0 && (
                        <div className="flex items-center text-red-600">
                          <ExclamationCircleIcon className="h-4 w-4 mr-1" />
                          {runStatus.errorMessages.length} error{runStatus.errorMessages.length !== 1 ? 's' : ''}
                        </div>
                      )}
                      {runStatus.warningMessages.length > 0 && (
                        <div className="flex items-center text-yellow-600">
                          <ExclamationTriangleIcon className="h-4 w-4 mr-1" />
                          {runStatus.warningMessages.length} warning{runStatus.warningMessages.length !== 1 ? 's' : ''}
                        </div>
                      )}
                    </div>
                  </div>

                  {/* Actions */}
                  <div className="ml-4 flex-shrink-0 flex items-center space-x-2">
                    <button
                      onClick={() => handleDeleteClick(runStatus)}
                      className="inline-flex items-center p-2 border border-red-300 rounded-md text-sm font-medium text-red-700 bg-white hover:bg-red-50"
                      title="Delete"
                    >
                      <TrashIcon className="h-5 w-5" />
                    </button>
                  </div>
                </div>
              </div>
            </div>
          ))}

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
        title="Delete Job Status"
        message={`Are you sure you want to delete this job status? This action cannot be undone.`}
        confirmText="Delete"
        cancelText="Cancel"
        variant="danger"
        onConfirm={handleDeleteConfirm}
        onCancel={handleDeleteCancel}
      />
    </div>
  );
};

export default RuntimeJobsPage;
