import React, { useState, useEffect } from 'react';
import {
  ArrowPathIcon,
  ClockIcon,
  ExclamationCircleIcon,
  ChevronLeftIcon,
  ChevronRightIcon
} from '@heroicons/react/24/outline';
import { runtimeJobsApi, RunStatus } from '../../services/api/runtimeJobsApi';
import RunStatusCard from './RunStatusCard';

const RuntimeJobsPage: React.FC = () => {
  const [allRunStatuses, setAllRunStatuses] = useState<RunStatus[]>([]);
  const [activeJobs, setActiveJobs] = useState<RunStatus[]>([]);
  const [completedJobs, setCompletedJobs] = useState<RunStatus[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [completedPage, setCompletedPage] = useState(0);

  const COMPLETED_PAGE_SIZE = 5;

  console.log('RuntimeJobsPage - Component rendered. loading:', loading, 'allRunStatuses.length:', allRunStatuses.length, 'activeJobs.length:', activeJobs.length, 'completedJobs.length:', completedJobs.length);

  useEffect(() => {
    console.log('RuntimeJobsPage - useEffect triggered, calling loadRunStatuses');
    loadRunStatuses();

    // Auto-refresh every 3 seconds
    const intervalId = setInterval(() => {
      console.log('RuntimeJobsPage - Auto-refresh triggered');
      loadRunStatuses();
    }, 3000);

    // Cleanup interval on unmount
    return () => clearInterval(intervalId);
  }, []);

  // Helper function to determine if a job is active
  const isActiveJob = (runStatus: RunStatus): boolean => {
    // Job is only active if progress is INITIALIZED, STARTED, or IN_PROGRESS
    if (!runStatus.progress) return false;

    // Normalize the progress value to handle both enum names and display values
    const progress = runStatus.progress.toUpperCase().replace(/\s+/g, '_');
    return progress === 'INITIALIZED' || progress === 'STARTED' || progress === 'IN_PROGRESS';
  };

  const loadRunStatuses = async () => {
    try {
      setLoading(true);
      setError(null);

      // Fetch all jobs (using max allowed page size of 100)
      const response = await runtimeJobsApi.list(0, 100);
      console.log('RuntimeJobsPage - API response:', response);

      if (response && response.status === 'success' && response.data) {
        const allJobs = response.data;
        console.log('RuntimeJobsPage - allJobs:', allJobs);
        setAllRunStatuses(allJobs);

        // Split into active and completed
        const active = allJobs.filter(isActiveJob);
        const completed = allJobs.filter(job => !isActiveJob(job));
        console.log('RuntimeJobsPage - active jobs:', active);
        console.log('RuntimeJobsPage - completed jobs:', completed);

        // Sort completed by end date (most recent first)
        completed.sort((a, b) => {
          if (!a.end) return 1;
          if (!b.end) return -1;
          return new Date(b.end).getTime() - new Date(a.end).getTime();
        });

        setActiveJobs(active);
        setCompletedJobs(completed);
        setCompletedPage(0); // Reset to first page
      } else {
        console.log('RuntimeJobsPage - Response does not match expected format. status:', response?.status, 'data:', response?.data);
        setAllRunStatuses([]);
        setActiveJobs([]);
        setCompletedJobs([]);
      }
    } catch (err: any) {
      console.error('Failed to load runtime jobs:', err);
      setError(err.message || 'Failed to load runtime jobs');
      setAllRunStatuses([]);
      setActiveJobs([]);
      setCompletedJobs([]);
    } finally {
      setLoading(false);
    }
  };

  const handleRefresh = () => {
    loadRunStatuses();
  };

  const handlePreviousPage = () => {
    setCompletedPage(prev => Math.max(0, prev - 1));
  };

  const handleNextPage = () => {
    const maxPage = Math.ceil(completedJobs.length / COMPLETED_PAGE_SIZE) - 1;
    setCompletedPage(prev => Math.min(maxPage, prev + 1));
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

  // Calculate pagination for completed jobs
  const completedStartIdx = completedPage * COMPLETED_PAGE_SIZE;
  const completedEndIdx = completedStartIdx + COMPLETED_PAGE_SIZE;
  const paginatedCompletedJobs = completedJobs.slice(completedStartIdx, completedEndIdx);
  const totalCompletedPages = Math.ceil(completedJobs.length / COMPLETED_PAGE_SIZE);

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
            <strong>{allRunStatuses.length}</strong> total job{allRunStatuses.length !== 1 ? 's' : ''}
          </span>
          <span>•</span>
          <span>
            <strong>{activeJobs.length}</strong> active
          </span>
          <span>•</span>
          <span>
            <strong>{completedJobs.length}</strong> completed
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
      {loading && (
        <div className="text-center py-12">
          <ArrowPathIcon className="h-12 w-12 text-gray-400 animate-spin mx-auto" />
          <p className="mt-2 text-sm text-gray-600">Loading runtime jobs...</p>
        </div>
      )}

      {/* Empty State */}
      {!loading && allRunStatuses.length === 0 && (
        <div className="text-center py-12">
          <ClockIcon className="h-12 w-12 text-gray-400 mx-auto" />
          <h3 className="mt-2 text-sm font-medium text-gray-900">No jobs found</h3>
          <p className="mt-1 text-sm text-gray-500">
            Job execution history will appear here.
          </p>
        </div>
      )}

      {/* Active Jobs Section */}
      {!loading && activeJobs.length > 0 && (
        <div className="mb-8">
          <h2 className="text-xl font-semibold text-gray-900 mb-4">
            Active Jobs ({activeJobs.length})
          </h2>
          <div className="space-y-4">
            {activeJobs.map((runStatus) => (
              <RunStatusCard
                key={runStatus.key}
                runStatus={runStatus}
                formatDateTime={formatDateTime}
                getDuration={getDuration}
              />
            ))}
          </div>
        </div>
      )}

      {/* Completed Jobs Section */}
      {!loading && completedJobs.length > 0 && (
        <div>
          <div className="flex justify-between items-center mb-4">
            <h2 className="text-xl font-semibold text-gray-900">
              Completed Jobs ({completedJobs.length})
            </h2>

            {/* Pagination Controls */}
            {totalCompletedPages > 1 && (
              <div className="flex items-center space-x-2">
                <button
                  onClick={handlePreviousPage}
                  disabled={completedPage === 0}
                  className="inline-flex items-center px-3 py-2 border border-gray-300 rounded-md text-sm font-medium text-gray-700 bg-white hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed"
                >
                  <ChevronLeftIcon className="h-4 w-4 mr-1" />
                  Previous
                </button>
                <span className="text-sm text-gray-600">
                  Page {completedPage + 1} of {totalCompletedPages}
                </span>
                <button
                  onClick={handleNextPage}
                  disabled={completedPage >= totalCompletedPages - 1}
                  className="inline-flex items-center px-3 py-2 border border-gray-300 rounded-md text-sm font-medium text-gray-700 bg-white hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed"
                >
                  Next
                  <ChevronRightIcon className="h-4 w-4 ml-1" />
                </button>
              </div>
            )}
          </div>

          <div className="space-y-4">
            {paginatedCompletedJobs.map((runStatus) => (
              <RunStatusCard
                key={runStatus.key}
                runStatus={runStatus}
                formatDateTime={formatDateTime}
                getDuration={getDuration}
              />
            ))}
          </div>
        </div>
      )}
    </div>
  );
};

export default RuntimeJobsPage;
