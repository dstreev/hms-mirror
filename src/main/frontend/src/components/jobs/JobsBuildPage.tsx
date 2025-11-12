import React, { useState, useEffect } from 'react';
import { useNavigate, useLocation } from 'react-router-dom';
import {
  PlusIcon,
  ArrowPathIcon,
  PencilIcon,
  TrashIcon,
  DocumentDuplicateIcon,
  MagnifyingGlassIcon,
  FunnelIcon,
  PlayIcon,
  DocumentTextIcon,
  CheckCircleIcon
} from '@heroicons/react/24/outline';
import { jobApi, JobListResponse } from '../../services/api/jobApi';
import { Job } from '../../types/Job';
import JobFilters, { JobListFilters } from './JobFilters';
import ConfirmationDialog from '../common/ConfirmationDialog';
import JobSummaryDialog from './JobSummaryDialog';
import JobValidationDialog from './JobValidationDialog';

const JobsBuildPage: React.FC = () => {
  const navigate = useNavigate();
  const location = useLocation();
  const [jobs, setJobs] = useState<Job[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [filters, setFilters] = useState<JobListFilters>({
    search: '',
    strategy: '',
    dataset: '',
    config: '',
    leftConnection: '',
    rightConnection: ''
  });
  const [showFilters, setShowFilters] = useState(false);
  const [deleteDialog, setDeleteDialog] = useState<{
    isOpen: boolean;
    job: Job | null;
  }>({ isOpen: false, job: null });
  const [summaryDialog, setSummaryDialog] = useState<{
    isOpen: boolean;
    job: Job | null;
  }>({ isOpen: false, job: null });
  const [validationDialog, setValidationDialog] = useState<{
    isOpen: boolean;
    jobName: string;
    isValid: boolean;
    message: string;
    errors: string[];
    warnings: string[];
  }>({
    isOpen: false,
    jobName: '',
    isValid: false,
    message: '',
    errors: [],
    warnings: []
  });
  const [validating, setValidating] = useState<string | null>(null);

  useEffect(() => {
    console.log('JobsBuildPage: Loading jobs, location.key:', location.key);
    setLoading(true);
    setError(null);
    fetchJobs();
  }, [location.key]);

  const fetchJobs = async () => {
    try {
      console.log('JobsBuildPage: Fetching jobs...');
      setLoading(true);

      const response = await jobApi.getJobs();
      console.log('JobsBuildPage: API response:', response);

      if (response && response.status === 'SUCCESS' && response.jobs) {
        const allJobs = processJobsFromAPI(response.jobs);
        console.log('JobsBuildPage: Processed jobs:', allJobs.length);
        setJobs(allJobs);
      } else {
        console.log('JobsBuildPage: No jobs found');
        setJobs([]);
      }
    } catch (error) {
      console.error('JobsBuildPage: Error fetching jobs:', error);
      setError('Failed to fetch jobs. The backend API may not be available yet.');
      setJobs([]);
    } finally {
      console.log('JobsBuildPage: Setting loading to false');
      setLoading(false);
    }
  };

  const processJobsFromAPI = (jobsData: any[]): Job[] => {
    const allJobs: Job[] = [];

    if (jobsData && Array.isArray(jobsData)) {
      jobsData.forEach((jobData: any) => {
        // Support both field name formats from wizard and API
        allJobs.push({
          jobKey: jobData.jobKey,
          name: jobData.name || 'Unnamed Job',
          description: jobData.description,
          configurationName: jobData.configurationName || jobData.configReference,
          datasetName: jobData.datasetName || jobData.datasetReference,
          leftConnectionName: jobData.leftConnectionName || jobData.leftConnectionReference,
          rightConnectionName: jobData.rightConnectionName || jobData.rightConnectionReference,
          strategy: jobData.strategy,
          status: jobData.status || 'draft',
          createdDate: jobData.createdDate || new Date().toISOString().split('T')[0],
          modifiedDate: jobData.modifiedDate || new Date().toISOString().split('T')[0],
          lastRunDate: jobData.lastRunDate
        });
      });
    }

    return allJobs.sort((a, b) => a.name.localeCompare(b.name));
  };

  const handleRefresh = () => {
    fetchJobs();
  };

  const handleCreateNew = () => {
    navigate('/jobs/build/wizard');
  };

  const handleEditJob = async (job: Job) => {
    if (!job.jobKey) {
      setError('Cannot edit job: missing job key');
      return;
    }

    try {
      // Fetch the full job data from the API
      const result = await jobApi.getJob(job.jobKey);
      if (result) {
        navigate('/jobs/build/wizard', {
          state: {
            job: result.job,
            jobKey: result.jobKey,
            mode: 'edit'
          }
        });
      } else {
        setError('Failed to load job data for editing');
      }
    } catch (error) {
      console.error('Error loading job for edit:', error);
      setError('Failed to load job data for editing');
    }
  };

  const handleCopyJob = async (job: Job, event: React.MouseEvent) => {
    event.stopPropagation();

    try {
      // Load the full job data from the backend
      const result = await jobApi.getJob(job.jobKey);

      if (result) {
        // Navigate to Job Wizard with copy mode - clear the name for user to provide new name
        navigate('/jobs/build/wizard', {
          state: {
            job: result.job,
            jobKey: '', // Clear jobKey for copy mode
            mode: 'copy'
          }
        });
      } else {
        setError('Failed to load job data for copying');
      }
    } catch (error) {
      console.error('Error loading job for copying:', error);
      setError('Failed to load job data for copying');
    }
  };

  const handleDeleteClick = (job: Job, event: React.MouseEvent) => {
    event.stopPropagation();
    setDeleteDialog({ isOpen: true, job });
  };

  const handleDeleteConfirm = async () => {
    if (!deleteDialog.job) return;

    try {
      setError(null);
      const result = await jobApi.deleteJob(deleteDialog.job.jobKey || deleteDialog.job.name);

      if (result.success) {
        setDeleteDialog({ isOpen: false, job: null });
        await fetchJobs();
      } else {
        setError(result.message || 'Failed to delete job');
        setDeleteDialog({ isOpen: false, job: null });
      }
    } catch (error) {
      console.error('Error deleting job:', error);
      setError('Failed to delete job');
      setDeleteDialog({ isOpen: false, job: null });
    }
  };

  const handleDeleteCancel = () => {
    setDeleteDialog({ isOpen: false, job: null });
  };

  const handleSummaryClick = (job: Job, event: React.MouseEvent) => {
    event.stopPropagation();
    setSummaryDialog({ isOpen: true, job });
  };

  const handleSummaryClose = () => {
    setSummaryDialog({ isOpen: false, job: null });
  };

  const handleValidateClick = async (job: Job, event: React.MouseEvent) => {
    event.stopPropagation();

    if (!job.jobKey) {
      setError('Cannot validate job: missing job key');
      return;
    }

    try {
      setValidating(job.jobKey);
      setError(null);

      const result = await jobApi.validateJob(job.jobKey);

      if (result.success) {
        setValidationDialog({
          isOpen: true,
          jobName: job.name,
          isValid: result.valid || false,
          message: result.message || '',
          errors: result.errors || [],
          warnings: result.warnings || []
        });
      } else {
        setError(result.message || 'Failed to validate job');
      }
    } catch (error) {
      console.error('Error validating job:', error);
      setError('Failed to validate job');
    } finally {
      setValidating(null);
    }
  };

  const handleValidationDialogClose = () => {
    setValidationDialog({
      isOpen: false,
      jobName: '',
      isValid: false,
      message: '',
      errors: [],
      warnings: []
    });
  };

  const handleRunClick = async (job: Job, event: React.MouseEvent) => {
    event.stopPropagation();
    // TODO: Implement run functionality
    console.log('Run job:', job.name);
    setError('Run functionality not yet implemented');
  };

  const formatDate = (dateString?: string) => {
    if (!dateString) return 'N/A';
    return new Date(dateString).toLocaleDateString('en-US', {
      year: 'numeric',
      month: 'short',
      day: 'numeric'
    });
  };

  const getStrategyColor = (strategy?: string) => {
    switch (strategy) {
      case 'SCHEMA_ONLY': return 'bg-blue-100 text-blue-800';
      case 'LINKED': return 'bg-green-100 text-green-800';
      case 'SQL': return 'bg-purple-100 text-purple-800';
      case 'EXPORT_IMPORT': return 'bg-orange-100 text-orange-800';
      case 'HYBRID': return 'bg-yellow-100 text-yellow-800';
      case 'COMMON': return 'bg-cyan-100 text-cyan-800';
      case 'STORAGE_MIGRATION': return 'bg-pink-100 text-pink-800';
      case 'CONVERT_LINKED': return 'bg-indigo-100 text-indigo-800';
      default: return 'bg-gray-100 text-gray-800';
    }
  };

  const filteredJobs = jobs.filter(job => {
    if (filters.search &&
        !job.name.toLowerCase().includes(filters.search.toLowerCase()) &&
        (!job.description || !job.description.toLowerCase().includes(filters.search.toLowerCase()))) {
      return false;
    }

    if (filters.strategy && job.strategy !== filters.strategy) {
      return false;
    }

    if (filters.dataset && job.datasetName !== filters.dataset) {
      return false;
    }

    if (filters.config && job.configurationName !== filters.config) {
      return false;
    }

    if (filters.leftConnection && job.leftConnectionName !== filters.leftConnection) {
      return false;
    }

    if (filters.rightConnection && job.rightConnectionName !== filters.rightConnection) {
      return false;
    }

    return true;
  });

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600"></div>
        <span className="ml-3 text-gray-600">Loading jobs...</span>
      </div>
    );
  }

  return (
    <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
      {/* Page Header */}
      <div className="mb-8">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-3xl font-bold text-gray-900 flex items-center">
              🚀 Job Management
            </h1>
            <p className="text-gray-600 mt-2">Build and manage HMS-Mirror migration jobs</p>
          </div>

          <div className="flex space-x-3">
            <button
              onClick={() => setShowFilters(!showFilters)}
              className="inline-flex items-center px-4 py-2 border border-gray-300 rounded-md shadow-sm text-sm font-medium text-gray-700 bg-white hover:bg-gray-50"
            >
              <FunnelIcon className="h-4 w-4 mr-2" />
              Filters
            </button>
            <button
              onClick={handleRefresh}
              className="inline-flex items-center px-4 py-2 border border-gray-300 rounded-md shadow-sm text-sm font-medium text-gray-700 bg-white hover:bg-gray-50"
            >
              <ArrowPathIcon className="h-4 w-4 mr-2" />
              Refresh
            </button>
            <button
              onClick={handleCreateNew}
              className="inline-flex items-center px-4 py-2 border border-transparent rounded-md shadow-sm text-sm font-medium text-white bg-blue-600 hover:bg-blue-700"
            >
              <PlusIcon className="h-4 w-4 mr-2" />
              New Job
            </button>
          </div>
        </div>

        {/* Search Bar */}
        <div className="mt-6 max-w-md">
          <div className="relative">
            <div className="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none">
              <MagnifyingGlassIcon className="h-5 w-5 text-gray-400" />
            </div>
            <input
              type="text"
              placeholder="Search jobs..."
              value={filters.search}
              onChange={(e) => setFilters({ ...filters, search: e.target.value })}
              className="block w-full pl-10 pr-3 py-2 border border-gray-300 rounded-md leading-5 bg-white placeholder-gray-500 focus:outline-none focus:placeholder-gray-400 focus:ring-1 focus:ring-blue-500 focus:border-blue-500"
            />
          </div>
        </div>
      </div>

      {/* Filters */}
      {showFilters && (
        <div className="mb-6">
          <JobFilters
            filters={filters}
            onFiltersChange={setFilters}
            onClose={() => setShowFilters(false)}
            jobs={jobs}
          />
        </div>
      )}

      {/* Error State */}
      {error && (
        <div className="mb-6 bg-red-50 border border-red-200 rounded-md p-4">
          <div className="flex">
            <div className="ml-3">
              <h3 className="text-sm font-medium text-red-800">Error loading jobs</h3>
              <div className="mt-2 text-sm text-red-700">{error}</div>
              <div className="mt-4">
                <button
                  onClick={handleRefresh}
                  className="text-sm bg-red-100 text-red-800 rounded-md px-2 py-1 hover:bg-red-200"
                >
                  Try again
                </button>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Empty State */}
      {filteredJobs.length === 0 && !loading && (
        <div className="text-center py-12">
          <div className="text-6xl mb-4">🚀</div>
          <h3 className="text-lg font-medium text-gray-900 mb-2">
            {jobs.length === 0 ? 'No jobs found' : 'No jobs match your filters'}
          </h3>
          <p className="text-gray-600 mb-6">
            {jobs.length === 0
              ? 'Create your first job to get started with HMS-Mirror'
              : 'Try adjusting your search or filter criteria'
            }
          </p>
          {jobs.length === 0 && (
            <button
              onClick={handleCreateNew}
              className="inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md text-white bg-blue-600 hover:bg-blue-700"
            >
              <PlusIcon className="h-4 w-4 mr-2" />
              Create Job
            </button>
          )}
          <div className="mt-4 text-sm text-gray-500">
            💡 Tip: Jobs combine configurations, datasets, and connections for execution
          </div>
        </div>
      )}

      {/* Job List */}
      {filteredJobs.length > 0 && (
        <>
          <div className="mb-4 flex items-center justify-between">
            <div className="flex items-center">
              <span className="text-sm text-gray-700">
                🚀 Showing {filteredJobs.length} job{filteredJobs.length !== 1 ? 's' : ''}
                {jobs.length !== filteredJobs.length && ` of ${jobs.length} total`}
              </span>
            </div>
          </div>

          <div className="space-y-4">
            {filteredJobs.map((job) => (
              <div
                key={job.name}
                className="bg-white border border-gray-200 rounded-lg p-6 hover:shadow-md transition-shadow"
              >
                <div className="flex items-center justify-between">
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center space-x-3">
                      <h3 className="text-lg font-medium text-gray-900 truncate">
                        {job.name}
                      </h3>
                      {job.strategy && (
                        <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${getStrategyColor(job.strategy)}`}>
                          {job.strategy}
                        </span>
                      )}
                    </div>
                    {job.description && (
                      <p className="text-sm text-gray-500 mt-1">{job.description}</p>
                    )}
                    <div className="flex items-center mt-2 text-xs text-gray-400 space-x-4 flex-wrap">
                      {job.datasetName && (
                        <span>📊 Dataset: {job.datasetName}</span>
                      )}
                      {job.configurationName && (
                        <span>⚙️ Config: {job.configurationName}</span>
                      )}
                      {job.leftConnectionName && (
                        <span>🔗 Left: {job.leftConnectionName}</span>
                      )}
                      {job.rightConnectionName && (
                        <span>🔗 Right: {job.rightConnectionName}</span>
                      )}
                    </div>
                    <div className="flex items-center mt-2 text-xs text-gray-400 space-x-4">
                      <span>📅 Created: {formatDate(job.createdDate)}</span>
                      <span>🕒 Modified: {formatDate(job.modifiedDate)}</span>
                      {job.lastRunDate && (
                        <span>▶️ Last Run: {formatDate(job.lastRunDate)}</span>
                      )}
                    </div>
                  </div>

                  <div className="flex items-center space-x-2 ml-4">
                    <button
                      onClick={(e) => handleValidateClick(job, e)}
                      disabled={validating === job.jobKey}
                      className="inline-flex items-center px-3 py-2 border border-green-300 rounded-md text-sm font-medium text-green-700 bg-white hover:bg-green-50 disabled:opacity-50 disabled:cursor-not-allowed"
                      title="Validate job configuration"
                    >
                      {validating === job.jobKey ? (
                        <>
                          <ArrowPathIcon className="h-4 w-4 mr-1 animate-spin" />
                          Validating...
                        </>
                      ) : (
                        <>
                          <CheckCircleIcon className="h-4 w-4 mr-1" />
                          Validate
                        </>
                      )}
                    </button>

                    <button
                      onClick={(e) => handleRunClick(job, e)}
                      className="inline-flex items-center px-3 py-2 border border-indigo-300 rounded-md text-sm font-medium text-indigo-700 bg-white hover:bg-indigo-50"
                      title="Run job"
                    >
                      <PlayIcon className="h-4 w-4 mr-1" />
                      Run
                    </button>

                    <button
                      onClick={(e) => handleSummaryClick(job, e)}
                      className="inline-flex items-center px-3 py-2 border border-blue-300 rounded-md text-sm font-medium text-blue-700 bg-white hover:bg-blue-50"
                      title="View job summary"
                    >
                      <DocumentTextIcon className="h-4 w-4 mr-1" />
                      Summary
                    </button>

                    <button
                      onClick={() => handleEditJob(job)}
                      className="inline-flex items-center px-3 py-2 border border-gray-300 rounded-md text-sm font-medium text-gray-700 bg-white hover:bg-gray-50"
                      title="Edit job"
                    >
                      <PencilIcon className="h-4 w-4 mr-1" />
                      Edit
                    </button>

                    <button
                      onClick={(e) => handleCopyJob(job, e)}
                      className="inline-flex items-center px-3 py-2 border border-gray-300 rounded-md text-sm font-medium text-gray-700 bg-white hover:bg-gray-50"
                      title="Copy job"
                    >
                      <DocumentDuplicateIcon className="h-4 w-4 mr-1" />
                      Copy
                    </button>

                    <button
                      onClick={(e) => handleDeleteClick(job, e)}
                      className="inline-flex items-center px-3 py-2 border border-red-300 rounded-md text-sm font-medium text-red-700 bg-white hover:bg-red-50"
                      title="Delete job"
                    >
                      <TrashIcon className="h-4 w-4 mr-1" />
                      Delete
                    </button>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </>
      )}

      {/* Delete Confirmation Dialog */}
      <ConfirmationDialog
        isOpen={deleteDialog.isOpen}
        title="Delete Job"
        message={`Are you sure you want to delete the job "${deleteDialog.job?.name}"? This action cannot be undone.`}
        confirmText="Delete"
        cancelText="Cancel"
        variant="danger"
        onConfirm={handleDeleteConfirm}
        onCancel={handleDeleteCancel}
      />

      {/* Job Summary Dialog */}
      {summaryDialog.isOpen && summaryDialog.job && (
        <JobSummaryDialog
          isOpen={summaryDialog.isOpen}
          job={summaryDialog.job}
          onClose={handleSummaryClose}
          onValidate={(jobName, isValid, message, errors, warnings) => {
            setValidationDialog({
              isOpen: true,
              jobName,
              isValid,
              message,
              errors,
              warnings
            });
          }}
        />
      )}

      {/* Job Validation Dialog */}
      <JobValidationDialog
        isOpen={validationDialog.isOpen}
        jobName={validationDialog.jobName}
        isValid={validationDialog.isValid}
        message={validationDialog.message}
        errors={validationDialog.errors}
        warnings={validationDialog.warnings}
        onClose={handleValidationDialogClose}
      />
    </div>
  );
};

export default JobsBuildPage;