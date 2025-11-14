import React from 'react';
import { useNavigate } from 'react-router-dom';
import {
  ClockIcon,
  ExclamationCircleIcon,
  ExclamationTriangleIcon,
  CheckCircleIcon,
  EyeIcon
} from '@heroicons/react/24/outline';
import { RunStatus } from '../../services/api/runtimeJobsApi';

interface RunStatusCardProps {
  runStatus: RunStatus;
  formatDateTime: (dateTimeStr?: string) => string;
  getDuration: (runStatus: RunStatus) => string;
}

const RunStatusCard: React.FC<RunStatusCardProps> = ({
  runStatus,
  formatDateTime,
  getDuration
}) => {
  const navigate = useNavigate();

  // Determine if job is truly running based on progress field
  const isRunning = runStatus.progress ?
    ['INITIALIZED', 'STARTED', 'IN_PROGRESS', 'Initialized', 'Started', 'In Progress'].includes(runStatus.progress) :
    false;
  const hasErrors = runStatus.errorMessages.length > 0;
  const hasWarnings = runStatus.warningMessages.length > 0;

  const handleViewDetails = () => {
    navigate(`/runtime/reports/details?key=${encodeURIComponent(runStatus.key)}`);
  };

  const getStatusBadge = () => {
    if (isRunning) {
      return (
        <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-blue-100 text-blue-800">
          <ClockIcon className="h-4 w-4 mr-1 animate-spin" />
          Running
        </span>
      );
    }

    // Check progress field for failed/cancelled states
    const progress = runStatus.progress?.toUpperCase().replace(/\s+/g, '_');
    const isFailedState = progress && ['FAILED', 'FAILED_VALIDATION', 'CANCEL_FAILED', 'ABANDONED'].includes(progress);
    const isCancelledState = progress === 'CANCELLED';

    if (isFailedState || hasErrors) {
      return (
        <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-red-100 text-red-800">
          <ExclamationCircleIcon className="h-4 w-4 mr-1" />
          Failed
        </span>
      );
    } else if (isCancelledState) {
      return (
        <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-gray-100 text-gray-800">
          <ExclamationCircleIcon className="h-4 w-4 mr-1" />
          Cancelled
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
    <div className="bg-white shadow rounded-lg border border-gray-200 hover:shadow-md transition-shadow">
      <div className="p-6">
        {/* Row 1: Status, Name, and View Details Button */}
        <div className="flex items-start justify-between mb-4">
          <div className="flex-1 min-w-0">
            <div className="flex items-center space-x-3 mb-2">
              {getStatusBadge()}
              {runStatus.progress && (
                <span className="text-xs text-gray-600 font-medium">
                  {runStatus.progress}
                </span>
              )}
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
          </div>

          {/* View Details Button - Only show if job is completed */}
          {!isRunning && runStatus.key && (
            <div className="ml-4 flex-shrink-0">
              <button
                onClick={handleViewDetails}
                className="inline-flex items-center px-4 py-2 bg-blue-600 text-white text-sm font-medium rounded-md hover:bg-blue-700 focus:ring-2 focus:ring-blue-500 focus:ring-offset-2"
              >
                <EyeIcon className="h-4 w-4 mr-2" />
                View Details
              </button>
            </div>
          )}
        </div>

        {/* Row 2: Two Column Grid */}
        <div className="grid grid-cols-2 gap-6">
          {/* Column 1: All other fields */}
          <div className="space-y-4">
            {/* Basic Details */}
            <div className="grid grid-cols-2 gap-4 text-sm">
              <div>
                <span className="text-gray-500">Started:</span>
                <span className="ml-2 text-gray-900 font-medium">{formatDateTime(runStatus.start)}</span>
              </div>
              <div>
                <span className="text-gray-500">Ended:</span>
                <span className="ml-2 text-gray-900 font-medium">{formatDateTime(runStatus.end)}</span>
              </div>
              <div>
                <span className="text-gray-500">Duration:</span>
                <span className="ml-2 text-gray-900 font-medium">{getDuration(runStatus)}</span>
              </div>
              {runStatus.unSuccessfulTableCount !== undefined && (
                <div>
                  <span className="text-gray-500">Unsuccessful Tables:</span>
                  <span className="ml-2 text-gray-900 font-medium">{runStatus.unSuccessfulTableCount}</span>
                </div>
              )}
            </div>

            {/* Operation Statistics */}
            {runStatus.operationStatistics && (
              <div>
                <h4 className="text-sm font-semibold text-gray-700 mb-2">Operation Statistics</h4>
                <div className="flex flex-wrap gap-4 text-sm">
                  {runStatus.operationStatistics.counts && (
                    <>
                      {runStatus.operationStatistics.counts.databases !== undefined && (
                        <div className="flex items-center">
                          <span className="text-gray-500">Databases:</span>
                          <span className="ml-2 font-medium text-gray-900">
                            {runStatus.operationStatistics.counts.databases}
                          </span>
                        </div>
                      )}
                      {runStatus.operationStatistics.counts.tables !== undefined && (
                        <div className="flex items-center">
                          <span className="text-gray-500">Tables:</span>
                          <span className="ml-2 font-medium text-gray-900">
                            {runStatus.operationStatistics.counts.tables}
                          </span>
                        </div>
                      )}
                    </>
                  )}
                  {runStatus.operationStatistics.successes && (
                    <>
                      {runStatus.operationStatistics.successes.databases !== undefined && (
                        <div className="flex items-center">
                          <span className="text-gray-500">Successful DBs:</span>
                          <span className="ml-2 font-medium text-green-600">
                            {runStatus.operationStatistics.successes.databases}
                          </span>
                        </div>
                      )}
                      {runStatus.operationStatistics.successes.tables !== undefined && (
                        <div className="flex items-center">
                          <span className="text-gray-500">Successful Tables:</span>
                          <span className="ml-2 font-medium text-green-600">
                            {runStatus.operationStatistics.successes.tables}
                          </span>
                        </div>
                      )}
                    </>
                  )}
                  {runStatus.operationStatistics.failures && (
                    <>
                      {runStatus.operationStatistics.failures.databases !== undefined && runStatus.operationStatistics.failures.databases > 0 && (
                        <div className="flex items-center">
                          <span className="text-gray-500">Failed DBs:</span>
                          <span className="ml-2 font-medium text-red-600">
                            {runStatus.operationStatistics.failures.databases}
                          </span>
                        </div>
                      )}
                      {runStatus.operationStatistics.failures.tables !== undefined && runStatus.operationStatistics.failures.tables > 0 && (
                        <div className="flex items-center">
                          <span className="text-gray-500">Failed Tables:</span>
                          <span className="ml-2 font-medium text-red-600">
                            {runStatus.operationStatistics.failures.tables}
                          </span>
                        </div>
                      )}
                    </>
                  )}
                </div>
              </div>
            )}

            {/* In-Progress Tables */}
            {runStatus.inProgressTables && runStatus.inProgressTables.length > 0 && (
              <div>
                <h4 className="text-sm font-semibold text-gray-700 mb-2">
                  In-Progress Tables ({runStatus.inProgressTables.length})
                </h4>
                <div className="bg-gray-50 rounded-md p-3 max-h-32 overflow-y-auto">
                  <ul className="text-sm text-gray-700 space-y-1">
                    {runStatus.inProgressTables.map((table, idx) => (
                      <li key={idx} className="flex items-center">
                        <ClockIcon className="h-4 w-4 text-blue-500 mr-2 flex-shrink-0" />
                        <span className="truncate">{typeof table === 'string' ? table : table.name || 'Unknown'}</span>
                      </li>
                    ))}
                  </ul>
                </div>
              </div>
            )}

            {/* Error and Warning Messages */}
            {(runStatus.errorMessages.length > 0 || runStatus.warningMessages.length > 0) && (
              <div className="space-y-3">
                {runStatus.errorMessages.length > 0 && (
                  <div>
                    <div className="flex items-center mb-2">
                      <ExclamationCircleIcon className="h-4 w-4 text-red-600 mr-1" />
                      <h4 className="text-sm font-semibold text-red-600">
                        Errors ({runStatus.errorMessages.length})
                      </h4>
                    </div>
                    <div className="bg-red-50 border border-red-200 rounded-md p-3 max-h-40 overflow-y-auto">
                      <ul className="text-sm text-red-800 space-y-1 list-disc list-inside">
                        {runStatus.errorMessages.map((error, idx) => (
                          <li key={idx}>{error}</li>
                        ))}
                      </ul>
                    </div>
                  </div>
                )}

                {runStatus.warningMessages.length > 0 && (
                  <div>
                    <div className="flex items-center mb-2">
                      <ExclamationTriangleIcon className="h-4 w-4 text-yellow-600 mr-1" />
                      <h4 className="text-sm font-semibold text-yellow-600">
                        Warnings ({runStatus.warningMessages.length})
                      </h4>
                    </div>
                    <div className="bg-yellow-50 border border-yellow-200 rounded-md p-3 max-h-40 overflow-y-auto">
                      <ul className="text-sm text-yellow-800 space-y-1 list-disc list-inside">
                        {runStatus.warningMessages.map((warning, idx) => (
                          <li key={idx}>{warning}</li>
                        ))}
                      </ul>
                    </div>
                  </div>
                )}
              </div>
            )}
          </div>

          {/* Column 2: Stages */}
          <div>
            {runStatus.stages && Object.keys(runStatus.stages).length > 0 && (
              <div>
                <h4 className="text-sm font-semibold text-gray-700 mb-2">Stages</h4>
                <div className="overflow-x-auto">
                  <table className="min-w-full divide-y divide-gray-200 border border-gray-200 rounded">
                    <thead className="bg-gray-50">
                      <tr>
                        <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          Stage
                        </th>
                        <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          Status
                        </th>
                      </tr>
                    </thead>
                    <tbody className="bg-white divide-y divide-gray-200">
                      {Object.entries(runStatus.stages).map(([stage, status]) => (
                        <tr key={stage}>
                          <td className="px-3 py-2 text-sm text-gray-900">{stage}</td>
                          <td className="px-3 py-2 text-sm">
                            <span className={`inline-flex px-2 py-0.5 text-xs font-medium rounded ${
                              status === 'COMPLETED' || status === 'SUCCESS' ? 'bg-green-100 text-green-800' :
                              status === 'IN_PROGRESS' || status === 'STARTED' ? 'bg-blue-100 text-blue-800' :
                              status === 'FAILED' || status === 'ERROR' ? 'bg-red-100 text-red-800' :
                              'bg-gray-100 text-gray-800'
                            }`}>
                              {status}
                            </span>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
};

export default RunStatusCard;
