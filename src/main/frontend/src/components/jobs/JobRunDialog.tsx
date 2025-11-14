import React, { useState } from 'react';
import { XMarkIcon, PlayIcon } from '@heroicons/react/24/outline';

interface JobRunDialogProps {
  isOpen: boolean;
  onClose: () => void;
  onRun: (dryRun: boolean) => void;
  jobName: string;
}

const JobRunDialog: React.FC<JobRunDialogProps> = ({
  isOpen,
  onClose,
  onRun,
  jobName
}) => {
  const [dryRun, setDryRun] = useState(true);

  if (!isOpen) return null;

  const handleRun = () => {
    onRun(dryRun);
  };

  return (
    <div className="fixed inset-0 z-50 overflow-y-auto">
      <div className="flex items-center justify-center min-h-screen px-4 pt-4 pb-20 text-center sm:block sm:p-0">
        {/* Background overlay */}
        <div
          className="fixed inset-0 transition-opacity bg-gray-500 bg-opacity-75"
          onClick={onClose}
        />

        {/* Dialog */}
        <div className="inline-block align-bottom bg-white rounded-lg text-left overflow-hidden shadow-xl transform transition-all sm:my-8 sm:align-middle sm:max-w-lg sm:w-full">
          {/* Header */}
          <div className="px-6 py-4 bg-indigo-600">
            <div className="flex items-center justify-between">
              <div className="flex items-center">
                <PlayIcon className="h-6 w-6 text-white mr-2" />
                <h3 className="text-lg font-medium text-white">
                  Run Job
                </h3>
              </div>
              <button
                onClick={onClose}
                className="text-white hover:text-gray-200 transition-colors"
              >
                <XMarkIcon className="h-6 w-6" />
              </button>
            </div>
          </div>

          {/* Content */}
          <div className="bg-white px-6 py-6">
            <div className="space-y-4">
              {/* Job Name */}
              <div>
                <span className="text-sm font-medium text-gray-600">Job:</span>
                <p className="text-sm text-gray-900 mt-1 font-semibold">{jobName}</p>
              </div>

              {/* Dry-Run Option */}
              <div className="bg-blue-50 border border-blue-200 rounded-md p-4">
                <div className="flex items-start">
                  <div className="flex items-center h-5">
                    <input
                      id="dryRun"
                      name="dryRun"
                      type="checkbox"
                      checked={dryRun}
                      onChange={(e) => setDryRun(e.target.checked)}
                      className="h-4 w-4 text-indigo-600 focus:ring-indigo-500 border-gray-300 rounded"
                    />
                  </div>
                  <div className="ml-3 text-sm">
                    <label htmlFor="dryRun" className="font-medium text-gray-700 cursor-pointer">
                      Dry-Run Mode
                    </label>
                    <p className="text-gray-500 mt-1">
                      When enabled, the job will simulate the migration without making actual changes.
                      This is useful for validating the migration plan and identifying potential issues.
                    </p>
                  </div>
                </div>
              </div>

              {/* Warning */}
              {!dryRun && (
                <div className="bg-yellow-50 border border-yellow-200 rounded-md p-4">
                  <div className="flex">
                    <div className="flex-shrink-0">
                      <svg className="h-5 w-5 text-yellow-400" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor" aria-hidden="true">
                        <path fillRule="evenodd" d="M8.257 3.099c.765-1.36 2.722-1.36 3.486 0l5.58 9.92c.75 1.334-.213 2.98-1.742 2.98H4.42c-1.53 0-2.493-1.646-1.743-2.98l5.58-9.92zM11 13a1 1 0 11-2 0 1 1 0 012 0zm-1-8a1 1 0 00-1 1v3a1 1 0 002 0V6a1 1 0 00-1-1z" clipRule="evenodd" />
                      </svg>
                    </div>
                    <div className="ml-3">
                      <h3 className="text-sm font-medium text-yellow-800">
                        Warning: Production Mode
                      </h3>
                      <div className="mt-2 text-sm text-yellow-700">
                        <p>
                          You are about to run this job in production mode. This will make actual changes to your data and metadata.
                          Please ensure you have reviewed and validated the job configuration before proceeding.
                        </p>
                      </div>
                    </div>
                  </div>
                </div>
              )}
            </div>
          </div>

          {/* Footer */}
          <div className="bg-gray-100 px-6 py-4 flex justify-between">
            <button
              onClick={onClose}
              className="px-4 py-2 bg-white border border-gray-300 rounded-md text-sm font-medium text-gray-700 hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-indigo-500"
            >
              Cancel
            </button>
            <button
              onClick={handleRun}
              className="px-4 py-2 bg-indigo-600 border border-transparent rounded-md text-sm font-medium text-white hover:bg-indigo-700 focus:outline-none focus:ring-2 focus:ring-indigo-500"
            >
              Go
            </button>
          </div>
        </div>
      </div>
    </div>
  );
};

export default JobRunDialog;
