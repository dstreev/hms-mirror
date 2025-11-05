import React from 'react';
import { XMarkIcon, CheckCircleIcon, XCircleIcon, ExclamationTriangleIcon } from '@heroicons/react/24/outline';

interface JobValidationDialogProps {
  isOpen: boolean;
  onClose: () => void;
  jobName: string;
  isValid: boolean;
  message: string;
  errors: string[];
  warnings: string[];
}

const JobValidationDialog: React.FC<JobValidationDialogProps> = ({
  isOpen,
  onClose,
  jobName,
  isValid,
  message,
  errors,
  warnings
}) => {
  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 z-50 overflow-y-auto">
      <div className="flex items-center justify-center min-h-screen px-4 pt-4 pb-20 text-center sm:block sm:p-0">
        {/* Background overlay */}
        <div
          className="fixed inset-0 transition-opacity bg-gray-500 bg-opacity-75"
          onClick={onClose}
        />

        {/* Dialog */}
        <div className="inline-block align-bottom bg-white rounded-lg text-left overflow-hidden shadow-xl transform transition-all sm:my-8 sm:align-middle sm:max-w-2xl sm:w-full">
          {/* Header */}
          <div className={`px-6 py-4 ${isValid ? 'bg-green-600' : 'bg-red-600'}`}>
            <div className="flex items-center justify-between">
              <div className="flex items-center">
                {isValid ? (
                  <CheckCircleIcon className="h-6 w-6 text-white mr-2" />
                ) : (
                  <XCircleIcon className="h-6 w-6 text-white mr-2" />
                )}
                <h3 className="text-lg font-medium text-white">
                  Job Validation {isValid ? 'Successful' : 'Failed'}
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
                <p className="text-sm text-gray-900 mt-1">{jobName}</p>
              </div>

              {/* Message */}
              <div>
                <span className="text-sm font-medium text-gray-600">Status:</span>
                <p className="text-sm text-gray-900 mt-1">{message}</p>
              </div>

              {/* Errors */}
              {errors && errors.length > 0 && (
                <div>
                  <div className="flex items-center mb-2">
                    <XCircleIcon className="h-5 w-5 text-red-600 mr-2" />
                    <span className="text-sm font-medium text-red-600">
                      Errors ({errors.length}):
                    </span>
                  </div>
                  <div className="bg-red-50 border border-red-200 rounded-md p-3">
                    <ul className="list-disc list-inside space-y-1">
                      {errors.map((error, index) => (
                        <li key={index} className="text-sm text-red-800">
                          {error}
                        </li>
                      ))}
                    </ul>
                  </div>
                </div>
              )}

              {/* Warnings */}
              {warnings && warnings.length > 0 && (
                <div>
                  <div className="flex items-center mb-2">
                    <ExclamationTriangleIcon className="h-5 w-5 text-yellow-600 mr-2" />
                    <span className="text-sm font-medium text-yellow-600">
                      Warnings ({warnings.length}):
                    </span>
                  </div>
                  <div className="bg-yellow-50 border border-yellow-200 rounded-md p-3">
                    <ul className="list-disc list-inside space-y-1">
                      {warnings.map((warning, index) => (
                        <li key={index} className="text-sm text-yellow-800">
                          {warning}
                        </li>
                      ))}
                    </ul>
                  </div>
                </div>
              )}

              {/* Success Message */}
              {isValid && errors.length === 0 && warnings.length === 0 && (
                <div className="bg-green-50 border border-green-200 rounded-md p-4">
                  <div className="flex items-center">
                    <CheckCircleIcon className="h-5 w-5 text-green-600 mr-2" />
                    <p className="text-sm text-green-800">
                      The job configuration is valid and ready to execute.
                    </p>
                  </div>
                </div>
              )}
            </div>
          </div>

          {/* Footer */}
          <div className="bg-gray-100 px-6 py-4 flex justify-end">
            <button
              onClick={onClose}
              className="px-4 py-2 bg-white border border-gray-300 rounded-md text-sm font-medium text-gray-700 hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-blue-500"
            >
              Close
            </button>
          </div>
        </div>
      </div>
    </div>
  );
};

export default JobValidationDialog;