import React from 'react';
import { XMarkIcon, CheckCircleIcon, XCircleIcon, ClockIcon } from '@heroicons/react/24/outline';
import { ConnectionTestResults } from '../../types/Connection';

interface TestProgressDialogProps {
  isOpen: boolean;
  onClose: () => void;
  testing: boolean;
  connectionName: string;
  hcfsTestResults?: ConnectionTestResults;
  hs2TestResults?: ConnectionTestResults;
  metastoreDirectTestResults?: ConnectionTestResults;
  testError?: string | null;
}

const TestProgressDialog: React.FC<TestProgressDialogProps> = ({
  isOpen,
  onClose,
  testing,
  connectionName,
  hcfsTestResults,
  hs2TestResults,
  metastoreDirectTestResults,
  testError
}) => {
  if (!isOpen) return null;

  const formatDate = (dateString: string | undefined) => {
    if (!dateString) return '';
    return new Date(dateString).toLocaleString();
  };

  const getStatusIcon = (status?: string) => {
    switch (status) {
      case 'SUCCESS':
        return <CheckCircleIcon className="h-5 w-5 text-green-600" />;
      case 'FAILED':
        return <XCircleIcon className="h-5 w-5 text-red-600" />;
      default:
        return <ClockIcon className="h-5 w-5 text-gray-400" />;
    }
  };

  const getStatusText = (status?: string) => {
    switch (status) {
      case 'SUCCESS':
        return <span className="text-green-600 font-medium">✅ Connected</span>;
      case 'FAILED':
        return <span className="text-red-600 font-medium">❌ Failed</span>;
      default:
        return <span className="text-gray-500">⏳ Pending</span>;
    }
  };

  const renderTestResult = (
    label: string,
    testResults?: ConnectionTestResults,
    isTesting?: boolean
  ) => {
    // Helper to safely convert message to string
    const formatMessage = (message: any): string => {
      if (!message) return '';
      if (typeof message === 'string') return message;
      if (Array.isArray(message)) return message.join('');
      return String(message);
    };

    // Helper to convert details object to readable string
    const formatDetailsAsString = (details: any): string => {
      if (!details) return '';

      // Check if this looks like a string that was split into an object with numeric keys
      const keys = Object.keys(details);
      const allNumeric = keys.every(key => /^\d+$/.test(key));

      if (allNumeric && keys.length > 0) {
        // Sort keys numerically and join the values
        const sortedKeys = keys.sort((a, b) => parseInt(a) - parseInt(b));
        const chars = sortedKeys.map(key => details[key]);
        return chars.join('');
      }

      // If not a character array, return JSON representation
      try {
        return JSON.stringify(details, null, 2);
      } catch {
        return String(details);
      }
    };

    return (
      <div className="bg-gray-50 rounded-lg p-4 border border-gray-200">
        <div className="flex items-start justify-between mb-2">
          <div className="flex items-center space-x-2">
            {getStatusIcon(testResults?.status)}
            <h4 className="font-medium text-gray-900">{label}</h4>
          </div>
          {isTesting && !testResults && (
            <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-blue-600"></div>
          )}
        </div>

        {testResults ? (
          <>
            <div className="mb-2">{getStatusText(testResults.status)}</div>

            {testResults.message && (
              <div className="text-sm text-gray-700 mb-2">
                <strong>Message:</strong> {formatMessage(testResults.message)}
              </div>
            )}

            {testResults.error && (
              <div className="text-sm text-red-700 mb-2 bg-red-50 p-3 rounded border border-red-200">
                <div className="font-semibold mb-1">Error Details:</div>
                <pre className="whitespace-pre-wrap break-words text-xs font-mono bg-white p-2 rounded border border-red-300 overflow-auto max-h-40">
                  {formatMessage(testResults.error)}
                </pre>
              </div>
            )}

            {testResults.details && Object.keys(testResults.details).length > 0 && (
              <div className="text-sm text-gray-700 mb-2 bg-gray-50 p-3 rounded border border-gray-200">
                <div className="font-semibold mb-1">Details:</div>
                <pre className="whitespace-pre-wrap break-words text-xs font-mono bg-white p-2 rounded border border-gray-300 overflow-auto max-h-40">
                  {formatDetailsAsString(testResults.details)}
                </pre>
              </div>
            )}

            {testResults.lastTested && (
              <div className="text-xs text-gray-500 mt-2">
                Last tested: {formatDate(testResults.lastTested)}
              </div>
            )}

            {testResults.duration && (
              <div className="text-xs text-gray-500">
                Duration: {testResults.duration}ms
              </div>
            )}
          </>
        ) : (
          <div className="text-sm text-gray-500">
            {isTesting ? 'Testing in progress...' : 'Not yet tested'}
          </div>
        )}
      </div>
    );
  };

  const hasAnyResults = hcfsTestResults || hs2TestResults || metastoreDirectTestResults;
  const allTestsPassed =
    hcfsTestResults?.status === 'SUCCESS' &&
    hs2TestResults?.status === 'SUCCESS' &&
    (!metastoreDirectTestResults || metastoreDirectTestResults.status === 'SUCCESS');

  const anyTestFailed =
    hcfsTestResults?.status === 'FAILED' ||
    hs2TestResults?.status === 'FAILED' ||
    metastoreDirectTestResults?.status === 'FAILED';

  // Determine if testing is actually in progress (not just the prop value)
  const isActuallyTesting = testing && !hasAnyResults && !testError;

  return (
    <div className="fixed inset-0 z-50 overflow-y-auto">
      <div className="flex items-center justify-center min-h-screen px-4 pt-4 pb-20 text-center sm:block sm:p-0">
        {/* Background overlay */}
        <div className="fixed inset-0 transition-opacity bg-gray-500 bg-opacity-75" onClick={onClose}></div>

        {/* Center the modal */}
        <span className="hidden sm:inline-block sm:align-middle sm:h-screen">&#8203;</span>

        {/* Modal panel */}
        <div className="inline-block align-bottom bg-white rounded-lg text-left overflow-hidden shadow-xl transform transition-all sm:my-8 sm:align-middle sm:max-w-2xl sm:w-full">
          {/* Header */}
          <div className="bg-white px-6 pt-5 pb-4 border-b border-gray-200">
            <div className="flex items-center justify-between">
              <div className="flex items-center space-x-3">
                <div className="text-2xl">🧪</div>
                <div>
                  <h3 className="text-lg font-medium text-gray-900">
                    Connection Test
                  </h3>
                  <p className="text-sm text-gray-500 mt-1">{connectionName}</p>
                </div>
              </div>
              <button
                onClick={onClose}
                disabled={isActuallyTesting}
                className="text-gray-400 hover:text-gray-500 disabled:opacity-50 disabled:cursor-not-allowed"
              >
                <XMarkIcon className="h-6 w-6" />
              </button>
            </div>
          </div>

          {/* Body */}
          <div className="bg-white px-6 py-4">
            {/* Overall Status Banner */}
            {isActuallyTesting && (
              <div className="mb-4 bg-blue-50 border border-blue-200 rounded-md p-4">
                <div className="flex items-center">
                  <div className="animate-spin rounded-full h-5 w-5 border-b-2 border-blue-600 mr-3"></div>
                  <p className="text-sm text-blue-700">
                    <strong>Testing connection endpoints...</strong> Please wait while we verify the connection.
                  </p>
                </div>
              </div>
            )}

            {!isActuallyTesting && hasAnyResults && allTestsPassed && !testError && (
              <div className="mb-4 bg-green-50 border border-green-200 rounded-md p-4">
                <div className="flex items-center">
                  <CheckCircleIcon className="h-5 w-5 text-green-600 mr-3" />
                  <p className="text-sm text-green-700">
                    <strong>All tests passed!</strong> The connection is ready to use.
                  </p>
                </div>
              </div>
            )}

            {!isActuallyTesting && hasAnyResults && anyTestFailed && (
              <div className="mb-4 bg-red-50 border border-red-200 rounded-md p-4">
                <div className="flex items-center">
                  <XCircleIcon className="h-5 w-5 text-red-600 mr-3" />
                  <p className="text-sm text-red-700">
                    <strong>Some tests failed.</strong> Please review the error details below.
                  </p>
                </div>
              </div>
            )}

            {testError && (
              <div className="mb-4 bg-red-50 border border-red-200 rounded-md p-4">
                <div className="flex items-start">
                  <XCircleIcon className="h-5 w-5 text-red-600 mr-3 mt-0.5 flex-shrink-0" />
                  <div className="flex-1 min-w-0">
                    <p className="text-sm font-semibold text-red-700 mb-2">
                      Test Error:
                    </p>
                    <pre className="text-xs text-red-600 whitespace-pre-wrap break-words bg-white p-2 rounded border border-red-300 overflow-auto max-h-32 font-mono">
                      {testError}
                    </pre>
                  </div>
                </div>
              </div>
            )}

            {/* Test Results */}
            <div className="space-y-4">
              <h4 className="text-sm font-semibold text-gray-700 mb-3">Test Results by Endpoint:</h4>

              {renderTestResult('HCFS Namespace', hcfsTestResults, testing)}
              {renderTestResult('HiveServer2', hs2TestResults, testing)}
              {renderTestResult('Metastore Direct', metastoreDirectTestResults, testing)}
            </div>

            {/* Info message */}
            {!isActuallyTesting && hasAnyResults && (
              <div className="mt-4 text-xs text-gray-500 bg-gray-50 p-3 rounded border border-gray-200">
                💡 <strong>Tip:</strong> Connection test results are saved and displayed on the connection card.
                You can re-test at any time to verify the connection status.
              </div>
            )}
          </div>

          {/* Footer */}
          <div className="bg-gray-50 px-6 py-4 border-t border-gray-200 flex justify-end">
            <button
              onClick={onClose}
              disabled={isActuallyTesting}
              className="px-4 py-2 bg-gray-200 text-gray-800 rounded-lg hover:bg-gray-300 focus:outline-none focus:ring-2 focus:ring-gray-500 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              {isActuallyTesting ? 'Testing...' : 'Close'}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
};

export default TestProgressDialog;
