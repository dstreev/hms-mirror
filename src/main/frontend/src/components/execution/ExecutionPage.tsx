import React, { useState } from 'react';
import { PlayIcon, StopIcon, InformationCircleIcon } from '@heroicons/react/24/outline';
import { useConfiguration } from '../../contexts/ConfigurationContext';

const ExecutionPage: React.FC = () => {
  const { state, updateConfig } = useConfiguration();
  
  // Get values from server state with defaults
  const execute = state.config?.execute || false;
  
  const [isRunning, setIsRunning] = useState(false);
  const [progress, setProgress] = useState(0);

  const handleBooleanChange = (field: string, value: boolean) => {
    updateConfig({ [field]: value });
  };

  const handleStartMigration = () => {
    if (!execute) {
      alert('Please enable "Execute Migration" to run the migration. Dry-run mode is currently active.');
      return;
    }
    setIsRunning(true);
    // TODO: Implement actual migration execution
    console.log('Starting migration...');
  };

  const handleStopMigration = () => {
    setIsRunning(false);
    setProgress(0);
    // TODO: Implement actual migration stop
    console.log('Stopping migration...');
  };

  return (
    <div className="p-8">
      <div className="max-w-4xl mx-auto">
        {/* Header */}
        <div className="mb-8">
          <h1 className="text-3xl font-bold text-gray-900">Migration Execution</h1>
          <p className="text-gray-600 mt-2">Configure execution settings and monitor migration progress</p>
        </div>

        {state.error && (
          <div className="mb-6 bg-red-50 border border-red-200 rounded-md p-4">
            <div className="text-sm text-red-700">{state.error}</div>
          </div>
        )}

        <div className="space-y-8">
          {/* Execution Control Settings */}
          <div className="bg-white rounded-lg shadow-lg p-6">
            <h3 className="text-lg font-medium text-gray-900 mb-4">Execution Control</h3>
            <div className="space-y-4">
              <div className="flex items-start">
                <div className="flex items-center h-5">
                  <input
                    type="checkbox"
                    checked={execute}
                    onChange={(e) => handleBooleanChange('execute', e.target.checked)}
                    disabled={isRunning}
                    className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded disabled:opacity-50"
                  />
                </div>
                <div className="ml-3 text-sm">
                  <label className="font-medium text-gray-900">Execute Migration</label>
                  <p className="text-gray-500">When enabled, migration operations will be executed. When disabled, only dry-run validation is performed.</p>
                </div>
              </div>

            </div>
          </div>

          {/* Execution Status */}
          <div className="bg-white rounded-lg shadow-lg p-6">
            <h3 className="text-lg font-medium text-gray-900 mb-4">Execution Status</h3>
            
            {/* Current Mode Indicator */}
            <div className="mb-6">
              <div className={`inline-flex items-center px-3 py-1 rounded-full text-sm font-medium ${
                execute 
                  ? 'bg-green-100 text-green-800 border border-green-200' 
                  : 'bg-yellow-100 text-yellow-800 border border-yellow-200'
              }`}>
                <span className={`w-2 h-2 ${execute ? 'bg-green-600' : 'bg-yellow-600'} rounded-full mr-2`}></span>
                {execute ? 'Execute Mode' : 'Dry-Run Mode'}
              </div>
              {!execute && (
                <p className="text-sm text-yellow-700 mt-2">
                  <InformationCircleIcon className="inline h-4 w-4 mr-1" />
                  Migration is in dry-run mode. Enable "Execute Migration" above to perform actual operations.
                </p>
              )}
            </div>

            {/* Progress Bar */}
            {isRunning && (
              <div className="mb-6">
                <div className="flex justify-between text-sm mb-2">
                  <span className="text-gray-600">Migration Progress</span>
                  <span className="font-medium">{progress}%</span>
                </div>
                <div className="w-full bg-gray-200 rounded-full h-2">
                  <div 
                    className="bg-blue-600 h-2 rounded-full transition-all duration-300"
                    style={{ width: `${progress}%` }}
                  />
                </div>
                <div className="mt-2 text-xs text-gray-600">
                  Processing tables... Please wait.
                </div>
              </div>
            )}

            {/* Action Buttons */}
            <div className="flex space-x-3">
              {!isRunning ? (
                <button
                  onClick={handleStartMigration}
                  className="flex-1 flex items-center justify-center space-x-2 bg-blue-600 text-white px-6 py-3 rounded-md hover:bg-blue-700 focus:ring-2 focus:ring-blue-500 focus:ring-offset-2 transition-colors"
                >
                  <PlayIcon className="w-5 h-5" />
                  <span>Start Migration</span>
                </button>
              ) : (
                <button
                  onClick={handleStopMigration}
                  className="flex-1 flex items-center justify-center space-x-2 bg-red-600 text-white px-6 py-3 rounded-md hover:bg-red-700 focus:ring-2 focus:ring-red-500 focus:ring-offset-2 transition-colors"
                >
                  <StopIcon className="w-5 h-5" />
                  <span>Stop Migration</span>
                </button>
              )}
            </div>

            {/* Status Message */}
            <div className="mt-4 text-center">
              {isRunning ? (
                <p className="text-sm text-blue-600 font-medium">Migration is running...</p>
              ) : (
                <p className="text-sm text-gray-500">Ready to start migration</p>
              )}
            </div>
          </div>

          {/* Execution Log */}
          <div className="bg-white rounded-lg shadow-lg p-6">
            <h3 className="text-lg font-medium text-gray-900 mb-4">Execution Log</h3>
            <div className="bg-gray-50 rounded-md p-4 h-64 overflow-y-auto font-mono text-sm">
              <div className="text-gray-500">
                {isRunning ? (
                  <>
                    <div>[2024-01-10 10:15:23] Starting migration process...</div>
                    <div>[2024-01-10 10:15:24] Validating configuration...</div>
                    <div>[2024-01-10 10:15:25] Configuration validated successfully</div>
                    <div>[2024-01-10 10:15:26] Connecting to source cluster...</div>
                    <div>[2024-01-10 10:15:27] Connected to source cluster</div>
                    <div>[2024-01-10 10:15:28] Processing database: default</div>
                  </>
                ) : (
                  <div className="text-center text-gray-400 mt-20">
                    No execution logs available. Start a migration to see logs here.
                  </div>
                )}
              </div>
            </div>
          </div>

          {/* Important Notes */}
          <div className="bg-blue-50 border border-blue-200 rounded-md p-4">
            <div className="flex">
              <div className="flex-shrink-0">
                <InformationCircleIcon className="h-5 w-5 text-blue-400" aria-hidden="true" />
              </div>
              <div className="ml-3">
                <h3 className="text-sm font-medium text-blue-800">Execution Notes</h3>
                <div className="mt-2 text-sm text-blue-700">
                  <ul className="list-disc pl-5 space-y-1">
                    <li><strong>Dry-Run Mode:</strong> When "Execute Migration" is disabled, no actual changes are made to your data</li>
                    <li><strong>Execute Mode:</strong> When enabled, the migration will perform actual operations on your clusters</li>
                    <li><strong>Progress Tracking:</strong> Monitor real-time progress of your migration above</li>
                    <li><strong>Logs:</strong> Detailed execution logs are displayed in the console area</li>
                  </ul>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default ExecutionPage;