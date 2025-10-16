import React, { useState } from 'react';
import { BeakerIcon, CheckCircleIcon, ExclamationTriangleIcon } from '@heroicons/react/24/outline';
import { ConnectionFormData, ConnectionTestResults } from '../../../types/Connection';

interface TestAndSaveStepProps {
  formData: ConnectionFormData;
  errors: Record<string, string>;
  onChange: (updates: Partial<ConnectionFormData>) => void;
  onNext: () => void;
  onBack: () => void;
  onSave: () => void;
  isLastStep: boolean;
  saving?: boolean;
  saveSuccess?: boolean;
}

const TestAndSaveStep: React.FC<TestAndSaveStepProps> = ({
  formData,
  errors,
  onChange,
  onBack,
  onSave,
  saving = false,
  saveSuccess = false
}) => {
  const [testResults, setTestResults] = useState<ConnectionTestResults | null>(null);
  const [testing, setTesting] = useState(false);
  const [saveTestResults, setSaveTestResults] = useState(true);

  const handleTestConnection = async () => {
    setTesting(true);
    setTestResults(null);

    try {
      // Simulate connection testing
      await new Promise(resolve => setTimeout(resolve, 3000));
      
      // Mock successful test results
      const mockResults: ConnectionTestResults = {
        status: 'SUCCESS',
        lastTested: new Date().toISOString(),
        duration: 2.3,
        results: [
          {
            component: 'HDFS',
            status: 'SUCCESS',
            message: 'Connected to ' + formData.hcfsNamespace,
            responseTime: 0.8
          },
          {
            component: 'HIVESERVER2',
            status: 'SUCCESS',
            message: 'Connected via JDBC',
            responseTime: 1.2,
            details: { driverVersion: '3.1.2' }
          },
          {
            component: 'METASTORE',
            status: formData.metastoreDirectEnabled ? 'SUCCESS' : 'SUCCESS',
            message: formData.metastoreDirectEnabled ? 'Direct database connection established' : 'Using HiveServer2 for metadata',
            responseTime: formData.metastoreDirectEnabled ? 0.9 : 0.1,
            details: formData.metastoreDirectEnabled ? { schemaVersion: '3.1.0' } : undefined
          }
        ]
      };

      setTestResults(mockResults);
    } catch (error) {
      // Mock failed test results
      const failedResults: ConnectionTestResults = {
        status: 'FAILED',
        lastTested: new Date().toISOString(),
        duration: 5.2,
        results: [
          {
            component: 'HDFS',
            status: 'FAILED',
            message: 'Connection timeout',
            responseTime: 5.0
          }
        ]
      };
      setTestResults(failedResults);
    } finally {
      setTesting(false);
    }
  };

  const renderTestResults = () => {
    if (!testResults) return null;

    const isSuccess = testResults.status === 'SUCCESS';

    return (
      <div className={`border rounded-lg p-4 ${
        isSuccess ? 'border-green-200 bg-green-50' : 'border-red-200 bg-red-50'
      }`}>
        <div className="flex items-center mb-3">
          {isSuccess ? (
            <CheckCircleIcon className="h-5 w-5 text-green-600 mr-2" />
          ) : (
            <ExclamationTriangleIcon className="h-5 w-5 text-red-600 mr-2" />
          )}
          <h4 className={`font-medium ${
            isSuccess ? 'text-green-900' : 'text-red-900'
          }`}>
            {isSuccess ? '✅ Connection Test Results' : '❌ Connection Test Failed'}
          </h4>
        </div>

        <div className="space-y-3">
          <div className={`text-sm ${
            isSuccess ? 'text-green-700' : 'text-red-700'
          }`}>
            🕐 Test completed in {testResults.duration} seconds
          </div>

          <div className="space-y-2">
            <h5 className={`text-sm font-medium ${
              isSuccess ? 'text-green-900' : 'text-red-900'
            }`}>
              Component Test Results
            </h5>
            
            {testResults.results?.map((result, index) => (
              <div key={index} className={`text-sm pl-4 border-l-2 ${
                result.status === 'SUCCESS' 
                  ? 'border-green-300 text-green-700' 
                  : 'border-red-300 text-red-700'
              }`}>
                <div className="font-medium">
                  {result.status === 'SUCCESS' ? '✅' : '❌'} {result.component} Connection
                </div>
                <div className="text-xs mt-1">
                  └─ {result.message}
                  {result.responseTime && ` (${result.responseTime}s)`}
                </div>
                {result.details && (
                  <div className="text-xs mt-1">
                    {Object.entries(result.details).map(([key, value]) => (
                      <div key={key}>└─ {key}: {value}</div>
                    ))}
                  </div>
                )}
              </div>
            ))}
          </div>
        </div>
      </div>
    );
  };

  return (
    <div className="space-y-6">
      {/* Connection Summary */}
      <div>
        <h3 className="text-lg font-medium text-gray-900 mb-4">📋 Connection Summary</h3>
        <div className="bg-gray-50 border border-gray-200 rounded-lg p-4 space-y-2 text-sm">
          <div><strong>Name:</strong> {formData.name}</div>
          {formData.environment && (
            <div><strong>Environment:</strong> {formData.environment}</div>
          )}
          {formData.platformType && (
            <div><strong>Platform:</strong> {formData.platformType}</div>
          )}
          {formData.hcfsNamespace && (
            <div><strong>HDFS:</strong> {formData.hcfsNamespace}</div>
          )}
          <div><strong>HiveServer2:</strong> {formData.hs2Uri}</div>
          {formData.metastoreDirectEnabled && formData.metastoreDirectUri && (
            <div><strong>Metastore Direct:</strong> {formData.metastoreDirectUri}</div>
          )}
          <div><strong>Pool:</strong> {formData.connectionPoolLib}</div>
        </div>
      </div>

      {/* Connection Test */}
      <div>
        <h3 className="text-lg font-medium text-gray-900 mb-4">🧪 Connection Test</h3>
        
        <div className="flex items-center space-x-4 mb-4">
          <button
            onClick={handleTestConnection}
            disabled={testing}
            className="inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md text-white bg-green-600 hover:bg-green-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-green-500 disabled:opacity-50"
          >
            <BeakerIcon className="h-4 w-4 mr-2" />
            {testing ? 'Testing Connection...' : '🧪 Test Connection'}
          </button>
          
          {testing && (
            <div className="flex items-center text-sm text-gray-600">
              <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-green-600 mr-2"></div>
              Testing connection components...
            </div>
          )}
        </div>

        {renderTestResults()}
      </div>

      {/* Save Options */}
      <div>
        <h3 className="text-lg font-medium text-gray-900 mb-4">Save Options</h3>
        
        <div className="space-y-3">
          <label className="flex items-center">
            <input
              type="checkbox"
              checked={saveTestResults}
              onChange={(e) => setSaveTestResults(e.target.checked)}
              className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
            />
            <span className="ml-3 text-sm text-gray-700">
              Save test results with connection
            </span>
          </label>
        </div>
      </div>

      {/* Action Buttons */}
      <div className="flex justify-between pt-6 border-t border-gray-200">
        <button
          onClick={onBack}
          disabled={saving}
          className="px-4 py-2 border border-gray-300 text-gray-700 rounded-lg hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-blue-500 disabled:opacity-50 disabled:cursor-not-allowed"
        >
          ← Back
        </button>
        
        <div className="flex space-x-3">
          {testResults && testResults.status === 'SUCCESS' && (
            <button
              onClick={handleTestConnection}
              className="px-4 py-2 border border-green-300 text-green-700 rounded-lg hover:bg-green-50 focus:outline-none focus:ring-2 focus:ring-green-500"
            >
              🧪 Test Again
            </button>
          )}
          
          <button
            onClick={onSave}
            disabled={saving || saveSuccess}
            className={`px-6 py-2 font-medium rounded-lg focus:outline-none focus:ring-2 disabled:cursor-not-allowed ${
              saveSuccess 
                ? 'bg-emerald-600 text-white' 
                : 'bg-green-600 text-white hover:bg-green-700 focus:ring-green-500 disabled:opacity-50'
            }`}
          >
            {saving ? (
              <div className="flex items-center">
                <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-white mr-2"></div>
                Saving Connection...
              </div>
            ) : saveSuccess ? (
              <div className="flex items-center">
                <CheckCircleIcon className="h-4 w-4 mr-2" />
                Connection Saved!
              </div>
            ) : (
              '💾 Save Connection'
            )}
          </button>
        </div>
      </div>

      {/* Help Text */}
      <div className="text-xs text-gray-500 text-center">
        The connection will be saved and available for use in migration configurations
      </div>
    </div>
  );
};

export default TestAndSaveStep;