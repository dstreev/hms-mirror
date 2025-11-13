import React, { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { BeakerIcon, CheckCircleIcon, ExclamationTriangleIcon } from '@heroicons/react/24/outline';
import { ConnectionFormData, ConnectionTestResults, Connection } from '../../../types/Connection';
import TestProgressDialog from '../TestProgressDialog';

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
  isEditMode?: boolean;
}

const TestAndSaveStep: React.FC<TestAndSaveStepProps> = ({
  formData,
  errors,
  onChange,
  onBack,
  onSave,
  saving = false,
  saveSuccess = false,
  isEditMode = false
}) => {
  const navigate = useNavigate();
  const [currentConnection, setCurrentConnection] = useState<Connection | null>(null);
  const [originalConfig, setOriginalConfig] = useState<Partial<ConnectionFormData> | null>(null);
  const [testing, setTesting] = useState(false);
  const [testError, setTestError] = useState<string | null>(null);
  const [savedDuringTest, setSavedDuringTest] = useState(false);
  const [showTestDialog, setShowTestDialog] = useState(false);

  // Load current connection test results when editing
  useEffect(() => {
    const loadCurrentConnection = async () => {
      const connectionKey = formData.key || formData.name;
      if (!connectionKey) return;

      try {
        const response = await fetch(`/hms-mirror/api/v1/connections/${connectionKey}`);
        if (response.ok) {
          const connection = await response.json();
          setCurrentConnection(connection);

          // Store original configuration for comparison
          setOriginalConfig({
            hcfsNamespace: formData.hcfsNamespace,
            hs2Uri: formData.hs2Uri,
            hs2Username: formData.hs2Username,
            hs2Password: formData.hs2Password,
            metastoreDirectEnabled: formData.metastoreDirectEnabled,
            metastoreDirectUri: formData.metastoreDirectUri,
            metastoreDirectUsername: formData.metastoreDirectUsername,
            metastoreDirectPassword: formData.metastoreDirectPassword
          });
        }
      } catch (error) {
        console.error('Error loading connection:', error);
      }
    };

    loadCurrentConnection();
  }, [formData.key, formData.name]);

  // Reset test results when configuration changes
  useEffect(() => {
    if (!originalConfig || !currentConnection) return;

    // Check if any critical configuration has changed
    const configChanged =
      formData.hcfsNamespace !== originalConfig.hcfsNamespace ||
      formData.hs2Uri !== originalConfig.hs2Uri ||
      formData.hs2Username !== originalConfig.hs2Username ||
      formData.hs2Password !== originalConfig.hs2Password ||
      formData.metastoreDirectEnabled !== originalConfig.metastoreDirectEnabled ||
      formData.metastoreDirectUri !== originalConfig.metastoreDirectUri ||
      formData.metastoreDirectUsername !== originalConfig.metastoreDirectUsername ||
      formData.metastoreDirectPassword !== originalConfig.metastoreDirectPassword;

    if (configChanged) {
      // Reset test results to null when configuration changes
      setCurrentConnection({
        ...currentConnection,
        hcfsTestResults: undefined,
        hs2TestResults: undefined,
        metastoreDirectTestResults: undefined
      });
    }
  }, [
    formData.hcfsNamespace,
    formData.hs2Uri,
    formData.hs2Username,
    formData.hs2Password,
    formData.metastoreDirectEnabled,
    formData.metastoreDirectUri,
    formData.metastoreDirectUsername,
    formData.metastoreDirectPassword,
    originalConfig,
    currentConnection
  ]);

  const handleTestConnection = async () => {
    setTesting(true);
    setTestError(null);
    setShowTestDialog(true); // Open the dialog when testing starts

    try {
      // Step 1: Save the connection with current formData values
      console.log('Saving connection before testing...');

      const requestPayload = {
        name: formData.name,
        description: formData.description,
        environment: formData.environment,
        config: {
          platformType: formData.platformType,
          hcfsNamespace: formData.hcfsNamespace,
          hiveServer2: {
            uri: formData.hs2Uri,
            connectionProperties: {
              user: formData.hs2Username,
              password: formData.hs2Password,
              ...formData.hs2ConnectionProperties
            }
          },
          metastoreDirect: formData.metastoreDirectEnabled ? {
            uri: formData.metastoreDirectUri,
            type: formData.metastoreDirectType,
            connectionProperties: {
              user: formData.metastoreDirectUsername,
              password: formData.metastoreDirectPassword
            },
            connectionPool: {
              min: formData.metastoreDirectMinConnections,
              max: formData.metastoreDirectMaxConnections
            }
          } : undefined,
          partitionDiscovery: {
            auto: formData.partitionDiscoveryAuto,
            initMSCK: formData.partitionDiscoveryInitMSCK,
            partitionBucketLimit: formData.partitionBucketLimit
          },
          createIfNotExists: formData.createIfNotExists,
          enableAutoTableStats: formData.enableAutoTableStats,
          enableAutoColumnStats: formData.enableAutoColumnStats
        }
      };

      const connectionKey = formData.key || formData.name;
      const url = formData.key
        ? `/hms-mirror/api/v1/connections/${formData.key}`
        : '/hms-mirror/api/v1/connections';
      const method = formData.key ? 'PUT' : 'POST';

      const saveResponse = await fetch(url, {
        method,
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(requestPayload)
      });

      if (!saveResponse.ok) {
        const errorText = await saveResponse.text();
        throw new Error(`Failed to save connection before testing: ${saveResponse.status} ${errorText}`);
      }

      const saveResult = await saveResponse.json();
      const savedKey = saveResult.key || connectionKey;

      console.log('Connection saved successfully, now testing with key:', savedKey);

      // Step 2: Test the connection
      const testResponse = await fetch(`/hms-mirror/api/v1/connections/${savedKey}/test`, {
        method: 'POST'
      });

      if (!testResponse.ok) {
        throw new Error(`Failed to test connection: ${testResponse.status} ${testResponse.statusText}`);
      }

      const testResult = await testResponse.json();
      console.log('Test result:', testResult);

      // Step 3: Update current connection with the test results from the response
      if (testResult.connection) {
        setCurrentConnection(testResult.connection);
      }

      // Check if tests failed and set appropriate error
      const testFailed = testResult.testPassed === false || testResult.error ||
        testResult.connection?.hcfsTestResults?.status === 'FAILED' ||
        testResult.connection?.hs2TestResults?.status === 'FAILED' ||
        testResult.connection?.metastoreDirectTestResults?.status === 'FAILED';

      if (testFailed) {
        setTestError(testResult.message || testResult.error || 'One or more connection tests failed');
      } else {
        setTestError(null);
      }

      // Update formData with key if it was a new connection
      if (!formData.key && savedKey) {
        onChange({ key: savedKey });
      }

      // Mark that the connection was saved during test (only for new connections)
      if (!isEditMode) {
        setSavedDuringTest(true);
      }
    } catch (error) {
      console.error('Error testing connection:', error);
      let errorMessage = 'Unknown error occurred';

      if (error instanceof Error) {
        errorMessage = error.message;
      } else if (typeof error === 'string') {
        errorMessage = error;
      } else if (error && typeof error === 'object') {
        try {
          errorMessage = JSON.stringify(error, null, 2);
        } catch {
          errorMessage = String(error);
        }
      }

      setTestError(errorMessage);
    } finally {
      // Always ensure testing is stopped
      setTesting(false);
    }
  };

  const renderConnectionStatus = () => {
    if (!currentConnection) {
      return (
        <div className="bg-gray-50 border border-gray-200 rounded-lg p-4">
          <div className="text-sm text-gray-600">
            No test results available yet. Save and test the connection to see status.
          </div>
        </div>
      );
    }

    const formatDate = (dateString: string | undefined) => {
      if (!dateString) return '';
      return new Date(dateString).toLocaleDateString();
    };

    return (
      <div className="bg-white border border-gray-200 rounded-lg p-4 space-y-3">
        <div className="text-xs font-semibold text-gray-700 mb-2">Connection Endpoint Status:</div>

        {/* HCFS Namespace Test Result */}
        <div className="flex items-center text-xs">
          <span className="w-40 font-medium text-gray-700">HCFS Namespace:</span>
          {currentConnection.hcfsTestResults ? (
            <span className={currentConnection.hcfsTestResults.status === 'SUCCESS' ? 'text-green-600' : currentConnection.hcfsTestResults.status === 'FAILED' ? 'text-red-600' : 'text-yellow-600'}>
              {currentConnection.hcfsTestResults.status === 'SUCCESS' ? '✅ Connected' : currentConnection.hcfsTestResults.status === 'FAILED' ? '❌ Failed' : '⚠️ Never Tested'}
              {currentConnection.hcfsTestResults.lastTested && (
                <span className="text-gray-400 ml-1">
                  ({formatDate(currentConnection.hcfsTestResults.lastTested)})
                </span>
              )}
            </span>
          ) : (
            <span className="text-yellow-600">⚠️ Never Tested</span>
          )}
        </div>

        {/* HiveServer2 Test Result */}
        <div className="flex items-center text-xs">
          <span className="w-40 font-medium text-gray-700">HiveServer2:</span>
          {currentConnection.hs2TestResults ? (
            <span className={currentConnection.hs2TestResults.status === 'SUCCESS' ? 'text-green-600' : currentConnection.hs2TestResults.status === 'FAILED' ? 'text-red-600' : 'text-yellow-600'}>
              {currentConnection.hs2TestResults.status === 'SUCCESS' ? '✅ Connected' : currentConnection.hs2TestResults.status === 'FAILED' ? '❌ Failed' : '⚠️ Never Tested'}
              {currentConnection.hs2TestResults.lastTested && (
                <span className="text-gray-400 ml-1">
                  ({formatDate(currentConnection.hs2TestResults.lastTested)})
                </span>
              )}
            </span>
          ) : (
            <span className="text-yellow-600">⚠️ Never Tested</span>
          )}
        </div>

        {/* Metastore Direct Test Result */}
        <div className="flex items-center text-xs">
          <span className="w-40 font-medium text-gray-700">Metastore Direct:</span>
          {currentConnection.metastoreDirectTestResults ? (
            <span className={currentConnection.metastoreDirectTestResults.status === 'SUCCESS' ? 'text-green-600' : currentConnection.metastoreDirectTestResults.status === 'FAILED' ? 'text-red-600' : 'text-yellow-600'}>
              {currentConnection.metastoreDirectTestResults.status === 'SUCCESS' ? '✅ Connected' : currentConnection.metastoreDirectTestResults.status === 'FAILED' ? '❌ Failed' : '⚠️ Never Tested'}
              {currentConnection.metastoreDirectTestResults.lastTested && (
                <span className="text-gray-400 ml-1">
                  ({formatDate(currentConnection.metastoreDirectTestResults.lastTested)})
                </span>
              )}
            </span>
          ) : (
            <span className="text-yellow-600">⚠️ Never Tested</span>
          )}
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
        </div>
      </div>

      {/* Connection Test */}
      <div>
        <h3 className="text-lg font-medium text-gray-900 mb-4">🧪 Connection Test</h3>

        {/* Current Connection Status */}
        <div className="mb-4">
          <h4 className="text-sm font-medium text-gray-700 mb-2">Current Status:</h4>
          {renderConnectionStatus()}
        </div>

        {/* Info message about auto-save before test */}
        <div className="mb-4 bg-blue-50 border border-blue-200 rounded-md p-3">
          <p className="text-sm text-blue-700">
            💡 <strong>Note:</strong> Testing will automatically save your configuration before running the connection tests.
            {!isEditMode && ' After a successful test, click OK to complete the setup.'}
          </p>
        </div>

        {/* Test Error */}
        {testError && (
          <div className="mb-4 bg-red-50 border border-red-200 rounded-md p-3">
            <p className="text-sm text-red-700">
              ❌ <strong>Error:</strong> {testError}
            </p>
          </div>
        )}

        {/* Test Success Message */}
        {currentConnection && !testing && !testError && (
          currentConnection.hcfsTestResults || currentConnection.hs2TestResults || currentConnection.metastoreDirectTestResults
        ) && (
          <div className="mb-4 bg-green-50 border border-green-200 rounded-md p-3">
            <p className="text-sm text-green-700">
              ✅ Connection endpoints have been tested. Status shown above.
            </p>
          </div>
        )}

        <div className="flex items-center space-x-4 mb-4">
          <button
            onClick={handleTestConnection}
            disabled={testing}
            className="inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md text-white bg-green-600 hover:bg-green-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-green-500 disabled:opacity-50 disabled:cursor-not-allowed"
            title="Save and test connection"
          >
            <BeakerIcon className="h-4 w-4 mr-2" />
            {testing ? 'Saving & Testing...' : '🧪 Test Connection'}
          </button>

          {testing && (
            <div className="flex items-center text-sm text-gray-600">
              <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-green-600 mr-2"></div>
              Saving and testing connection endpoints...
            </div>
          )}
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
          {/* Show Save button only if not saved during test (for new connections) or in edit mode or explicitly saved */}
          {(!savedDuringTest || isEditMode) && (
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
          )}

          {/* Show Done button when saved (either through test or explicit save) */}
          {(saveSuccess || savedDuringTest) && (
            <button
              onClick={() => navigate('/connections')}
              className="px-6 py-2 font-medium rounded-lg bg-blue-600 text-white hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-blue-500"
            >
              ✓ OK
            </button>
          )}
        </div>
      </div>

      {/* Help Text */}
      <div className="text-xs text-gray-500 text-center">
        The connection will be saved and available for use in migration configurations
      </div>

      {/* Test Progress Dialog */}
      <TestProgressDialog
        isOpen={showTestDialog}
        onClose={() => setShowTestDialog(false)}
        testing={testing}
        connectionName={formData.name}
        hcfsTestResults={currentConnection?.hcfsTestResults}
        hs2TestResults={currentConnection?.hs2TestResults}
        metastoreDirectTestResults={currentConnection?.metastoreDirectTestResults}
        testError={testError}
      />
    </div>
  );
};

export default TestAndSaveStep;