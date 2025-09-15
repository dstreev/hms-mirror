import React from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { ArrowLeftIcon, InformationCircleIcon } from '@heroicons/react/24/outline';
import { useConfiguration } from '../../../contexts/ConfigurationContext';

const AdvancedOptionsSection: React.FC = () => {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const { state, updateConfig } = useConfiguration();

  // Get values from server state with defaults
  const readOnly = state.config?.readOnly || false;
  const noPurge = state.config?.noPurge || false;
  const replace = state.config?.replace || false;
  const resetRight = state.config?.resetRight || false;
  const saveWorkingTables = state.config?.saveWorkingTables || false;
  const dumpTestData = state.config?.dumpTestData || false;
  const copyAvroSchemaUrls = state.config?.copyAvroSchemaUrls || false;

  const handleBooleanChange = (field: string, value: boolean) => {
    updateConfig({ [field]: value });
  };

  const handleBack = () => {
    if (id === 'current') {
      navigate('/config/current');
    } else {
      navigate(`/config/edit/${id}`);
    }
  };

  return (
    <div className="p-8">
      <div className="max-w-4xl mx-auto">
        {/* Header */}
        <div className="mb-8">
          <button
            onClick={handleBack}
            className="flex items-center text-blue-600 hover:text-blue-700 mb-4"
          >
            <ArrowLeftIcon className="h-4 w-4 mr-2" />
            Back to Configuration Overview
          </button>
          <h1 className="text-3xl font-bold text-gray-900">Advanced Options</h1>
          <p className="text-gray-600 mt-2">Configure advanced settings and execution options</p>
        </div>

        {state.error && (
          <div className="mb-6 bg-red-50 border border-red-200 rounded-md p-4">
            <div className="text-sm text-red-700">{state.error}</div>
          </div>
        )}

        <div className="space-y-8">
          {/* Data Handling Options */}
          <div className="bg-white rounded-lg shadow-lg p-6">
            <h3 className="text-lg font-medium text-gray-900 mb-4">Data Handling</h3>
            <div className="space-y-4">
              <div className="flex items-start">
                <div className="flex items-center h-5">
                  <input
                    type="checkbox"
                    checked={readOnly}
                    onChange={(e) => handleBooleanChange('readOnly', e.target.checked)}
                    className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                  />
                </div>
                <div className="ml-3 text-sm">
                  <label className="font-medium text-gray-900">Read-Only Mode</label>
                  <p className="text-gray-500">Perform migration in read-only mode, removing purge properties to avoid filesystem modifications</p>
                </div>
              </div>

              <div className="flex items-start">
                <div className="flex items-center h-5">
                  <input
                    type="checkbox"
                    checked={noPurge}
                    onChange={(e) => handleBooleanChange('noPurge', e.target.checked)}
                    className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                  />
                </div>
                <div className="ml-3 text-sm">
                  <label className="font-medium text-gray-900">No Purge</label>
                  <p className="text-gray-500">Similar to read-only but without filesystem restrictions that would invalidate the target</p>
                </div>
              </div>

              <div className="flex items-start">
                <div className="flex items-center h-5">
                  <input
                    type="checkbox"
                    checked={replace}
                    onChange={(e) => handleBooleanChange('replace', e.target.checked)}
                    className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                  />
                </div>
                <div className="ml-3 text-sm">
                  <label className="font-medium text-gray-900">Replace Tables</label>
                  <p className="text-gray-500">Replace original tables with tables pointing to common storage location (SQL Data Strategy)</p>
                </div>
              </div>

              <div className="flex items-start">
                <div className="flex items-center h-5">
                  <input
                    type="checkbox"
                    checked={copyAvroSchemaUrls}
                    onChange={(e) => handleBooleanChange('copyAvroSchemaUrls', e.target.checked)}
                    className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                  />
                </div>
                <div className="ml-3 text-sm">
                  <label className="font-medium text-gray-900">Copy Avro Schema URLs</label>
                  <p className="text-gray-500">Copy Avro schema URLs during migration</p>
                </div>
              </div>
            </div>
          </div>

          {/* Database Operations */}
          <div className="bg-white rounded-lg shadow-lg p-6">
            <h3 className="text-lg font-medium text-gray-900 mb-4">Database Operations</h3>
            <div className="space-y-4">
              <div className="flex items-start">
                <div className="flex items-center h-5">
                  <input
                    type="checkbox"
                    checked={resetRight}
                    onChange={(e) => handleBooleanChange('resetRight', e.target.checked)}
                    className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                  />
                </div>
                <div className="ml-3 text-sm">
                  <label className="font-medium text-gray-900">Reset Right Database</label>
                  <p className="text-red-600 font-medium">⚠️ WARNING: This will DROP the RIGHT database before recreation!</p>
                  <p className="text-gray-500">Use with extreme caution - this permanently deletes the target database</p>
                </div>
              </div>

              <div className="flex items-start">
                <div className="flex items-center h-5">
                  <input
                    type="checkbox"
                    checked={saveWorkingTables}
                    onChange={(e) => handleBooleanChange('saveWorkingTables', e.target.checked)}
                    className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                  />
                </div>
                <div className="ml-3 text-sm">
                  <label className="font-medium text-gray-900">Save Working Tables</label>
                  <p className="text-gray-500">Keep intermediate tables created during migration (normally removed automatically)</p>
                </div>
              </div>
            </div>
          </div>


          {/* Testing and Debugging */}
          <div className="bg-white rounded-lg shadow-lg p-6">
            <h3 className="text-lg font-medium text-gray-900 mb-4">Testing and Debugging</h3>
            <div className="space-y-4">
              <div className="flex items-start">
                <div className="flex items-center h-5">
                  <input
                    type="checkbox"
                    checked={dumpTestData}
                    onChange={(e) => handleBooleanChange('dumpTestData', e.target.checked)}
                    className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                  />
                </div>
                <div className="ml-3 text-sm">
                  <label className="font-medium text-gray-900">Dump Test Data</label>
                  <p className="text-gray-500">Generate test data output for debugging purposes</p>
                </div>
              </div>
            </div>
          </div>

          {/* Important Warnings */}
          <div className="bg-yellow-50 border border-yellow-200 rounded-md p-4">
            <div className="flex">
              <div className="flex-shrink-0">
                <InformationCircleIcon className="h-5 w-5 text-yellow-400" aria-hidden="true" />
              </div>
              <div className="ml-3">
                <h3 className="text-sm font-medium text-yellow-800">Important Notes</h3>
                <div className="mt-2 text-sm text-yellow-700">
                  <ul className="list-disc pl-5 space-y-1">
                    <li><strong>Reset Right:</strong> Extremely dangerous - permanently deletes target database</li>
                    <li><strong>Read-Only vs No-Purge:</strong> Both prevent data deletion but with different scopes</li>
                    <li><strong>Save Working Tables:</strong> Keep intermediate tables for debugging purposes</li>
                  </ul>
                </div>
              </div>
            </div>
          </div>
        </div>

        {/* Actions */}
        <div className="mt-8 text-center">
          <div className="text-sm text-gray-500">
            Changes are applied but NOT saved. Use the 'Save or Save As' to persist the current configuration
          </div>
        </div>
      </div>
    </div>
  );
};

export default AdvancedOptionsSection;