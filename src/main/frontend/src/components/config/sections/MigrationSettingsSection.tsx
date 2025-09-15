import React from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { ArrowLeftIcon, CheckIcon, InformationCircleIcon } from '@heroicons/react/24/outline';
import { useConfiguration } from '../../../contexts/ConfigurationContext';

const MigrationSettingsSection: React.FC = () => {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const { state, updateConfig } = useConfiguration();

  // Get values from server state with defaults
  const dataStrategy = state.config?.dataStrategy || 'SCHEMA_ONLY';
  const warehouseDirectory = state.config?.transfer?.warehouse?.managedDirectory || '/warehouse/tablespace/managed/hive';
  const warehouseExternalDirectory = state.config?.transfer?.warehouse?.externalDirectory || '/warehouse/tablespace/external/hive';
  const skipLinkCheck = state.config?.skipLinkCheck || false;
  const readOnly = state.config?.readOnly || false;
  const sync = state.config?.sync || false;
  const skipFeatures = state.config?.skipFeatures || false;
  const skipStatsCollection = state.config?.optimization?.skipStatsCollection || false;
  const sortDynamicPartition = state.config?.optimization?.sortDynamicPartitionInserts || false;
  const autoTune = state.config?.optimization?.autoTune || false;

  const handleDataStrategyChange = (value: string) => {
    updateConfig({ dataStrategy: value });
  };

  const handleWarehouseChange = (field: string, value: string) => {
    const warehouseUpdate = {
      transfer: {
        ...state.config?.transfer,
        warehouse: {
          ...state.config?.transfer?.warehouse,
          [field]: value
        }
      }
    };
    updateConfig(warehouseUpdate);
  };

  const handleBooleanChange = (field: string, value: boolean) => {
    updateConfig({ [field]: value });
  };

  const handleOptimizationChange = (field: string, value: boolean) => {
    const optimizationUpdate = {
      optimization: {
        ...state.config?.optimization,
        [field]: value
      }
    };
    updateConfig(optimizationUpdate);
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
          <h1 className="text-3xl font-bold text-gray-900">Migration Settings</h1>
          <p className="text-gray-600 mt-2">Configure migration strategy and data transfer options</p>
        </div>

        {state.error && (
          <div className="mb-6 bg-red-50 border border-red-200 rounded-md p-4">
            <div className="text-sm text-red-700">{state.error}</div>
          </div>
        )}

        <div className="space-y-8">
          {/* Data Strategy Display */}
          <div className="bg-white rounded-lg shadow-lg p-6">
            <h3 className="text-lg font-medium text-gray-900 mb-4">Data Strategy</h3>
            <div className="bg-gray-50 rounded-md p-4">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm font-medium text-gray-700">Current Migration Strategy</p>
                  <p className="text-lg font-semibold text-gray-900 mt-1">{dataStrategy}</p>
                </div>
                <InformationCircleIcon className="h-5 w-5 text-gray-400" />
              </div>
              <p className="text-sm text-gray-500 mt-2">
                The migration strategy is set when creating a new configuration and cannot be changed here.
              </p>
            </div>
          </div>

          {/* Warehouse Settings */}
          <div className="bg-white rounded-lg shadow-lg p-6">
            <h3 className="text-lg font-medium text-gray-900 mb-4">Warehouse Directories</h3>
            <div className="space-y-4">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Managed Table Directory
                </label>
                <input
                  type="text"
                  value={warehouseDirectory}
                  onChange={(e) => handleWarehouseChange('managedDirectory', e.target.value)}
                  className="w-full p-3 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                  placeholder="/warehouse/tablespace/managed/hive"
                />
                <p className="text-sm text-gray-500 mt-1">
                  Directory for managed Hive tables
                </p>
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  External Table Directory
                </label>
                <input
                  type="text"
                  value={warehouseExternalDirectory}
                  onChange={(e) => handleWarehouseChange('externalDirectory', e.target.value)}
                  className="w-full p-3 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                  placeholder="/warehouse/tablespace/external/hive"
                />
                <p className="text-sm text-gray-500 mt-1">
                  Directory for external Hive tables
                </p>
              </div>
            </div>
          </div>

          {/* Transfer Options */}
          <div className="bg-white rounded-lg shadow-lg p-6">
            <h3 className="text-lg font-medium text-gray-900 mb-4">Transfer Options</h3>
            <div className="space-y-4">
              <div className="flex items-start">
                <div className="flex items-center h-5">
                  <input
                    type="checkbox"
                    checked={skipLinkCheck}
                    onChange={(e) => handleBooleanChange('skipLinkCheck', e.target.checked)}
                    className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                  />
                </div>
                <div className="ml-3 text-sm">
                  <label className="font-medium text-gray-900">Skip Link Check</label>
                  <p className="text-gray-500">Skip validation of storage links during migration</p>
                </div>
              </div>

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
                  <p className="text-gray-500">Perform migration in read-only mode (no data modifications)</p>
                </div>
              </div>

              <div className="flex items-start">
                <div className="flex items-center h-5">
                  <input
                    type="checkbox"
                    checked={sync}
                    onChange={(e) => handleBooleanChange('sync', e.target.checked)}
                    className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                  />
                </div>
                <div className="ml-3 text-sm">
                  <label className="font-medium text-gray-900">Sync Mode</label>
                  <p className="text-gray-500">Enable synchronous data transfer</p>
                </div>
              </div>
            </div>
          </div>

          {/* Feature Options */}
          <div className="bg-white rounded-lg shadow-lg p-6">
            <h3 className="text-lg font-medium text-gray-900 mb-4">Feature Handling</h3>
            <div className="space-y-4">
              <div className="flex items-start">
                <div className="flex items-center h-5">
                  <input
                    type="checkbox"
                    checked={!skipFeatures}
                    onChange={(e) => handleBooleanChange('skipFeatures', !e.target.checked)}
                    className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                  />
                </div>
                <div className="ml-3 text-sm">
                  <label className="font-medium text-gray-900">Enable Features</label>
                  <p className="text-gray-500">Enable advanced feature processing during migration</p>
                </div>
              </div>
            </div>
          </div>

          {/* Optimization */}
          <div className="bg-white rounded-lg shadow-lg p-6">
            <h3 className="text-lg font-medium text-gray-900 mb-4">Optimization Settings</h3>
            <div className="space-y-4">
              <div className="flex items-start">
                <div className="flex items-center h-5">
                  <input
                    type="checkbox"
                    checked={autoTune}
                    onChange={(e) => handleOptimizationChange('autoTune', e.target.checked)}
                    className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                  />
                </div>
                <div className="ml-3 text-sm">
                  <label className="font-medium text-gray-900">Auto Tune</label>
                  <p className="text-gray-500">Automatically tune performance settings</p>
                </div>
              </div>

              <div className="flex items-start">
                <div className="flex items-center h-5">
                  <input
                    type="checkbox"
                    checked={skipStatsCollection}
                    onChange={(e) => handleOptimizationChange('skipStatsCollection', e.target.checked)}
                    className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                  />
                </div>
                <div className="ml-3 text-sm">
                  <label className="font-medium text-gray-900">Skip Stats Collection</label>
                  <p className="text-gray-500">Skip automatic statistics collection during migration</p>
                </div>
              </div>

              <div className="flex items-start">
                <div className="flex items-center h-5">
                  <input
                    type="checkbox"
                    checked={sortDynamicPartition}
                    onChange={(e) => handleOptimizationChange('sortDynamicPartitionInserts', e.target.checked)}
                    className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                  />
                </div>
                <div className="ml-3 text-sm">
                  <label className="font-medium text-gray-900">Sort Dynamic Partitions</label>
                  <p className="text-gray-500">Enable sorting for dynamic partition operations</p>
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

export default MigrationSettingsSection;