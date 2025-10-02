import React, { useState } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { ArrowLeftIcon, CheckIcon, InformationCircleIcon } from '@heroicons/react/24/outline';
import { useConfiguration } from '../../../contexts/ConfigurationContext';

const MigrationSettingsSection: React.FC = () => {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const { state, updateConfig } = useConfiguration();
  const [activeTab, setActiveTab] = useState('dataMovement');

  // Get values from server state with defaults
  const dataStrategy = state.config?.dataStrategy || 'SCHEMA_ONLY';
  
  // Data Movement Settings
  const targetNamespace = state.config?.transfer?.targetNamespace || '';
  const intermediateStorage = state.config?.transfer?.intermediateStorage || '';
  const translationType = state.config?.transfer?.storageMigration?.translationType || 'ALIGNED';
  const dataMovementStrategy = state.config?.transfer?.storageMigration?.dataMovementStrategy || 'DISTCP';
  const dataFlow = state.config?.transfer?.storageMigration?.dataFlow || 'PULL';
  const consolidateTables = state.config?.transfer?.storageMigration?.consolidateTablesForDistcp || false;
  const strict = state.config?.transfer?.storageMigration?.strict || false;
  const skipDatabaseLocationAdjustments = state.config?.transfer?.storageMigration?.skipDatabaseLocationAdjustments || false;
  const createArchive = state.config?.transfer?.storageMigration?.createArchive || false;
  
  // Global Warehouse Locations
  const warehouseDirectory = state.config?.transfer?.warehouse?.managedDirectory || '/warehouse/tablespace/managed/hive';
  const warehouseExternalDirectory = state.config?.transfer?.warehouse?.externalDirectory || '/warehouse/tablespace/external/hive';
  
  // Miscellaneous Settings
  const skipLinkCheck = state.config?.skipLinkCheck || false;
  const copyAvroSchemaUrls = state.config?.copyAvroSchemaUrls || false;
  const databaseOnly = state.config?.databaseOnly || false;
  const readOnly = state.config?.readOnly || false;
  const noPurge = state.config?.noPurge || false;
  const replace = state.config?.replace || false;
  const resetRight = state.config?.resetRight || false;
  const saveWorkingTables = state.config?.saveWorkingTables || false;
  const skipFeatures = state.config?.skipFeatures || false;
  const skipLegacyTranslation = state.config?.skipLegacyTranslation || false;
  const sync = state.config?.sync || false;
  const transferOwnershipDb = state.config?.ownershipTransfer?.database || false;
  const transferOwnershipTbl = state.config?.ownershipTransfer?.table || false;
  const beta = state.config?.beta || false;
  const execute = state.config?.execute || false;
  const quiet = state.config?.quiet || false;
  const encryptedPasswords = state.config?.encryptedPasswords || false;
  
  // Migrate Options
  const migrateACIDOn = state.config?.migrateACID?.on || false;
  const migrateACIDOnly = state.config?.migrateACID?.only || false;
  const artificialBucketThreshold = state.config?.migrateACID?.artificialBucketThreshold || 2;
  const acidDowngrade = state.config?.migrateACID?.downgrade || false;
  const acidInplace = state.config?.migrateACID?.inplace || false;
  const acidPartitionLimit = state.config?.migrateACID?.partitionLimit || 100;
  const migrateVIEWOn = state.config?.migrateVIEW?.on || false;
  const migrateNonNative = state.config?.migrateNonNative || false;
  const icebergEnabled = state.config?.icebergConversion?.enable || false;
  const icebergVersion = state.config?.icebergConversion?.version || 'v2';
  
  // Transfer Options
  const transferPrefix = state.config?.transfer?.transferPrefix || '';
  const shadowPrefix = state.config?.transfer?.shadowPrefix || 'hms_mirror_shadow_';
  const exportBaseDirPrefix = state.config?.transfer?.exportBaseDirPrefix || '/apps/hive/warehouse/export_';
  const remoteWorkingDirectory = state.config?.transfer?.remoteWorkingDirectory || 'hms_mirror_working';
  const storageMigrationPostfix = state.config?.transfer?.storageMigrationPostfix || '';
  
  // Hybrid Config
  const hybridSqlPartitionLimit = state.config?.hybrid?.sqlPartitionLimit || 3000;
  const hybridSqlSizeLimit = state.config?.hybrid?.sqlSizeLimit || '1073741824';
  const hybridExportImportPartitionLimit = state.config?.hybrid?.exportImportPartitionLimit || 100;
  
  // Optimizations
  const skipStatsCollection = state.config?.optimization?.skipStatsCollection || false;
  const buildShadowStatistics = state.config?.optimization?.buildShadowStatistics || false;
  const sortDynamicPartition = state.config?.optimization?.sortDynamicPartitionInserts || false;
  const autoTune = state.config?.optimization?.autoTune || false;
  const compressTextOutput = state.config?.optimization?.compressTextOutput || false;
  const skipOptimization = state.config?.optimization?.skip || false;

  // Handler functions
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

  const handleTransferChange = (field: string, value: any) => {
    const transferUpdate = {
      transfer: {
        ...state.config?.transfer,
        [field]: value
      }
    };
    updateConfig(transferUpdate);
  };

  const handleStorageMigrationChange = (field: string, value: any) => {
    const update = {
      transfer: {
        ...state.config?.transfer,
        storageMigration: {
          ...state.config?.transfer?.storageMigration,
          [field]: value
        }
      }
    };
    updateConfig(update);
  };

  const handleMigrateACIDChange = (field: string, value: any) => {
    const update = {
      migrateACID: {
        ...state.config?.migrateACID,
        [field]: value
      }
    };
    updateConfig(update);
  };

  const handleMigrateVIEWChange = (field: string, value: boolean) => {
    const update = {
      migrateVIEW: {
        ...state.config?.migrateVIEW,
        [field]: value
      }
    };
    updateConfig(update);
  };

  const handleIcebergChange = (field: string, value: any) => {
    const update = {
      icebergConversion: {
        ...state.config?.icebergConversion,
        [field]: value
      }
    };
    updateConfig(update);
  };

  const handleHybridChange = (field: string, value: any) => {
    const update = {
      hybrid: {
        ...state.config?.hybrid,
        [field]: value
      }
    };
    updateConfig(update);
  };

  const handleOwnershipChange = (field: string, value: boolean) => {
    const update = {
      ownershipTransfer: {
        ...state.config?.ownershipTransfer,
        [field]: value
      }
    };
    updateConfig(update);
  };

  const handleBack = () => {
    if (id === 'current') {
      navigate('/config/current');
    } else {
      navigate(`/config/edit/${id}`);
    }
  };

  const tabs = [
    { id: 'dataMovement', label: 'Data Movement' },
    { id: 'globalWarehouse', label: 'Global Warehouse Locations' },
    { id: 'miscellaneous', label: 'Miscellaneous' },
    { id: 'migrateOptions', label: 'Migrate Options' },
    { id: 'transferOptions', label: 'Transfer Options' },
    { id: 'optimizations', label: 'Optimizations' },
    { id: 'propertyOverrides', label: 'Property Overrides' }
  ];

  return (
    <div className="p-8">
      <div className="max-w-6xl mx-auto">
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

        {/* Tab Navigation */}
        <div className="border-b border-gray-200 mb-6">
          <nav className="-mb-px flex space-x-8 overflow-x-auto">
            {tabs.map(tab => (
              <button
                key={tab.id}
                onClick={() => setActiveTab(tab.id)}
                className={`
                  whitespace-nowrap py-2 px-1 border-b-2 font-medium text-sm
                  ${activeTab === tab.id 
                    ? 'border-blue-500 text-blue-600' 
                    : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'}
                `}
              >
                {tab.label}
              </button>
            ))}
          </nav>
        </div>

        <div className="space-y-8">
          {/* Data Movement Tab */}
          {activeTab === 'dataMovement' && (
            <>
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

              <div className="bg-white rounded-lg shadow-lg p-6">
                <h3 className="text-lg font-medium text-gray-900 mb-4">Data Movement Configuration</h3>
                <div className="space-y-4">
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">
                      Target Namespace
                    </label>
                    <input
                      type="text"
                      value={targetNamespace}
                      onChange={(e) => handleTransferChange('targetNamespace', e.target.value)}
                      className="w-full p-3 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                      placeholder="Set Target Namespace"
                    />
                    <p className="text-sm text-gray-500 mt-1">Namespace for the target environment</p>
                  </div>

                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">
                      Intermediate Storage
                    </label>
                    <input
                      type="text"
                      value={intermediateStorage}
                      onChange={(e) => handleTransferChange('intermediateStorage', e.target.value)}
                      className="w-full p-3 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                      placeholder="Set Intermediate Storage"
                    />
                    <p className="text-sm text-gray-500 mt-1">Storage location for intermediate data during migration</p>
                  </div>

                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">
                      Location Translation Strategy
                    </label>
                    <select
                      value={translationType}
                      onChange={(e) => handleStorageMigrationChange('translationType', e.target.value)}
                      className="w-full p-3 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                    >
                      <option value="ALIGNED">ALIGNED</option>
                      <option value="RELATIVE">RELATIVE</option>
                    </select>
                  </div>

                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">
                      Data Movement Strategy
                    </label>
                    <select
                      value={dataMovementStrategy}
                      onChange={(e) => handleStorageMigrationChange('dataMovementStrategy', e.target.value)}
                      className="w-full p-3 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                    >
                      <option value="DISTCP">DISTCP</option>
                      <option value="SQL">SQL</option>
                      <option value="EXPORT_IMPORT">EXPORT_IMPORT</option>
                      <option value="HYBRID">HYBRID</option>
                    </select>
                  </div>

                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">
                      Data Flow
                    </label>
                    <select
                      value={dataFlow}
                      onChange={(e) => handleStorageMigrationChange('dataFlow', e.target.value)}
                      className="w-full p-3 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                    >
                      <option value="PULL">PULL</option>
                      <option value="PUSH">PUSH</option>
                      <option value="REMOTE_PUSH">REMOTE_PUSH</option>
                    </select>
                  </div>

                  <div className="flex items-start">
                    <div className="flex items-center h-5">
                      <input
                        type="checkbox"
                        checked={skipDatabaseLocationAdjustments}
                        onChange={(e) => handleStorageMigrationChange('skipDatabaseLocationAdjustments', e.target.checked)}
                        className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                      />
                    </div>
                    <div className="ml-3 text-sm">
                      <label className="font-medium text-gray-900">Skip Database Location Adjustments</label>
                      <p className="text-gray-500">Skip adjusting database locations (STORAGE_MIGRATION only)</p>
                    </div>
                  </div>

                  <div className="flex items-start">
                    <div className="flex items-center h-5">
                      <input
                        type="checkbox"
                        checked={createArchive}
                        onChange={(e) => handleStorageMigrationChange('createArchive', e.target.checked)}
                        className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                      />
                    </div>
                    <div className="ml-3 text-sm">
                      <label className="font-medium text-gray-900">Create Archive</label>
                      <p className="text-gray-500">Create archive tables during migration (STORAGE_MIGRATION only)</p>
                    </div>
                  </div>

                  <div className="flex items-start">
                    <div className="flex items-center h-5">
                      <input
                        type="checkbox"
                        checked={consolidateTables}
                        onChange={(e) => handleStorageMigrationChange('consolidateTablesForDistcp', e.target.checked)}
                        className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                      />
                    </div>
                    <div className="ml-3 text-sm">
                      <label className="font-medium text-gray-900">Consolidate Source Tables</label>
                      <p className="text-gray-500">Consolidate source tables for DISTCP operations</p>
                    </div>
                  </div>

                  <div className="flex items-start">
                    <div className="flex items-center h-5">
                      <input
                        type="checkbox"
                        checked={strict}
                        onChange={(e) => handleStorageMigrationChange('strict', e.target.checked)}
                        className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                      />
                    </div>
                    <div className="ml-3 text-sm">
                      <label className="font-medium text-gray-900">Strict Mode</label>
                      <p className="text-gray-500">Enable strict validation during migration</p>
                    </div>
                  </div>
                </div>
              </div>
            </>
          )}

          {/* Global Warehouse Locations Tab */}
          {activeTab === 'globalWarehouse' && (
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
              {/* Note: Global Location Maps would be added here if needed */}
            </div>
          )}

          {/* Miscellaneous Tab */}
          {activeTab === 'miscellaneous' && (
            <div className="bg-white rounded-lg shadow-lg p-6">
              <h3 className="text-lg font-medium text-gray-900 mb-4">Miscellaneous Options</h3>
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
                      checked={copyAvroSchemaUrls}
                      onChange={(e) => handleBooleanChange('copyAvroSchemaUrls', e.target.checked)}
                      className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                    />
                  </div>
                  <div className="ml-3 text-sm">
                    <label className="font-medium text-gray-900">Copy Avro Schema URLs</label>
                    <p className="text-gray-500">Copy Avro schema URLs to the target</p>
                  </div>
                </div>

                <div className="flex items-start">
                  <div className="flex items-center h-5">
                    <input
                      type="checkbox"
                      checked={databaseOnly}
                      onChange={(e) => handleBooleanChange('databaseOnly', e.target.checked)}
                      className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                    />
                  </div>
                  <div className="ml-3 text-sm">
                    <label className="font-medium text-gray-900">Database Only</label>
                    <p className="text-gray-500">Migrate database metadata only</p>
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
                      checked={noPurge}
                      onChange={(e) => handleBooleanChange('noPurge', e.target.checked)}
                      className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                    />
                  </div>
                  <div className="ml-3 text-sm">
                    <label className="font-medium text-gray-900">No Purge</label>
                    <p className="text-gray-500">Remove purge option from tables</p>
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
                    <label className="font-medium text-gray-900">Replace</label>
                    <p className="text-gray-500">Replace tables with common storage location</p>
                  </div>
                </div>

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
                    <label className="font-medium text-gray-900">Reset Right</label>
                    <p className="text-gray-500">DROP and recreate the RIGHT database</p>
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
                    <p className="text-gray-500">Keep intermediate tables after migration</p>
                  </div>
                </div>

                <div className="flex items-start">
                  <div className="flex items-center h-5">
                    <input
                      type="checkbox"
                      checked={skipFeatures}
                      onChange={(e) => handleBooleanChange('skipFeatures', e.target.checked)}
                      className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                    />
                  </div>
                  <div className="ml-3 text-sm">
                    <label className="font-medium text-gray-900">Skip Features</label>
                    <p className="text-gray-500">Skip feature processing during migration</p>
                  </div>
                </div>

                <div className="flex items-start">
                  <div className="flex items-center h-5">
                    <input
                      type="checkbox"
                      checked={skipLegacyTranslation}
                      onChange={(e) => handleBooleanChange('skipLegacyTranslation', e.target.checked)}
                      className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                    />
                  </div>
                  <div className="ml-3 text-sm">
                    <label className="font-medium text-gray-900">Skip Legacy Translation</label>
                    <p className="text-gray-500">Skip legacy managed table translation</p>
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

                <div className="flex items-start">
                  <div className="flex items-center h-5">
                    <input
                      type="checkbox"
                      checked={transferOwnershipDb}
                      onChange={(e) => handleOwnershipChange('database', e.target.checked)}
                      className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                    />
                  </div>
                  <div className="ml-3 text-sm">
                    <label className="font-medium text-gray-900">Transfer Database Ownership</label>
                    <p className="text-gray-500">Transfer ownership of databases</p>
                  </div>
                </div>

                <div className="flex items-start">
                  <div className="flex items-center h-5">
                    <input
                      type="checkbox"
                      checked={transferOwnershipTbl}
                      onChange={(e) => handleOwnershipChange('table', e.target.checked)}
                      className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                    />
                  </div>
                  <div className="ml-3 text-sm">
                    <label className="font-medium text-gray-900">Transfer Table Ownership</label>
                    <p className="text-gray-500">Transfer ownership of tables</p>
                  </div>
                </div>

                <div className="flex items-start">
                  <div className="flex items-center h-5">
                    <input
                      type="checkbox"
                      checked={beta}
                      onChange={(e) => handleBooleanChange('beta', e.target.checked)}
                      className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                    />
                  </div>
                  <div className="ml-3 text-sm">
                    <label className="font-medium text-gray-900">Beta Features</label>
                    <p className="text-gray-500">Enable beta features</p>
                  </div>
                </div>

                <div className="flex items-start">
                  <div className="flex items-center h-5">
                    <input
                      type="checkbox"
                      checked={execute}
                      onChange={(e) => handleBooleanChange('execute', e.target.checked)}
                      className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                    />
                  </div>
                  <div className="ml-3 text-sm">
                    <label className="font-medium text-gray-900">Execute</label>
                    <p className="text-gray-500">Execute migration operations</p>
                  </div>
                </div>

                <div className="flex items-start">
                  <div className="flex items-center h-5">
                    <input
                      type="checkbox"
                      checked={quiet}
                      onChange={(e) => handleBooleanChange('quiet', e.target.checked)}
                      className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                    />
                  </div>
                  <div className="ml-3 text-sm">
                    <label className="font-medium text-gray-900">Quiet Mode</label>
                    <p className="text-gray-500">Suppress output messages</p>
                  </div>
                </div>

                <div className="flex items-start">
                  <div className="flex items-center h-5">
                    <input
                      type="checkbox"
                      checked={encryptedPasswords}
                      onChange={(e) => handleBooleanChange('encryptedPasswords', e.target.checked)}
                      className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                    />
                  </div>
                  <div className="ml-3 text-sm">
                    <label className="font-medium text-gray-900">Encrypted Passwords</label>
                    <p className="text-gray-500">Use encrypted passwords in configuration</p>
                  </div>
                </div>
              </div>
            </div>
          )}

          {/* Migrate Options Tab */}
          {activeTab === 'migrateOptions' && (
            <>
              <div className="bg-white rounded-lg shadow-lg p-6">
                <h3 className="text-lg font-medium text-gray-900 mb-4">ACID Migration Options</h3>
                <div className="space-y-4">
                  <div className="flex items-start">
                    <div className="flex items-center h-5">
                      <input
                        type="checkbox"
                        checked={migrateACIDOn}
                        onChange={(e) => handleMigrateACIDChange('on', e.target.checked)}
                        className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                      />
                    </div>
                    <div className="ml-3 text-sm">
                      <label className="font-medium text-gray-900">Migrate ACID Tables</label>
                      <p className="text-gray-500">Enable migration of ACID tables</p>
                    </div>
                  </div>

                  <div className="flex items-start">
                    <div className="flex items-center h-5">
                      <input
                        type="checkbox"
                        checked={migrateACIDOnly}
                        onChange={(e) => handleMigrateACIDChange('only', e.target.checked)}
                        className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                      />
                    </div>
                    <div className="ml-3 text-sm">
                      <label className="font-medium text-gray-900">Migrate ACID Only</label>
                      <p className="text-gray-500">Migrate only ACID tables</p>
                    </div>
                  </div>

                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">
                      Artificial Bucket Threshold
                    </label>
                    <input
                      type="number"
                      value={artificialBucketThreshold}
                      onChange={(e) => handleMigrateACIDChange('artificialBucketThreshold', parseInt(e.target.value))}
                      className="w-full p-3 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                    />
                  </div>

                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">
                      ACID Partition Limit
                    </label>
                    <input
                      type="number"
                      value={acidPartitionLimit}
                      onChange={(e) => handleMigrateACIDChange('partitionLimit', parseInt(e.target.value))}
                      className="w-full p-3 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                    />
                  </div>

                  <div className="flex items-start">
                    <div className="flex items-center h-5">
                      <input
                        type="checkbox"
                        checked={acidDowngrade}
                        onChange={(e) => handleMigrateACIDChange('downgrade', e.target.checked)}
                        className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                      />
                    </div>
                    <div className="ml-3 text-sm">
                      <label className="font-medium text-gray-900">ACID Downgrade</label>
                      <p className="text-gray-500">Downgrade ACID tables to non-ACID</p>
                    </div>
                  </div>

                  <div className="flex items-start">
                    <div className="flex items-center h-5">
                      <input
                        type="checkbox"
                        checked={acidInplace}
                        onChange={(e) => handleMigrateACIDChange('inplace', e.target.checked)}
                        className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                      />
                    </div>
                    <div className="ml-3 text-sm">
                      <label className="font-medium text-gray-900">ACID In-place</label>
                      <p className="text-gray-500">Perform in-place ACID migration</p>
                    </div>
                  </div>
                </div>
              </div>

              <div className="bg-white rounded-lg shadow-lg p-6">
                <h3 className="text-lg font-medium text-gray-900 mb-4">Migration Alternatives</h3>
                <div className="space-y-4">
                  <div className="flex items-start">
                    <div className="flex items-center h-5">
                      <input
                        type="checkbox"
                        checked={migrateVIEWOn}
                        onChange={(e) => handleMigrateVIEWChange('on', e.target.checked)}
                        className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                      />
                    </div>
                    <div className="ml-3 text-sm">
                      <label className="font-medium text-gray-900">Migrate VIEWs</label>
                      <p className="text-gray-500">Enable migration of VIEWs</p>
                    </div>
                  </div>

                  <div className="flex items-start">
                    <div className="flex items-center h-5">
                      <input
                        type="checkbox"
                        checked={migrateNonNative}
                        onChange={(e) => handleBooleanChange('migrateNonNative', e.target.checked)}
                        className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                      />
                    </div>
                    <div className="ml-3 text-sm">
                      <label className="font-medium text-gray-900">Migrate Non-Native Tables</label>
                      <p className="text-gray-500">Enable migration of non-native Hive tables</p>
                    </div>
                  </div>
                </div>
              </div>

              <div className="bg-white rounded-lg shadow-lg p-6">
                <h3 className="text-lg font-medium text-gray-900 mb-4">Iceberg Migration</h3>
                <div className="space-y-4">
                  <div className="flex items-start">
                    <div className="flex items-center h-5">
                      <input
                        type="checkbox"
                        checked={icebergEnabled}
                        onChange={(e) => handleIcebergChange('enable', e.target.checked)}
                        className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                      />
                    </div>
                    <div className="ml-3 text-sm">
                      <label className="font-medium text-gray-900">Enable Iceberg Conversion</label>
                      <p className="text-gray-500">Convert tables to Iceberg format</p>
                    </div>
                  </div>

                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">
                      Iceberg Version
                    </label>
                    <select
                      value={icebergVersion}
                      onChange={(e) => handleIcebergChange('version', e.target.value)}
                      className="w-full p-3 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                    >
                      <option value="v1">v1</option>
                      <option value="v2">v2</option>
                    </select>
                  </div>
                </div>
              </div>
            </>
          )}

          {/* Transfer Options Tab */}
          {activeTab === 'transferOptions' && (
            <>
              <div className="bg-white rounded-lg shadow-lg p-6">
                <h3 className="text-lg font-medium text-gray-900 mb-4">Transfer Configuration</h3>
                <div className="space-y-4">
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">
                      Transfer Prefix
                    </label>
                    <input
                      type="text"
                      value={transferPrefix}
                      onChange={(e) => handleTransferChange('transferPrefix', e.target.value)}
                      className="w-full p-3 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                      placeholder="Set Transfer Prefix"
                    />
                  </div>

                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">
                      Shadow Prefix
                    </label>
                    <input
                      type="text"
                      value={shadowPrefix}
                      onChange={(e) => handleTransferChange('shadowPrefix', e.target.value)}
                      className="w-full p-3 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                      placeholder="hms_mirror_shadow_"
                    />
                  </div>

                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">
                      Export Base Directory Prefix
                    </label>
                    <input
                      type="text"
                      value={exportBaseDirPrefix}
                      onChange={(e) => handleTransferChange('exportBaseDirPrefix', e.target.value)}
                      className="w-full p-3 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                      placeholder="/apps/hive/warehouse/export_"
                    />
                  </div>

                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">
                      Remote Working Directory
                    </label>
                    <input
                      type="text"
                      value={remoteWorkingDirectory}
                      onChange={(e) => handleTransferChange('remoteWorkingDirectory', e.target.value)}
                      className="w-full p-3 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                      placeholder="hms_mirror_working"
                    />
                  </div>

                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">
                      Storage Migration Postfix
                    </label>
                    <input
                      type="text"
                      value={storageMigrationPostfix}
                      onChange={(e) => handleTransferChange('storageMigrationPostfix', e.target.value)}
                      className="w-full p-3 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                      placeholder="Set Storage Migration Postfix"
                    />
                  </div>
                </div>
              </div>

              {dataStrategy === 'HYBRID' && (
                <div className="bg-white rounded-lg shadow-lg p-6">
                  <h3 className="text-lg font-medium text-gray-900 mb-4">Hybrid Configuration</h3>
                  <div className="space-y-4">
                    <div>
                      <label className="block text-sm font-medium text-gray-700 mb-1">
                        SQL Partition Limit
                      </label>
                      <input
                        type="number"
                        value={hybridSqlPartitionLimit}
                        onChange={(e) => handleHybridChange('sqlPartitionLimit', parseInt(e.target.value))}
                        className="w-full p-3 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                      />
                    </div>

                    <div>
                      <label className="block text-sm font-medium text-gray-700 mb-1">
                        SQL Size Limit (bytes)
                      </label>
                      <input
                        type="text"
                        value={hybridSqlSizeLimit}
                        onChange={(e) => handleHybridChange('sqlSizeLimit', e.target.value)}
                        className="w-full p-3 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                      />
                    </div>

                    <div>
                      <label className="block text-sm font-medium text-gray-700 mb-1">
                        Export/Import Partition Limit
                      </label>
                      <input
                        type="number"
                        value={hybridExportImportPartitionLimit}
                        onChange={(e) => handleHybridChange('exportImportPartitionLimit', parseInt(e.target.value))}
                        className="w-full p-3 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                      />
                    </div>
                  </div>
                </div>
              )}
            </>
          )}

          {/* Optimizations Tab */}
          {activeTab === 'optimizations' && (
            <div className="bg-white rounded-lg shadow-lg p-6">
              <h3 className="text-lg font-medium text-gray-900 mb-4">Optimization Settings</h3>
              <div className="space-y-4">
                <div className="flex items-start">
                  <div className="flex items-center h-5">
                    <input
                      type="checkbox"
                      checked={skipOptimization}
                      onChange={(e) => handleOptimizationChange('skip', e.target.checked)}
                      className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                    />
                  </div>
                  <div className="ml-3 text-sm">
                    <label className="font-medium text-gray-900">Skip Optimizations</label>
                    <p className="text-gray-500">Skip all optimization steps</p>
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
                      checked={buildShadowStatistics}
                      onChange={(e) => handleOptimizationChange('buildShadowStatistics', e.target.checked)}
                      className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                    />
                  </div>
                  <div className="ml-3 text-sm">
                    <label className="font-medium text-gray-900">Build Shadow Statistics</label>
                    <p className="text-gray-500">Build statistics for shadow tables</p>
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
                      checked={compressTextOutput}
                      onChange={(e) => handleOptimizationChange('compressTextOutput', e.target.checked)}
                      className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                    />
                  </div>
                  <div className="ml-3 text-sm">
                    <label className="font-medium text-gray-900">Compress Text Output</label>
                    <p className="text-gray-500">Enable compression for text output files</p>
                  </div>
                </div>
              </div>
            </div>
          )}

          {/* Property Overrides Tab */}
          {activeTab === 'propertyOverrides' && (
            <div className="bg-white rounded-lg shadow-lg p-6">
              <h3 className="text-lg font-medium text-gray-900 mb-4">Property Overrides</h3>
              <p className="text-sm text-gray-500 mb-4">
                Property overrides allow you to set Hive properties for the migration process.
                This section would typically include a table or form to add/remove property overrides.
              </p>
              <div className="bg-gray-50 rounded-md p-4">
                <p className="text-sm text-gray-600">
                  Property override management will be implemented here.
                </p>
              </div>
            </div>
          )}
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