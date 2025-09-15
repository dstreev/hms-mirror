import React, { useState } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { 
  ArrowLeftIcon, 
  PlusIcon, 
  TrashIcon,
  InformationCircleIcon,
  FunnelIcon,
  DocumentTextIcon,
  FolderIcon,
  ArrowsRightLeftIcon,
  ExclamationTriangleIcon
} from '@heroicons/react/24/outline';
import { useConfiguration } from '../../../contexts/ConfigurationContext';

const DatabaseFiltersSection: React.FC = () => {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const { state, updateConfig } = useConfiguration();
  
  const [activeTab, setActiveTab] = useState<'skipProperties' | 'warehousePlans' | 'filters' | 'limits' | 'redirects'>('skipProperties');
  const [newSkipProperty, setNewSkipProperty] = useState('');
  const [newWarehousePlan, setNewWarehousePlan] = useState({
    database: '',
    externalDirectory: '',
    managedDirectory: ''
  });

  const dataStrategy = state.config?.dataStrategy || 'SCHEMA_ONLY';
  const filter = state.config?.filter || {};
  const dbPropertySkipList = filter.dbPropertySkipList || [];
  const warehousePlans = state.config?.translator?.warehouseMapBuilder?.warehousePlans || {};
  const hybrid = state.config?.hybrid || {};
  const migrateACID = state.config?.migrateACID || {};

  const handleBack = () => {
    if (id === 'current') {
      navigate('/config/current');
    } else {
      navigate(`/config/edit/${id}`);
    }
  };

  // Skip Database Properties handlers
  const handleAddSkipProperty = () => {
    if (newSkipProperty.trim()) {
      const updatedList = [...dbPropertySkipList, newSkipProperty.trim()];
      updateConfig({
        filter: {
          ...filter,
          dbPropertySkipList: updatedList
        }
      });
      setNewSkipProperty('');
    }
  };

  const handleRemoveSkipProperty = (index: number) => {
    const updatedList = dbPropertySkipList.filter((_, i) => i !== index);
    updateConfig({
      filter: {
        ...filter,
        dbPropertySkipList: updatedList
      }
    });
  };

  // Warehouse Plans handlers
  const handleAddWarehousePlan = () => {
    if (newWarehousePlan.database && newWarehousePlan.externalDirectory && newWarehousePlan.managedDirectory) {
      const updatedPlans = {
        ...warehousePlans,
        [newWarehousePlan.database]: {
          externalDirectory: newWarehousePlan.externalDirectory,
          managedDirectory: newWarehousePlan.managedDirectory
        }
      };
      
      updateConfig({
        translator: {
          ...state.config?.translator,
          warehouseMapBuilder: {
            ...state.config?.translator?.warehouseMapBuilder,
            warehousePlans: updatedPlans
          }
        }
      });
      
      setNewWarehousePlan({
        database: '',
        externalDirectory: '',
        managedDirectory: ''
      });
    }
  };

  const handleRemoveWarehousePlan = (database: string) => {
    const updatedPlans = { ...warehousePlans };
    delete updatedPlans[database];
    
    updateConfig({
      translator: {
        ...state.config?.translator,
        warehouseMapBuilder: {
          ...state.config?.translator?.warehouseMapBuilder,
          warehousePlans: updatedPlans
        }
      }
    });
  };

  // Filter handlers
  const handleFilterChange = (field: string, value: string | number | undefined) => {
    updateConfig({
      filter: {
        ...filter,
        [field]: value || undefined
      }
    });
  };

  // Hybrid config handlers
  const handleHybridChange = (field: string, value: number | undefined) => {
    updateConfig({
      hybrid: {
        ...hybrid,
        [field]: value || undefined
      }
    });
  };

  // MigrateACID config handlers
  const handleMigrateACIDChange = (field: string, value: number | undefined) => {
    updateConfig({
      migrateACID: {
        ...migrateACID,
        [field]: value || undefined
      }
    });
  };

  // Redirect handlers
  const handleRedirectChange = (field: 'dbPrefix' | 'dbRename', value: string) => {
    updateConfig({
      [field]: value || undefined
    });
  };

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
          <h1 className="text-3xl font-bold text-gray-900">Database Filters & Configuration</h1>
          <p className="text-gray-600 mt-2">Configure database filtering, warehouse plans, and migration limits</p>
        </div>

        {/* Tab Navigation */}
        <div className="border-b border-gray-200 mb-6">
          <nav className="-mb-px flex space-x-8">
            <button
              onClick={() => setActiveTab('skipProperties')}
              className={`py-2 px-1 border-b-2 font-medium text-sm ${
                activeTab === 'skipProperties'
                  ? 'border-blue-500 text-blue-600'
                  : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
              }`}
            >
              <DocumentTextIcon className="h-5 w-5 inline mr-2" />
              Skip Database Properties
            </button>
            <button
              onClick={() => setActiveTab('warehousePlans')}
              className={`py-2 px-1 border-b-2 font-medium text-sm ${
                activeTab === 'warehousePlans'
                  ? 'border-blue-500 text-blue-600'
                  : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
              }`}
            >
              <FolderIcon className="h-5 w-5 inline mr-2" />
              Warehouse Plans
            </button>
            <button
              onClick={() => setActiveTab('filters')}
              className={`py-2 px-1 border-b-2 font-medium text-sm ${
                activeTab === 'filters'
                  ? 'border-blue-500 text-blue-600'
                  : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
              }`}
            >
              <FunnelIcon className="h-5 w-5 inline mr-2" />
              DB/Table Filters
            </button>
            <button
              onClick={() => setActiveTab('limits')}
              className={`py-2 px-1 border-b-2 font-medium text-sm ${
                activeTab === 'limits'
                  ? 'border-blue-500 text-blue-600'
                  : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
              }`}
            >
              <ExclamationTriangleIcon className="h-5 w-5 inline mr-2" />
              Limits
            </button>
            <button
              onClick={() => setActiveTab('redirects')}
              className={`py-2 px-1 border-b-2 font-medium text-sm ${
                activeTab === 'redirects'
                  ? 'border-blue-500 text-blue-600'
                  : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
              }`}
            >
              <ArrowsRightLeftIcon className="h-5 w-5 inline mr-2" />
              Redirects
            </button>
          </nav>
        </div>

        {/* Tab Content */}
        <div className="space-y-6">
          {/* Skip Database Properties Tab */}
          {activeTab === 'skipProperties' && (
            <div className="bg-white rounded-lg shadow-lg p-6">
              <h3 className="text-lg font-medium text-gray-900 mb-4">Skip Database Properties</h3>
              <p className="text-gray-500 text-sm mb-4">
                Properties listed here will be skipped during database migration
              </p>
              
              {/* List of skip properties */}
              {dbPropertySkipList.length > 0 && (
                <div className="mb-4 space-y-2">
                  {dbPropertySkipList.map((property, index) => (
                    <div key={index} className="flex items-center justify-between bg-gray-50 p-3 rounded">
                      <span className="text-gray-700">{property}</span>
                      <button
                        onClick={() => handleRemoveSkipProperty(index)}
                        className="text-red-600 hover:text-red-700"
                      >
                        <TrashIcon className="h-5 w-5" />
                      </button>
                    </div>
                  ))}
                </div>
              )}

              {/* Add new property */}
              <div className="flex items-center space-x-3">
                <input
                  type="text"
                  value={newSkipProperty}
                  onChange={(e) => setNewSkipProperty(e.target.value)}
                  onKeyPress={(e) => e.key === 'Enter' && handleAddSkipProperty()}
                  placeholder="Enter property name to skip"
                  className="flex-1 p-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                />
                <button
                  onClick={handleAddSkipProperty}
                  className="px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 focus:ring-2 focus:ring-blue-500 focus:ring-offset-2"
                >
                  <PlusIcon className="h-5 w-5" />
                </button>
              </div>
            </div>
          )}

          {/* Warehouse Plans Tab */}
          {activeTab === 'warehousePlans' && (
            <div className="bg-white rounded-lg shadow-lg p-6">
              <h3 className="text-lg font-medium text-gray-900 mb-4">Warehouse Plans</h3>
              <p className="text-gray-500 text-sm mb-4">
                Define warehouse directory mappings for databases
              </p>

              {/* List of warehouse plans */}
              {warehousePlans && typeof warehousePlans === 'object' && Object.keys(warehousePlans).length > 0 && (
                <div className="mb-6">
                  <table className="min-w-full divide-y divide-gray-200">
                    <thead className="bg-gray-50">
                      <tr>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          Database
                        </th>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          External Directory
                        </th>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          Managed Directory
                        </th>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          Actions
                        </th>
                      </tr>
                    </thead>
                    <tbody className="bg-white divide-y divide-gray-200">
                      {Object.entries(warehousePlans).map(([database, plan]) => {
                        if (!plan || typeof plan !== 'object') return null;
                        return (
                          <tr key={database}>
                            <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                              {database}
                            </td>
                            <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                              {plan.externalDirectory || ''}
                            </td>
                            <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                              {plan.managedDirectory || ''}
                            </td>
                            <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                              <button
                                onClick={() => handleRemoveWarehousePlan(database)}
                                className="text-red-600 hover:text-red-700"
                              >
                                <TrashIcon className="h-5 w-5" />
                              </button>
                            </td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
              )}

              {/* Add new warehouse plan */}
              <div className="space-y-4 border-t pt-4">
                <h4 className="font-medium text-gray-900">Add Warehouse Plan</h4>
                <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                  <input
                    type="text"
                    value={newWarehousePlan.database}
                    onChange={(e) => setNewWarehousePlan({...newWarehousePlan, database: e.target.value})}
                    placeholder="Database name"
                    className="p-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                  />
                  <input
                    type="text"
                    value={newWarehousePlan.externalDirectory}
                    onChange={(e) => setNewWarehousePlan({...newWarehousePlan, externalDirectory: e.target.value})}
                    placeholder="External directory path"
                    className="p-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                  />
                  <input
                    type="text"
                    value={newWarehousePlan.managedDirectory}
                    onChange={(e) => setNewWarehousePlan({...newWarehousePlan, managedDirectory: e.target.value})}
                    placeholder="Managed directory path"
                    className="p-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                  />
                </div>
                <button
                  onClick={handleAddWarehousePlan}
                  className="px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 focus:ring-2 focus:ring-blue-500 focus:ring-offset-2"
                >
                  Add Warehouse Plan
                </button>
              </div>
            </div>
          )}

          {/* DB/Table Filters Tab */}
          {activeTab === 'filters' && (
            <div className="bg-white rounded-lg shadow-lg p-6">
              <h3 className="text-lg font-medium text-gray-900 mb-4">Database and Table Filters</h3>
              <p className="text-gray-500 text-sm mb-4">
                Use regular expressions to filter databases and tables
              </p>

              <div className="space-y-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    Database RegEx Filter
                  </label>
                  <input
                    type="text"
                    value={filter.dbRegEx || ''}
                    onChange={(e) => handleFilterChange('dbRegEx', e.target.value)}
                    placeholder="e.g., ^test_.*"
                    className="w-full p-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                  />
                  <p className="text-xs text-gray-500 mt-1">Regular expression to match database names</p>
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    Table RegEx Filter
                  </label>
                  <input
                    type="text"
                    value={filter.tblRegEx || ''}
                    onChange={(e) => handleFilterChange('tblRegEx', e.target.value)}
                    placeholder="e.g., ^tbl_.*"
                    className="w-full p-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                  />
                  <p className="text-xs text-gray-500 mt-1">Regular expression to match table names</p>
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    Table Exclude RegEx Filter
                  </label>
                  <input
                    type="text"
                    value={filter.tblExcludeRegEx || ''}
                    onChange={(e) => handleFilterChange('tblExcludeRegEx', e.target.value)}
                    placeholder="e.g., .*_temp$"
                    className="w-full p-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                  />
                  <p className="text-xs text-gray-500 mt-1">Regular expression to exclude table names</p>
                </div>
              </div>
            </div>
          )}

          {/* Limits Tab */}
          {activeTab === 'limits' && (
            <div className="space-y-6">
              {/* Movement Limits */}
              <div className="bg-white rounded-lg shadow-lg p-6">
                <h3 className="text-lg font-medium text-gray-900 mb-4">Movement Limits</h3>
                <p className="text-gray-500 text-sm mb-4">
                  Set limits for data movement operations
                </p>

                <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">
                      Table Size Limit (bytes)
                    </label>
                    <input
                      type="number"
                      value={filter.tblSizeLimit || ''}
                      onChange={(e) => handleFilterChange('tblSizeLimit', e.target.value ? parseInt(e.target.value) : undefined)}
                      placeholder="e.g., 1073741824"
                      className="w-full p-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                    />
                    <p className="text-xs text-gray-500 mt-1">Maximum table size for migration</p>
                  </div>

                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">
                      Table Partition Limit
                    </label>
                    <input
                      type="number"
                      value={filter.tblPartitionLimit || ''}
                      onChange={(e) => handleFilterChange('tblPartitionLimit', e.target.value ? parseInt(e.target.value) : undefined)}
                      placeholder="e.g., 1000"
                      className="w-full p-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                    />
                    <p className="text-xs text-gray-500 mt-1">Maximum number of partitions per table</p>
                  </div>
                </div>
              </div>

              {/* Table-specific Limits */}
              <div className="bg-white rounded-lg shadow-lg p-6">
                <h3 className="text-lg font-medium text-gray-900 mb-4">Table-specific Limits</h3>
                <p className="text-gray-500 text-sm mb-4">
                  Configure limits for specific migration strategies
                </p>

                <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">
                      ACID Partition Limit
                    </label>
                    <input
                      type="number"
                      value={migrateACID.partitionLimit || ''}
                      onChange={(e) => handleMigrateACIDChange('partitionLimit', e.target.value ? parseInt(e.target.value) : undefined)}
                      placeholder="e.g., 500"
                      className="w-full p-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                    />
                    <p className="text-xs text-gray-500 mt-1">Maximum partitions for ACID table migration</p>
                  </div>

                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">
                      SQL Partition Limit
                    </label>
                    <input
                      type="number"
                      value={hybrid?.sqlPartitionLimit || ''}
                      onChange={(e) => handleHybridChange('sqlPartitionLimit', e.target.value ? parseInt(e.target.value) : undefined)}
                      placeholder="e.g., 3000"
                      className="w-full p-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                    />
                    <p className="text-xs text-gray-500 mt-1">Maximum partitions for SQL strategy</p>
                  </div>

                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">
                      Export/Import Partition Limit
                    </label>
                    <input
                      type="number"
                      value={hybrid?.exportImportPartitionLimit || ''}
                      onChange={(e) => handleHybridChange('exportImportPartitionLimit', e.target.value ? parseInt(e.target.value) : undefined)}
                      placeholder="e.g., 100"
                      className="w-full p-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                    />
                    <p className="text-xs text-gray-500 mt-1">Maximum partitions for Export/Import strategy</p>
                  </div>

                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">
                      SQL Size Limit (bytes)
                    </label>
                    <input
                      type="number"
                      value={hybrid?.sqlSizeLimit || ''}
                      onChange={(e) => handleHybridChange('sqlSizeLimit', e.target.value ? parseInt(e.target.value) : undefined)}
                      placeholder="e.g., 1073741824"
                      className="w-full p-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                    />
                    <p className="text-xs text-gray-500 mt-1">Maximum size for SQL strategy</p>
                  </div>
                </div>
              </div>
            </div>
          )}

          {/* Redirects Tab */}
          {activeTab === 'redirects' && (
            <div className="bg-white rounded-lg shadow-lg p-6">
              <h3 className="text-lg font-medium text-gray-900 mb-4">Database Redirects</h3>
              <p className="text-gray-500 text-sm mb-4">
                Configure database name transformations
              </p>

              <div className="space-y-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    Database Prefix
                  </label>
                  <input
                    type="text"
                    value={state.config?.dbPrefix || ''}
                    onChange={(e) => handleRedirectChange('dbPrefix', e.target.value)}
                    placeholder="e.g., prod_"
                    className="w-full p-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                  />
                  <p className="text-xs text-gray-500 mt-1">
                    Prefix to add to all database names in the target environment
                  </p>
                </div>

                {dataStrategy !== 'STORAGE_MIGRATION' && (
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">
                      Database Rename
                    </label>
                    <input
                      type="text"
                      value={state.config?.dbRename || ''}
                      onChange={(e) => handleRedirectChange('dbRename', e.target.value)}
                      placeholder="e.g., new_database_name"
                      className="w-full p-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                    />
                    <p className="text-xs text-gray-500 mt-1">
                      Rename the database to this name in the target environment
                    </p>
                  </div>
                )}

                <div className="bg-yellow-50 border border-yellow-200 rounded-md p-4 mt-4">
                  <div className="flex">
                    <InformationCircleIcon className="h-5 w-5 text-yellow-400 flex-shrink-0" />
                    <div className="ml-3">
                      <p className="text-sm text-yellow-700">
                        <strong>Note:</strong> Database prefix and rename settings will apply to all databases in the migration.
                        {dataStrategy === 'STORAGE_MIGRATION' && ' Database rename is not available for Storage Migration strategy.'}
                      </p>
                    </div>
                  </div>
                </div>
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

export default DatabaseFiltersSection;