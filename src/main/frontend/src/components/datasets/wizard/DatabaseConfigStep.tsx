import React, { useState, useEffect } from 'react';
import { PlusIcon, TrashIcon, AdjustmentsHorizontalIcon, XMarkIcon } from '@heroicons/react/24/outline';
import { DatasetWizardStepProps, createDefaultDatabaseSpec, DEFAULT_TABLE_FILTER, DatabaseSpec } from '../../../types/Dataset';
import ConnectionSelector from './ConnectionSelector';
import DatabaseSelector from './DatabaseSelector';
import TableMultiSelector from './TableMultiSelector';
import AddDatabaseDialog from './AddDatabaseDialog';
import FieldWithTooltip from '../../common/FieldWithTooltip';
import schemaService from '../../../services/schemaService';

const DatabaseConfigStep: React.FC<DatasetWizardStepProps> = ({ formData, errors, updateFormData }) => {
  const [activeTabIndex, setActiveTabIndex] = useState<number>(0);
  const [showAddDialog, setShowAddDialog] = useState(false);
  const [expandedSections, setExpandedSections] = useState({
    tables: true,
    filter: false,
    warehouse: false,
    locationMappings: false,
    rename: false
  });
  const [schemaDescriptions, setSchemaDescriptions] = useState<Map<string, string>>(new Map());

  // Fetch schema descriptions on mount
  useEffect(() => {
    const fetchDescriptions = async () => {
      const descriptions = await schemaService.getClassDescriptions('DatasetDto');
      setSchemaDescriptions(descriptions);
    };
    fetchDescriptions();
  }, []);

  const addDatabase = (databaseName: string, sourceConnectionKey?: string) => {
    const newDatabase = createDefaultDatabaseSpec();
    newDatabase.databaseName = databaseName;

    if (sourceConnectionKey) {
      newDatabase.entryMode = 'connection';
      newDatabase.sourceConnectionKey = sourceConnectionKey;
    } else {
      newDatabase.entryMode = 'manual';
    }

    const newDatabases = [...formData.databases, newDatabase];
    updateFormData({ databases: newDatabases });
    setActiveTabIndex(newDatabases.length - 1);
  };

  const removeDatabase = (index: number) => {
    const newDatabases = formData.databases.filter((_, i) => i !== index);
    updateFormData({ databases: newDatabases });

    // Adjust active tab if needed
    if (activeTabIndex >= newDatabases.length) {
      setActiveTabIndex(Math.max(0, newDatabases.length - 1));
    }
  };

  const updateDatabase = (index: number, updates: Partial<DatabaseSpec>) => {
    const newDatabases = [...formData.databases];
    newDatabases[index] = { ...newDatabases[index], ...updates };
    updateFormData({ databases: newDatabases });
  };

  const updateTable = (dbIndex: number, tableIndex: number, value: string) => {
    const newDatabases = [...formData.databases];
    newDatabases[dbIndex].tables[tableIndex] = value;
    updateFormData({ databases: newDatabases });
  };

  const removeTable = (dbIndex: number, tableIndex: number) => {
    const newDatabases = [...formData.databases];
    newDatabases[dbIndex].tables.splice(tableIndex, 1);
    updateFormData({ databases: newDatabases });
  };

  const toggleFilter = (dbIndex: number) => {
    const newDatabases = [...formData.databases];
    if (newDatabases[dbIndex].filter) {
      newDatabases[dbIndex].filter = undefined;
    } else {
      newDatabases[dbIndex].filter = { ...DEFAULT_TABLE_FILTER };
      newDatabases[dbIndex].tables = [];
    }
    updateFormData({ databases: newDatabases });
  };

  const addTable = (dbIndex: number) => {
    const newDatabases = [...formData.databases];
    if (newDatabases[dbIndex].filter) {
      newDatabases[dbIndex].filter = undefined;
    }
    newDatabases[dbIndex].tables.push('');
    updateFormData({ databases: newDatabases });
  };

  const toggleEntryMode = (dbIndex: number) => {
    const newDatabases = [...formData.databases];
    const currentMode = newDatabases[dbIndex].entryMode || 'manual';
    const newMode = currentMode === 'manual' ? 'connection' : 'manual';

    newDatabases[dbIndex].entryMode = newMode;

    if (newMode === 'connection') {
      newDatabases[dbIndex].sourceConnectionKey = undefined;
      newDatabases[dbIndex].databaseName = '';
      newDatabases[dbIndex].tables = [];
      newDatabases[dbIndex].filter = undefined;
    } else {
      newDatabases[dbIndex].sourceConnectionKey = undefined;
    }

    updateFormData({ databases: newDatabases });
  };

  const toggleSection = (section: keyof typeof expandedSections) => {
    setExpandedSections(prev => ({
      ...prev,
      [section]: !prev[section]
    }));
  };

  const currentDatabase = formData.databases[activeTabIndex];

  return (
    <div className="space-y-4">
      {/* Header with Add Database Button */}
      <div className="flex justify-between items-center">
        <div>
          <h3 className="text-lg font-medium text-gray-900">Database Configuration</h3>
          <p className="text-sm text-gray-500 mt-1">
            Configure the databases and tables to include in your dataset.
          </p>
        </div>
        <button
          type="button"
          onClick={() => setShowAddDialog(true)}
          className="inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md shadow-sm text-white bg-blue-600 hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500"
        >
          <PlusIcon className="h-4 w-4 mr-2" />
          Add Database
        </button>
      </div>

      {/* Error for no databases */}
      {errors.databases && (
        <div className="p-3 bg-red-50 border border-red-200 rounded-md">
          <p className="text-sm text-red-600">{errors.databases}</p>
        </div>
      )}

      {/* Empty State or Tabs */}
      {formData.databases.length === 0 ? (
        <div className="text-center py-12 bg-gray-50 rounded-lg border-2 border-dashed border-gray-300">
          <svg className="mx-auto h-12 w-12 text-gray-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 7v10c0 2.21 1.79 4 4 4h8c2.21 0 4-1.79 4-4V7c0-2.21-1.79-4-4-4H8c-2.21 0-4 1.79-4 4z" />
          </svg>
          <h3 className="mt-2 text-sm font-medium text-gray-900">No databases configured</h3>
          <p className="mt-1 text-sm text-gray-500">
            Get started by adding your first database to the dataset.
          </p>
          <div className="mt-6">
            <button
              type="button"
              onClick={() => setShowAddDialog(true)}
              className="inline-flex items-center px-4 py-2 border border-transparent shadow-sm text-sm font-medium rounded-md text-white bg-blue-600 hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500"
            >
              <PlusIcon className="h-4 w-4 mr-2" />
              Add Database
            </button>
          </div>
        </div>
      ) : (
        <>
          {/* Tabs Header */}
          <div className="border-b border-gray-200">
            <div className="flex space-x-2 overflow-x-auto">
              {formData.databases.map((database, index) => (
                <div
                  key={index}
                  className={`flex items-center space-x-2 px-4 py-2 border-b-2 cursor-pointer transition-colors ${
                    activeTabIndex === index
                      ? 'border-blue-600 text-blue-600'
                      : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
                  }`}
                >
                  <button
                    type="button"
                    onClick={() => setActiveTabIndex(index)}
                    className="flex items-center space-x-2 flex-1 min-w-0"
                  >
                    <span className="font-medium truncate">
                      {database.databaseName || `Database ${index + 1}`}
                    </span>
                    {database.tables.length > 0 && (
                      <span className="text-xs bg-gray-100 px-2 py-0.5 rounded-full">
                        {database.tables.length} tables
                      </span>
                    )}
                  </button>
                  <button
                    type="button"
                    onClick={(e) => {
                      e.stopPropagation();
                      removeDatabase(index);
                    }}
                    className="p-1 text-gray-400 hover:text-red-600 focus:outline-none focus:ring-2 focus:ring-red-500 rounded"
                    title="Remove database"
                  >
                    <XMarkIcon className="h-4 w-4" />
                  </button>
                </div>
              ))}
            </div>
          </div>

          {/* Tab Content */}
          {currentDatabase && (
            <div className="bg-white border border-gray-200 rounded-lg p-6">
              {/* Configuration Error */}
              {errors[`database_${activeTabIndex}_config`] && (
                <div className="mb-4 p-3 bg-red-50 border border-red-200 rounded-md">
                  <p className="text-sm text-red-600">{errors[`database_${activeTabIndex}_config`]}</p>
                </div>
              )}

              {/* Entry Mode Toggle */}
              <div className="mb-6 p-3 bg-blue-50 border border-blue-200 rounded-md">
                <div className="flex items-center justify-between">
                  <div>
                    <label className="text-sm font-medium text-gray-900">Table Entry Mode</label>
                    <p className="text-xs text-gray-600 mt-0.5">
                      Choose how to specify tables for this database
                    </p>
                  </div>
                  <div className="flex space-x-2">
                    <button
                      type="button"
                      onClick={() => toggleEntryMode(activeTabIndex)}
                      className={`px-3 py-1.5 text-xs font-medium rounded-md ${
                        (currentDatabase.entryMode || 'manual') === 'manual'
                          ? 'bg-blue-600 text-white'
                          : 'bg-white text-gray-700 border border-gray-300'
                      }`}
                    >
                      Manual Entry
                    </button>
                    <button
                      type="button"
                      onClick={() => toggleEntryMode(activeTabIndex)}
                      className={`px-3 py-1.5 text-xs font-medium rounded-md ${
                        currentDatabase.entryMode === 'connection'
                          ? 'bg-blue-600 text-white'
                          : 'bg-white text-gray-700 border border-gray-300'
                      }`}
                    >
                      From Connection
                    </button>
                  </div>
                </div>
              </div>

              {/* Connection-Based Table Discovery */}
              {currentDatabase.entryMode === 'connection' && (
                <div className="space-y-4 mb-6 p-4 bg-gray-50 border border-gray-200 rounded-md">
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-2">
                      1. Select Connection
                    </label>
                    <ConnectionSelector
                      value={currentDatabase.sourceConnectionKey}
                      onChange={(connectionKey) => {
                        updateDatabase(activeTabIndex, {
                          sourceConnectionKey: connectionKey,
                          tables: []
                        });
                      }}
                    />
                  </div>

                  {currentDatabase.sourceConnectionKey && currentDatabase.databaseName && (
                    <div>
                      <label className="block text-sm font-medium text-gray-700 mb-2">
                        2. Select Tables
                      </label>
                      <TableMultiSelector
                        connectionKey={currentDatabase.sourceConnectionKey}
                        databaseName={currentDatabase.databaseName}
                        selectedTables={currentDatabase.tables}
                        onChange={(tables) => {
                          updateDatabase(activeTabIndex, { tables });
                        }}
                      />
                    </div>
                  )}
                </div>
              )}

              {/* Manual Entry: Tables Section */}
              {(currentDatabase.entryMode || 'manual') === 'manual' && (
                <div className="mb-6">
                  <div className="flex justify-between items-center mb-3">
                    <div>
                      <label className="block text-sm font-medium text-gray-700">
                        Specific Tables
                      </label>
                      {currentDatabase.filter && (
                        <p className="text-xs text-amber-600 mt-1">
                          ⚠️ Table filter is enabled - specific tables list is disabled
                        </p>
                      )}
                    </div>
                    <button
                      type="button"
                      onClick={() => addTable(activeTabIndex)}
                      disabled={currentDatabase.filter !== undefined}
                      className={`inline-flex items-center px-3 py-1.5 text-sm font-medium rounded-md ${
                        currentDatabase.filter
                          ? 'text-gray-400 bg-gray-100 cursor-not-allowed'
                          : 'text-blue-600 bg-blue-50 hover:bg-blue-100'
                      }`}
                    >
                      <PlusIcon className="h-4 w-4 mr-1" />
                      Add Table
                    </button>
                  </div>

                  <div className="space-y-2">
                    {currentDatabase.tables.length === 0 && !currentDatabase.filter ? (
                      <div className="text-center py-6 bg-gray-50 rounded-lg border border-dashed border-gray-300">
                        <p className="text-sm text-gray-500">No tables specified</p>
                        <p className="text-xs text-gray-400 mt-1">Add tables or enable table filter</p>
                      </div>
                    ) : (
                      currentDatabase.tables.map((table, tableIndex) => (
                        <div key={tableIndex} className="flex items-center space-x-2">
                          <input
                            type="text"
                            value={table}
                            onChange={(e) => updateTable(activeTabIndex, tableIndex, e.target.value)}
                            className="flex-1 px-3 py-2 border border-gray-300 rounded-md shadow-sm placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500"
                            placeholder="Table name"
                          />
                          <button
                            type="button"
                            onClick={() => removeTable(activeTabIndex, tableIndex)}
                            className="p-2 text-red-400 hover:text-red-600 focus:outline-none focus:ring-2 focus:ring-red-500 rounded-md"
                          >
                            <TrashIcon className="h-4 w-4" />
                          </button>
                        </div>
                      ))
                    )}
                  </div>
                </div>
              )}

              {/* Filter Toggle */}
              <div className="mb-6 border-t border-gray-200 pt-6">
                <div className="flex items-center justify-between mb-4">
                  <div>
                    <h4 className="text-sm font-medium text-gray-700">Table Filter</h4>
                    <p className="text-sm text-gray-500">
                      Use filters instead of specific table names
                      {currentDatabase.tables.length > 0 && (
                        <span className="text-amber-600 block text-xs mt-1">
                          ⚠️ {currentDatabase.tables.length} specific table(s) defined - enabling filter will clear them
                        </span>
                      )}
                    </p>
                  </div>
                  <button
                    type="button"
                    onClick={() => toggleFilter(activeTabIndex)}
                    className={`relative inline-flex h-6 w-11 flex-shrink-0 cursor-pointer rounded-full border-2 border-transparent transition-colors duration-200 ease-in-out focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-offset-2 ${
                      currentDatabase.filter ? 'bg-blue-600' : 'bg-gray-200'
                    }`}
                  >
                    <span
                      className={`pointer-events-none inline-block h-5 w-5 transform rounded-full bg-white shadow ring-0 transition duration-200 ease-in-out ${
                        currentDatabase.filter ? 'translate-x-5' : 'translate-x-0'
                      }`}
                    />
                  </button>
                </div>

                {/* Filter Configuration */}
                {currentDatabase.filter && (
                  <div className="space-y-4 bg-gray-50 p-4 rounded-md">
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                      <div>
                        <FieldWithTooltip
                          label="Include Pattern"
                          tooltip={schemaDescriptions.get('databases.filter.tblRegEx')}
                          htmlFor="tblRegEx"
                          className="mb-1"
                        />
                        <input
                          type="text"
                          value={currentDatabase.filter.tblRegEx || ''}
                          onChange={(e) => updateDatabase(activeTabIndex, {
                            filter: { ...currentDatabase.filter, tblRegEx: e.target.value }
                          })}
                          className="block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500"
                          placeholder="e.g., .*_prod.*"
                        />
                      </div>
                      <div>
                        <FieldWithTooltip
                          label="Exclude Pattern"
                          tooltip={schemaDescriptions.get('databases.filter.tblExcludeRegEx')}
                          htmlFor="tblExcludeRegEx"
                          className="mb-1"
                        />
                        <input
                          type="text"
                          value={currentDatabase.filter.tblExcludeRegEx || ''}
                          onChange={(e) => updateDatabase(activeTabIndex, {
                            filter: { ...currentDatabase.filter, tblExcludeRegEx: e.target.value }
                          })}
                          className="block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500"
                          placeholder="e.g., .*_temp.*"
                        />
                      </div>
                      <div>
                        <FieldWithTooltip
                          label="Max Size (MB)"
                          tooltip={schemaDescriptions.get('databases.filter.maxSizeMb')}
                          htmlFor="maxSizeMb"
                          className="mb-1"
                        />
                        <input
                          type="number"
                          value={currentDatabase.filter?.maxSizeMb || ''}
                          onChange={(e) => updateDatabase(activeTabIndex, {
                            filter: { ...currentDatabase.filter, maxSizeMb: parseInt(e.target.value) || 0 }
                          })}
                          className="block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500"
                          placeholder="0 = no limit"
                          min="0"
                        />
                      </div>
                      <div>
                        <FieldWithTooltip
                          label="Max Partitions"
                          tooltip={schemaDescriptions.get('databases.filter.maxPartitions')}
                          htmlFor="maxPartitions"
                          className="mb-1"
                        />
                        <input
                          type="number"
                          value={currentDatabase.filter?.maxPartitions || ''}
                          onChange={(e) => updateDatabase(activeTabIndex, {
                            filter: { ...currentDatabase.filter, maxPartitions: parseInt(e.target.value) || 0 }
                          })}
                          className="block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500"
                          placeholder="0 = no limit"
                          min="0"
                        />
                      </div>
                    </div>
                  </div>
                )}
              </div>

              {/* Warehouse Configuration - Collapsible */}
              <div className="mb-6 border-t border-gray-200 pt-6">
                <button
                  type="button"
                  onClick={() => toggleSection('warehouse')}
                  className="flex items-center justify-between w-full text-left mb-4"
                >
                  <div>
                    <h4 className="text-sm font-medium text-gray-700">Warehouse Configuration</h4>
                    <p className="text-xs text-gray-500 mt-0.5">Database-specific warehouse locations</p>
                  </div>
                  <AdjustmentsHorizontalIcon className={`h-5 w-5 text-gray-400 transition-transform ${expandedSections.warehouse ? 'rotate-180' : ''}`} />
                </button>

                {expandedSections.warehouse && (
                  <div className="space-y-4">
                    <div className="bg-blue-50 border border-blue-200 rounded-md p-3">
                      <p className="text-xs text-blue-700">
                        Override system default warehouse locations for this database.
                        Paths must start with <code className="bg-blue-100 px-1 rounded">/</code>.
                      </p>
                    </div>
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                      <div>
                        <FieldWithTooltip
                          label="Managed Directory"
                          tooltip={schemaDescriptions.get('databases.warehouse.managedDirectory')}
                          htmlFor="managedDirectory"
                          className="mb-1"
                        />
                        <input
                          type="text"
                          value={currentDatabase.warehouse.managedDirectory || ''}
                          onChange={(e) => updateDatabase(activeTabIndex, {
                            warehouse: { ...currentDatabase.warehouse, managedDirectory: e.target.value }
                          })}
                          className="block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500"
                          placeholder="/warehouse/managed"
                        />
                      </div>
                      <div>
                        <FieldWithTooltip
                          label="External Directory"
                          tooltip={schemaDescriptions.get('databases.warehouse.externalDirectory')}
                          htmlFor="externalDirectory"
                          className="mb-1"
                        />
                        <input
                          type="text"
                          value={currentDatabase.warehouse.externalDirectory || ''}
                          onChange={(e) => updateDatabase(activeTabIndex, {
                            warehouse: { ...currentDatabase.warehouse, externalDirectory: e.target.value }
                          })}
                          className="block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500"
                          placeholder="/warehouse/external"
                        />
                      </div>
                    </div>
                  </div>
                )}
              </div>

              {/* Database Rename Options - Collapsible */}
              <div className="border-t border-gray-200 pt-6">
                <button
                  type="button"
                  onClick={() => toggleSection('rename')}
                  className="flex items-center justify-between w-full text-left mb-4"
                >
                  <div>
                    <h4 className="text-sm font-medium text-gray-700">
                      Database Naming <span className="text-xs text-gray-500 font-normal">(Testing Only)</span>
                    </h4>
                    <p className="text-xs text-gray-500 mt-0.5">Modify database naming during migration</p>
                  </div>
                  <AdjustmentsHorizontalIcon className={`h-5 w-5 text-gray-400 transition-transform ${expandedSections.rename ? 'rotate-180' : ''}`} />
                </button>

                {expandedSections.rename && (
                  <div className="space-y-4">
                    <div className="bg-yellow-50 border border-yellow-200 rounded-md p-3">
                      <p className="text-xs text-yellow-700">
                        ⚠️ Testing only. Prefix and Rename are mutually exclusive.
                      </p>
                    </div>
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                      <div>
                        <FieldWithTooltip
                          label="Database Prefix"
                          tooltip={schemaDescriptions.get('databases.dbPrefix')}
                          htmlFor="dbPrefix"
                          className="mb-1"
                        />
                        <input
                          type="text"
                          value={currentDatabase.dbPrefix || ''}
                          onChange={(e) => {
                            const value = e.target.value.trim();
                            if (value) {
                              updateDatabase(activeTabIndex, { dbPrefix: value, dbRename: undefined });
                            } else {
                              updateDatabase(activeTabIndex, { dbPrefix: undefined });
                            }
                          }}
                          className="block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500"
                          placeholder="test_"
                        />
                      </div>
                      <div>
                        <FieldWithTooltip
                          label="Database Rename"
                          tooltip={schemaDescriptions.get('databases.dbRename')}
                          htmlFor="dbRename"
                          className="mb-1"
                        />
                        <input
                          type="text"
                          value={currentDatabase.dbRename || ''}
                          onChange={(e) => {
                            const value = e.target.value.trim();
                            if (value) {
                              updateDatabase(activeTabIndex, { dbRename: value, dbPrefix: undefined });
                            } else {
                              updateDatabase(activeTabIndex, { dbRename: undefined });
                            }
                          }}
                          className="block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500"
                          placeholder="new_database_name"
                        />
                      </div>
                    </div>
                  </div>
                )}
              </div>
            </div>
          )}
        </>
      )}

      {/* Info Box */}
      <div className="bg-blue-50 border border-blue-200 rounded-md p-4">
        <div className="flex">
          <div className="flex-shrink-0">
            <svg className="h-5 w-5 text-blue-400" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor">
              <path fillRule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7-4a1 1 0 11-2 0 1 1 0 012 0zM9 9a1 1 0 000 2v3a1 1 0 001 1h1a1 1 0 100-2v-3a1 1 0 00-1-1H9z" clipRule="evenodd" />
            </svg>
          </div>
          <div className="ml-3">
            <h3 className="text-sm font-medium text-blue-800">
              Configuration Tips
            </h3>
            <div className="mt-2 text-sm text-blue-700">
              <ul className="list-disc list-inside space-y-1">
                <li>Each database has its own tab - click tabs to switch between databases</li>
                <li>Tables and filters are mutually exclusive - enabling one clears the other</li>
                <li>Warehouse paths override system defaults for each database</li>
                <li>Use connection mode to automatically discover and select tables</li>
              </ul>
            </div>
          </div>
        </div>
      </div>

      {/* Add Database Dialog */}
      <AddDatabaseDialog
        isOpen={showAddDialog}
        onClose={() => setShowAddDialog(false)}
        onAdd={addDatabase}
      />
    </div>
  );
};

export default DatabaseConfigStep;
