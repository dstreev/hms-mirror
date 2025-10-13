import React, { useState } from 'react';
import { PlusIcon, TrashIcon, AdjustmentsHorizontalIcon } from '@heroicons/react/24/outline';
import { DatasetWizardStepProps, createDefaultDatabaseSpec, DEFAULT_TABLE_FILTER, DatabaseSpec } from '../../../types/Dataset';

const DatabaseConfigStep: React.FC<DatasetWizardStepProps> = ({ formData, errors, updateFormData }) => {
  const [expandedDatabase, setExpandedDatabase] = useState<number | null>(null);

  const addDatabase = () => {
    const newDatabases = [...formData.databases, createDefaultDatabaseSpec()];
    updateFormData({ databases: newDatabases });
  };

  const removeDatabase = (index: number) => {
    const newDatabases = formData.databases.filter((_, i) => i !== index);
    updateFormData({ databases: newDatabases });
    if (expandedDatabase === index) {
      setExpandedDatabase(null);
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
      // Turning off filter - keep existing tables
      newDatabases[dbIndex].filter = undefined;
    } else {
      // Turning on filter - clear any existing tables (mutually exclusive)
      newDatabases[dbIndex].filter = { ...DEFAULT_TABLE_FILTER };
      newDatabases[dbIndex].tables = [];
    }
    updateFormData({ databases: newDatabases });
  };

  const addTable = (dbIndex: number) => {
    const newDatabases = [...formData.databases];
    // If filter is enabled, clear it when adding tables (mutually exclusive)
    if (newDatabases[dbIndex].filter) {
      newDatabases[dbIndex].filter = undefined;
    }
    newDatabases[dbIndex].tables.push('');
    updateFormData({ databases: newDatabases });
  };

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex justify-between items-center">
        <div>
          <h3 className="text-lg font-medium text-gray-900">Database Configuration</h3>
          <p className="text-sm text-gray-500 mt-1">
            Configure the databases and tables to include in your dataset.
          </p>
        </div>
        <button
          type="button"
          onClick={addDatabase}
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

      {/* Database List */}
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
              onClick={addDatabase}
              className="inline-flex items-center px-4 py-2 border border-transparent shadow-sm text-sm font-medium rounded-md text-white bg-blue-600 hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500"
            >
              <PlusIcon className="h-4 w-4 mr-2" />
              Add Database
            </button>
          </div>
        </div>
      ) : (
        <div className="space-y-4">
          {formData.databases.map((database, dbIndex) => (
            <div key={dbIndex} className="bg-white border border-gray-200 rounded-lg p-4">
              {/* Database Header */}
              <div className="flex items-center justify-between mb-4">
                <div className="flex-1">
                  <label htmlFor={`database-${dbIndex}`} className="block text-sm font-medium text-gray-700 mb-2">
                    Database Name *
                  </label>
                  <input
                    type="text"
                    id={`database-${dbIndex}`}
                    value={database.databaseName}
                    onChange={(e) => updateDatabase(dbIndex, { databaseName: e.target.value })}
                    className={`block w-full px-3 py-2 border rounded-md shadow-sm placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 ${
                      errors[`database_${dbIndex}_name`] ? 'border-red-300' : 'border-gray-300'
                    }`}
                    placeholder="Enter database name"
                  />
                  {errors[`database_${dbIndex}_name`] && (
                    <p className="mt-1 text-sm text-red-600">{errors[`database_${dbIndex}_name`]}</p>
                  )}
                </div>
                <div className="ml-4 flex space-x-2">
                  <button
                    type="button"
                    onClick={() => setExpandedDatabase(expandedDatabase === dbIndex ? null : dbIndex)}
                    className="p-2 text-gray-400 hover:text-gray-600 focus:outline-none focus:ring-2 focus:ring-blue-500 rounded-md"
                    title="Expand configuration"
                  >
                    <AdjustmentsHorizontalIcon className="h-5 w-5" />
                  </button>
                  <button
                    type="button"
                    onClick={() => removeDatabase(dbIndex)}
                    className="p-2 text-red-400 hover:text-red-600 focus:outline-none focus:ring-2 focus:ring-red-500 rounded-md"
                    title="Remove database"
                  >
                    <TrashIcon className="h-5 w-5" />
                  </button>
                </div>
              </div>

              {/* Configuration Error */}
              {errors[`database_${dbIndex}_config`] && (
                <div className="mb-4 p-3 bg-red-50 border border-red-200 rounded-md">
                  <p className="text-sm text-red-600">{errors[`database_${dbIndex}_config`]}</p>
                </div>
              )}

              {/* Expanded Configuration */}
              {expandedDatabase === dbIndex && (
                <div className="space-y-4 border-t border-gray-200 pt-4">
                  {/* Tables Section */}
                  <div>
                    <div className="flex justify-between items-center mb-2">
                      <div>
                        <label className="block text-sm font-medium text-gray-700">
                          Specific Tables
                        </label>
                        {database.filter && (
                          <p className="text-xs text-amber-600 mt-1">
                            ⚠️ Table filter is enabled - specific tables list is disabled
                          </p>
                        )}
                      </div>
                      <button
                        type="button"
                        onClick={() => addTable(dbIndex)}
                        disabled={database.filter !== undefined}
                        className={`text-sm font-medium ${
                          database.filter 
                            ? 'text-gray-400 cursor-not-allowed' 
                            : 'text-blue-600 hover:text-blue-800'
                        }`}
                      >
                        Add Table
                      </button>
                    </div>
                    {database.tables.map((table, tableIndex) => (
                      <div key={tableIndex} className="flex items-center space-x-2 mb-2">
                        <input
                          type="text"
                          value={table}
                          onChange={(e) => updateTable(dbIndex, tableIndex, e.target.value)}
                          className="flex-1 px-3 py-2 border border-gray-300 rounded-md shadow-sm placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500"
                          placeholder="Table name"
                        />
                        <button
                          type="button"
                          onClick={() => removeTable(dbIndex, tableIndex)}
                          className="p-2 text-red-400 hover:text-red-600 focus:outline-none focus:ring-2 focus:ring-red-500 rounded-md"
                        >
                          <TrashIcon className="h-4 w-4" />
                        </button>
                      </div>
                    ))}
                  </div>

                  {/* Filter Toggle */}
                  <div className="border-t border-gray-200 pt-4">
                    <div className="flex items-center justify-between">
                      <div>
                        <h4 className="text-sm font-medium text-gray-700">Table Filter</h4>
                        <p className="text-sm text-gray-500">
                          Use filters instead of specific table names
                          {database.tables.length > 0 && (
                            <span className="text-amber-600 block text-xs mt-1">
                              ⚠️ {database.tables.length} specific table(s) defined - enabling filter will clear them
                            </span>
                          )}
                        </p>
                      </div>
                      <button
                        type="button"
                        onClick={() => toggleFilter(dbIndex)}
                        className={`relative inline-flex h-6 w-11 flex-shrink-0 cursor-pointer rounded-full border-2 border-transparent transition-colors duration-200 ease-in-out focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-offset-2 ${
                          database.filter ? 'bg-blue-600' : 'bg-gray-200'
                        }`}
                      >
                        <span
                          className={`pointer-events-none inline-block h-5 w-5 transform rounded-full bg-white shadow ring-0 transition duration-200 ease-in-out ${
                            database.filter ? 'translate-x-5' : 'translate-x-0'
                          }`}
                        />
                      </button>
                    </div>

                    {/* Filter Configuration */}
                    {database.filter && (
                      <div className="mt-4 space-y-4 bg-gray-50 p-4 rounded-md">
                        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                          <div>
                            <label className="block text-sm font-medium text-gray-700 mb-1">
                              Include Pattern
                            </label>
                            <input
                              type="text"
                              value={database.filter.tblRegEx || ''}
                              onChange={(e) => updateDatabase(dbIndex, {
                                filter: { ...database.filter, tblRegEx: e.target.value }
                              })}
                              className="block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500"
                              placeholder="e.g., .*_prod.*"
                            />
                          </div>
                          <div>
                            <label className="block text-sm font-medium text-gray-700 mb-1">
                              Exclude Pattern
                            </label>
                            <input
                              type="text"
                              value={database.filter.tblExcludeRegEx || ''}
                              onChange={(e) => updateDatabase(dbIndex, {
                                filter: { ...database.filter, tblExcludeRegEx: e.target.value }
                              })}
                              className="block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500"
                              placeholder="e.g., .*_temp.*"
                            />
                          </div>
                          <div>
                            <label className="block text-sm font-medium text-gray-700 mb-1">
                              Size Limit (bytes)
                            </label>
                            <input
                              type="number"
                              value={database.filter.tblSizeLimit || ''}
                              onChange={(e) => updateDatabase(dbIndex, {
                                filter: { ...database.filter, tblSizeLimit: parseInt(e.target.value) || 0 }
                              })}
                              className="block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500"
                              placeholder="0 = no limit"
                              min="0"
                            />
                          </div>
                          <div>
                            <label className="block text-sm font-medium text-gray-700 mb-1">
                              Partition Limit
                            </label>
                            <input
                              type="number"
                              value={database.filter.tblPartitionLimit || ''}
                              onChange={(e) => updateDatabase(dbIndex, {
                                filter: { ...database.filter, tblPartitionLimit: parseInt(e.target.value) || 0 }
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

                  {/* Warehouse Configuration */}
                  <div className="border-t border-gray-200 pt-4">
                    <h4 className="text-sm font-medium text-gray-700 mb-3">Warehouse Configuration</h4>
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                      <div>
                        <label className="block text-sm font-medium text-gray-700 mb-1">
                          Managed Directory
                        </label>
                        <input
                          type="text"
                          value={database.warehouse.managedDirectory || ''}
                          onChange={(e) => updateDatabase(dbIndex, {
                            warehouse: { ...database.warehouse, managedDirectory: e.target.value }
                          })}
                          className="block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500"
                          placeholder="/warehouse/managed"
                        />
                      </div>
                      <div>
                        <label className="block text-sm font-medium text-gray-700 mb-1">
                          External Directory
                        </label>
                        <input
                          type="text"
                          value={database.warehouse.externalDirectory || ''}
                          onChange={(e) => updateDatabase(dbIndex, {
                            warehouse: { ...database.warehouse, externalDirectory: e.target.value }
                          })}
                          className="block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500"
                          placeholder="/warehouse/external"
                        />
                      </div>
                    </div>
                  </div>
                </div>
              )}
            </div>
          ))}
        </div>
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
              Table Selection Rules
            </h3>
            <div className="mt-2 text-sm text-blue-700">
              <p><strong>Important:</strong> For each database, table specification methods are mutually exclusive:</p>
              <ul className="list-disc list-inside mt-1 space-y-1">
                <li><strong>Specific tables:</strong> List exact table names to include</li>
                <li><strong>Table filters:</strong> Define patterns and limits to automatically select tables</li>
              </ul>
              <div className="mt-3 p-2 bg-amber-50 border border-amber-200 rounded text-amber-800">
                <p className="font-medium text-xs">⚠️ Mutual Exclusivity:</p>
                <ul className="text-xs mt-1 space-y-1">
                  <li>• Enabling table filter will clear any specific tables you've defined</li>
                  <li>• Adding specific tables will disable and clear any active table filter</li>
                  <li>• If no tables are specified and no filter is used, all available tables will be processed</li>
                </ul>
              </div>
              <p className="mt-2"><strong>Warehouse paths:</strong> Configure managed and external directory locations for each database.</p>
              <p className="mt-1">Click the settings icon next to each database to configure these options.</p>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default DatabaseConfigStep;