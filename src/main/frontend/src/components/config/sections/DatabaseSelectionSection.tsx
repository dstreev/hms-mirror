import React, { useState, useEffect } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { ArrowLeftIcon, MagnifyingGlassIcon, PlusIcon, XMarkIcon, AdjustmentsHorizontalIcon } from '@heroicons/react/24/outline';
import { useConfiguration } from '../../../contexts/ConfigurationContext';

const DatabaseSelectionSection: React.FC = () => {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const { state, updateConfig } = useConfiguration();
  
  const [searchTerm, setSearchTerm] = useState('');
  const [newDatabase, setNewDatabase] = useState('');
  const [isAddingDatabase, setIsAddingDatabase] = useState(false);

  // Get databases from server state
  const databases = Array.from(state.config?.databases || []);
  const filteredDatabases = databases.filter(db => 
    db.toLowerCase().includes(searchTerm.toLowerCase())
  );

  const handleAddDatabase = () => {
    if (newDatabase.trim() && !databases.includes(newDatabase.trim())) {
      const updatedDatabases = new Set([...databases, newDatabase.trim()]);
      updateConfig({ databases: Array.from(updatedDatabases) });
      setNewDatabase('');
      setIsAddingDatabase(false);
    }
  };

  const handleRemoveDatabase = (dbName: string) => {
    const updatedDatabases = databases.filter(db => db !== dbName);
    updateConfig({ databases: updatedDatabases });
  };

  const handleKeyPress = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter') {
      handleAddDatabase();
    } else if (e.key === 'Escape') {
      setIsAddingDatabase(false);
      setNewDatabase('');
    }
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
          <h1 className="text-3xl font-bold text-gray-900">Database Selection</h1>
          <p className="text-gray-600 mt-2">Select specific databases to include in the migration</p>
        </div>

        {/* Link to Filters and Advanced Configuration */}
        <div className="mb-6">
          <button
            onClick={() => navigate(id === 'current' ? '/config/edit/current/filters' : `/config/edit/${id}/filters`)}
            className="inline-flex items-center px-4 py-2 bg-purple-600 text-white rounded-md hover:bg-purple-700 focus:ring-2 focus:ring-purple-500 focus:ring-offset-2"
          >
            <AdjustmentsHorizontalIcon className="h-5 w-5 mr-2" />
            Configure Filters, Warehouse Plans & Limits
          </button>
        </div>

        {state.error && (
          <div className="mb-6 bg-red-50 border border-red-200 rounded-md p-4">
            <div className="text-sm text-red-700">{state.error}</div>
          </div>
        )}

        <div className="space-y-6">
          {/* Database Filter Info */}
          <div className="bg-yellow-50 border border-yellow-200 rounded-md p-4">
            <div className="text-sm text-yellow-700">
              <strong>Important:</strong> Only databases specified below will be processed during migration. 
              If no databases are listed, NO databases will be migrated.
            </div>
            <div className="text-sm text-yellow-700 mt-2">
              <strong>Note:</strong> Database selections are temporary and will not be persisted to configuration files. 
              They must be re-selected for each migration session.
            </div>
          </div>

          {/* Search and Add */}
          <div className="bg-white rounded-lg shadow-lg p-6">
            <div className="flex items-center justify-between mb-4">
              <h3 className="text-lg font-medium text-gray-900">Database Names</h3>
              <button
                onClick={() => setIsAddingDatabase(true)}
                className="inline-flex items-center px-3 py-2 border border-gray-300 shadow-sm text-sm leading-4 font-medium rounded-md text-gray-700 bg-white hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500"
              >
                <PlusIcon className="h-4 w-4 mr-1" />
                Add Database
              </button>
            </div>

            {/* Search */}
            {databases.length > 0 && (
              <div className="mb-4">
                <div className="relative">
                  <div className="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none">
                    <MagnifyingGlassIcon className="h-5 w-5 text-gray-400" />
                  </div>
                  <input
                    type="text"
                    placeholder="Search databases..."
                    value={searchTerm}
                    onChange={(e) => setSearchTerm(e.target.value)}
                    className="block w-full pl-10 pr-3 py-2 border border-gray-300 rounded-md leading-5 bg-white placeholder-gray-500 focus:outline-none focus:placeholder-gray-400 focus:ring-1 focus:ring-blue-500 focus:border-blue-500"
                  />
                </div>
              </div>
            )}

            {/* Add Database Input */}
            {isAddingDatabase && (
              <div className="mb-4 p-4 bg-gray-50 rounded-md">
                <div className="flex items-center space-x-3">
                  <input
                    type="text"
                    placeholder="Enter database name"
                    value={newDatabase}
                    onChange={(e) => setNewDatabase(e.target.value)}
                    onKeyPress={handleKeyPress}
                    autoFocus
                    className="flex-1 p-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                  />
                  <button
                    onClick={handleAddDatabase}
                    disabled={!newDatabase.trim() || databases.includes(newDatabase.trim())}
                    className="px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 focus:ring-2 focus:ring-blue-500 focus:ring-offset-2 disabled:opacity-50 disabled:cursor-not-allowed"
                  >
                    Add
                  </button>
                  <button
                    onClick={() => {
                      setIsAddingDatabase(false);
                      setNewDatabase('');
                    }}
                    className="px-4 py-2 border border-gray-300 text-gray-700 rounded-md hover:bg-gray-50 focus:ring-2 focus:ring-gray-500 focus:ring-offset-2"
                  >
                    Cancel
                  </button>
                </div>
                {newDatabase.trim() && databases.includes(newDatabase.trim()) && (
                  <p className="text-sm text-red-600 mt-1">Database already exists</p>
                )}
              </div>
            )}

            {/* Database List */}
            {databases.length === 0 ? (
              <div className="text-center py-12">
                <div className="text-gray-500">
                  <svg className="mx-auto h-12 w-12 text-gray-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 7v10c0 2.21 1.79 4 4 4h8c0-2.21-1.79-4-4-4H4V7zM4 7c0-2.21 1.79-4 4-4h8c2.21 0 4 1.79 4 4v10c0 2.21-1.79 4-4 4" />
                  </svg>
                  <h3 className="mt-2 text-sm font-medium text-gray-900">No databases selected</h3>
                  <p className="mt-1 text-sm text-red-600 font-medium">⚠️ NO databases will be processed during migration</p>
                  <p className="mt-1 text-sm text-gray-500">Add database names above to include them in the migration</p>
                </div>
              </div>
            ) : (
              <div className="space-y-2">
                {filteredDatabases.length === 0 ? (
                  <div className="text-center py-8 text-gray-500">
                    No databases match your search
                  </div>
                ) : (
                  filteredDatabases.map((dbName) => (
                    <div key={dbName} className="flex items-center justify-between p-3 bg-gray-50 rounded-md">
                      <div className="flex items-center">
                        <div className="flex-shrink-0 w-2 h-2 bg-green-400 rounded-full"></div>
                        <span className="ml-3 text-sm font-medium text-gray-900">{dbName}</span>
                      </div>
                      <button
                        onClick={() => handleRemoveDatabase(dbName)}
                        className="text-gray-400 hover:text-red-600 focus:outline-none focus:ring-2 focus:ring-red-500 focus:ring-offset-2 rounded-md"
                        title="Remove database"
                      >
                        <XMarkIcon className="h-5 w-5" />
                      </button>
                    </div>
                  ))
                )}
              </div>
            )}

            {databases.length > 0 && (
              <div className="mt-4 text-sm text-gray-600">
                {databases.length} database{databases.length !== 1 ? 's' : ''} selected for migration
              </div>
            )}
          </div>

          {/* Filter Information */}
          <div className="bg-white rounded-lg shadow-lg p-6">
            <h3 className="text-lg font-medium text-gray-900 mb-4">Filter Behavior</h3>
            <div className="space-y-3 text-sm text-gray-600">
              <p>
                <strong>Empty List:</strong> If no databases are specified above, NO databases will be processed during migration.
              </p>
              <p>
                <strong>Explicit Selection:</strong> Only databases explicitly listed above will be included in the migration process.
              </p>
              <p>
                <strong>Exact Names:</strong> Database names must match exactly. Wildcard patterns are not supported in this interface.
              </p>
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

export default DatabaseSelectionSection;