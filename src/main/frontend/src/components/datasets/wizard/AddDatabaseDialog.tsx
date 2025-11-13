import React, { useState } from 'react';
import { XMarkIcon } from '@heroicons/react/24/outline';
import ConnectionSelector from './ConnectionSelector';
import DatabaseSelector from './DatabaseSelector';

interface AddDatabaseDialogProps {
  isOpen: boolean;
  onClose: () => void;
  onAdd: (databaseName: string, sourceConnectionKey?: string) => void;
}

const AddDatabaseDialog: React.FC<AddDatabaseDialogProps> = ({ isOpen, onClose, onAdd }) => {
  const [entryMode, setEntryMode] = useState<'manual' | 'connection'>('manual');
  const [manualDatabaseName, setManualDatabaseName] = useState('');
  const [selectedConnection, setSelectedConnection] = useState<string>('');
  const [selectedDatabase, setSelectedDatabase] = useState('');
  const [error, setError] = useState('');

  if (!isOpen) return null;

  const handleAdd = () => {
    if (entryMode === 'manual') {
      if (!manualDatabaseName.trim()) {
        setError('Please enter a database name');
        return;
      }
      onAdd(manualDatabaseName.trim());
    } else {
      if (!selectedDatabase) {
        setError('Please select a database');
        return;
      }
      onAdd(selectedDatabase, selectedConnection);
    }

    // Reset state
    setManualDatabaseName('');
    setSelectedConnection('');
    setSelectedDatabase('');
    setError('');
    onClose();
  };

  const handleClose = () => {
    setManualDatabaseName('');
    setSelectedConnection('');
    setSelectedDatabase('');
    setError('');
    onClose();
  };

  return (
    <div className="fixed inset-0 z-50 overflow-y-auto">
      <div className="flex items-center justify-center min-h-screen px-4 pt-4 pb-20 text-center sm:block sm:p-0">
        {/* Background overlay */}
        <div className="fixed inset-0 transition-opacity bg-gray-500 bg-opacity-75" onClick={handleClose}></div>

        {/* Center modal */}
        <span className="hidden sm:inline-block sm:align-middle sm:h-screen">&#8203;</span>

        {/* Modal panel */}
        <div className="inline-block align-bottom bg-white rounded-lg text-left overflow-hidden shadow-xl transform transition-all sm:my-8 sm:align-middle sm:max-w-lg sm:w-full">
          {/* Header */}
          <div className="bg-white px-6 pt-5 pb-4 border-b border-gray-200">
            <div className="flex items-center justify-between">
              <h3 className="text-lg font-medium text-gray-900">
                Add Database to Dataset
              </h3>
              <button
                onClick={handleClose}
                className="text-gray-400 hover:text-gray-500"
              >
                <XMarkIcon className="h-6 w-6" />
              </button>
            </div>
          </div>

          {/* Body */}
          <div className="bg-white px-6 py-4">
            {/* Entry Mode Toggle */}
            <div className="mb-4 p-3 bg-blue-50 border border-blue-200 rounded-md">
              <div className="flex items-center justify-between">
                <div>
                  <label className="text-sm font-medium text-gray-900">Entry Mode</label>
                  <p className="text-xs text-gray-600 mt-0.5">
                    Choose how to add the database
                  </p>
                </div>
                <div className="flex space-x-2">
                  <button
                    type="button"
                    onClick={() => {
                      setEntryMode('manual');
                      setError('');
                    }}
                    className={`px-3 py-1.5 text-xs font-medium rounded-md ${
                      entryMode === 'manual'
                        ? 'bg-blue-600 text-white'
                        : 'bg-white text-gray-700 border border-gray-300'
                    }`}
                  >
                    Manual Entry
                  </button>
                  <button
                    type="button"
                    onClick={() => {
                      setEntryMode('connection');
                      setError('');
                    }}
                    className={`px-3 py-1.5 text-xs font-medium rounded-md ${
                      entryMode === 'connection'
                        ? 'bg-blue-600 text-white'
                        : 'bg-white text-gray-700 border border-gray-300'
                    }`}
                  >
                    From Connection
                  </button>
                </div>
              </div>
            </div>

            {/* Manual Entry Mode */}
            {entryMode === 'manual' && (
              <div>
                <label htmlFor="databaseName" className="block text-sm font-medium text-gray-700 mb-2">
                  Database Name
                </label>
                <input
                  type="text"
                  id="databaseName"
                  value={manualDatabaseName}
                  onChange={(e) => {
                    setManualDatabaseName(e.target.value);
                    setError('');
                  }}
                  className="block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500"
                  placeholder="Enter database name"
                />
              </div>
            )}

            {/* Connection Entry Mode */}
            {entryMode === 'connection' && (
              <div className="space-y-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    1. Select Connection
                  </label>
                  <ConnectionSelector
                    value={selectedConnection}
                    onChange={(connectionKey) => {
                      setSelectedConnection(connectionKey);
                      setSelectedDatabase('');
                      setError('');
                    }}
                  />
                </div>

                {selectedConnection && (
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-2">
                      2. Select Database
                    </label>
                    <DatabaseSelector
                      connectionKey={selectedConnection}
                      value={selectedDatabase}
                      onChange={(databaseName) => {
                        setSelectedDatabase(databaseName);
                        setError('');
                      }}
                    />
                  </div>
                )}
              </div>
            )}

            {/* Error Message */}
            {error && (
              <div className="mt-4 p-3 bg-red-50 border border-red-200 rounded-md">
                <p className="text-sm text-red-600">{error}</p>
              </div>
            )}
          </div>

          {/* Footer */}
          <div className="bg-gray-50 px-6 py-4 border-t border-gray-200 flex justify-end space-x-3">
            <button
              onClick={handleClose}
              className="px-4 py-2 border border-gray-300 text-gray-700 rounded-md hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-blue-500"
            >
              Cancel
            </button>
            <button
              onClick={handleAdd}
              className="px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-blue-500"
            >
              Add Database
            </button>
          </div>
        </div>
      </div>
    </div>
  );
};

export default AddDatabaseDialog;
