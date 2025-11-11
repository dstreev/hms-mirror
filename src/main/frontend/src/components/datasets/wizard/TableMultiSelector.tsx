import React, { useState, useEffect } from 'react';
import { discoveryApi } from '../../../services/api/discoveryApi';

interface TableMultiSelectorProps {
  connectionKey: string | undefined;
  databaseName: string;
  selectedTables: string[];
  onChange: (tables: string[]) => void;
  disabled?: boolean;
  className?: string;
}

/**
 * Component for selecting multiple tables from a database using checkboxes.
 * Fetches tables from a tested HiveServer2 connection and allows multi-selection.
 */
const TableMultiSelector: React.FC<TableMultiSelectorProps> = ({
  connectionKey,
  databaseName,
  selectedTables,
  onChange,
  disabled = false,
  className = ''
}) => {
  const [tables, setTables] = useState<string[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [searchTerm, setSearchTerm] = useState('');

  useEffect(() => {
    if (connectionKey && databaseName) {
      fetchTables();
    } else {
      setTables([]);
      setError(null);
    }
  }, [connectionKey, databaseName]);

  const fetchTables = async () => {
    if (!connectionKey || !databaseName) return;

    setLoading(true);
    setError(null);

    try {
      const fetchedTables = await discoveryApi.getTables(connectionKey, databaseName);

      if (fetchedTables) {
        setTables(fetchedTables);
      } else {
        setError('Failed to fetch tables. Please check the connection and database.');
        setTables([]);
      }
    } catch (err: any) {
      console.error('Error fetching tables:', err);
      setError('Failed to load tables: ' + err.message);
      setTables([]);
    } finally {
      setLoading(false);
    }
  };

  const handleToggleTable = (tableName: string) => {
    if (selectedTables.includes(tableName)) {
      // Remove table from selection
      onChange(selectedTables.filter((t) => t !== tableName));
    } else {
      // Add table to selection
      onChange([...selectedTables, tableName]);
    }
  };

  const handleSelectAll = () => {
    if (selectedTables.length === filteredTables.length) {
      // Deselect all
      onChange([]);
    } else {
      // Select all filtered tables
      onChange(filteredTables);
    }
  };

  // Filter tables based on search term
  const filteredTables = tables.filter((table) =>
    table.toLowerCase().includes(searchTerm.toLowerCase())
  );

  const allFilteredSelected =
    filteredTables.length > 0 && selectedTables.length === filteredTables.length;

  if (!connectionKey || !databaseName) {
    return (
      <div className={className}>
        <div className="p-4 bg-gray-50 border border-gray-300 rounded-md text-center">
          <p className="text-sm text-gray-500">
            Select a connection and database first
          </p>
        </div>
      </div>
    );
  }

  if (loading) {
    return (
      <div className={className}>
        <div className="p-4 bg-gray-50 border border-gray-300 rounded-md text-center">
          <p className="text-sm text-gray-600">Loading tables...</p>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className={className}>
        <div className="p-4 bg-red-50 border border-red-300 rounded-md">
          <p className="text-sm text-red-600">{error}</p>
          <button
            type="button"
            onClick={fetchTables}
            className="mt-2 text-xs text-blue-600 hover:text-blue-800"
          >
            Retry
          </button>
        </div>
      </div>
    );
  }

  if (tables.length === 0) {
    return (
      <div className={className}>
        <div className="p-4 bg-amber-50 border border-amber-300 rounded-md text-center">
          <p className="text-sm text-amber-600">
            No tables found in database "{databaseName}"
          </p>
        </div>
      </div>
    );
  }

  return (
    <div className={className}>
      {/* Search and Select All Header */}
      <div className="mb-3">
        <input
          type="text"
          value={searchTerm}
          onChange={(e) => setSearchTerm(e.target.value)}
          placeholder="Search tables..."
          disabled={disabled}
          className="block w-full px-3 py-2 mb-2 border border-gray-300 rounded-md shadow-sm placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 text-sm"
        />

        <div className="flex items-center justify-between">
          <label className="flex items-center cursor-pointer">
            <input
              type="checkbox"
              checked={allFilteredSelected}
              onChange={handleSelectAll}
              disabled={disabled}
              className="h-4 w-4 text-blue-600 border-gray-300 rounded focus:ring-blue-500"
            />
            <span className="ml-2 text-sm font-medium text-gray-700">
              Select All {searchTerm && 'Filtered'} ({filteredTables.length})
            </span>
          </label>

          {selectedTables.length > 0 && (
            <span className="text-xs text-blue-600">
              {selectedTables.length} table{selectedTables.length !== 1 ? 's' : ''} selected
            </span>
          )}
        </div>
      </div>

      {/* Table List with Checkboxes */}
      <div className="max-h-64 overflow-y-auto border border-gray-300 rounded-md">
        {filteredTables.length === 0 ? (
          <div className="p-4 text-center text-sm text-gray-500">
            No tables match "{searchTerm}"
          </div>
        ) : (
          <div className="divide-y divide-gray-200">
            {filteredTables.map((table) => (
              <label
                key={table}
                className={`flex items-center px-3 py-2 cursor-pointer hover:bg-gray-50 ${
                  disabled ? 'opacity-50 cursor-not-allowed' : ''
                }`}
              >
                <input
                  type="checkbox"
                  checked={selectedTables.includes(table)}
                  onChange={() => handleToggleTable(table)}
                  disabled={disabled}
                  className="h-4 w-4 text-blue-600 border-gray-300 rounded focus:ring-blue-500"
                />
                <span className="ml-3 text-sm text-gray-900">{table}</span>
              </label>
            ))}
          </div>
        )}
      </div>

      {/* Footer Info */}
      <div className="mt-2 text-xs text-gray-500">
        {tables.length} table{tables.length !== 1 ? 's' : ''} available in "{databaseName}"
        {searchTerm && ` (showing ${filteredTables.length})`}
      </div>
    </div>
  );
};

export default TableMultiSelector;
