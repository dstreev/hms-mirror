import React, { useState, useEffect } from 'react';
import { discoveryApi } from '../../../services/api/discoveryApi';

interface DatabaseSelectorProps {
  connectionKey: string | undefined;
  value: string;
  onChange: (databaseName: string) => void;
  disabled?: boolean;
  className?: string;
}

/**
 * Component for selecting a database from a connection.
 * Fetches and displays databases from a tested HiveServer2 connection.
 */
const DatabaseSelector: React.FC<DatabaseSelectorProps> = ({
  connectionKey,
  value,
  onChange,
  disabled = false,
  className = ''
}) => {
  const [databases, setDatabases] = useState<string[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [searchTerm, setSearchTerm] = useState('');

  useEffect(() => {
    if (connectionKey) {
      fetchDatabases();
    } else {
      setDatabases([]);
      setError(null);
    }
  }, [connectionKey]);

  const fetchDatabases = async () => {
    if (!connectionKey) return;

    setLoading(true);
    setError(null);

    try {
      const fetchedDatabases = await discoveryApi.getDatabases(connectionKey);

      if (fetchedDatabases) {
        setDatabases(fetchedDatabases);
      } else {
        setError('Failed to fetch databases. Please check the connection.');
        setDatabases([]);
      }
    } catch (err: any) {
      console.error('Error fetching databases:', err);
      setError('Failed to load databases: ' + err.message);
      setDatabases([]);
    } finally {
      setLoading(false);
    }
  };

  // Filter databases based on search term
  const filteredDatabases = databases.filter((db) =>
    db.toLowerCase().includes(searchTerm.toLowerCase())
  );

  if (!connectionKey) {
    return (
      <div className={className}>
        <select
          disabled
          className="block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm bg-gray-50 text-gray-500"
        >
          <option>Select a connection first</option>
        </select>
        <p className="mt-1 text-xs text-gray-500">
          Choose a connection above to see available databases
        </p>
      </div>
    );
  }

  if (loading) {
    return (
      <div className={className}>
        <select
          disabled
          className="block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm bg-gray-50"
        >
          <option>Loading databases...</option>
        </select>
      </div>
    );
  }

  if (error) {
    return (
      <div className={className}>
        <select
          disabled
          className="block w-full px-3 py-2 border border-red-300 rounded-md shadow-sm bg-red-50"
        >
          <option>{error}</option>
        </select>
        <p className="mt-1 text-xs text-red-600">{error}</p>
        <button
          type="button"
          onClick={fetchDatabases}
          className="mt-2 text-xs text-blue-600 hover:text-blue-800"
        >
          Retry
        </button>
      </div>
    );
  }

  return (
    <div className={className}>
      {/* Search input for filtering databases (shown when there are many databases) */}
      {databases.length > 10 && (
        <input
          type="text"
          value={searchTerm}
          onChange={(e) => setSearchTerm(e.target.value)}
          placeholder="Search databases..."
          className="block w-full px-3 py-2 mb-2 border border-gray-300 rounded-md shadow-sm placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 text-sm"
        />
      )}

      <select
        value={value}
        onChange={(e) => onChange(e.target.value)}
        disabled={disabled || databases.length === 0}
        className={`block w-full px-3 py-2 border rounded-md shadow-sm focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 ${
          disabled || databases.length === 0
            ? 'bg-gray-50 text-gray-500 cursor-not-allowed border-gray-300'
            : 'bg-white border-gray-300'
        }`}
      >
        <option value="">
          {databases.length === 0 ? 'No databases found' : 'Select a database'}
        </option>
        {filteredDatabases.map((db) => (
          <option key={db} value={db}>
            {db}
          </option>
        ))}
      </select>

      {searchTerm && filteredDatabases.length === 0 && (
        <p className="mt-1 text-xs text-amber-600">
          No databases match "{searchTerm}"
        </p>
      )}

      {databases.length > 0 && (
        <p className="mt-1 text-xs text-gray-500">
          {databases.length} database{databases.length !== 1 ? 's' : ''} available
          {searchTerm && ` (showing ${filteredDatabases.length})`}
        </p>
      )}
    </div>
  );
};

export default DatabaseSelector;
