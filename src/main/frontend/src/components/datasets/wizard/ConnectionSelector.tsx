import React, { useState, useEffect } from 'react';

export interface Connection {
  key: string;
  name: string;
  description?: string;
  hs2TestResults?: {
    status: string;
  };
}

interface ConnectionSelectorProps {
  value: string | undefined;
  onChange: (connectionKey: string) => void;
  disabled?: boolean;
  className?: string;
}

/**
 * Component for selecting a tested HiveServer2 connection.
 * Only shows connections with successfully tested HS2 connections.
 */
const ConnectionSelector: React.FC<ConnectionSelectorProps> = ({
  value,
  onChange,
  disabled = false,
  className = ''
}) => {
  const [connections, setConnections] = useState<Connection[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    fetchConnections();
  }, []);

  const fetchConnections = async () => {
    setLoading(true);
    setError(null);

    try {
      const response = await fetch('/hms-mirror/api/v1/connections');

      if (!response.ok) {
        throw new Error('Failed to fetch connections');
      }

      const data = await response.json();

      // Filter connections with successful HS2 tests
      const testedConnections = (data.connections || []).filter((conn: any) =>
        conn.hs2TestResults?.status === 'SUCCESS'
      );

      setConnections(testedConnections);

      if (testedConnections.length === 0) {
        setError('No successfully tested HiveServer2 connections found. Please test a connection first.');
      }
    } catch (err: any) {
      console.error('Error fetching connections:', err);
      setError('Failed to load connections: ' + err.message);
    } finally {
      setLoading(false);
    }
  };

  if (loading) {
    return (
      <div className={`relative ${className}`}>
        <select
          disabled
          className="block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm bg-gray-50"
        >
          <option>Loading connections...</option>
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
      </div>
    );
  }

  return (
    <div className={className}>
      <select
        value={value || ''}
        onChange={(e) => onChange(e.target.value)}
        disabled={disabled || connections.length === 0}
        className={`block w-full px-3 py-2 border rounded-md shadow-sm focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 ${
          disabled || connections.length === 0
            ? 'bg-gray-50 text-gray-500 cursor-not-allowed border-gray-300'
            : 'bg-white border-gray-300'
        }`}
      >
        <option value="">Select a connection</option>
        {connections.map((conn) => (
          <option key={conn.key} value={conn.key}>
            {conn.name}
            {conn.description ? ` - ${conn.description}` : ''}
          </option>
        ))}
      </select>

      {connections.length === 0 && !error && (
        <p className="mt-1 text-xs text-amber-600">
          No tested connections available. Please create and test a connection first.
        </p>
      )}
    </div>
  );
};

export default ConnectionSelector;
