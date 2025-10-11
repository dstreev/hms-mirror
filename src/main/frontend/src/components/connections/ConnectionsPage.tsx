import React, { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { 
  PlusIcon, 
  ArrowPathIcon,
  MagnifyingGlassIcon,
  FunnelIcon
} from '@heroicons/react/24/outline';
import { Connection, ConnectionListFilters } from '../../types/Connection';
import ConnectionCard from './ConnectionCard';
import ConnectionFilters from './ConnectionFilters';
import BulkActions from './BulkActions';

const ConnectionsPage: React.FC = () => {
  const navigate = useNavigate();
  const [connections, setConnections] = useState<Connection[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [selectedIds, setSelectedIds] = useState<string[]>([]);
  const [filters, setFilters] = useState<ConnectionListFilters>({
    search: '',
    environment: '',
    status: 'all'
  });
  const [showFilters, setShowFilters] = useState(false);

  useEffect(() => {
    loadConnections();
  }, [filters]);

  const loadConnections = async () => {
    try {
      setLoading(true);
      setError(null);
      
      const params = new URLSearchParams();
      if (filters.search) params.append('search', filters.search);
      if (filters.environment) params.append('environment', filters.environment);
      if (filters.status && filters.status !== 'all') params.append('status', filters.status);
      
      const response = await fetch('/hms-mirror/api/v1/connections?' + params.toString());
      
      if (!response.ok) {
        throw new Error('Failed to load connections');
      }
      
      const data = await response.json();
      setConnections(data.connections || []);
    } catch (err) {
      console.error('Error loading connections:', err);
      setError(err instanceof Error ? err.message : 'Failed to load connections');
      setConnections([]);
    } finally {
      setLoading(false);
    }
  };


  const handleCreateNew = () => {
    navigate('/connections/new');
  };

  const handleRefresh = () => {
    loadConnections();
  };

  const handleConnectionSelect = (id: string, selected: boolean) => {
    if (selected) {
      setSelectedIds([...selectedIds, id]);
    } else {
      setSelectedIds(selectedIds.filter(sid => sid !== id));
    }
  };

  const handleSelectAll = (selected: boolean) => {
    if (selected) {
      setSelectedIds(connections.map(c => c.id));
    } else {
      setSelectedIds([]);
    }
  };

  const handleBulkAction = (action: string) => {
    // TODO: Implement bulk actions
    console.log('Bulk action:', action, 'for connections:', selectedIds);
    setSelectedIds([]);
  };

  const testConnection = async (connectionId: string) => {
    try {
      const response = await fetch(`/hms-mirror/api/v1/connections/${connectionId}/test`, {
        method: 'POST'
      });
      
      if (!response.ok) {
        throw new Error('Failed to test connection');
      }
      
      // Refresh connections to get updated test results
      await loadConnections();
    } catch (err) {
      console.error('Error testing connection:', err);
      setError(err instanceof Error ? err.message : 'Failed to test connection');
    }
  };

  const filteredConnections = connections.filter(connection => {
    if (filters.search && !connection.name.toLowerCase().includes(filters.search.toLowerCase())) {
      return false;
    }
    if (filters.environment && connection.environment !== filters.environment) {
      return false;
    }
    if (filters.status !== 'all') {
      const status = connection.testResults?.status || 'NEVER_TESTED';
      if (filters.status === 'success' && status !== 'SUCCESS') return false;
      if (filters.status === 'failed' && status !== 'FAILED') return false;
      if (filters.status === 'never_tested' && status !== 'NEVER_TESTED') return false;
    }
    return true;
  });

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600"></div>
        <span className="ml-3 text-gray-600">Loading connections...</span>
      </div>
    );
  }

  return (
    <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
      {/* Page Header */}
      <div className="mb-8">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-3xl font-bold text-gray-900 flex items-center">
              🔗 Connection Management
            </h1>
            <p className="text-gray-600 mt-2">Manage reusable cluster connection profiles</p>
          </div>
          
          <div className="flex space-x-3">
            <button
              onClick={() => setShowFilters(!showFilters)}
              className="inline-flex items-center px-4 py-2 border border-gray-300 rounded-md shadow-sm text-sm font-medium text-gray-700 bg-white hover:bg-gray-50"
            >
              <FunnelIcon className="h-4 w-4 mr-2" />
              Filters
            </button>
            <button
              onClick={handleRefresh}
              className="inline-flex items-center px-4 py-2 border border-gray-300 rounded-md shadow-sm text-sm font-medium text-gray-700 bg-white hover:bg-gray-50"
            >
              <ArrowPathIcon className="h-4 w-4 mr-2" />
              Refresh
            </button>
            <button
              onClick={handleCreateNew}
              className="inline-flex items-center px-4 py-2 border border-transparent rounded-md shadow-sm text-sm font-medium text-white bg-blue-600 hover:bg-blue-700"
            >
              <PlusIcon className="h-4 w-4 mr-2" />
              New Connection
            </button>
          </div>
        </div>
        
        {/* Search Bar */}
        <div className="mt-6 max-w-md">
          <div className="relative">
            <div className="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none">
              <MagnifyingGlassIcon className="h-5 w-5 text-gray-400" />
            </div>
            <input
              type="text"
              placeholder="Search connections..."
              value={filters.search}
              onChange={(e) => setFilters({ ...filters, search: e.target.value })}
              className="block w-full pl-10 pr-3 py-2 border border-gray-300 rounded-md leading-5 bg-white placeholder-gray-500 focus:outline-none focus:placeholder-gray-400 focus:ring-1 focus:ring-blue-500 focus:border-blue-500"
            />
          </div>
        </div>
      </div>

      {/* Filters */}
      {showFilters && (
        <div className="mb-6">
          <ConnectionFilters
            filters={filters}
            onFiltersChange={setFilters}
            onClose={() => setShowFilters(false)}
          />
        </div>
      )}

      {/* Bulk Actions */}
      {selectedIds.length > 0 && (
        <div className="mb-6">
          <BulkActions
            selectedCount={selectedIds.length}
            onAction={handleBulkAction}
            onClear={() => setSelectedIds([])}
          />
        </div>
      )}

      {/* Error State */}
      {error && (
        <div className="mb-6 bg-red-50 border border-red-200 rounded-md p-4">
          <div className="flex">
            <div className="ml-3">
              <h3 className="text-sm font-medium text-red-800">Error loading connections</h3>
              <div className="mt-2 text-sm text-red-700">{error}</div>
              <div className="mt-4">
                <button
                  onClick={handleRefresh}
                  className="text-sm bg-red-100 text-red-800 rounded-md px-2 py-1 hover:bg-red-200"
                >
                  Try again
                </button>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Empty State */}
      {filteredConnections.length === 0 && !loading && (
        <div className="text-center py-12">
          <div className="text-6xl mb-4">🔗</div>
          <h3 className="text-lg font-medium text-gray-900 mb-2">
            {connections.length === 0 ? 'No connection profiles found' : 'No connections match your filters'}
          </h3>
          <p className="text-gray-600 mb-6">
            {connections.length === 0 
              ? 'Create your first cluster connection profile to get started with HMS-Mirror'
              : 'Try adjusting your search or filter criteria'
            }
          </p>
          {connections.length === 0 && (
            <button
              onClick={handleCreateNew}
              className="inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md text-white bg-blue-600 hover:bg-blue-700"
            >
              <PlusIcon className="h-4 w-4 mr-2" />
              Create Connection
            </button>
          )}
          <div className="mt-4 text-sm text-gray-500">
            💡 Tip: Connection profiles can be reused across multiple migration configurations
          </div>
        </div>
      )}

      {/* Connection List */}
      {filteredConnections.length > 0 && (
        <>
          <div className="mb-4 flex items-center justify-between">
            <div className="flex items-center">
              <span className="text-sm text-gray-700">
                📊 Showing {filteredConnections.length} connection profile{filteredConnections.length !== 1 ? 's' : ''}
                {connections.length !== filteredConnections.length && ` of ${connections.length} total`}
              </span>
            </div>
            
            {connections.length > 1 && (
              <label className="flex items-center space-x-2 text-sm text-gray-600">
                <input
                  type="checkbox"
                  checked={selectedIds.length === filteredConnections.length && filteredConnections.length > 0}
                  onChange={(e) => handleSelectAll(e.target.checked)}
                  className="rounded border-gray-300 text-blue-600 focus:ring-blue-500"
                />
                <span>Select All</span>
              </label>
            )}
          </div>

          <div className="space-y-4">
            {filteredConnections.map((connection) => (
              <ConnectionCard
                key={connection.id}
                connection={connection}
                selected={selectedIds.includes(connection.id)}
                onSelect={(selected) => handleConnectionSelect(connection.id, selected)}
                onEdit={() => navigate(`/connections/edit/${connection.id}`)}
                onTest={() => testConnection(connection.id)}
                onDuplicate={() => {/* TODO: Implement duplicate */}}
                onDelete={() => {/* TODO: Implement delete */}}
              />
            ))}
          </div>
        </>
      )}
    </div>
  );
};

export default ConnectionsPage;