import React, { useState, useEffect } from 'react';
import { useNavigate, useLocation } from 'react-router-dom';
import {
  PlusIcon,
  ArrowPathIcon,
  MagnifyingGlassIcon,
  FunnelIcon,
  ArrowUpTrayIcon
} from '@heroicons/react/24/outline';
import { Connection, ConnectionListFilters, ConnectionTestResults } from '../../types/Connection';
import ConnectionCard from './ConnectionCard';
import ConnectionFilters from './ConnectionFilters';
import BulkActions from './BulkActions';
import ConfirmationDialog from '../common/ConfirmationDialog';
import ImportDialog from '../common/ImportDialog';
import TestProgressDialog from './TestProgressDialog';
import { exportToJson, generateExportFilename, maskPasswords } from '../../utils/importExport';

const ConnectionsPage: React.FC = () => {
  const navigate = useNavigate();
  const location = useLocation();
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
  const [deleteDialog, setDeleteDialog] = useState<{
    isOpen: boolean;
    connection: Connection | null;
  }>({ isOpen: false, connection: null });
  const [showImportDialog, setShowImportDialog] = useState(false);
  const [testDialog, setTestDialog] = useState<{
    isOpen: boolean;
    testing: boolean;
    connectionName: string;
    connectionKey: string;
    hcfsTestResults?: ConnectionTestResults;
    hs2TestResults?: ConnectionTestResults;
    metastoreDirectTestResults?: ConnectionTestResults;
    testError?: string | null;
  }>({
    isOpen: false,
    testing: false,
    connectionName: '',
    connectionKey: '',
    testError: null
  });

  useEffect(() => {
    loadConnections();
  }, [filters, location.key]);

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
    // Find the connection to get its name
    const connection = connections.find(c => c.key === connectionId);
    if (!connection) return;

    // Open dialog and show testing state
    setTestDialog({
      isOpen: true,
      testing: true,
      connectionName: connection.name,
      connectionKey: connectionId,
      testError: null
    });

    try {
      const response = await fetch(`/hms-mirror/api/v1/connections/${connectionId}/test`, {
        method: 'POST'
      });

      if (!response.ok) {
        throw new Error(`Failed to test connection: ${response.status} ${response.statusText}`);
      }

      const result = await response.json();
      console.log('Test API response:', result);

      // Check if tests failed - either explicitly or by checking individual results
      const hasFailed = !result.testPassed ||
        result.connection?.hcfsTestResults?.status === 'FAILED' ||
        result.connection?.hs2TestResults?.status === 'FAILED' ||
        result.connection?.metastoreDirectTestResults?.status === 'FAILED';

      // Update dialog with test results
      setTestDialog(prev => ({
        ...prev,
        testing: false,
        hcfsTestResults: result.connection?.hcfsTestResults,
        hs2TestResults: result.connection?.hs2TestResults,
        metastoreDirectTestResults: result.connection?.metastoreDirectTestResults,
        testError: (result.testPassed === false || result.error) ? (result.message || result.error || 'Test failed') : null
      }));

      // Update the specific connection with the test results from the response
      if (result.connection) {
        setConnections(prevConnections =>
          prevConnections.map(conn =>
            conn.key === connectionId ? result.connection : conn
          )
        );
      } else {
        // Fallback: refresh all connections if response doesn't include the updated connection
        await loadConnections();
      }
    } catch (err) {
      console.error('Error testing connection:', err);
      let errorMessage = 'Failed to test connection';

      if (err instanceof Error) {
        errorMessage = err.message;
      } else if (typeof err === 'string') {
        errorMessage = err;
      } else if (err && typeof err === 'object') {
        try {
          errorMessage = JSON.stringify(err, null, 2);
        } catch {
          errorMessage = String(err);
        }
      }

      // Update dialog with error - ensure testing is stopped
      setTestDialog(prev => ({
        ...prev,
        testing: false,
        testError: errorMessage
      }));
    }
  };

  const handleCloseTestDialog = () => {
    setTestDialog({
      isOpen: false,
      testing: false,
      connectionName: '',
      connectionKey: '',
      testError: null
    });
  };

  const handleDeleteClick = (connection: Connection) => {
    setDeleteDialog({ isOpen: true, connection });
  };

  const handleDeleteConfirm = async () => {
    if (!deleteDialog.connection) return;
    
    try {
      setError(null);
      
      const response = await fetch(`/hms-mirror/api/v1/connections/${deleteDialog.connection.key}`, {
        method: 'DELETE'
      });
      
      if (!response.ok) {
        throw new Error('Failed to delete connection');
      }
      
      // Close dialog and refresh connections list
      setDeleteDialog({ isOpen: false, connection: null });
      await loadConnections();
    } catch (err) {
      console.error('Error deleting connection:', err);
      setError(err instanceof Error ? err.message : 'Failed to delete connection');
      setDeleteDialog({ isOpen: false, connection: null });
    }
  };

  const handleDeleteCancel = () => {
    setDeleteDialog({ isOpen: false, connection: null });
  };

  const handleDuplicateClick = async (connection: Connection) => {
    try {
      // Load the full connection data from the backend
      const response = await fetch(`/hms-mirror/api/v1/connections/${connection.key}`);

      if (!response.ok) {
        throw new Error('Failed to load connection');
      }

      const connectionData = await response.json();

      // Navigate to Connection Wizard with copy mode - clear the name for user to provide new name
      navigate('/connections/new', {
        state: {
          connectionData: connectionData,
          mode: 'copy'
        }
      });
    } catch (err) {
      console.error('Error loading connection for copying:', err);
      setError(err instanceof Error ? err.message : 'Failed to load connection for copying');
    }
  };

  const handleExport = (connection: Connection) => {
    // Mask passwords before exporting for security
    const maskedConnection = maskPasswords(connection);
    const filename = generateExportFilename(connection.name, 'connection');
    exportToJson(maskedConnection, filename);
  };

  const handleImport = async (data: Connection, newName?: string): Promise<{ success: boolean; message?: string; status?: number }> => {
    try {
      // If a new name was provided (due to conflict), update both name and key
      if (newName) {
        data = { ...data, name: newName, key: newName };
      }

      const response = await fetch('/hms-mirror/api/v1/connections', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(data)
      });

      const result = await response.json();

      // Check for name conflict
      if (response.status === 409) {
        return {
          success: false,
          status: 409,
          message: result.message || `A connection with the name "${data.name}" already exists.`
        };
      }

      if (response.ok) {
        // Success - reload connections list
        await loadConnections();
        return { success: true };
      }

      // Other error
      return {
        success: false,
        message: result.message || 'Failed to import connection'
      };
    } catch (error: any) {
      console.error('Error importing connection:', error);
      return {
        success: false,
        message: error.message || 'Network error occurred while importing connection'
      };
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
              onClick={() => setShowImportDialog(true)}
              className="inline-flex items-center px-4 py-2 border border-gray-300 rounded-md shadow-sm text-sm font-medium text-gray-700 bg-white hover:bg-gray-50"
            >
              <ArrowUpTrayIcon className="h-4 w-4 mr-2" />
              Import
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

          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            {filteredConnections.map((connection) => (
              <ConnectionCard
                key={connection.key}
                connection={connection}
                selected={selectedIds.includes(connection.key)}
                onSelect={(selected) => handleConnectionSelect(connection.key, selected)}
                onEdit={() => navigate(`/connections/edit/${connection.key}`)}
                onTest={() => testConnection(connection.key)}
                onDuplicate={() => handleDuplicateClick(connection)}
                onDelete={() => handleDeleteClick(connection)}
                onExport={() => handleExport(connection)}
              />
            ))}
          </div>
        </>
      )}

      {/* Delete Confirmation Dialog */}
      <ConfirmationDialog
        isOpen={deleteDialog.isOpen}
        title="Delete Connection"
        message={`Are you sure you want to delete the connection "${deleteDialog.connection?.name}"? This action cannot be undone.`}
        confirmText="Delete"
        cancelText="Cancel"
        variant="danger"
        onConfirm={handleDeleteConfirm}
        onCancel={handleDeleteCancel}
      />

      {/* Import Dialog */}
      <ImportDialog<Connection>
        isOpen={showImportDialog}
        onClose={() => setShowImportDialog(false)}
        onImport={handleImport}
        title="Import Connection"
        itemType="Connection"
        getNameFromData={(data) => data.name}
        setNameInData={(data, newName) => ({ ...data, name: newName, key: newName })}
      />

      {/* Test Progress Dialog */}
      <TestProgressDialog
        isOpen={testDialog.isOpen}
        onClose={handleCloseTestDialog}
        testing={testDialog.testing}
        connectionName={testDialog.connectionName}
        hcfsTestResults={testDialog.hcfsTestResults}
        hs2TestResults={testDialog.hs2TestResults}
        metastoreDirectTestResults={testDialog.metastoreDirectTestResults}
        testError={testDialog.testError}
      />
    </div>
  );
};

export default ConnectionsPage;