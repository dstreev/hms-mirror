import React, { useState, useEffect } from 'react';
import { useNavigate, useLocation } from 'react-router-dom';
import {
  PlusIcon,
  ArrowPathIcon,
  DocumentTextIcon,
  TrashIcon,
  PencilIcon,
  FolderIcon,
  DocumentDuplicateIcon,
  MagnifyingGlassIcon,
  FunnelIcon
} from '@heroicons/react/24/outline';
import { configApi } from '../../services/api/configApi';
import ConfigurationFilters, { ConfigurationListFilters } from './ConfigurationFilters';
import ConfirmationDialog from '../common/ConfirmationDialog';

interface Configuration {
  name: string;
  strategy: string;
  createdDate: string;
  modifiedDate: string;
  description?: string;
}


const ViewConfigurationsPage: React.FC = () => {
  const navigate = useNavigate();
  const location = useLocation();
  const [configurations, setConfigurations] = useState<Configuration[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [filters, setFilters] = useState<ConfigurationListFilters>({
    search: '',
    acidConversion: '',
    icebergConversion: ''
  });
  const [showFilters, setShowFilters] = useState(false);
  const [deleteDialog, setDeleteDialog] = useState<{
    isOpen: boolean;
    configuration: Configuration | null;
  }>({ isOpen: false, configuration: null });

  useEffect(() => {
    // Reset state and fetch configurations whenever we navigate to this page
    // Using location.key ensures this runs on every navigation, including back/forward
    console.log('ViewConfigurationsPage: Loading configurations, location.key:', location.key);
    setLoading(true);
    setError(null);
    fetchConfigurations();
  }, [location.key]);

  const fetchConfigurations = async () => {
    try {
      console.log('ViewConfigurationsPage: Fetching configurations...');
      setLoading(true);

      // Use new standardized REST API
      const response = await configApi.getConfigurations();
      console.log('ViewConfigurationsPage: API response:', response);

      if (response && response.status === 'success' && response.data) {
        const allConfigs = processConfigurationsFromAPI(response.data);
        console.log('ViewConfigurationsPage: Processed configs:', allConfigs.length);
        setConfigurations(allConfigs);
      } else {
        console.log('ViewConfigurationsPage: No configurations found');
        // No configurations found - show empty state
        setConfigurations([]);
      }
    } catch (error) {
      console.error('ViewConfigurationsPage: Error fetching configurations:', error);
      setError('Failed to fetch configurations. Please check if the application is running with RocksDB enabled.');
      // Show empty list on error instead of mock data
      setConfigurations([]);
    } finally {
      console.log('ViewConfigurationsPage: Setting loading to false');
      setLoading(false);
    }
  };

  const processConfigurationsFromAPI = (configurationsByStrategy: any): Configuration[] => {
    // Process configurations from new REST API and return flat array sorted alphabetically
    const allConfigurations: Configuration[] = [];
    
    if (configurationsByStrategy && typeof configurationsByStrategy === 'object') {
      Object.entries(configurationsByStrategy).forEach(([strategy, configList]) => {
        if (Array.isArray(configList) && configList.length > 0) {
          const configurations: Configuration[] = configList.map((configData: any) => {
            // configData contains: name, yamlConfig, createdDate, modifiedDate, description
            return {
              name: configData.name || 'unknown',
              strategy: strategy,
              createdDate: configData.createdDate || new Date().toISOString().split('T')[0],
              modifiedDate: configData.modifiedDate || new Date().toISOString().split('T')[0],
              description: configData.description
            };
          });
          
          allConfigurations.push(...configurations);
        }
      });
    }
    
    // Sort configurations alphabetically by name
    return allConfigurations.sort((a, b) => a.name.localeCompare(b.name));
  };



  const handleConfigurationClick = async (config: Configuration) => {
    try {
      // First, load the full configuration data from the backend using only config name
      const response = await configApi.getConfiguration(config.name);
      
      if (response && response.status === 'SUCCESS') {
        // Navigate to Config Wizard for all strategies
        navigate('/wizards/config', {
          state: {
            configurationData: response.configuration,
            configName: config.name,
            dataStrategy: config.strategy,
            isEditing: true
          }
        });
      } else {
        throw new Error('Failed to load configuration data');
      }
    } catch (error) {
      console.error('Error loading configuration for editing:', error);
      setError('Failed to load configuration data for editing');
    }
  };

  const handleCopyConfiguration = async (config: Configuration, event: React.MouseEvent) => {
    event.stopPropagation();
    
    try {
      // Load the full configuration data from the backend
      const response = await configApi.getConfiguration(config.name);
      
      if (response && response.status === 'SUCCESS') {
        // Navigate to Config Wizard with copy mode - clear the name for user to provide new name
        navigate('/wizards/config', {
          state: {
            configurationData: response.configuration,
            configName: '', // Clear name for copy mode
            dataStrategy: config.strategy,
            isEditing: false, // Set to false since this is a copy, not an edit
            isCopying: true // Add flag to indicate copy mode
          }
        });
      } else {
        throw new Error('Failed to load configuration data for copying');
      }
    } catch (error) {
      console.error('Error loading configuration for copying:', error);
      setError('Failed to load configuration data for copying');
    }
  };

  const handleRefresh = () => {
    fetchConfigurations();
  };

  const handleCreateNew = () => {
    navigate('/config/new');
  };

  const handleDeleteClick = (config: Configuration, event: React.MouseEvent) => {
    event.stopPropagation();
    setDeleteDialog({ isOpen: true, configuration: config });
  };

  const handleDeleteConfirm = async () => {
    if (!deleteDialog.configuration) return;

    try {
      setError(null);

      const success = await configApi.deleteConfiguration(deleteDialog.configuration.name);

      if (success) {
        console.log('Configuration deleted successfully:', deleteDialog.configuration.name);
        // Close dialog and refresh the list
        setDeleteDialog({ isOpen: false, configuration: null });
        await fetchConfigurations();
      } else {
        throw new Error('Delete request failed');
      }
    } catch (error) {
      console.error('Error deleting configuration:', error);
      setError('Failed to delete configuration');
      setDeleteDialog({ isOpen: false, configuration: null });
    }
  };

  const handleDeleteCancel = () => {
    setDeleteDialog({ isOpen: false, configuration: null });
  };

  const formatDate = (dateString: string) => {
    return new Date(dateString).toLocaleDateString('en-US', {
      year: 'numeric',
      month: 'short',
      day: 'numeric'
    });
  };

  const filteredConfigurations = configurations.filter(config => {
    if (filters.search &&
        !config.name.toLowerCase().includes(filters.search.toLowerCase()) &&
        (!config.description || !config.description.toLowerCase().includes(filters.search.toLowerCase()))) {
      return false;
    }

    // TODO: Add acid/iceberg conversion filtering when backend provides this data
    // if (filters.acidConversion === 'enabled' && !config.acidConversionEnabled) {
    //   return false;
    // }
    // if (filters.acidConversion === 'disabled' && config.acidConversionEnabled) {
    //   return false;
    // }
    // if (filters.icebergConversion === 'enabled' && !config.icebergConversionEnabled) {
    //   return false;
    // }
    // if (filters.icebergConversion === 'disabled' && config.icebergConversionEnabled) {
    //   return false;
    // }

    return true;
  });

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600"></div>
        <span className="ml-3 text-gray-600">Loading configurations...</span>
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
              ⚙️ Configuration Management
            </h1>
            <p className="text-gray-600 mt-2">Manage migration configurations for HMS-Mirror</p>
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
              New Configuration
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
              placeholder="Search configurations..."
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
          <ConfigurationFilters
            filters={filters}
            onFiltersChange={setFilters}
            onClose={() => setShowFilters(false)}
          />
        </div>
      )}

      {/* Error State */}
      {error && (
        <div className="mb-6 bg-red-50 border border-red-200 rounded-md p-4">
          <div className="flex">
            <div className="ml-3">
              <h3 className="text-sm font-medium text-red-800">Error loading configurations</h3>
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
      {filteredConfigurations.length === 0 && !loading && (
        <div className="text-center py-12">
          <div className="text-6xl mb-4">⚙️</div>
          <h3 className="text-lg font-medium text-gray-900 mb-2">
            {configurations.length === 0 ? 'No configurations found' : 'No configurations match your filters'}
          </h3>
          <p className="text-gray-600 mb-6">
            {configurations.length === 0
              ? 'Create your first configuration to get started with HMS-Mirror'
              : 'Try adjusting your search or filter criteria'
            }
          </p>
          {configurations.length === 0 && (
            <button
              onClick={handleCreateNew}
              className="inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md text-white bg-blue-600 hover:bg-blue-700"
            >
              <PlusIcon className="h-4 w-4 mr-2" />
              Create Configuration
            </button>
          )}
          <div className="mt-4 text-sm text-gray-500">
            💡 Tip: Configurations define how HMS-Mirror will migrate your Hive metastore
          </div>
        </div>
      )}

      {/* Configuration List */}
      {filteredConfigurations.length > 0 && (
        <>
          <div className="mb-4 flex items-center justify-between">
            <div className="flex items-center">
              <span className="text-sm text-gray-700">
                ⚙️ Showing {filteredConfigurations.length} configuration{filteredConfigurations.length !== 1 ? 's' : ''}
                {configurations.length !== filteredConfigurations.length && ` of ${configurations.length} total`}
              </span>
            </div>
          </div>
          <div className="space-y-4">
            {filteredConfigurations.map((config) => {
              return (
                <div
                  key={config.name}
                  className="bg-white border border-gray-200 rounded-lg p-6 hover:shadow-md transition-shadow"
                >
                  <div className="flex items-center justify-between">
                    <div className="flex-1 min-w-0">
                      <div className="flex items-center space-x-3">
                        <h3 className="text-lg font-medium text-gray-900 truncate">
                          {config.name}
                        </h3>
                      </div>
                      {config.description && (
                        <p className="text-sm text-gray-500 mt-1">{config.description}</p>
                      )}
                      <div className="flex items-center mt-2 text-xs text-gray-400 space-x-4">
                        <span>📅 Created: {formatDate(config.createdDate)}</span>
                        <span>🕒 Modified: {formatDate(config.modifiedDate)}</span>
                      </div>
                    </div>

                    <div className="flex items-center space-x-2 ml-4">
                      <button
                        onClick={() => handleConfigurationClick(config)}
                        className="inline-flex items-center px-3 py-2 border border-gray-300 rounded-md text-sm font-medium text-gray-700 bg-white hover:bg-gray-50"
                        title="Edit configuration"
                      >
                        <PencilIcon className="h-4 w-4 mr-1" />
                        Edit
                      </button>

                      <button
                        onClick={(e) => handleCopyConfiguration(config, e)}
                        className="inline-flex items-center px-3 py-2 border border-gray-300 rounded-md text-sm font-medium text-gray-700 bg-white hover:bg-gray-50"
                        title="Copy configuration"
                      >
                        <DocumentDuplicateIcon className="h-4 w-4 mr-1" />
                        Copy
                      </button>

                      <button
                        onClick={(e) => handleDeleteClick(config, e)}
                        className="inline-flex items-center px-3 py-2 border border-red-300 rounded-md text-sm font-medium text-red-700 bg-white hover:bg-red-50"
                        title="Delete configuration"
                      >
                        <TrashIcon className="h-4 w-4 mr-1" />
                        Delete
                      </button>
                    </div>
                  </div>
                </div>
              );
            })}
          </div>
        </>
      )}

      {/* Delete Confirmation Dialog */}
      <ConfirmationDialog
        isOpen={deleteDialog.isOpen}
        title="Delete Configuration"
        message={`Are you sure you want to delete the configuration "${deleteDialog.configuration?.name}"? This action cannot be undone.`}
        confirmText="Delete"
        cancelText="Cancel"
        variant="danger"
        onConfirm={handleDeleteConfirm}
        onCancel={handleDeleteCancel}
      />
    </div>
  );
};

export default ViewConfigurationsPage;