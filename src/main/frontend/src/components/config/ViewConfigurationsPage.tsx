import React, { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { 
  ArrowLeftIcon, 
  DocumentTextIcon, 
  TrashIcon, 
  PencilIcon,
  FolderIcon,
  DocumentDuplicateIcon
} from '@heroicons/react/24/outline';
import { configApi } from '../../services/api/configApi';

interface Configuration {
  name: string;
  strategy: string;
  createdDate: string;
  modifiedDate: string;
  description?: string;
}


const ViewConfigurationsPage: React.FC = () => {
  const navigate = useNavigate();
  const [configurations, setConfigurations] = useState<Configuration[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Strategy display information
  const strategyInfo = {
    SQL: {
      name: 'SQL',
      description: 'SQL-based data migration strategy',
      color: 'blue',
      icon: '🔄'
    },
    HYBRID: {
      name: 'HYBRID',
      description: 'Mixed SQL and Export/Import approach',
      color: 'purple',
      icon: '🔀'
    },
    EXPORT_IMPORT: {
      name: 'EXPORT_IMPORT',
      description: 'Hive Export/Import mechanism',
      color: 'green',
      icon: '📊'
    },
    SCHEMA_ONLY: {
      name: 'SCHEMA_ONLY',
      description: 'Metadata migration only',
      color: 'yellow',
      icon: '📋'
    },
    STORAGE_MIGRATION: {
      name: 'STORAGE_MIGRATION',
      description: 'In-cluster storage migration',
      color: 'indigo',
      icon: '📦'
    },
    LINKED: {
      name: 'LINKED',
      description: 'Read-only testing access',
      color: 'gray',
      icon: '🔗'
    },
    COMMON: {
      name: 'COMMON',
      description: 'Shared storage metadata migration',
      color: 'teal',
      icon: '🤝'
    },
    DUMP: {
      name: 'DUMP',
      description: 'Schema extraction only',
      color: 'orange',
      icon: '💾'
    }
  };

  useEffect(() => {
    fetchConfigurations();
  }, []);

  const fetchConfigurations = async () => {
    try {
      setLoading(true);
      
      // Use new standardized REST API
      const response = await configApi.getConfigurations();
      
      if (response && response.status === 'success' && response.data) {
        const allConfigs = processConfigurationsFromAPI(response.data);
        setConfigurations(allConfigs);
      } else {
        // No configurations found - show empty state
        setConfigurations([]);
      }
    } catch (error) {
      console.error('Error fetching configurations:', error);
      setError('Failed to fetch configurations. Please check if the application is running with RocksDB enabled.');
      // Show empty list on error instead of mock data
      setConfigurations([]);
    } finally {
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
            // configData contains: name, yamlConfig, createdDate, modifiedDate
            return {
              name: configData.name || 'unknown',
              strategy: strategy,
              createdDate: configData.createdDate || new Date().toISOString().split('T')[0],
              modifiedDate: configData.modifiedDate || new Date().toISOString().split('T')[0],
              description: `${strategy} configuration loaded via REST API`
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

  const handleDeleteConfiguration = async (config: Configuration, event: React.MouseEvent) => {
    event.stopPropagation();
    
    if (window.confirm(`Are you sure you want to delete "${config.name}"?`)) {
      try {
        // Use new standardized REST API with only config name
        const success = await configApi.deleteConfiguration(config.name);
        
        if (success) {
          console.log('Configuration deleted successfully:', config.name);
          // Refresh the list
          await fetchConfigurations();
        } else {
          throw new Error('Delete request failed');
        }
      } catch (error) {
        console.error('Error deleting configuration:', error);
        setError('Failed to delete configuration');
      }
    }
  };

  const getStrategyColorClasses = (strategy: string) => {
    const info = strategyInfo[strategy as keyof typeof strategyInfo];
    if (!info) return 'bg-gray-100 text-gray-800 border-gray-200';
    
    const colorMap = {
      blue: 'bg-blue-100 text-blue-800 border-blue-200',
      purple: 'bg-purple-100 text-purple-800 border-purple-200',
      green: 'bg-green-100 text-green-800 border-green-200',
      yellow: 'bg-yellow-100 text-yellow-800 border-yellow-200',
      indigo: 'bg-indigo-100 text-indigo-800 border-indigo-200',
      gray: 'bg-gray-100 text-gray-800 border-gray-200',
      teal: 'bg-teal-100 text-teal-800 border-teal-200',
      orange: 'bg-orange-100 text-orange-800 border-orange-200'
    };
    
    return colorMap[info.color as keyof typeof colorMap] || 'bg-gray-100 text-gray-800 border-gray-200';
  };

  const formatDate = (dateString: string) => {
    return new Date(dateString).toLocaleDateString('en-US', {
      year: 'numeric',
      month: 'short',
      day: 'numeric'
    });
  };

  if (loading) {
    return (
      <div className="p-8">
        <div className="max-w-6xl mx-auto">
          <div className="flex items-center justify-center h-64">
            <div className="text-gray-500">Loading configurations...</div>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="p-8">
      <div className="max-w-6xl mx-auto">
        {/* Header */}
        <div className="mb-8">
          <div className="flex items-center mb-4">
            <button
              onClick={() => navigate('/config')}
              className="mr-4 p-2 text-gray-600 hover:text-gray-900 hover:bg-gray-100 rounded-lg transition-colors"
            >
              <ArrowLeftIcon className="h-5 w-5" />
            </button>
            <div>
              <h1 className="text-3xl font-bold text-gray-900">View Configurations</h1>
              <p className="text-gray-600 mt-1">Browse existing configurations</p>
            </div>
          </div>
        </div>

        {error && (
          <div className="mb-6 bg-red-50 border border-red-200 rounded-md p-4">
            <div className="text-sm text-red-700">{error}</div>
          </div>
        )}

        {configurations.length === 0 ? (
          <div className="text-center py-12">
            <FolderIcon className="mx-auto h-12 w-12 text-gray-400" />
            <h3 className="mt-2 text-sm font-medium text-gray-900">No configurations found</h3>
            <p className="mt-1 text-sm text-gray-500">
              Get started by creating your first configuration.
            </p>
            <div className="mt-6">
              <button
                onClick={() => navigate('/config/new')}
                className="px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 focus:ring-2 focus:ring-blue-500 focus:ring-offset-2"
              >
                Create Configuration
              </button>
            </div>
          </div>
        ) : (
          <div className="bg-white rounded-lg shadow-lg border border-gray-200">
            {configurations.map((config) => {
              return (
                <div
                  key={config.name}
                  className="p-6 border-b border-gray-100 last:border-b-0 hover:bg-gray-50 transition-colors cursor-pointer"
                  onClick={() => handleConfigurationClick(config)}
                >
                  <div className="flex items-center justify-between">
                    <div className="flex items-center space-x-4">
                      <DocumentTextIcon className="h-8 w-8 text-gray-400" />
                      <div>
                        <h4 className="text-base font-medium text-gray-900">{config.name}</h4>
                        {config.description && (
                          <p className="text-sm text-gray-600 mt-1">{config.description}</p>
                        )}
                        <div className="flex items-center space-x-4 mt-2 text-xs text-gray-500">
                          <span>Created: {formatDate(config.createdDate)}</span>
                          <span>Modified: {formatDate(config.modifiedDate)}</span>
                        </div>
                      </div>
                    </div>
                    <div className="flex items-center space-x-2">
                      <button
                        onClick={(e) => {
                          e.stopPropagation();
                          handleConfigurationClick(config);
                        }}
                        className="p-2 text-gray-400 hover:text-blue-600 hover:bg-blue-50 rounded-lg transition-colors"
                        title="Edit configuration"
                      >
                        <PencilIcon className="h-4 w-4" />
                      </button>
                      <button
                        onClick={(e) => handleCopyConfiguration(config, e)}
                        className="p-2 text-gray-400 hover:text-green-600 hover:bg-green-50 rounded-lg transition-colors"
                        title="Copy configuration"
                      >
                        <DocumentDuplicateIcon className="h-4 w-4" />
                      </button>
                      <button
                        onClick={(e) => handleDeleteConfiguration(config, e)}
                        className="p-2 text-gray-400 hover:text-red-600 hover:bg-red-50 rounded-lg transition-colors"
                        title="Delete configuration"
                      >
                        <TrashIcon className="h-4 w-4" />
                      </button>
                    </div>
                  </div>
                </div>
              );
            })}
          </div>
        )}
      </div>
    </div>
  );
};

export default ViewConfigurationsPage;