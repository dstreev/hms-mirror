import React, { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { configApi, ConfigurationDto } from '../../services/api/configApi';

interface ConfigFormData {
  dataStrategy: string;
  configName: string;
}

const NewConfigurationPage: React.FC = () => {
  const navigate = useNavigate();
  const [formData, setFormData] = useState<ConfigFormData>({
    dataStrategy: 'HYBRID',
    configName: ''
  });
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [dataStrategies, setDataStrategies] = useState<Array<{value: string, label: string}>>([]);

  useEffect(() => {
    const loadDataStrategies = async () => {
      try {
        const response = await configApi.getDataStrategies();
        if (response && response.status === 'success') {
          setDataStrategies(response.strategies);
        }
      } catch (error) {
        console.error('Failed to load data strategies:', error);
        // Fallback to default strategies
        setDataStrategies([
          {value: 'HYBRID', label: 'HYBRID - Recommended for most migrations'},
          {value: 'SQL', label: 'SQL - SQL-based migration'},
          {value: 'EXPORT_IMPORT', label: 'EXPORT_IMPORT - Export/Import approach'},
          {value: 'SCHEMA_ONLY', label: 'SCHEMA_ONLY - Schema migration only'}
        ]);
      }
    };

    loadDataStrategies();
  }, []);

  const handleInputChange = (e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement>) => {
    const { name, value } = e.target;
    setFormData(prev => ({
      ...prev,
      [name]: value
    }));
  };

  const handleCreateConfiguration = async (e: React.FormEvent) => {
    e.preventDefault();
    
    if (!formData.configName.trim()) {
      setError('Configuration name is required');
      return;
    }

    setIsLoading(true);
    setError(null);

    try {
      // Create configuration using the new standardized API
      const configDto: ConfigurationDto = {
        name: formData.configName.trim(),
        dataStrategy: formData.dataStrategy,
        comment: `Configuration created on ${new Date().toLocaleDateString()}`,
        beta: false,
        execute: false,
        databaseOnly: false,
        migrateNonNative: false,
        readOnly: false,
        noPurge: false,
        replace: false,
        resetRight: false,
        sync: false,
        quiet: false,
        skipFeatures: false,
        skipLegacyTranslation: false,
        skipLinkCheck: false,
        suppressCliWarnings: false,
        saveWorkingTables: false,
        copyAvroSchemaUrls: false,
        dumpTestData: false,
        encryptedPasswords: false,
        databases: [],
        dumpSource: 'LEFT'
      };

      console.log('Creating configuration:', configDto);
      
      const success = await configApi.saveConfiguration(configDto);
      
      if (success) {
        setIsLoading(false);
        alert(`Configuration "${formData.configName}" created successfully!`);
        // Navigate to the view configurations page to see the new config
        navigate('/config/view');
      } else {
        throw new Error('Failed to save configuration');
      }

    } catch (error) {
      console.error('Failed to create configuration:', error);
      setError('Failed to create configuration. Please try again.');
      setIsLoading(false);
    }
  };

  const handleCancel = () => {
    navigate('/config');
  };

  return (
    <div className="p-8">
      <div className="max-w-2xl mx-auto">
        <div className="mb-6">
          <h1 className="text-3xl font-bold text-gray-900">Create New Configuration</h1>
          <p className="text-gray-600 mt-2">Set up a new HMS migration configuration</p>
        </div>

        <div className="bg-white rounded-lg shadow-lg p-6">
          <form onSubmit={handleCreateConfiguration} className="space-y-6">
            {error && (
              <div className="bg-red-50 border border-red-200 rounded-md p-4">
                <div className="text-sm text-red-700">{error}</div>
              </div>
            )}

            <div>
              <label htmlFor="configName" className="block text-sm font-medium text-gray-700 mb-2">
                Configuration Name *
              </label>
              <input
                type="text"
                id="configName"
                name="configName"
                value={formData.configName}
                onChange={handleInputChange}
                className="w-full p-3 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                placeholder="Enter a name for this configuration..."
                required
                disabled={isLoading}
              />
              <p className="text-sm text-gray-500 mt-1">
                Choose a descriptive name for easy identification
              </p>
            </div>

            <div>
              <label htmlFor="dataStrategy" className="block text-sm font-medium text-gray-700 mb-2">
                Data Strategy *
              </label>
              <select
                id="dataStrategy"
                name="dataStrategy"
                value={formData.dataStrategy}
                onChange={handleInputChange}
                className="w-full p-3 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                required
                disabled={isLoading}
              >
                <option value="HYBRID">HYBRID - Recommended for most migrations</option>
                <option value="SQL">SQL - SQL-based migration</option>
                <option value="EXPORT_IMPORT">EXPORT_IMPORT - Export/Import approach</option>
                <option value="SCHEMA_ONLY">SCHEMA_ONLY - Schema migration only</option>
              </select>
              <p className="text-sm text-gray-500 mt-1">
                Choose the migration strategy that best fits your use case
              </p>
            </div>

            <div className="border-t pt-6">
              <div className="flex justify-between">
                <button
                  type="button"
                  onClick={handleCancel}
                  className="px-6 py-2 border border-gray-300 rounded-md text-gray-700 hover:bg-gray-50 focus:ring-2 focus:ring-gray-500 focus:ring-offset-2"
                  disabled={isLoading}
                >
                  Cancel
                </button>
                <button
                  type="submit"
                  className="px-6 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 focus:ring-2 focus:ring-blue-500 focus:ring-offset-2 disabled:opacity-50 disabled:cursor-not-allowed"
                  disabled={isLoading}
                >
                  {isLoading ? (
                    <>
                      <svg className="animate-spin -ml-1 mr-3 h-5 w-5 text-white inline" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                        <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
                        <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                      </svg>
                      Creating...
                    </>
                  ) : (
                    'Create Configuration'
                  )}
                </button>
              </div>
            </div>
          </form>
        </div>
      </div>
    </div>
  );
};

export default NewConfigurationPage;