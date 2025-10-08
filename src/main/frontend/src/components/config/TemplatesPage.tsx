import React, { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { ArrowLeftIcon } from '@heroicons/react/24/outline';
import { useConfiguration } from '../../contexts/ConfigurationContext';
import DataStrategySelectionModal from './DataStrategySelectionModal';

interface ConfigFile {
  filename: string;
  dataStrategy: string;
  lastModified: number;
  size: number;
}

const TemplatesPage: React.FC = () => {
  const navigate = useNavigate();
  const { loadConfigurationAsTemplate, updateConfig } = useConfiguration();
  const [configFiles, setConfigFiles] = useState<ConfigFile[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [isLoadingFiles, setIsLoadingFiles] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [selectedConfig, setSelectedConfig] = useState<ConfigFile | null>(null);
  const [showStrategyModal, setShowStrategyModal] = useState(false);

  useEffect(() => {
    loadConfigFiles();
  }, []);

  const loadConfigFiles = async () => {
    try {
      setIsLoadingFiles(true);
      setError(null);
      
      // Get metadata for config files from backend - same as Load Configuration page
      const response = await fetch('/hms-mirror/api/v1/config/files/metadata');
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }
      
      const configFiles: ConfigFile[] = await response.json();
      
      setConfigFiles(configFiles);
      setIsLoadingFiles(false);
      
    } catch (error) {
      console.error('Failed to load configuration files:', error);
      setError(error instanceof Error ? error.message : 'Failed to load configuration files');
      setIsLoadingFiles(false);
    }
  };

  const handleUseAsTemplate = (file: ConfigFile) => {
    setSelectedConfig(file);
    setShowStrategyModal(true);
  };

  const handleStrategySelect = async (newStrategy: string) => {
    if (selectedConfig) {
      setIsLoading(true);
      setError(null);

      try {
        // Load the selected configuration as a template (creates new config)
        console.log(`Loading template configuration: ${selectedConfig.filename}`);
        await loadConfigurationAsTemplate(selectedConfig.filename);
        
        // Update the data strategy to the selected one
        updateConfig({ 
          dataStrategy: newStrategy
        });
        
        console.log(`Created new configuration from template "${selectedConfig.filename}" with strategy: ${newStrategy}`);
        
        // Navigate to the current configuration page for editing
        navigate('/config/current');
        
      } catch (error) {
        console.error('Failed to create configuration from template:', error);
        setError(error instanceof Error ? error.message : 'Failed to create configuration from template');
      } finally {
        setIsLoading(false);
      }
    }
  };

  const handleBack = () => {
    navigate('/config');
  };

  const getStrategyBadgeClass = (strategy: string) => {
    switch (strategy) {
      case 'HYBRID': return 'bg-purple-100 text-purple-800';
      case 'SCHEMA_ONLY': return 'bg-blue-100 text-blue-800';
      case 'EXPORT_IMPORT': return 'bg-green-100 text-green-800';
      case 'SQL': return 'bg-yellow-100 text-yellow-800';
      case 'LINKED': return 'bg-indigo-100 text-indigo-800';
      default: return 'bg-gray-100 text-gray-800';
    }
  };

  return (
    <div className="p-8">
      <div className="max-w-2xl mx-auto">
        <div className="mb-6">
          <button
            onClick={handleBack}
            className="flex items-center text-blue-600 hover:text-blue-700 mb-4"
          >
            <ArrowLeftIcon className="h-4 w-4 mr-2" />
            Back to Configuration Management
          </button>
          <h1 className="text-3xl font-bold text-gray-900">Create from Template</h1>
          <p className="text-gray-600 mt-2">
            Select an existing configuration as a template. You'll be able to choose a new data strategy and save it with a new name.
          </p>
        </div>

        <div className="bg-white rounded-lg shadow-lg p-6">
          {error && (
            <div className="bg-red-50 border border-red-200 rounded-md p-4 mb-6">
              <div className="text-sm text-red-700">{error}</div>
            </div>
          )}

          <div>
            <label className="block text-sm font-medium text-gray-700 mb-3">
              Select Configuration Template
              {isLoading && (
                <span className="text-sm text-blue-600 ml-2">Creating from template...</span>
              )}
            </label>
            
            {isLoadingFiles ? (
              <div className="text-center py-8">
                <svg className="animate-spin h-8 w-8 text-gray-400 mx-auto" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                  <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
                  <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                </svg>
                <p className="text-gray-500 mt-2">Loading configuration files...</p>
              </div>
            ) : configFiles.length === 0 ? (
              <div className="text-center py-8 bg-gray-50 rounded-lg">
                <p className="text-gray-500">No configuration files found</p>
                <p className="text-sm text-gray-400 mt-1">Create a configuration first to use as a template</p>
              </div>
            ) : (
              <div className="space-y-3">
                {configFiles.map((file) => (
                  <div
                    key={file.filename}
                    className={`
                      p-4 border-2 rounded-lg transition-colors cursor-pointer
                      ${selectedConfig?.filename === file.filename
                        ? 'border-blue-500 bg-blue-50'
                        : isLoading 
                        ? 'border-gray-200 bg-gray-50 cursor-not-allowed'
                        : 'border-gray-200 hover:border-blue-300 hover:bg-blue-50'
                      }
                    `}
                    onClick={() => !isLoading && handleUseAsTemplate(file)}
                  >
                    <div className="flex items-center justify-between">
                      <div className="flex-1">
                        <div className="flex items-center justify-between mb-1">
                          <h4 className={`text-sm font-medium ${isLoading && selectedConfig?.filename !== file.filename ? 'text-gray-400' : 'text-gray-900'}`}>
                            {file.filename.replace(/\.(yaml|yml)$/i, '')}
                          </h4>
                          <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${getStrategyBadgeClass(file.dataStrategy)}`}>
                            {file.dataStrategy}
                          </span>
                        </div>
                        <p className={`text-xs ${isLoading && selectedConfig?.filename !== file.filename ? 'text-gray-400' : 'text-gray-500'}`}>
                          Modified {new Date(file.lastModified).toLocaleDateString()}
                          {!isLoading && (
                            <span className="ml-2 text-blue-600">Click to use as template</span>
                          )}
                        </p>
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>

          <div className="border-t pt-6 mt-6">
            <button
              onClick={handleBack}
              className="px-6 py-2 border border-gray-300 rounded-md text-gray-700 hover:bg-gray-50 focus:ring-2 focus:ring-gray-500 focus:ring-offset-2 disabled:opacity-50 disabled:cursor-not-allowed"
              disabled={isLoading}
            >
              Cancel
            </button>
          </div>
        </div>

        <DataStrategySelectionModal
          isOpen={showStrategyModal}
          onClose={() => setShowStrategyModal(false)}
          onSelect={handleStrategySelect}
          title={`Select Data Strategy for New Configuration`}
          selectedStrategy={selectedConfig?.dataStrategy}
        />
      </div>
    </div>
  );
};

export default TemplatesPage;