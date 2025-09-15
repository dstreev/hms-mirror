import React, { useState, useEffect, useRef } from 'react';
import { useNavigate } from 'react-router-dom';
import { ArrowLeftIcon, DocumentArrowDownIcon, DocumentArrowUpIcon } from '@heroicons/react/24/outline';
import { configApi } from '../../services/api/configApi';
import { useConfiguration } from '../../contexts/ConfigurationContext';

interface ConfigFile {
  filename: string;
  dataStrategy: string;
  lastModified: number;
  size: number;
}

const LoadConfigurationPage: React.FC = () => {
  const navigate = useNavigate();
  const { loadConfiguration, updateConfig } = useConfiguration();
  const [configFiles, setConfigFiles] = useState<ConfigFile[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [isLoadingFiles, setIsLoadingFiles] = useState(true);
  const [loadingConfigName, setLoadingConfigName] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [isUploading, setIsUploading] = useState(false);
  const fileInputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    loadConfigFiles();
  }, []);

  const loadConfigFiles = async () => {
    try {
      setIsLoadingFiles(true);
      setError(null);
      
      // Get metadata for config files from backend
      const response = await fetch('/hms-mirror/api/v2/config/files/metadata');
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

  const handleLoadConfiguration = async (filename: string) => {
    setIsLoading(true);
    setLoadingConfigName(filename);
    setError(null);

    try {
      console.log('Loading configuration:', filename);
      
      // Use the ConfigurationContext's loadConfiguration to ensure state is updated
      await loadConfiguration(filename);
      console.log('Configuration loaded successfully via context');
      
      setIsLoading(false);
      setLoadingConfigName(null);
      
      // Navigate to the current configuration page to view the loaded config
      navigate('/config/current');

    } catch (error) {
      console.error('Failed to load configuration:', error);
      setError(error instanceof Error ? error.message : 'Failed to load configuration. Please try again.');
      setIsLoading(false);
      setLoadingConfigName(null);
    }
  };

  const handleBack = () => {
    navigate('/config');
  };

  const handleDownloadConfig = async (filename: string, event: React.MouseEvent) => {
    event.stopPropagation(); // Prevent loading the configuration when clicking download
    
    try {
      // Fetch the configuration file content
      const response = await fetch(`/hms-mirror/api/v2/config/file/${encodeURIComponent(filename)}`);
      if (!response.ok) {
        throw new Error(`Failed to download configuration: ${response.statusText}`);
      }
      
      const blob = await response.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = filename;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(url);
    } catch (error) {
      console.error('Failed to download configuration:', error);
      setError(error instanceof Error ? error.message : 'Failed to download configuration');
    }
  };

  const handleFileUpload = async (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (!file) return;

    // Validate file extension
    if (!file.name.endsWith('.yaml') && !file.name.endsWith('.yml')) {
      setError('Please select a YAML configuration file (.yaml or .yml)');
      return;
    }

    setIsUploading(true);
    setError(null);

    try {
      // Read the file content
      const fileContent = await file.text();
      
      // Parse the YAML content (the backend will do the actual parsing)
      // For now, we'll send it as a JSON string to the backend
      const formData = new FormData();
      formData.append('file', file);
      formData.append('filename', file.name);

      // Upload and load the configuration
      const response = await fetch('/hms-mirror/api/v2/config/upload', {
        method: 'POST',
        body: formData,
      });

      if (!response.ok) {
        throw new Error(`Failed to upload configuration: ${response.statusText}`);
      }

      const uploadedConfig = await response.json();
      
      // Update the configuration in the context
      updateConfig(uploadedConfig);
      
      // Navigate to the current configuration page
      navigate('/config/current');
      
    } catch (error) {
      console.error('Failed to upload configuration:', error);
      setError(error instanceof Error ? error.message : 'Failed to upload configuration');
    } finally {
      setIsUploading(false);
      // Reset the file input
      if (fileInputRef.current) {
        fileInputRef.current.value = '';
      }
    }
  };

  const handleUploadClick = () => {
    fileInputRef.current?.click();
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
          <h1 className="text-3xl font-bold text-gray-900">Load Configuration</h1>
          <p className="text-gray-600 mt-2">
            Click on any configuration below to load it. Once loaded, you can modify and save changes back to the file.
          </p>
        </div>

        <div className="bg-white rounded-lg shadow-lg p-6">
          {error && (
            <div className="bg-red-50 border border-red-200 rounded-md p-4 mb-6">
              <div className="text-sm text-red-700">{error}</div>
            </div>
          )}

          {/* Upload Section */}
          <div className="mb-6 pb-6 border-b border-gray-200">
            <h3 className="text-sm font-medium text-gray-700 mb-3">Upload Configuration</h3>
            <div className="flex items-center space-x-4">
              <button
                onClick={handleUploadClick}
                disabled={isUploading || isLoading}
                className="inline-flex items-center px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 focus:ring-2 focus:ring-blue-500 focus:ring-offset-2 disabled:opacity-50 disabled:cursor-not-allowed"
              >
                <DocumentArrowUpIcon className="h-4 w-4 mr-2" />
                {isUploading ? 'Uploading...' : 'Upload YAML File'}
              </button>
              <p className="text-sm text-gray-500">
                Upload a configuration file from your computer to load it
              </p>
            </div>
            <input
              ref={fileInputRef}
              type="file"
              accept=".yaml,.yml"
              onChange={handleFileUpload}
              className="hidden"
            />
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-700 mb-3">
              Available Configuration Files
              {isLoading && (
                <span className="text-sm text-blue-600 ml-2">Loading configuration...</span>
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
                <p className="text-sm text-gray-400 mt-1">Create a new configuration to get started</p>
              </div>
            ) : (
              <div className="space-y-3">
                {configFiles.map((file) => (
                  <div
                    key={file.filename}
                    className={`
                      flex items-center p-4 border-2 rounded-lg transition-colors relative
                      ${loadingConfigName === file.filename
                        ? 'border-blue-500 bg-blue-50'
                        : isLoading 
                        ? 'border-gray-200 bg-gray-50'
                        : 'border-gray-200 hover:border-blue-300 hover:bg-blue-50'
                      }
                    `}
                  >
                    <div 
                      className="flex-1 cursor-pointer"
                      onClick={() => !isLoading && handleLoadConfiguration(file.filename)}
                    >
                      <div className="flex items-center justify-between mb-1">
                        <h4 className={`text-sm font-medium ${isLoading && loadingConfigName !== file.filename ? 'text-gray-400' : 'text-gray-900'}`}>
                          {file.filename.replace(/\.(yaml|yml)$/i, '')}
                        </h4>
                        <div className="flex items-center space-x-2">
                          <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${getStrategyBadgeClass(file.dataStrategy)}`}>
                            {file.dataStrategy}
                          </span>
                          {loadingConfigName === file.filename && (
                            <svg className="animate-spin h-4 w-4 text-blue-600" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                              <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
                              <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                            </svg>
                          )}
                        </div>
                      </div>
                      <div className="flex items-center justify-between">
                        <p className={`text-xs ${isLoading && loadingConfigName !== file.filename ? 'text-gray-400' : 'text-gray-500'}`}>
                          Modified {new Date(file.lastModified).toLocaleDateString()}
                          {!isLoading && (
                            <span className="ml-2 text-blue-600">Click to load</span>
                          )}
                        </p>
                      </div>
                    </div>
                    <div className="ml-4">
                      <button
                        onClick={(e) => handleDownloadConfig(file.filename, e)}
                        disabled={isLoading}
                        className="inline-flex items-center px-3 py-1.5 bg-gray-600 text-white text-xs rounded-md hover:bg-gray-700 focus:ring-2 focus:ring-gray-500 focus:ring-offset-2 disabled:opacity-50 disabled:cursor-not-allowed"
                        title="Download configuration file"
                      >
                        <DocumentArrowDownIcon className="h-4 w-4" />
                        <span className="ml-1">Download</span>
                      </button>
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
              Back to Configuration Management
            </button>
          </div>
        </div>
      </div>
    </div>
  );
};

export default LoadConfigurationPage;