import React, { useState, useEffect } from 'react';
import { DocumentArrowDownIcon, ClipboardDocumentIcon, CheckIcon } from '@heroicons/react/24/outline';
import { useConfiguration } from '../../contexts/ConfigurationContext';
import { configApi } from '../../services/api/configApi';
import YamlViewer from '../reports/YamlViewer';

const SummaryViewPage: React.FC = () => {
  const { state, saveToFile } = useConfiguration();
  const [yamlContent, setYamlContent] = useState<string>('');
  const [isLoading, setIsLoading] = useState(false);
  const [copied, setCopied] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchYamlContent = async () => {
      if (!state.config) {
        setYamlContent('# No configuration loaded\n# Please create or load a configuration first');
        return;
      }

      setIsLoading(true);
      setError(null);
      
      try {
        // Convert the configuration to YAML format
        const yaml = await configApi.getConfigAsYaml(state.config);
        if (yaml) {
          setYamlContent(yaml);
        } else {
          // Fallback: Display as formatted JSON if YAML conversion fails
          setYamlContent('# Configuration (JSON format)\n' + JSON.stringify(state.config, null, 2));
        }
      } catch (err) {
        console.error('Failed to get YAML configuration:', err);
        setError('Failed to generate YAML view');
        // Fallback to JSON display
        setYamlContent('# Configuration (JSON format)\n' + JSON.stringify(state.config, null, 2));
      } finally {
        setIsLoading(false);
      }
    };

    fetchYamlContent();
  }, [state.config]);

  const handleCopyToClipboard = async () => {
    try {
      await navigator.clipboard.writeText(yamlContent);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } catch (err) {
      console.error('Failed to copy to clipboard:', err);
      // Fallback for browsers that don't support clipboard API
      const textarea = document.createElement('textarea');
      textarea.value = yamlContent;
      document.body.appendChild(textarea);
      textarea.select();
      document.execCommand('copy');
      document.body.removeChild(textarea);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    }
  };

  const handleDownload = () => {
    const blob = new Blob([yamlContent], { type: 'text/yaml' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `${state.configName || 'config'}.yaml`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  };

  const handleSaveConfiguration = async () => {
    const success = await saveToFile();
    if (success) {
      alert('Configuration saved successfully');
    }
  };

  return (
    <div className="p-8">
      <div className="max-w-7xl mx-auto">
        {/* Header */}
        <div className="mb-8">
          <div className="flex items-center justify-between">
            <div>
              <h1 className="text-3xl font-bold text-gray-900">Configuration Summary View</h1>
              <p className="text-gray-600 mt-2">
                View and manage your HmsMirrorConfig configuration with tree view and search capabilities
              </p>
            </div>
          </div>
        </div>

        {error && (
          <div className="mb-6 bg-red-50 border border-red-200 rounded-md p-4">
            <div className="text-sm text-red-700">{error}</div>
          </div>
        )}

        {/* Configuration Status and Actions */}
        <div className="mb-4 space-y-4">
          <div className="bg-white rounded-lg shadow-lg p-4">
            <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
              <div>
                <p className="text-sm text-gray-600">Configuration Name</p>
                <p className="font-medium text-gray-900">{state.configName || 'Not Set'}</p>
              </div>
              <div>
                <p className="text-sm text-gray-600">Data Strategy</p>
                <p className="font-medium text-gray-900">{state.config?.dataStrategy || 'Not Set'}</p>
              </div>
              <div>
                <p className="text-sm text-gray-600">Save Status</p>
                <p className={`font-medium ${state.isNew ? 'text-blue-600' : (state.isDirty ? 'text-yellow-600' : 'text-green-600')}`}>
                  {state.isNew ? 'New Configuration' : (state.isDirty ? 'Unsaved Changes' : 'Saved')}
                </p>
              </div>
              <div>
                <p className="text-sm text-gray-600">Password Encryption</p>
                <p className={`font-medium ${state.config?.encryptedPasswords ? 'text-green-600' : 'text-yellow-600'}`}>
                  {state.config?.encryptedPasswords ? 'Encrypted' : 'Not Encrypted'}
                </p>
              </div>
            </div>
          </div>
          
          {/* Action Buttons */}
          <div className="bg-white rounded-lg shadow-lg p-4">
            <div className="flex items-center justify-between">
              <div className="text-sm text-gray-600">
                Use the tree view below to explore your configuration. Toggle between tree and raw YAML views, and use search to find specific settings.
              </div>
              <div className="flex items-center space-x-3">
                <button
                  onClick={handleCopyToClipboard}
                  className="inline-flex items-center px-4 py-2 bg-gray-600 text-white rounded-md hover:bg-gray-700 focus:ring-2 focus:ring-gray-500 focus:ring-offset-2 transition-colors"
                >
                  {copied ? (
                    <>
                      <CheckIcon className="h-4 w-4 mr-2" />
                      Copied!
                    </>
                  ) : (
                    <>
                      <ClipboardDocumentIcon className="h-4 w-4 mr-2" />
                      Copy YAML
                    </>
                  )}
                </button>
                <button
                  onClick={handleDownload}
                  className="inline-flex items-center px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 focus:ring-2 focus:ring-blue-500 focus:ring-offset-2 transition-colors"
                >
                  <DocumentArrowDownIcon className="h-4 w-4 mr-2" />
                  Download
                </button>
                <button
                  onClick={handleSaveConfiguration}
                  disabled={!state.config}
                  className="inline-flex items-center px-4 py-2 bg-green-600 text-white rounded-md hover:bg-green-700 focus:ring-2 focus:ring-green-500 focus:ring-offset-2 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
                >
                  <DocumentArrowDownIcon className="h-4 w-4 mr-2" />
                  Save Config
                </button>
              </div>
            </div>
          </div>
        </div>

        {/* YAML Content */}
        {isLoading ? (
          <div className="bg-white rounded-lg shadow-lg p-8">
            <div className="text-center py-8">
              <svg className="animate-spin h-8 w-8 text-gray-400 mx-auto" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
                <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
              </svg>
              <p className="text-gray-500 mt-2">Loading configuration...</p>
            </div>
          </div>
        ) : (
          <YamlViewer 
            content={yamlContent}
            title="Current Configuration"
          />
        )}

        {/* Information Note */}
        <div className="mt-6 bg-blue-50 border border-blue-200 rounded-md p-4">
          <div className="flex">
            <div className="ml-3">
              <h3 className="text-sm font-medium text-blue-800">Enhanced Configuration Viewer</h3>
              <div className="mt-2 text-sm text-blue-700">
                <p>
                  This enhanced viewer provides the same powerful features used in the Session and Run Status reports:
                </p>
                <ul className="list-disc pl-5 mt-2 space-y-1">
                  <li><strong>Tree View:</strong> Navigate your configuration hierarchically with collapsible sections</li>
                  <li><strong>Search:</strong> Quickly find specific configuration settings using the search box</li>
                  <li><strong>Raw YAML:</strong> Toggle to view the raw YAML format</li>
                  <li><strong>Smart Icons:</strong> Visual indicators for different configuration types</li>
                  <li><strong>Color Coding:</strong> Values are color-coded by type (boolean, number, string)</li>
                  <li><strong>Sensitive Data:</strong> Passwords and secrets are automatically masked</li>
                  <li><strong>Export Options:</strong> Copy to clipboard or download as a YAML file</li>
                </ul>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default SummaryViewPage;