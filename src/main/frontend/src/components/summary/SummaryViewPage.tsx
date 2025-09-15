import React, { useState, useEffect } from 'react';
import { DocumentArrowDownIcon, ClipboardDocumentIcon, CheckIcon } from '@heroicons/react/24/outline';
import { useConfiguration } from '../../contexts/ConfigurationContext';
import { configApi } from '../../services/api/configApi';

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
                Current HmsMirrorConfig configuration in YAML format
              </p>
            </div>
            <div className="flex items-center space-x-3">
              <button
                onClick={handleCopyToClipboard}
                className="inline-flex items-center px-4 py-2 bg-gray-600 text-white rounded-md hover:bg-gray-700 focus:ring-2 focus:ring-gray-500 focus:ring-offset-2"
              >
                {copied ? (
                  <>
                    <CheckIcon className="h-4 w-4 mr-2" />
                    Copied!
                  </>
                ) : (
                  <>
                    <ClipboardDocumentIcon className="h-4 w-4 mr-2" />
                    Copy to Clipboard
                  </>
                )}
              </button>
              <button
                onClick={handleDownload}
                className="inline-flex items-center px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 focus:ring-2 focus:ring-blue-500 focus:ring-offset-2"
              >
                <DocumentArrowDownIcon className="h-4 w-4 mr-2" />
                Download YAML
              </button>
              <button
                onClick={handleSaveConfiguration}
                disabled={!state.config}
                className="inline-flex items-center px-4 py-2 bg-green-600 text-white rounded-md hover:bg-green-700 focus:ring-2 focus:ring-green-500 focus:ring-offset-2 disabled:opacity-50 disabled:cursor-not-allowed"
              >
                <DocumentArrowDownIcon className="h-4 w-4 mr-2" />
                Save Configuration
              </button>
            </div>
          </div>
        </div>

        {error && (
          <div className="mb-6 bg-red-50 border border-red-200 rounded-md p-4">
            <div className="text-sm text-red-700">{error}</div>
          </div>
        )}

        {/* Configuration Status */}
        <div className="mb-4 bg-white rounded-lg shadow-lg p-4">
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

        {/* YAML Content */}
        <div className="bg-white rounded-lg shadow-lg">
          <div className="border-b border-gray-200 bg-gray-50 px-6 py-3 rounded-t-lg">
            <h3 className="text-lg font-medium text-gray-900">YAML Configuration</h3>
          </div>
          <div className="p-6">
            {isLoading ? (
              <div className="text-center py-8">
                <svg className="animate-spin h-8 w-8 text-gray-400 mx-auto" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                  <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
                  <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                </svg>
                <p className="text-gray-500 mt-2">Loading configuration...</p>
              </div>
            ) : (
              <div className="relative">
                <pre className="bg-gray-900 text-gray-100 p-4 rounded-md overflow-x-auto max-h-[600px] overflow-y-auto">
                  <code className="text-sm font-mono">{yamlContent}</code>
                </pre>
                <div className="absolute top-2 right-2">
                  <button
                    onClick={handleCopyToClipboard}
                    className="p-2 bg-gray-800 hover:bg-gray-700 rounded-md transition-colors"
                    title="Copy to clipboard"
                  >
                    {copied ? (
                      <CheckIcon className="h-4 w-4 text-green-400" />
                    ) : (
                      <ClipboardDocumentIcon className="h-4 w-4 text-gray-400" />
                    )}
                  </button>
                </div>
              </div>
            )}
          </div>
        </div>

        {/* Information Note */}
        <div className="mt-6 bg-blue-50 border border-blue-200 rounded-md p-4">
          <div className="flex">
            <div className="ml-3">
              <h3 className="text-sm font-medium text-blue-800">About This View</h3>
              <div className="mt-2 text-sm text-blue-700">
                <p>
                  This page displays the current HmsMirrorConfig configuration object in YAML format. 
                  This is the same format used when saving configuration files. You can:
                </p>
                <ul className="list-disc pl-5 mt-2 space-y-1">
                  <li>Copy the configuration to clipboard for sharing or documentation</li>
                  <li>Download the configuration as a YAML file</li>
                  <li>Save the configuration to persist changes</li>
                  <li>Review all settings in a single, comprehensive view</li>
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