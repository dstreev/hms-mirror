import React, { useState, useEffect } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { configApi } from '../../services/api/configApi';
import { useConfiguration } from '../../contexts/ConfigurationContext';
import { 
  CogIcon, 
  CircleStackIcon, 
  WifiIcon, 
  DocumentTextIcon,
  ArrowLeftIcon,
  CheckIcon
} from '@heroicons/react/24/outline';

interface ConfigSection {
  id: string;
  name: string;
  icon: React.ComponentType<any>;
  description: string;
  completed: boolean;
}

const ConfigurationEditor: React.FC = () => {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const { state, loadConfiguration, saveToFile, canSave, getSaveStatus } = useConfiguration();

  const configSections: ConfigSection[] = [
    {
      id: 'clusters',
      name: 'Cluster Configuration',
      icon: WifiIcon,
      description: 'Configure source and target cluster connections',
      completed: !!((state.config?.clusters?.LEFT && state.config.clusters.LEFT.hiveServer2?.uri) && 
                    (state.config?.clusters?.RIGHT && state.config.clusters.RIGHT.hiveServer2?.uri))
    },
    {
      id: 'databases',
      name: 'Database Selection & Filters',
      icon: CircleStackIcon,
      description: 'Select databases and configure filters, warehouse plans, and limits',
      completed: !!(state.config?.databases && state.config.databases.length > 0)
    },
    {
      id: 'migration',
      name: 'Migration Settings',
      icon: CogIcon,
      description: 'Configure migration strategy and options',
      completed: true  // Migration Settings has no required fields
    },
    {
      id: 'advanced',
      name: 'Advanced Options',
      icon: DocumentTextIcon,
      description: 'Advanced configuration and optimization settings',
      completed: !(state.config?.encryptedPasswords && !state.config?.passwordKey)
    }
  ];

  useEffect(() => {
    initializeConfiguration();
  }, [id, state.isConfigurationLoaded]);

  // If no configuration is loaded and we're not in a creation flow, don't render anything
  // (the useEffect above will handle the redirect)
  if (!state.isConfigurationLoaded && id !== 'new' && id !== 'loaded' && id !== 'from-template') {
    return null;
  }

  const initializeConfiguration = async () => {
    try {
      // If no configuration is loaded and not in a new/load flow, redirect to config management
      if (!state.isConfigurationLoaded && id !== 'new' && id !== 'loaded' && id !== 'from-template') {
        console.log('No configuration loaded, redirecting to configuration management');
        navigate('/config');
        return;
      }

      // Only load from backend if we're in an explicit creation/load flow
      if ((id === 'new' || id === 'loaded' || id === 'from-template') && !state.isConfigurationLoaded) {
        // Initialize new configuration or load current
        await loadConfiguration();
      } else if (id && id !== 'new' && id !== 'loaded' && id !== 'from-template' && !state.isConfigurationLoaded) {
        // Load existing configuration by filename
        await loadConfiguration(id);
      }
    } catch (error) {
      console.error('Failed to initialize configuration:', error);
    }
  };

  const handleSectionClick = (section: ConfigSection) => {
    navigate(`/config/edit/${id}/${section.id}`);
  };

  const handleBack = () => {
    navigate('/config');
  };

  const handleSave = async () => {
    const success = await saveToFile();
    if (success) {
      // Update URL if this was a new config that got saved
      if (state.filename && (id === 'new' || id === 'loaded' || id === 'from-template')) {
        navigate(`/config/edit/${state.filename}`, { replace: true });
      }
    }
  };

  const handleSaveAs = async () => {
    const filename = prompt('Enter a name for this configuration:');
    if (filename?.trim()) {
      const success = await saveToFile(filename.trim());
      if (success && state.filename) {
        navigate(`/config/edit/${state.filename}`, { replace: true });
      }
    }
  };

  if (state.isLoading) {
    return (
      <div className="p-8">
        <div className="text-center py-8">
          <svg className="animate-spin h-8 w-8 text-gray-400 mx-auto" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
            <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
            <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
          </svg>
          <p className="text-gray-500 mt-2">Loading configuration...</p>
        </div>
      </div>
    );
  }

  if (state.error) {
    return (
      <div className="p-8">
        <div className="bg-red-50 border border-red-200 rounded-md p-4">
          <div className="text-sm text-red-700">{state.error}</div>
        </div>
      </div>
    );
  }

  const saveStatus = getSaveStatus();
  const getStatusLabel = () => {
    switch (saveStatus) {
      case 'new': return '• Unsaved';
      case 'unsaved': return '• Unsaved Changes';
      case 'saved': return '✓ Saved';
      default: return '';
    }
  };

  const getStatusClass = () => {
    switch (saveStatus) {
      case 'new': return 'bg-blue-100 text-blue-800';
      case 'unsaved': return 'bg-yellow-100 text-yellow-800';
      case 'saved': return 'bg-green-100 text-green-800';
      default: return 'bg-gray-100 text-gray-800';
    }
  };

  return (
    <div className="p-8">
      <div className="max-w-4xl mx-auto">
        {/* Header */}
        <div className="mb-8">
          <button
            onClick={handleBack}
            className="flex items-center text-blue-600 hover:text-blue-700 mb-4"
          >
            <ArrowLeftIcon className="h-4 w-4 mr-2" />
            Back to Configuration Management
          </button>
          <div className="flex items-center justify-between mb-4">
            <div>
              <h1 className="text-3xl font-bold text-gray-900">{state.configName}</h1>
              <div className="flex items-center mt-2 space-x-4">
                <p className="text-gray-600">Configure your migration settings</p>
                <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${getStatusClass()}`}>
                  {getStatusLabel()}
                </span>
              </div>
            </div>
            <div className="text-right">
              <div className="text-sm text-gray-500 mb-1">Data Strategy</div>
              <div className="inline-flex items-center px-3 py-1 rounded-full text-sm font-medium bg-purple-100 text-purple-800 border border-purple-200">
                <span className="w-2 h-2 bg-purple-600 rounded-full mr-2"></span>
                {state.config?.dataStrategy || 'Not Set'}
              </div>
            </div>
          </div>
        </div>

        {/* Progress Overview */}
        <div className="bg-white rounded-lg shadow-lg p-6 mb-8">
          <h2 className="text-lg font-medium text-gray-900 mb-4">Configuration Progress</h2>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
            {configSections.map((section) => (
              <div key={section.id} className="text-center">
                <div className={`mx-auto w-12 h-12 rounded-full flex items-center justify-center mb-2 ${
                  section.completed ? 'bg-green-100' : 'bg-gray-100'
                }`}>
                  {section.completed ? (
                    <CheckIcon className="h-6 w-6 text-green-600" />
                  ) : (
                    <section.icon className="h-6 w-6 text-gray-400" />
                  )}
                </div>
                <div className="text-sm font-medium text-gray-900">{section.name}</div>
                <div className={`text-xs ${section.completed ? 'text-green-600' : 'text-gray-500'}`}>
                  {section.completed ? 'Complete' : 'Pending'}
                </div>
              </div>
            ))}
          </div>
        </div>

        {/* Configuration Sections */}
        <div className="space-y-6 mb-8">
          {configSections.map((section) => (
            <div
              key={section.id}
              onClick={() => handleSectionClick(section)}
              className="bg-white rounded-lg shadow-lg border border-gray-200 p-6 cursor-pointer hover:shadow-xl transition-shadow"
            >
              <div className="flex items-start">
                <div className={`rounded-lg p-3 mr-4 ${
                  section.completed ? 'bg-green-100' : 'bg-blue-100'
                }`}>
                  {section.completed ? (
                    <CheckIcon className="h-6 w-6 text-green-600" />
                  ) : (
                    <section.icon className={`h-6 w-6 ${
                      section.completed ? 'text-green-600' : 'text-blue-600'
                    }`} />
                  )}
                </div>
                <div className="flex-1">
                  <div className="flex items-center justify-between">
                    <h3 className="text-lg font-semibold text-gray-900">{section.name}</h3>
                    <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${
                      section.completed 
                        ? 'bg-green-100 text-green-800' 
                        : section.id === 'migration' ? 'hidden' : 'bg-yellow-100 text-yellow-800'
                    }`}>
                      {section.completed ? 'Complete' : (section.id === 'migration' ? '' : 'Incomplete')}
                    </span>
                  </div>
                  <p className="text-gray-600 mt-1">{section.description}</p>
                  <div className="mt-3">
                    <span className="text-blue-600 text-sm font-medium hover:text-blue-700">
                      Configure →
                    </span>
                  </div>
                </div>
              </div>
            </div>
          ))}
        </div>

        {/* Actions */}
        <div className="flex justify-between">
          <button
            onClick={handleBack}
            className="px-6 py-2 border border-gray-300 rounded-md text-gray-700 hover:bg-gray-50 focus:ring-2 focus:ring-gray-500 focus:ring-offset-2"
          >
            Back to Configuration List
          </button>
          <div className="flex space-x-3">
            <button
              onClick={handleSave}
              disabled={!canSave()}
              className="px-6 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 focus:ring-2 focus:ring-blue-500 focus:ring-offset-2 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              {state.isNew ? 'Save As...' : 'Save'}
            </button>
            {!state.isNew && (
              <button
                onClick={handleSaveAs}
                className="px-6 py-2 bg-green-600 text-white rounded-md hover:bg-green-700 focus:ring-2 focus:ring-green-500 focus:ring-offset-2"
              >
                Save As...
              </button>
            )}
          </div>
        </div>
      </div>
    </div>
  );
};

export default ConfigurationEditor;