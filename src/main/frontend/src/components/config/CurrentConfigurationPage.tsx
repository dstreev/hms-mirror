import React, { useEffect, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { 
  CogIcon, 
  CircleStackIcon, 
  WifiIcon, 
  DocumentTextIcon,
  ExclamationTriangleIcon,
  CheckCircleIcon,
  DocumentArrowDownIcon,
  DocumentDuplicateIcon,
  KeyIcon
} from '@heroicons/react/24/outline';
import { useConfiguration } from '../../contexts/ConfigurationContext';
import SaveAsDialog from './SaveAsDialog';

interface ConfigSection {
  id: string;
  name: string;
  icon: React.ComponentType<any>;
  description: string;
  completed: boolean;
}

const CurrentConfigurationPage: React.FC = () => {
  const navigate = useNavigate();
  const { state, loadConfiguration, saveToFile, canSave, getSaveStatus } = useConfiguration();
  const [showSaveAsDialog, setShowSaveAsDialog] = useState(false);

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
      completed: state.config?.databases && state.config.databases.length > 0
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
    const initializeConfiguration = async () => {
      if (!state.isConfigurationLoaded) {
        // If no configuration is loaded, redirect to config management
        navigate('/config');
      } else if (!state.config) {
        // Configuration is marked as loaded but we don't have the actual data
        // This happens after CREATE_NEW_CONFIG - load the actual HmsMirrorConfig
        try {
          await loadConfiguration();
        } catch (error) {
          console.error('Failed to load configuration:', error);
        }
      }
    };

    initializeConfiguration();
  }, [state.isConfigurationLoaded, state.config, loadConfiguration, navigate]);

  const handleSectionClick = (section: ConfigSection) => {
    navigate(`/config/edit/current/${section.id}`);
  };

  const handleSave = async () => {
    const success = await saveToFile();
    // Success/error handling is done within the context
  };

  const handleSaveAs = async (filename: string) => {
    const success = await saveToFile(filename);
    if (success) {
      setShowSaveAsDialog(false);
    }
  };

  // Show loading state while configuration is being loaded
  if (state.isLoading) {
    return (
      <div className="p-8">
        <div className="text-center py-8">
          <svg className="animate-spin h-8 w-8 text-gray-400 mx-auto" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
            <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
            <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
          </svg>
          <p className="text-gray-500 mt-2">Loading current configuration...</p>
        </div>
      </div>
    );
  }

  // Show error if configuration failed to load
  if (state.error) {
    return (
      <div className="p-8">
        <div className="max-w-4xl mx-auto">
          <div className="text-center py-12">
            <ExclamationTriangleIcon className="h-16 w-16 text-red-400 mx-auto mb-4" />
            <h1 className="text-2xl font-bold text-gray-900 mb-2">Configuration Error</h1>
            <p className="text-red-600 mb-8">{state.error}</p>
            
            <a
              href="/config/manage"
              className="inline-block px-6 py-3 bg-blue-600 text-white rounded-md hover:bg-blue-700 focus:ring-2 focus:ring-blue-500 focus:ring-offset-2"
            >
              Go to Create/Load Configuration
            </a>
          </div>
        </div>
      </div>
    );
  }

  // If no configuration is loaded, don't render (useEffect handles redirect)
  if (!state.isConfigurationLoaded) {
    return null;
  }

  // Include password key in progress calculation if passwords are encrypted
  const passwordKeyRequired = state.config?.encryptedPasswords;
  const passwordKeyComplete = passwordKeyRequired && state.config?.passwordKey;
  
  const completedSections = configSections.filter(s => s.completed).length + (passwordKeyComplete ? 1 : 0);
  const totalSections = configSections.length + (passwordKeyRequired ? 1 : 0);
  const progressPercentage = totalSections > 0 ? (completedSections / totalSections) * 100 : 0;
  
  const saveStatus = getSaveStatus();
  const getStatusInfo = () => {
    switch (saveStatus) {
      case 'new':
        return { label: 'Unsaved', class: 'bg-blue-100 text-blue-800' };
      case 'unsaved':
        return { label: 'Unsaved Changes', class: 'bg-yellow-100 text-yellow-800' };
      case 'saved':
        return { label: 'Saved', class: 'bg-green-100 text-green-800' };
      default:
        return { label: '', class: '' };
    }
  };
  const statusInfo = getStatusInfo();

  return (
    <div className="p-8">
      <div className="max-w-4xl mx-auto">
        {/* Header */}
        <div className="mb-8">
          <div className="flex items-center justify-between mb-4">
            <div>
              <h1 className="text-3xl font-bold text-gray-900">{state.configName}</h1>
              <div className="flex items-center mt-2 space-x-4">
                <p className="text-gray-600">Current configuration in memory - configure settings and save when ready</p>
                {statusInfo.class && (
                  <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${statusInfo.class}`}>
                    • {statusInfo.label}
                  </span>
                )}
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
          
          {/* Progress Bar */}
          <div className="mt-4">
            <div className="flex items-center justify-between text-sm text-gray-600 mb-2">
              <span>Configuration Progress</span>
              <span>{completedSections} of {totalSections} sections completed</span>
            </div>
            <div className="w-full bg-gray-200 rounded-full h-2">
              <div 
                className="bg-blue-600 h-2 rounded-full transition-all duration-300"
                style={{ width: `${progressPercentage}%` }}
              ></div>
            </div>
          </div>
          
          {/* Save Buttons */}
          <div className="mt-4 flex justify-end space-x-3">
            <button
              onClick={() => setShowSaveAsDialog(true)}
              className="inline-flex items-center px-4 py-2 bg-gray-600 text-white rounded-md hover:bg-gray-700 focus:ring-2 focus:ring-gray-500 focus:ring-offset-2"
            >
              <DocumentDuplicateIcon className="h-4 w-4 mr-2" />
              Save As
            </button>
            {canSave() && (
              <button
                onClick={handleSave}
                className="inline-flex items-center px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 focus:ring-2 focus:ring-blue-500 focus:ring-offset-2"
              >
                <DocumentArrowDownIcon className="h-4 w-4 mr-2" />
                {state.isNew ? 'Save Configuration' : 'Save Changes'}
              </button>
            )}
          </div>
        </div>

        {/* Progress Overview */}
        <div className="bg-white rounded-lg shadow-lg p-6 mb-8">
          <h2 className="text-lg font-medium text-gray-900 mb-4">Section Status</h2>
          <div className={`grid grid-cols-1 md:grid-cols-2 ${state.config?.encryptedPasswords ? 'lg:grid-cols-5' : 'lg:grid-cols-4'} gap-4`}>
            {configSections.map((section) => (
              <div key={section.id} className="text-center">
                <div className={`mx-auto w-12 h-12 rounded-full flex items-center justify-center mb-2 ${
                  section.completed ? 'bg-green-100' : 'bg-gray-100'
                }`}>
                  {section.completed ? (
                    <CheckCircleIcon className="h-6 w-6 text-green-600" />
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
            {/* Password Key status - only show when passwords are encrypted */}
            {state.config?.encryptedPasswords && (
              <div className="text-center">
                <div className={`mx-auto w-12 h-12 rounded-full flex items-center justify-center mb-2 ${
                  state.config?.passwordKey ? 'bg-green-100' : 'bg-yellow-100'
                }`}>
                  {state.config?.passwordKey ? (
                    <CheckCircleIcon className="h-6 w-6 text-green-600" />
                  ) : (
                    <KeyIcon className="h-6 w-6 text-yellow-600" />
                  )}
                </div>
                <div className="text-sm font-medium text-gray-900">Password Key</div>
                <div className={`text-xs ${state.config?.passwordKey ? 'text-green-600' : 'text-yellow-600'}`}>
                  {state.config?.passwordKey ? 'Complete' : 'Pending'}
                </div>
              </div>
            )}
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
                    <CheckCircleIcon className="h-6 w-6 text-green-600" />
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

        {/* Save As Dialog */}
        <SaveAsDialog
          isOpen={showSaveAsDialog}
          onClose={() => setShowSaveAsDialog(false)}
          onSave={handleSaveAs}
          currentFilename={state.filename || undefined}
        />
      </div>
    </div>
  );
};

export default CurrentConfigurationPage;