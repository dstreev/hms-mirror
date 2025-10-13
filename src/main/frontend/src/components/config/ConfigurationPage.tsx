import React, { useState, useEffect } from 'react';
import { useNavigate, useLocation } from 'react-router-dom';
import { PlusIcon, DocumentTextIcon, CogIcon, DocumentArrowDownIcon, FolderOpenIcon } from '@heroicons/react/24/outline';
import { useConfiguration } from '../../contexts/ConfigurationContext';

const ConfigurationPage: React.FC = () => {
  const navigate = useNavigate();
  const location = useLocation();
  const [error, setError] = useState<string | null>(null);
  const [userWantsToManage, setUserWantsToManage] = useState(false);
  const { state, saveToFile, canSave, getSaveStatus } = useConfiguration();

  // No automatic redirect - users must explicitly navigate after loading/creating config

  const handleNewConfiguration = () => {
    // Show data strategy selection dialog before creating config
    navigate('/config/new');
  };

  const handleViewConfigurations = () => {
    navigate('/config/view');
  };


  const handleSaveConfiguration = async () => {
    const success = await saveToFile();
    if (success) {
      setError(null);
    }
  };

  const getStatusInfo = () => {
    const saveStatus = getSaveStatus();
    switch (saveStatus) {
      case 'new':
        return { label: 'New Configuration', class: 'bg-blue-100 text-blue-800', canSave: true };
      case 'unsaved':
        return { label: `${state.configName} (Unsaved Changes)`, class: 'bg-yellow-100 text-yellow-800', canSave: true };
      case 'saved':
        return { label: state.configName, class: 'bg-green-100 text-green-800', canSave: false };
      default:
        return { label: 'Configuration Management', class: '', canSave: false };
    }
  };

  const statusInfo = getStatusInfo();


  return (
    <div className="p-8">
      <div className="max-w-6xl mx-auto">
        <div className="mb-8">
          <div className="flex items-center justify-between">
            <div>
              <h1 className="text-3xl font-bold text-gray-900">Create Configuration</h1>
              <div className="flex items-center mt-2 space-x-4">
                <p className="text-gray-600">Start by creating a new migration configuration</p>
                {statusInfo.class && (
                  <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${statusInfo.class}`}>
                    {statusInfo.label}
                  </span>
                )}
              </div>
            </div>
          </div>
        </div>

        {error && (
          <div className="mb-6 bg-red-50 border border-red-200 rounded-md p-4">
            <div className="text-sm text-red-700">{error}</div>
          </div>
        )}

        {state.isConfigurationLoaded && state.config && (
          <div className="mb-6 bg-green-50 border border-green-200 rounded-md p-4">
            <div className="flex items-center justify-between">
              <div>
                <div className="text-sm font-medium text-green-800">Configuration Loaded Successfully</div>
                <div className="text-sm text-green-700 mt-1">
                  Configuration "{state.configName}" is now active. You can edit it or proceed with migration.
                </div>
              </div>
              <button
                onClick={() => navigate('/config/current')}
                className="px-4 py-2 bg-green-600 text-white rounded-md hover:bg-green-700 focus:ring-2 focus:ring-green-500 focus:ring-offset-2"
              >
                Go to Configuration
              </button>
            </div>
          </div>
        )}

        {/* Action Cards */}
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-8 max-w-4xl">
          <div 
            onClick={handleNewConfiguration}
            className="bg-white rounded-lg shadow-lg border border-gray-200 p-6 cursor-pointer hover:shadow-xl transition-shadow"
          >
            <div className="flex items-center mb-4">
              <div className="bg-blue-100 rounded-lg p-3">
                <PlusIcon className="h-6 w-6 text-blue-600" />
              </div>
              <h3 className="ml-3 text-lg font-semibold text-gray-900">New Configuration</h3>
            </div>
            <p className="text-gray-600 text-sm">Create a new migration configuration from scratch</p>
          </div>

          <div 
            onClick={handleViewConfigurations}
            className="bg-white rounded-lg shadow-lg border border-gray-200 p-6 cursor-pointer hover:shadow-xl transition-shadow"
          >
            <div className="flex items-center mb-4">
              <div className="bg-green-100 rounded-lg p-3">
                <FolderOpenIcon className="h-6 w-6 text-green-600" />
              </div>
              <h3 className="ml-3 text-lg font-semibold text-gray-900">View Configurations</h3>
            </div>
            <p className="text-gray-600 text-sm">Browse existing configurations organized by data strategy</p>
          </div>
        </div>

      </div>
    </div>
  );
};

export default ConfigurationPage;