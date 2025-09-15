import React from 'react';
import { Navigate } from 'react-router-dom';
import { useConfiguration } from '../../contexts/ConfigurationContext';
import { ExclamationTriangleIcon } from '@heroicons/react/24/outline';

interface RequireConfigurationProps {
  children: React.ReactNode;
}

const RequireConfiguration: React.FC<RequireConfigurationProps> = ({ children }) => {
  const { state } = useConfiguration();
  const hasConfig = state.isConfigurationLoaded && state.config;

  if (!hasConfig) {
    return (
      <div className="p-8">
        <div className="max-w-4xl mx-auto">
          <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-8 text-center">
            <ExclamationTriangleIcon className="h-12 w-12 text-yellow-600 mx-auto mb-4" />
            <h2 className="text-xl font-semibold text-gray-900 mb-2">Configuration Required</h2>
            <p className="text-gray-600 mb-6">
              This page requires an active configuration. Please create a new configuration or load an existing one to continue.
            </p>
            <a
              href="/config"
              className="inline-flex items-center px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 focus:ring-2 focus:ring-blue-500 focus:ring-offset-2"
            >
              Go to Configuration
            </a>
          </div>
        </div>
      </div>
    );
  }

  return <>{children}</>;
};

export default RequireConfiguration;