import React, { useEffect } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { useConfiguration } from '../../contexts/ConfigurationContext';

interface ConfigurationSectionWrapperProps {
  children: React.ReactNode;
  sectionName: string;
}

const ConfigurationSectionWrapper: React.FC<ConfigurationSectionWrapperProps> = ({ children, sectionName }) => {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const { state, loadConfiguration } = useConfiguration();

  useEffect(() => {
    const initializeConfiguration = async () => {
      try {
        // Handle specific loading scenarios for non-current routes first
        if (id && id !== 'current' && id !== 'new' && id !== 'loaded' && id !== 'from-template' && !state.isConfigurationLoaded) {
          console.log(`Loading configuration by id: ${id}`);
          await loadConfiguration(id);
          return;
        }

        // If we're on a "current" route but no config is loaded, redirect to config management
        if (id === 'current' && !state.isConfigurationLoaded && !state.isLoading) {
          console.log('Current route but no configuration loaded, redirecting to configuration management');
          navigate('/config');
          return;
        }

        // Only try to load from backend as last resort, and only if there's truly no config loaded
        // This prevents overwriting already loaded configs when navigating between sections
        if (!state.isConfigurationLoaded && !state.isLoading && !id) {
          console.log('No configuration loaded and no specific ID, attempting to load current config from backend');
          await loadConfiguration();
          return;
        }
      } catch (error) {
        console.error('Failed to initialize configuration:', error);
        // Only redirect on error after attempting to load
        if (!state.isConfigurationLoaded) {
          console.log('Failed to load configuration, redirecting to configuration management');
          navigate('/config');
        }
      }
    };

    initializeConfiguration();
  }, [id, state.isConfigurationLoaded, state.isLoading, loadConfiguration, navigate]);

  // If no configuration is loaded and we're not loading, and we have no ID context, show loading state
  // This prevents blank screens while configuration is being loaded, but only when appropriate
  if (!state.isConfigurationLoaded && !state.isLoading && !id) {
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

  // If we have an ID context (like "current") and no config loaded, redirect to avoid blank screens
  if (!state.isConfigurationLoaded && !state.isLoading && id === 'current') {
    navigate('/config');
    return null;
  }

  // Show loading state while configuration is being loaded
  if (state.isLoading) {
    return (
      <div className="p-8">
        <div className="text-center py-8">
          <svg className="animate-spin h-8 w-8 text-gray-400 mx-auto" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
            <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
            <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
          </svg>
          <p className="text-gray-500 mt-2">Loading {sectionName} configuration...</p>
        </div>
      </div>
    );
  }

  // Show error state if configuration failed to load
  if (state.error) {
    return (
      <div className="p-8">
        <div className="bg-red-50 border border-red-200 rounded-md p-4">
          <div className="text-sm text-red-700">{state.error}</div>
          <button
            onClick={() => navigate('/config')}
            className="mt-4 px-4 py-2 bg-red-600 text-white rounded-md hover:bg-red-700"
          >
            Back to Configuration Management
          </button>
        </div>
      </div>
    );
  }

  // Render the actual configuration section
  return <>{children}</>;
};

export default ConfigurationSectionWrapper;