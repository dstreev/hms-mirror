import React, { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { ArrowLeftIcon } from '@heroicons/react/24/outline';
import DataStrategySelectionModal from '../components/config/DataStrategySelectionModal';
import { useConfiguration } from '../contexts/ConfigurationContext';

const NewConfigPage: React.FC = () => {
  const navigate = useNavigate();
  const { dispatch, loadConfiguration, state } = useConfiguration();
  const [showStrategyModal, setShowStrategyModal] = useState(true);

  const handleStrategySelect = async (strategy: string) => {
    try {
      // Create new config with selected data strategy
      dispatch({ 
        type: 'CREATE_NEW_CONFIG', 
        payload: { dataStrategy: strategy } 
      });
      
      // Load the actual configuration from backend with the pending data strategy
      await loadConfiguration();
      
      // Navigate to current configuration overview
      navigate('/config/current');
    } catch (error) {
      console.error('Failed to create new configuration:', error);
      // Still navigate, error handling will happen in CurrentConfigurationPage
      navigate('/config/current');
    }
  };

  const handleBack = () => {
    // Always go back to the config management page when back button is clicked
    navigate('/config');
  };

  return (
    <div className="p-8">
      <div className="max-w-4xl mx-auto">
        <div className="mb-8">
          <button
            onClick={handleBack}
            className="flex items-center text-blue-600 hover:text-blue-700 mb-4"
          >
            <ArrowLeftIcon className="h-4 w-4 mr-2" />
            Back to Configuration Management
          </button>
          <h1 className="text-3xl font-bold text-gray-900">New Configuration</h1>
          <p className="text-gray-600 mt-2">Create a new HMS migration configuration</p>
        </div>

        <div className="bg-white rounded-lg shadow-lg p-6">
          <div className="text-center py-8">
            <div className="bg-blue-100 rounded-full p-4 w-16 h-16 mx-auto mb-4 flex items-center justify-center">
              <svg className="w-8 h-8 text-blue-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 6v6m0 0v6m0-6h6m-6 0H6" />
              </svg>
            </div>
            <h3 className="text-lg font-medium text-gray-900 mb-2">Select Data Migration Strategy</h3>
            <p className="text-gray-500 mb-4">
              Choose the strategy that best fits your migration requirements.
              <br />
              <span className="text-sm font-medium text-orange-600">
                Note: This choice cannot be changed after configuration creation.
              </span>
            </p>
          </div>
        </div>

        <DataStrategySelectionModal
          isOpen={showStrategyModal}
          onClose={() => {
            // When modal is cancelled, always go back to config management page
            navigate('/config');
          }}
          onSelect={handleStrategySelect}
          title="Select Data Migration Strategy"
        />
      </div>
    </div>
  );
};

export default NewConfigPage;