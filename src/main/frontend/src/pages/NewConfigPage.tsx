import React, { useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { ArrowLeftIcon } from '@heroicons/react/24/outline';

const NewConfigPage: React.FC = () => {
  const navigate = useNavigate();

  // Navigate directly to Config Wizard when this page loads
  useEffect(() => {
    navigate('/wizards/config', {
      state: {
        dataStrategy: 'SQL', // Default to SQL strategy
        isEditing: false
      }
    });
  }, [navigate]);

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
            <h3 className="text-lg font-medium text-gray-900 mb-2">Redirecting to Configuration Wizard</h3>
            <p className="text-gray-500 mb-4">
              You will be redirected to the configuration wizard shortly...
            </p>
          </div>
        </div>
      </div>
    </div>
  );
};

export default NewConfigPage;