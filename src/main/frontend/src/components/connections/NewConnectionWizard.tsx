import React, { useState, useEffect } from 'react';
import { useNavigate, useParams, useLocation } from 'react-router-dom';
import { ArrowLeftIcon } from '@heroicons/react/24/outline';
import {
  ConnectionFormData,
  DEFAULT_CONNECTION_FORM,
  validateBasicInfoStep,
  validateHiveServer2Step,
  validateMetastoreDirectStep,
  validateConnectionPoolStep,
  validateConnectionForm
} from '../../types/Connection';
import BasicInfoStep from './wizard/BasicInfoStep';
import HiveServer2Step from './wizard/HiveServer2Step';
import MetastoreDirectStep from './wizard/MetastoreDirectStep';
import ConnectionPoolStep from './wizard/ConnectionPoolStep';
import TestAndSaveStep from './wizard/TestAndSaveStep';
import WizardProgress from './wizard/WizardProgress';
import ErrorDialog from '../common/ErrorDialog';

const NewConnectionWizard: React.FC = () => {
  const navigate = useNavigate();
  const { id } = useParams<{ id: string }>();
  const location = useLocation();
  const isEditMode = Boolean(id);
  const isCopyMode = location.state?.mode === 'copy';
  const [currentStep, setCurrentStep] = useState(0);
  const [formData, setFormData] = useState<ConnectionFormData>(DEFAULT_CONNECTION_FORM);
  const [errors, setErrors] = useState<Record<string, string>>({});
  const [loading, setLoading] = useState(isEditMode);
  const [saving, setSaving] = useState(false);
  const [saveSuccess, setSaveSuccess] = useState(false);
  const [showErrorDialog, setShowErrorDialog] = useState(false);

  const steps = [
    { 
      id: 'basic', 
      title: 'Basic Info',
      component: BasicInfoStep
    },
    { 
      id: 'hiveserver2', 
      title: 'HiveServer2',
      component: HiveServer2Step
    },
    { 
      id: 'metastore', 
      title: 'Metastore',
      component: MetastoreDirectStep
    },
    { 
      id: 'test', 
      title: 'Test & Save',
      component: TestAndSaveStep
    }
  ];

  // Load existing connection data in edit mode or copy mode
  useEffect(() => {
    if (isCopyMode && location.state?.connectionData) {
      // Copy mode: load data from navigation state and clear name (don't copy key)
      const connectionData = location.state.connectionData;
      const convertedFormData: ConnectionFormData = {
        // Don't copy key for new connection
        name: '', // Clear name for copy mode
        description: connectionData.description || '',
        environment: connectionData.environment || 'DEV',
        platformType: connectionData.config?.platformType || '',
        hcfsNamespace: connectionData.config?.hcfsNamespace || '',
        hs2Uri: connectionData.config?.hiveServer2?.uri || '',
        hs2Username: connectionData.config?.hiveServer2?.connectionProperties?.user || '',
        hs2Password: connectionData.config?.hiveServer2?.connectionProperties?.password || '',
        hs2ConnectionProperties: connectionData.config?.hiveServer2?.connectionProperties || {},
        metastoreDirectEnabled: Boolean(connectionData.config?.metastoreDirect?.uri),
        metastoreDirectUri: connectionData.config?.metastoreDirect?.uri || '',
        metastoreDirectType: connectionData.config?.metastoreDirect?.type || '',
        metastoreDirectUsername: connectionData.config?.metastoreDirect?.connectionProperties?.user || '',
        metastoreDirectPassword: connectionData.config?.metastoreDirect?.connectionProperties?.password || '',
        metastoreDirectMinConnections: connectionData.config?.metastoreDirect?.connectionPool?.min || 2,
        metastoreDirectMaxConnections: connectionData.config?.metastoreDirect?.connectionPool?.max || 10,
        partitionDiscoveryAuto: connectionData.config?.partitionDiscovery?.auto ?? true,
        partitionDiscoveryInitMSCK: connectionData.config?.partitionDiscovery?.initMSCK ?? true,
        partitionBucketLimit: connectionData.config?.partitionDiscovery?.partitionBucketLimit || 100,
        createIfNotExists: connectionData.config?.createIfNotExists || false,
        enableAutoTableStats: connectionData.config?.enableAutoTableStats || false,
        enableAutoColumnStats: connectionData.config?.enableAutoColumnStats || false
      };
      setFormData(convertedFormData);
      setLoading(false);
    } else if (isEditMode && id) {
      const loadConnection = async () => {
        try {
          setLoading(true);
          console.log('🔍 [Edit Mode] Fetching connection data for id:', id);
          const response = await fetch(`/hms-mirror/api/v1/connections/${id}`);

          if (!response.ok) {
            throw new Error('Failed to load connection');
          }

          const connectionData = await response.json();
          console.log('📦 [Edit Mode] Raw API response:', connectionData);
          console.log('📝 [Edit Mode] Extracted name:', connectionData.name);
          console.log('📝 [Edit Mode] Extracted description:', connectionData.description);

          // Convert the flattened connection data back to form format
          const convertedFormData: ConnectionFormData = {
            key: connectionData.key, // Preserve the key for updates
            name: connectionData.name || '',
            description: connectionData.description || '',
            environment: connectionData.environment || 'DEV',
            platformType: connectionData.config?.platformType || '',
            hcfsNamespace: connectionData.config?.hcfsNamespace || '',
            hs2Uri: connectionData.config?.hiveServer2?.uri || '',
            hs2Username: connectionData.config?.hiveServer2?.connectionProperties?.user || '',
            hs2Password: connectionData.config?.hiveServer2?.connectionProperties?.password || '',
            hs2ConnectionProperties: connectionData.config?.hiveServer2?.connectionProperties || {},
            metastoreDirectEnabled: Boolean(connectionData.config?.metastoreDirect?.uri),
            metastoreDirectUri: connectionData.config?.metastoreDirect?.uri || '',
            metastoreDirectType: connectionData.config?.metastoreDirect?.type || '',
            metastoreDirectUsername: connectionData.config?.metastoreDirect?.connectionProperties?.user || '',
            metastoreDirectPassword: connectionData.config?.metastoreDirect?.connectionProperties?.password || '',
            metastoreDirectMinConnections: connectionData.config?.metastoreDirect?.connectionPool?.min || 2,
            metastoreDirectMaxConnections: connectionData.config?.metastoreDirect?.connectionPool?.max || 10,
            partitionDiscoveryAuto: connectionData.config?.partitionDiscovery?.auto ?? true,
            partitionDiscoveryInitMSCK: connectionData.config?.partitionDiscovery?.initMSCK ?? true,
            partitionBucketLimit: connectionData.config?.partitionDiscovery?.partitionBucketLimit || 100,
            createIfNotExists: connectionData.config?.createIfNotExists || false,
            enableAutoTableStats: connectionData.config?.enableAutoTableStats || false,
            enableAutoColumnStats: connectionData.config?.enableAutoColumnStats || false
          };

          console.log('✅ [Edit Mode] Converted form data:', convertedFormData);
          console.log('✅ [Edit Mode] Form data name:', convertedFormData.name);
          console.log('✅ [Edit Mode] Form data description:', convertedFormData.description);
          setFormData(convertedFormData);
          console.log('✅ [Edit Mode] setFormData called successfully');
        } catch (error) {
          console.error('❌ [Edit Mode] Error loading connection:', error);
          setErrors({ general: 'Failed to load connection data' });
        } finally {
          setLoading(false);
        }
      };

      loadConnection();
    }
  }, [isEditMode, id]);

  const handleBack = () => {
    if (currentStep === 0) {
      navigate('/connections');
    } else {
      setCurrentStep(currentStep - 1);
    }
  };

  const handleNext = () => {
    // Validate current step based on step index
    let validation;
    switch (currentStep) {
      case 0: // Basic Info
        validation = validateBasicInfoStep(formData);
        break;
      case 1: // HiveServer2 Connection (only validate HiveServer2, not Metastore)
        validation = validateHiveServer2Step(formData);
        break;
      case 2: // Metastore Direct (Optional - only validate if enabled)
        validation = validateMetastoreDirectStep(formData);
        break;
      default:
        validation = { isValid: true, errors: {} };
    }

    if (!validation.isValid) {
      setErrors(validation.errors);
      setShowErrorDialog(true);
      return;
    }

    setErrors({});

    if (currentStep < steps.length - 1) {
      setCurrentStep(currentStep + 1);
    }
  };

  const handleFormChange = (updates: Partial<ConnectionFormData>) => {
    setFormData({ ...formData, ...updates });
    // Clear errors for fields that are being updated
    const newErrors = { ...errors };
    Object.keys(updates).forEach(key => {
      delete newErrors[key];
    });
    setErrors(newErrors);
  };

  const handleSave = async () => {
    console.log('Save button clicked');
    setSaving(true);
    setErrors({});
    
    // Final validation of all fields before saving
    const finalValidation = validateConnectionForm(formData);
    console.log('Validation result:', finalValidation);
    
    if (!finalValidation.isValid) {
      console.log('Validation failed:', finalValidation.errors);
      setErrors(finalValidation.errors);
      setShowErrorDialog(true);
      setSaving(false);
      return;
    }
    
    const requestPayload = {
      name: formData.name,
      description: formData.description,
      environment: formData.environment,
      config: {
        platformType: formData.platformType,
        hcfsNamespace: formData.hcfsNamespace,
        hiveServer2: {
          uri: formData.hs2Uri,
          connectionProperties: {
            user: formData.hs2Username,
            password: formData.hs2Password,
            ...formData.hs2ConnectionProperties
          }
        },
        metastoreDirect: formData.metastoreDirectEnabled ? {
          uri: formData.metastoreDirectUri,
          type: formData.metastoreDirectType,
          connectionProperties: {
            user: formData.metastoreDirectUsername,
            password: formData.metastoreDirectPassword
          },
          connectionPool: {
            min: formData.metastoreDirectMinConnections,
            max: formData.metastoreDirectMaxConnections
          }
        } : undefined,
        partitionDiscovery: {
          auto: formData.partitionDiscoveryAuto,
          initMSCK: formData.partitionDiscoveryInitMSCK,
          partitionBucketLimit: formData.partitionBucketLimit
        },
        createIfNotExists: formData.createIfNotExists,
        enableAutoTableStats: formData.enableAutoTableStats,
        enableAutoColumnStats: formData.enableAutoColumnStats
      }
    };
    
    console.log('Sending request payload:', requestPayload);
    
    try {
      const url = isEditMode 
        ? `/hms-mirror/api/v1/connections/${id}`
        : '/hms-mirror/api/v1/connections';
      const method = isEditMode ? 'PUT' : 'POST';
      
      const response = await fetch(url, {
        method,
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(requestPayload)
      });

      console.log('Response status:', response.status);
      console.log('Response ok:', response.ok);

      if (!response.ok) {
        const errorText = await response.text();
        console.error('Server error response:', errorText);
        
        // Try to parse as JSON first
        try {
          const errorData = JSON.parse(errorText);
          const errorMessage = errorData.message || errorData.error || `HTTP ${response.status}`;
          throw new Error(errorMessage);
        } catch (parseError) {
          // If JSON parsing fails, use the raw text
          throw new Error(`Failed to ${isEditMode ? 'update' : 'create'} connection: ${response.status} ${errorText}`);
        }
      }

      const result = await response.json();
      console.log('Success response:', result);

      // Update formData with the returned connection key so testing can work
      if (result.key) {
        console.log('Updating formData with connection key:', result.key);
        setFormData({ ...formData, key: result.key });
      }

      // Show success message - user can now test the connection
      console.log(`Connection ${isEditMode ? 'updated' : 'created'} successfully`);
      setSaveSuccess(true);

      // Don't auto-navigate - let user test the connection first
      // Navigation will happen when user clicks "Done" or "Back to Connections"
    } catch (error) {
      console.error('Error creating connection:', error);
      const errorMessage = error instanceof Error ? error.message : 'Unknown error occurred';
      setErrors({ general: errorMessage });
      setShowErrorDialog(true);
    } finally {
      setSaving(false);
    }
  };

  const CurrentStepComponent = steps[currentStep].component;

  // Show loading spinner while loading connection data in edit mode
  if (loading) {
    return (
      <div className="max-w-4xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        <div className="flex items-center justify-center h-64">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600"></div>
          <span className="ml-3 text-gray-600">Loading connection data...</span>
        </div>
      </div>
    );
  }

  return (
    <div className="max-w-4xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
      {/* Header */}
      <div className="mb-8">
        <button
          onClick={handleBack}
          className="flex items-center text-blue-600 hover:text-blue-700 mb-4"
        >
          <ArrowLeftIcon className="h-4 w-4 mr-2" />
          {currentStep === 0 ? 'Back to Connections' : 'Back'}
        </button>
        
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-3xl font-bold text-gray-900">
              {isEditMode ? 'Edit Connection' : 'New Connection'}
            </h1>
            <p className="text-gray-600 mt-2">
              {isEditMode ? 'Modify cluster connection profile' : 'Create a new cluster connection profile'}
            </p>
          </div>
        </div>
      </div>

      {/* Progress Indicator */}
      <div className="mb-8">
        <WizardProgress 
          steps={steps}
          currentStep={currentStep}
        />
      </div>

      {/* Step Content */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200">
        <div className="px-6 py-4 border-b border-gray-200">
          <h2 className="text-lg font-semibold text-gray-900">
            Step {currentStep + 1} of {steps.length}: {steps[currentStep].title}
          </h2>
        </div>
        
        <div className="p-6">
          <CurrentStepComponent
            formData={formData}
            errors={errors}
            onChange={handleFormChange}
            onNext={handleNext}
            onBack={handleBack}
            onSave={handleSave}
            isLastStep={currentStep === steps.length - 1}
            saving={saving}
            saveSuccess={saveSuccess}
            isEditMode={isEditMode && !isCopyMode}
          />
        </div>
      </div>

      {/* Navigation Buttons */}
      {currentStep < steps.length - 1 && (
        <div className="mt-6 flex justify-between">
          <button
            onClick={handleBack}
            className="px-4 py-2 border border-gray-300 text-gray-700 rounded-lg hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-blue-500"
          >
            {currentStep === 0 ? 'Cancel' : '← Back'}
          </button>
          
          <button
            onClick={handleNext}
            className="px-6 py-2 bg-blue-600 text-white font-medium rounded-lg hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-blue-500"
          >
            Continue →
          </button>
        </div>
      )}
      
      {/* Error Dialog */}
      <ErrorDialog
        isOpen={showErrorDialog}
        onClose={() => setShowErrorDialog(false)}
        title="Validation Errors"
        errors={errors}
      />
    </div>
  );
};

export default NewConnectionWizard;